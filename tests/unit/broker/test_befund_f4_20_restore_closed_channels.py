"""Nachweis F4-20 (behoben in 0.14.0): aio-pika stellt geschlossene oder verworfene ``RobustChannel`` nach einem Reconnect wieder her.

Register: docs/befunde/fehlersuche-runde-4-2026-10.md (F4-20; Nebenbeobachtung 1
im Spike ``docs/solutions/spikes/2026-10-spike-kanal-close-nach-unblocked.md``,
Gegenprüfung 0.14, Probe A und C).

aio-pika 10.0.1: ``RobustConnection._on_connected`` ruft ``restore()`` für jeden
Kanal ihres ``WeakSet``. ``RobustChannel.close()`` leert nur ``__restored``,
``restore()`` öffnet den Kanal dann trotzdem neu, und aiormq hält ihn danach
stark. Jeder Reconnect bringt so alle geschlossenen, noch nicht eingesammelten
Kanäle zurück; ein wiederhergestellter Kanal mit passiv deklarierter, inzwischen
gelöschter Queue lässt den Reconnect endlos an NOT_FOUND scheitern. Verletzt:
``close_channel_quietly`` (``close()`` verhindere das Wiederöffnen, Review MR 10,
P0-1) und „kein Leck“ in E-025 (Nachtrag 2026-10-19) und E-078.

Echtes aio-pika (``RobustConnection``, ``RobustChannel``, ``Channel``) über einem
Fake der aiormq-Verbindung; kein Broker. Der Reconnect läuft wie in aio-pika:
Transport weg, alte aiormq-Verbindung zu, neuer Transport, ``_on_connected``.
Starke Referenzen auf alle Kanalobjekte halten die Garbage Collection heraus.

Test-Liste:
# [x] verworfener Publish-Kanal: nach dem Reconnect öffnet aio-pika nur den lebenden Kanal neu
# [x] Tiefe (``queue_depth``): kein Wegwerf-Kanal kommt zurück
# [x] Deklaration (``declare_queue``, Kette aus ``arg_conflict``): kein Wegwerf-Kanal kommt zurück
# [x] DLQ (``purge_dead_letters``): kein Wegwerf-Kanal kommt zurück
"""

from __future__ import annotations

import asyncio
import gc
from collections.abc import Awaitable, Callable, Iterator
from types import SimpleNamespace
from typing import Any

import aio_pika
import aiormq.exceptions
import pytest
from yarl import URL

from swarmq.broker.rabbitmq import RabbitMQBroker

pytestmark = pytest.mark.timeout(5)

_UNBLOCKED_ATTR = "_Connection__connection_unblocked"


class _FakeAiormqChannel:
    """Das Nötige eines ``aiormq.Channel`` für aio-pikas ``Channel``/``RobustChannel``."""

    def __init__(self, connection: _FakeAiormq, number: int) -> None:
        self.connection = connection
        self.number = number
        self.closing: asyncio.Future[object] = asyncio.get_running_loop().create_future()
        self.on_return_callbacks: set[object] = set()
        # aio-pika 10.2 meldet hier seine Consumer-Wiederherstellung an
        # (``RobustChannel._on_open``).
        self.on_consumer_cancel_callbacks: set[object] = set()

    @property
    def is_closed(self) -> bool:
        return self.closing.done()

    async def basic_qos(self, **_: Any) -> None:
        return None

    async def queue_declare(self, **kwargs: Any) -> SimpleNamespace:
        return SimpleNamespace(queue=kwargs["queue"], message_count=0, consumer_count=0)

    async def queue_purge(self, *_: Any, **__: Any) -> SimpleNamespace:
        return SimpleNamespace(message_count=0)

    async def close(self, exc: object = None) -> None:
        del exc
        self.connection.channels.pop(self.number, None)
        if not self.closing.done():
            self.closing.set_result(None)


class _FakeAiormq:
    """Eine aiormq-Verbindung: zählt die geöffneten Kanäle."""

    def __init__(self) -> None:
        self.closing: asyncio.Future[object] = asyncio.get_running_loop().create_future()
        self.channels: dict[int, _FakeAiormqChannel] = {}
        self.opened: list[int] = []
        unblocked = asyncio.Event()
        unblocked.set()
        setattr(self, _UNBLOCKED_ATTR, unblocked)

    @property
    def is_closed(self) -> bool:
        return self.closing.done()

    async def ready(self) -> None:
        return None

    async def channel(self, **_: Any) -> _FakeAiormqChannel:
        if self.is_closed:
            raise aiormq.exceptions.ConnectionClosed(320, "connection closed")
        number = max(self.channels, default=0) + 1
        channel = _FakeAiormqChannel(self, number)
        self.channels[number] = channel
        self.opened.append(number)
        return channel

    async def drop(self) -> None:
        """Abriss: alle Kanäle und die Verbindung enden."""
        for channel in list(self.channels.values()):
            await channel.close()
        self.closing.set_result(None)


class _FakeTransport:
    def __init__(self, connection: _FakeAiormq) -> None:
        self.connection = connection

    async def ready(self) -> None:
        return None


@pytest.fixture
def no_collection() -> Iterator[None]:
    """Keine Garbage Collection während des Tests: Der Befund hängt sonst davon
    ab, ob der Zyklus eines geschlossenen ``RobustChannel`` vor dem Reconnect
    eingesammelt ist (Spike, Nebenbeobachtung 1)."""
    gc.disable()
    try:
        yield
    finally:
        gc.enable()


async def _connected_broker() -> tuple[RabbitMQBroker, aio_pika.RobustConnection, _FakeAiormq]:
    connection = aio_pika.RobustConnection(URL("amqp://guest:guest@localhost/"))
    aiormq_connection = _FakeAiormq()
    connection.transport = _FakeTransport(aiormq_connection)  # type: ignore[assignment]  # Fake statt UnderlayConnection
    connection.connected.set()
    broker = RabbitMQBroker(connection=connection)
    await broker.connect()
    return broker, connection, aiormq_connection


async def _reconnect(connection: aio_pika.RobustConnection, dropped: _FakeAiormq) -> _FakeAiormq:
    """Der Reconnect wie in aio-pika: Transport weg, alte Verbindung zu, neuer
    Transport, ``_on_connected`` stellt die Kanäle des ``WeakSet`` wieder her."""
    connection.transport = None
    connection.connected.clear()
    await dropped.drop()
    await _settle()
    fresh = _FakeAiormq()
    connection.transport = _FakeTransport(fresh)  # type: ignore[assignment]  # Fake statt UnderlayConnection
    await connection._on_connected()
    await _settle()
    return fresh


async def _settle() -> None:
    """Wartet die Rückrufe ab, die aio-pika per ``call_soon`` und als Tasks
    startet (``_on_close``, ``restore``), bis keiner mehr nachkommt."""
    loop = asyncio.get_running_loop()
    for _ in range(10):
        turn: asyncio.Future[None] = loop.create_future()
        loop.call_soon(turn.set_result, None)
        await turn
        others = asyncio.all_tasks() - {asyncio.current_task()}
        if not others:
            return
        await asyncio.wait(others, timeout=1.0)


async def _discard_publish_channel(broker: RabbitMQBroker) -> None:
    await broker._discard_publish_channel(broker._publish_channel)
    await asyncio.gather(*broker._pending_channel_closes)


async def _queue_depth(broker: RabbitMQBroker) -> None:
    await broker.queue_depth("tasks")


async def _declare_queue(broker: RabbitMQBroker) -> None:
    await broker.declare_queue("tasks")


async def _purge_dead_letters(broker: RabbitMQBroker) -> None:
    await broker.purge_dead_letters("tasks")


@pytest.mark.befund("F4-20")
@pytest.mark.entscheidung("E-078")
@pytest.mark.usefixtures("no_collection")
@pytest.mark.parametrize(
    "operation",
    [_discard_publish_channel, _queue_depth, _declare_queue, _purge_dead_letters],
    ids=["discarded-publish-channel", "queue-depth", "declare-queue", "purge-dead-letters"],
)
async def test_a_reconnect_restores_only_the_live_channels(
    operation: Callable[[RabbitMQBroker], Awaitable[None]],
) -> None:
    broker, connection, first = await _connected_broker()
    try:
        await operation(broker)
        _ = await broker._ensure_publish_channel()
        live = sorted(first.channels)

        fresh = await _reconnect(connection, first)

        assert sorted(fresh.channels) == [1], (
            f"after the reconnect only the live publish channel (before: {live}) is open again, got {sorted(fresh.channels)}"
        )
    finally:
        await broker.close()
