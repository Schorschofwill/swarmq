"""Kanäle ohne Beobachter-Leck an der Verbindung (KTD6, ``broker/rabbitmq/underlay.py``).

aio-pika 10.2.1 lässt je geschlossenem Kanal zwei Beobachter von
``connection.closing`` an ``_closing`` der aiormq-Verbindung hängen: den aus
``UnderlayChannel.create`` (er hält den ganzen Kanal) und einen leeren aus
``UnderlayChannel.close``. swarmqs Kanäle halten ihren Beobachter selbst und
brechen ihn ab, sobald der aiormq-Kanal zu ist, gleich auf welchem Weg (Plan
docs/plans/2026-10-29-1945-chore-aiormq-7-2-anheben-plan.md, KTD6 Weg 1, U6).

Echtes aio-pika über einer aiormq-Verbindung aus ``aiormq.base.Base``: deren
``closing`` hat die echte Beobachter-Semantik von aiormq 7.2. Gezählt werden
die Callbacks am inneren Future ``_closing``.

Test-Liste:
# [x] Wegwerf-Kanal öffnen und schließen: kein Callback bleibt an ``_closing`` der Verbindung
# [x] Publish-Kanal auf swarmqs eigener Verbindung verworfen (E-078) und neu geöffnet: flach
# [x] vom Broker geschlossener Publish-Kanal, den aio-pika selbst wiederherstellt: flach
# [x] Öffnen abgebrochen, während aiormq den Kanal öffnet (F4-24): flach
# [x] Ende der Verbindung: der Kanal erfährt es weiterhin (``close_callbacks``)
# [x] vom Broker geschlossener Wegwerf-Kanal: ``close()`` danach ohne Fehler, flach; ``reopen()`` öffnet ihn wieder
# [x] Fehler in aio-pikas ``_on_open`` (``Basic.Qos``): Unterbau zu, Fehler beim Aufrufer, flach
# [x] ohne Transport: ``ChannelInvalidStateError`` wie bei aio-pika
# [x] aio-pikas ``Channel._open`` geändert: fail-open über aio-pikas eigenes Öffnen, eine WARNING je Verbindung
# [x] Wegwerf-Kanal auf einer einfachen ``aio_pika.Connection`` (injiziert): flach
"""

from __future__ import annotations

import asyncio
from typing import Any, override

import aio_pika
import aiormq.exceptions
import pytest
from aiormq.abc import ExceptionType
from aiormq.base import Base
from yarl import URL

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.broker.rabbitmq.robust_channels import open_throwaway_channel

pytestmark = pytest.mark.timeout(5)


def _callbacks(future: asyncio.Future[Any]) -> int:
    """Zahl der Done-Callbacks; privat, asyncio bietet keine öffentliche Zählung."""
    return len(future._callbacks or ())


class _AiormqChannel(Base):
    """Das Nötige eines ``aiormq.Channel`` für aio-pikas ``Channel``/``RobustChannel``."""

    def __init__(self, connection: _AiormqConnection, number: int) -> None:
        super().__init__(loop=asyncio.get_running_loop(), parent=connection)
        self.connection = connection
        self.number = number
        self.on_return_callbacks: set[object] = set()
        self.on_consumer_cancel_callbacks: set[object] = set()

    @override
    async def _on_close(self, exc: ExceptionType | None = None) -> None:
        self.connection.channels.pop(self.number, None)

    @property
    @override
    def is_closed(self) -> bool:
        return self._closing.done()

    @override
    def __str__(self) -> str:
        return f"channel {self.number}"

    async def basic_qos(self, **_: Any) -> None:
        if self.connection.qos_error is not None:
            raise self.connection.qos_error


class _AiormqConnection(Base):
    """Eine aiormq-Verbindung mit aiormqs ``closing``: je Zugriff ein neuer Beobachter."""

    def __init__(self) -> None:
        super().__init__(loop=asyncio.get_running_loop())
        self.channels: dict[int, _AiormqChannel] = {}
        self.qos_error: BaseException | None = None

    @override
    async def _on_close(self, exc: ExceptionType | None = None) -> None:
        return None

    @property
    @override
    def is_closed(self) -> bool:
        return self._closing.done()

    @override
    def __str__(self) -> str:
        return "connection"

    async def ready(self) -> None:
        return None

    async def channel(self, **_: Any) -> _AiormqChannel:
        number = max(self.channels, default=0) + 1
        channel = _AiormqChannel(self, number)
        self.channels[number] = channel
        return channel

    @property
    def inner_closing(self) -> asyncio.Future[Any]:
        return self._closing


class _HangingAiormqConnection(_AiormqConnection):
    """``channel()`` hängt wie ein ``Channel.Open`` ohne Antwort."""

    def __init__(self) -> None:
        super().__init__()
        self.opening = asyncio.Event()

    @override
    async def channel(self, **_: Any) -> _AiormqChannel:
        self.opening.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")


class _Transport:
    def __init__(self, connection: _AiormqConnection) -> None:
        self.connection = connection

    async def ready(self) -> None:
        return None


def _robust_connection(
    cls: type[aio_pika.RobustConnection] = aio_pika.RobustConnection,
    aiormq_connection: _AiormqConnection | None = None,
) -> tuple[aio_pika.RobustConnection, _AiormqConnection]:
    connection = cls(URL("amqp://guest:guest@localhost/"))
    aiormq_connection = aiormq_connection or _AiormqConnection()
    connection.transport = _Transport(aiormq_connection)  # type: ignore[assignment]  # Fake statt UnderlayConnection
    connection.connected.set()
    return connection, aiormq_connection


def _is_open(channel: aio_pika.abc.AbstractChannel) -> bool:
    """Liest ``is_closed`` neu (ohne die Verengung durch ein früheres ``assert``)."""
    return not channel.is_closed


async def _settle() -> None:
    """Ein paar Runden der Event-Loop: aiormq entfernt den Callback eines
    abgebrochenen Beobachters per ``call_soon``, aio-pika ruft ``_on_close`` als Task."""
    loop = asyncio.get_running_loop()
    for _ in range(10):
        turn: asyncio.Future[None] = loop.create_future()
        loop.call_soon(turn.set_result, None)
        await turn


async def test_a_throwaway_channel_leaves_no_callback_on_the_connection() -> None:
    connection, aiormq_connection = _robust_connection()
    before = _callbacks(aiormq_connection.inner_closing)

    channel = await open_throwaway_channel(connection)
    await channel.close()
    await _settle()

    assert _callbacks(aiormq_connection.inner_closing) == before


@pytest.fixture
def owned(monkeypatch: pytest.MonkeyPatch) -> list[_AiormqConnection]:
    """``aio_pika.connect_robust`` ohne Netz: die Verbindungsklasse, die swarmq
    übergibt, über dem Fake; liefert die aiormq-Fakes der Verbindungen."""
    created: list[_AiormqConnection] = []

    async def connect_robust(url: str, *, connection_class: type[aio_pika.RobustConnection]) -> Any:
        del url
        connection, aiormq_connection = _robust_connection(connection_class)
        created.append(aiormq_connection)
        return connection

    monkeypatch.setattr(aio_pika, "connect_robust", connect_robust)
    return created


async def test_a_discarded_publish_channel_on_an_owned_connection_leaves_no_callback(
    owned: list[_AiormqConnection],
) -> None:
    broker = RabbitMQBroker("amqp://guest:guest@localhost/")
    await broker.connect()
    (aiormq_connection,) = owned
    before = _callbacks(aiormq_connection.inner_closing)

    await broker._discard_publish_channel(broker._publish_channel)
    await asyncio.gather(*broker._pending_channel_closes)
    _ = await broker._ensure_publish_channel()
    await _settle()

    assert _callbacks(aiormq_connection.inner_closing) == before


async def test_a_publish_channel_closed_by_the_broker_and_restored_by_aio_pika_leaves_no_callback(
    owned: list[_AiormqConnection],
) -> None:
    broker = RabbitMQBroker("amqp://guest:guest@localhost/")
    await broker.connect()
    (aiormq_connection,) = owned
    channel = broker._publish_channel
    assert isinstance(channel, aio_pika.RobustChannel)
    before = _callbacks(aiormq_connection.inner_closing)
    closed = await channel.get_underlay_channel()

    # Der Broker schließt den Kanal (etwa 406); aio-pika stellt ihn ohne close() wieder her.
    await closed.close(aiormq.exceptions.ChannelPreconditionFailed("PRECONDITION_FAILED"))
    await _settle()

    restored = await channel.get_underlay_channel()
    assert restored is not closed, "precondition: aio-pika restored the channel"
    assert not channel.is_closed
    assert _callbacks(aiormq_connection.inner_closing) == before


async def test_an_open_cancelled_while_aiormq_opens_the_channel_leaves_no_callback() -> None:
    connection, aiormq_connection = _robust_connection(aiormq_connection=_HangingAiormqConnection())
    assert isinstance(aiormq_connection, _HangingAiormqConnection)
    before = _callbacks(aiormq_connection.inner_closing)

    opening = asyncio.ensure_future(open_throwaway_channel(connection))
    await aiormq_connection.opening.wait()
    opening.cancel()
    with pytest.raises(asyncio.CancelledError):
        _ = await opening
    await _settle()

    assert _callbacks(aiormq_connection.inner_closing) == before


async def test_the_end_of_the_connection_still_reaches_the_channel() -> None:
    connection, aiormq_connection = _robust_connection()
    channel = await open_throwaway_channel(connection)
    closed = asyncio.Event()

    def on_close(*_: object) -> None:
        closed.set()

    channel.close_callbacks.add(on_close)
    await aiormq_connection.close(aiormq.exceptions.ConnectionClosed(320, "CONNECTION_FORCED"))

    await asyncio.wait_for(closed.wait(), timeout=1)
    assert channel.is_closed


async def test_a_throwaway_channel_closed_by_the_broker_closes_quietly_and_can_reopen() -> None:
    connection, aiormq_connection = _robust_connection()
    before = _callbacks(aiormq_connection.inner_closing)
    channel = await open_throwaway_channel(connection)
    underlay = await channel.get_underlay_channel()
    await underlay.close(aiormq.exceptions.ChannelNotFoundEntity("NOT_FOUND"))
    await _settle()
    assert channel.is_closed

    await channel.close()
    await _settle()
    assert _callbacks(aiormq_connection.inner_closing) == before

    await channel.reopen()
    assert _is_open(channel)
    await channel.close()
    await _settle()
    assert _callbacks(aiormq_connection.inner_closing) == before


async def test_a_failing_qos_while_opening_closes_the_underlay_and_reaches_the_caller(
    owned: list[_AiormqConnection],
) -> None:
    broker = RabbitMQBroker("amqp://guest:guest@localhost/")
    await broker.connect()
    (aiormq_connection,) = owned
    connection = broker._connection
    assert connection is not None
    before = _callbacks(aiormq_connection.inner_closing)
    aiormq_connection.qos_error = aiormq.exceptions.ChannelAccessRefused("ACCESS_REFUSED")

    with pytest.raises(aiormq.exceptions.ChannelAccessRefused):
        _ = await connection.channel()
    await _settle()

    assert list(aiormq_connection.channels) == [1], "only the publish channel is open in aiormq"
    assert _callbacks(aiormq_connection.inner_closing) == before


async def test_without_a_transport_the_open_fails_like_in_aio_pika() -> None:
    connection, _ = _robust_connection()
    channel = open_throwaway_channel(connection)
    connection.transport = None

    with pytest.raises(aiormq.exceptions.ChannelInvalidStateError):
        _ = await channel


async def test_a_changed_open_in_aio_pika_fails_open_with_one_warning_per_connection(
    monkeypatch: pytest.MonkeyPatch, swarmq_logs: list[dict[str, Any]]
) -> None:
    original = aio_pika.Channel._open
    opened_by_aio_pika: list[object] = []

    async def changed_open(self: aio_pika.Channel) -> None:
        opened_by_aio_pika.append(self)
        await original(self)

    monkeypatch.setattr(aio_pika.Channel, "_open", changed_open)
    connection, _ = _robust_connection()
    other, _ = _robust_connection()

    first = await open_throwaway_channel(connection)
    second = await open_throwaway_channel(connection)
    third = await open_throwaway_channel(other)

    assert opened_by_aio_pika == [first, second, third]
    assert not any(channel.is_closed for channel in (first, second, third))
    warnings = [e["event"] for e in swarmq_logs if e["log_level"] == "warning"]
    assert warnings == ["channel_observer_release_unavailable"] * 2


async def test_a_throwaway_channel_on_a_plain_connection_leaves_no_callback() -> None:
    connection = aio_pika.Connection(URL("amqp://guest:guest@localhost/"))
    aiormq_connection = _AiormqConnection()
    connection.transport = _Transport(aiormq_connection)  # type: ignore[assignment]  # Fake statt UnderlayConnection
    connection.connected.set()
    before = _callbacks(aiormq_connection.inner_closing)

    channel = await open_throwaway_channel(connection)
    await channel.close()
    await _settle()

    assert _callbacks(aiormq_connection.inner_closing) == before
