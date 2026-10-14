"""Nachweis FS-08: Ein Publish, der beim Verbindungsabriss auf aiormqs
Channel-Sperre wartet, haengt bis ``publish_timeout_s`` auf dem toten Channel
und endet dann mit einem falschen ``PublishIndeterminate``.

Register: docs/befunde/fehlersuche-runde-2-2026-10.md (FS-08, Quelle N-1).

``aiormq.Channel.basic_publish`` serialisiert ueber ``self.lock``; die
Property prueft ``is_closed`` nur beim Zugriff. Reisst die Verbindung,
lehnt ``FutureStore.reject_all`` das ``drain_future`` des Halters ab, der
Halter gibt die Sperre frei -- und der Wartende bekommt sie auf dem toten
Channel, reiht seine Frames in die ``write_queue`` der alten Verbindung
(kein Writer mehr) und wartet auf ein ``drain_future``, das nie erfuellt
und nie abgelehnt wird. swarmq begrenzt den Vorgang nur mit
``asyncio.timeout(publish_timeout_s)`` (120 s Default) und meldet danach
``PublishIndeterminate``, obwohl der Frame nie geschrieben wurde.

Unit statt Toxiproxy: Der Fehler braucht einen Publish, der genau im
Moment des Abrisses in der Sperren-Warteschlange steht. Mit Toxiproxy ist
das ein Timing-Glueck; hier laeuft der echte aiormq-Code
(``aiormq.Channel`` auf einem Verbindungs-Nachbau ohne Socket wie in
``repro_aiormq_lock_waiter.py``) deterministisch in diesen Zustand. Nur der
Channel unterscheidet sich in einem Punkt: Seine Sperre meldet, wenn ein
Wartender ankommt (Synchronisation ohne ``sleep``).

``publish_timeout_s`` steht im Test auf 0,3 s statt 120 s. Vor dem Fix
dauerte der Test deshalb rund 0,3 s: Er wartete auf den Haenger, den er
nachweist. Seit dem Fix (Publish gegen ``closing``, Plan 2026-10-14-0748, U8)
endet er sofort.

Offene Produktfrage 5 (FS-02/FS-08): akzeptiert werden Erfolg auf einem
neuen Channel (empfohlen: es wurde nachweislich nichts gesendet, also
sicher wiederholbar) oder ein eindeutiger "nicht gesendet"-Fehler
(``BrokerConnectionError``, aber nicht ``PublishIndeterminate``).

Testliste:
# [x] Publish wartet beim Abriss auf die Sperre -> Erfolg nach Reconnect oder
#     eindeutiger "nicht gesendet"-Fehler, kein PublishIndeterminate nach
#     publish_timeout_s
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from types import SimpleNamespace
from typing import Any, Literal, cast, override
from unittest.mock import AsyncMock, Mock

import aio_pika
import pytest
from aiormq.abc import AbstractConnection, ExceptionType
from aiormq.base import Base
from aiormq.channel import Channel
from aiormq.exceptions import ChannelInvalidStateError, ConnectionClosed

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.contracts import PublishReceipt
from swarmq.exceptions import BrokerConnectionError, PublishIndeterminate
from tests.support.aiormq_transport import FakePublishChannel, FakeTransport

pytestmark = pytest.mark.timeout(5)

_FS08_PUBLISH_TIMEOUT_S = 0.3


class Fs08Connector(Base):
    """Verbindungsseite ohne Writer: geschriebene Frames gehen nie raus."""

    def __init__(self) -> None:
        super().__init__(loop=asyncio.get_running_loop())
        self.publisher_confirms = True
        self.connection_tune = SimpleNamespace(frame_max=131072)
        self.write_queue: asyncio.Queue[Any] = asyncio.Queue()

    @override
    async def _on_close(self, exc: ExceptionType | None = None) -> None:
        return None

    @override
    def __str__(self) -> str:
        return "fs08-connector"


class Fs08Lock(asyncio.Lock):
    """Meldet, wenn ein Wartender ankommt (die Sperre ist schon belegt)."""

    def __init__(self) -> None:
        super().__init__()
        self.waiter_arrived = asyncio.Event()

    @override
    async def acquire(self) -> Literal[True]:
        if self.locked():
            self.waiter_arrived.set()
        return await super().acquire()


class Fs08Channel(Channel):
    """Echter aiormq-Channel; nur die Sperre ist beobachtbar."""

    def __init__(self, connector: Fs08Connector) -> None:
        super().__init__(cast("AbstractConnection", connector), 1, publisher_confirms=True)
        self.fs08_lock = Fs08Lock()

    @property
    @override
    def lock(self) -> asyncio.Lock:
        # Dieselbe Pruefung wie aiormq: nur beim Zugriff.
        if self.is_closed:
            raise ChannelInvalidStateError(f"{self!r} closed")
        return self.fs08_lock


class Fs08AioPikaChannel:
    """aio-pika-Seite ueber dem echten aiormq-Channel; ``is_closed`` folgt ihm."""

    def __init__(self, underlay: Fs08Channel) -> None:
        self.underlay = underlay
        self.default_exchange = aio_pika.Exchange(cast("aio_pika.abc.AbstractChannel", self), "")

    @property
    def is_closed(self) -> bool:
        return self.underlay.is_closed

    async def get_underlay_channel(self) -> Fs08Channel:
        return self.underlay

    async def close(self) -> None:
        return None


async def _fs08_healthy_wire(transport: FakeTransport) -> None:
    while True:
        frame = await transport.next_frame()
        frame.drain()
        transport.confirm_all()


@pytest.fixture
async def fs08_writers() -> AsyncIterator[list[asyncio.Task[None]]]:
    writers: list[asyncio.Task[None]] = []
    try:
        yield writers
    finally:
        for writer in writers:
            writer.cancel()
        await asyncio.gather(*writers, return_exceptions=True)


@pytest.fixture(autouse=True)
def _fs08_no_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    # Backoff zwischen Versuchen (0.05 * 2**n s) ohne Wartezeit, wie in test_publish_connection_loss.py.
    original = asyncio.sleep

    async def _instant(_delay: float, *args: object) -> None:
        await original(0)

    monkeypatch.setattr(asyncio, "sleep", _instant)


@pytest.mark.befund("FS-08")
async def test_fs08_publish_waiting_for_the_channel_lock_survives_a_connection_cut(
    fs08_writers: list[asyncio.Task[None]],
) -> None:
    connector = Fs08Connector()
    dead = Fs08Channel(connector)

    opened: list[object] = []

    def _next_channel() -> Any:
        """Erster Channel: der echte aiormq-Channel, danach gesunde Ersatz-Channels."""
        opened.append(object())
        if len(opened) == 1:
            return Fs08AioPikaChannel(dead)
        transport = FakeTransport()
        fs08_writers.append(asyncio.create_task(_fs08_healthy_wire(transport)))
        return FakePublishChannel(transport)

    broker = RabbitMQBroker("amqp://guest:guest@localhost/")
    broker._connection = Mock(is_closed=False, transport=None)
    broker._connection.ready = AsyncMock()
    broker._publish_channel_factory = AsyncMock(side_effect=_next_channel)
    broker.publish_timeout_s = _FS08_PUBLISH_TIMEOUT_S

    holder = asyncio.create_task(broker.publish("tasks", b'{"fs08": "holder"}'))
    await connector.write_queue.get()  # Halter: Frame eingereiht, nie geschrieben, haelt die Sperre
    waiter = asyncio.create_task(broker.publish("tasks", b'{"fs08": "waiter"}'))
    await dead.fs08_lock.waiter_arrived.wait()  # Wartender steht in der Sperren-Warteschlange

    loop = asyncio.get_running_loop()
    cut_at = loop.time()
    await connector.close(ConnectionClosed(320, "fs08 cut"))
    await asyncio.wait({holder, waiter})
    elapsed = loop.time() - cut_at

    assert isinstance(holder.result(), PublishReceipt), "Voraussetzung: der Halter gelingt auf einem neuen Channel"
    outcome: BaseException | PublishReceipt = waiter.exception() or waiter.result()
    assert isinstance(outcome, PublishReceipt) or (
        isinstance(outcome, BrokerConnectionError) and not isinstance(outcome, PublishIndeterminate)
    ), (
        f"Wartender endete nach {elapsed:.2f} s (publish_timeout_s={_FS08_PUBLISH_TIMEOUT_S}) mit "
        f"{type(outcome).__name__}: {outcome} -- er hing auf dem toten Channel, und nichts wurde gesendet"
    )
