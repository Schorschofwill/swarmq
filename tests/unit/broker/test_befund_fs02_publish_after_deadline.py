"""Nachweis FS-02 (a): Ein Publish nach verstrichener Publish-Endfrist geht
trotzdem auf die Leitung, gemeldet wird aber ``PublishIndeterminate``.

Register: docs/befunde/fehlersuche-runde-2-2026-10.md (FS-02, Quelle N-3).

``_effective_publish_timeout`` liefert nach der Endfrist ``0.0``; der Publish
laeuft dann unter ``asyncio.timeout(0)``. Der Abbruch kommt erst beim ersten
Suspendieren, auf aiormqs Fast-Path ist das ``await drain_future`` -- der
Frame liegt da schon in der ``write_queue`` und der Writer schreibt ihn im
selben Loop-Durchlauf. Der Aufrufer bekommt trotzdem einen Fehler und
behandelt die Kopie als nicht gesendet: ``retry_later``/``dead_letter``
scheitern, der Worker stellt das Original per ``redeliver`` zurueck, die
Kopie liegt aber in der DQ/XQ -> Doppelausfuehrung.

Offene Produktfrage 5 (FS-02/FS-08): empfohlen ist, nach der Endfrist gar
nicht erst zu publizieren und einen eindeutigen "nicht gesendet"-Fehler zu
werfen. Der Test prueft nur die Invariante, die jede Option erfuellen muss:
Meldet die Operation einen Fehler, ist nichts auf der Leitung; ist etwas auf
der Leitung, meldet sie Erfolg.

Writer und Broker spielt der Test (``FakeTransport`` mit echtem
``aiormq.Channel.basic_publish``); bestaetigt wird sofort nach dem Schreiben.

Testliste:
# [x] publish nach Fristende: nie "gescheitert" und zugleich gesendet
# [x] retry_later nach Fristende: ebenso (Kopie in die DQ)
# [x] dead_letter nach Fristende: ebenso (Kopie in die XQ)
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest
from aiormq.abc import ChannelFrame

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.contracts import Delivery
from tests.support.aiormq_transport import FakePublishChannel, FakeTransport

pytestmark = pytest.mark.timeout(5)


class Fs02Wire:
    """Writer und Broker: schreibt jeden Frame sofort und bestaetigt ihn."""

    def __init__(self) -> None:
        self.transport = FakeTransport()
        self.on_wire: list[ChannelFrame] = []

    async def run(self) -> None:
        while True:
            frame = await self.transport.next_frame()
            self.on_wire.append(frame)
            frame.drain()
            self.transport.confirm_all()


@pytest.fixture
async def fs02_wire() -> AsyncIterator[Fs02Wire]:
    wire = Fs02Wire()
    writer = asyncio.create_task(wire.run())
    try:
        yield wire
    finally:
        writer.cancel()
        await asyncio.gather(writer, return_exceptions=True)


def _fs02_broker(wire: Fs02Wire) -> RabbitMQBroker:
    broker = RabbitMQBroker("amqp://guest:guest@localhost/")
    broker._connection = Mock(is_closed=False, transport=None)
    broker._connection.ready = AsyncMock()
    broker._publish_channel_factory = AsyncMock(return_value=FakePublishChannel(wire.transport))
    broker.publish_timeout_s = 60.0
    return broker


def _fs02_delivery() -> Delivery:
    return Delivery(body=b'{"fs02": 1}', headers={}, queue="tasks", handle=AsyncMock(consumer_tag=None))


Operation = Callable[[RabbitMQBroker], Awaitable[Any]]

_OPERATIONS: dict[str, Operation] = {
    "publish": lambda broker: broker.publish("tasks", b'{"fs02": 1}'),
    "retry_later": lambda broker: broker.retry_later(_fs02_delivery(), delay_ms=1000, headers={}),
    "dead_letter": lambda broker: broker.dead_letter(_fs02_delivery(), headers={}),
}


@pytest.mark.befund("FS-02")
@pytest.mark.parametrize("operation", list(_OPERATIONS))
async def test_fs02_publish_after_elapsed_deadline_is_never_failed_and_sent(
    fs02_wire: Fs02Wire, operation: str
) -> None:
    broker = _fs02_broker(fs02_wire)
    broker.set_publish_deadline(asyncio.get_running_loop().time() - 1.0)

    try:
        await _OPERATIONS[operation](broker)
    except Exception as exc:
        failure: Exception | None = exc
    else:
        failure = None
    # Der Writer schreibt im selben Loop-Durchlauf; eine Runde abwarten,
    # damit ein schon eingereihter Frame sicher gezaehlt ist.
    await asyncio.sleep(0)

    if failure is not None:
        assert fs02_wire.on_wire == [], (
            f"{operation} meldete {type(failure).__name__} ({failure}), "
            f"aber {len(fs02_wire.on_wire)} Frame(s) gingen auf die Leitung: Aufrufer stellt das Original "
            "zurueck, obwohl die Kopie beim Broker liegt (Doppelausfuehrung)"
        )
    else:
        assert fs02_wire.on_wire, f"{operation} meldete Erfolg, ohne etwas zu senden"
