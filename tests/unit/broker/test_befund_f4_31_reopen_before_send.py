"""Nachweis F4-31: Scheitert das Neuöffnen des Publish-Kanals an der gerade
sterbenden Verbindung, endet der Publish sofort mit ``BrokerConnectionError``,
ohne zweiten Versuch.

Register: docs/befunde/fehlersuche-runde-4-2026-10.md (F4-31; Signal S-011;
Chaos-Gate 2026-10-30, Seed 3287596344, und Gegenprüfung 2026-10-31: 1 von 20
Läufen von ``tests/chaos/test_publish_deadline.py::TestPublishDeadlineAgainstRealBroker::test_publish_recovers_after_the_stall_clears``
unter Halblast rot).

``_publish_with_retry`` (``broker/rabbitmq/delivery.py``) ruft
``_ensure_publish_channel`` vor dem ``try`` der Wiederholschleife auf. Endet
der Stall, schließt Toxiproxy die alte Verbindung; erreicht der Publish
``connection.ready()`` noch vor dem EOF, wirft aiormqs ``connection.channel()``
``RuntimeError("... closed")``, und swarmq meldet „channel reopen failed“,
obwohl nichts gesendet war und aio-pika sofort neu verbindet.

Erwartet: ``website/explanation/delivery-semantics.md`` („A connection loss
before the message reached the socket is retried internally first“), E-042
(„Riss die Verbindung, bevor der Frame geschrieben war, wiederholt swarmq auf
einem neuen Kanal“), Docstring ``_publish_with_retry`` („recreating the channel
between attempts“). Die Tests mit Marker E-078 prüfen ``_ensure_publish_channel``
allein und widersprechen einer Wiederholung im Publish nicht.

Testliste:
# [ ] Kanalöffnen scheitert einmal an geschlossener Verbindung -> zweiter Versuch, Publish gelingt
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, Mock

import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from tests.support.aiormq_transport import FakePublishChannel, FakeTransport

pytestmark = [pytest.mark.timeout(5)]


@pytest.fixture(autouse=True)
def _no_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    # Die Wiederholungen warten 0.05 * 2**n s; im Unit-Test ohne Wartezeit.
    original = asyncio.sleep

    async def _instant(_delay: float, *args: object) -> None:
        await original(0)

    monkeypatch.setattr(asyncio, "sleep", _instant)


@pytest.mark.entscheidung("E-042")
@pytest.mark.befund("F4-31")
@pytest.mark.xfail(strict=True, reason="F4-31: Kanalöffnen liegt vor dem try der Wiederholschleife")
async def test_a_reopen_failing_on_the_dying_connection_is_retried_before_anything_was_sent() -> None:
    transport = FakeTransport()
    broker = RabbitMQBroker("amqp://guest:guest@localhost/")
    broker._connection = Mock(is_closed=False, transport=None)
    broker._connection.ready = AsyncMock()
    broker._publish_channel_factory = AsyncMock(
        side_effect=[
            RuntimeError('<Connection: "amqp://guest:******@127.0.0.1/"> closed'),
            FakePublishChannel(transport),
        ]
    )
    publish = asyncio.create_task(broker.publish("tasks", b"{}"))
    frame = asyncio.ensure_future(transport.next_frame())
    await asyncio.wait({frame, publish}, return_when=asyncio.FIRST_COMPLETED)
    if not frame.done():
        frame.cancel()
        pytest.fail(f"publish ended before a second attempt: {publish.exception()!r}")

    frame.result().drain()
    transport.confirm_all()

    receipt = await publish
    assert receipt.queue == "tasks"
    assert broker._publish_channel_factory.await_count == 2
