"""Nachweis F4-31 (behoben): Scheiterte das Neuöffnen des Publish-Kanals an der
gerade sterbenden Verbindung, endete der Publish sofort mit
``BrokerConnectionError``, ohne zweiten Versuch. Seit dem Fix wiederholt die
Schleife auch ein gescheitertes Kanalöffnen; eine verstrichene
Bereitschaftsfrist nicht.

Register: docs/befunde/fehlersuche-runde-4-2026-10.md (F4-31; Signal S-011;
Chaos-Gate 2026-10-30, Seed 3287596344, und Gegenprüfung 2026-10-31: 1 von 20
Läufen von ``tests/chaos/test_publish_deadline.py::TestPublishDeadlineAgainstRealBroker::test_publish_recovers_after_the_stall_clears``
unter Halblast rot).

Vorher rief ``_publish_with_retry`` (``broker/rabbitmq/delivery.py``)
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
# [x] Kanalöffnen scheitert einmal an geschlossener Verbindung -> zweiter Versuch, Publish gelingt
# [x] Kanalöffnen scheitert bei jedem Versuch -> BrokerConnectionError nach dem Wiederholbudget
# [x] Verbindung wird nicht bereit (Frist verstrichen) -> kein zweites Warten
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, Mock

import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.broker.rabbitmq.connection import _PUBLISH_MAX_ATTEMPTS
from swarmq.exceptions import BrokerConnectionError, PublishIndeterminate
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


@pytest.mark.entscheidung("E-042")
@pytest.mark.befund("F4-31")
async def test_a_reopen_that_keeps_failing_ends_as_a_connection_error_after_the_retry_budget() -> None:
    broker = RabbitMQBroker("amqp://guest:guest@localhost/")
    broker._connection = Mock(is_closed=False, transport=None)
    broker._connection.ready = AsyncMock()
    broker._publish_channel_factory = AsyncMock(side_effect=RuntimeError("connection closed"))

    with pytest.raises(BrokerConnectionError, match="failed after") as raised:
        await broker.publish("tasks", b"{}")

    assert not isinstance(raised.value, PublishIndeterminate)
    assert broker._publish_channel_factory.await_count == _PUBLISH_MAX_ATTEMPTS


@pytest.mark.befund("F4-31")
async def test_a_connection_that_never_became_ready_is_not_waited_for_again() -> None:
    # Die Bereitschaftsfrist ist schon ganz verstrichen; eine Wiederholung
    # vervielfachte sie, statt dem Aufrufer zu melden.
    broker = RabbitMQBroker("amqp://guest:guest@localhost/")
    broker._connection = Mock(is_closed=False, transport=None)
    broker._connection.ready = AsyncMock(side_effect=TimeoutError)
    broker._publish_channel_factory = AsyncMock()

    with pytest.raises(BrokerConnectionError, match="not ready"):
        await broker.publish("tasks", b"{}")

    assert broker._connection.ready.await_count == 1
    broker._publish_channel_factory.assert_not_awaited()
