"""Verbindungsabriss waehrend eines Publish (B24, Loadsuite v0.12.0rc1).

Reisst die Verbindung, waehrend ein Publish laeuft, endete er mit einem
rohen ``aiormq.exceptions.AMQPConnectionError``; die Nachricht war meist
schon beim Broker. Der Ausgang haengt an der Phase:

* Der Frame wurde nicht geschrieben (aiormqs ``drain_future`` scheitert):
  nichts gesendet, der bestehende Wiederholpfad greift, am Ende
  ``BrokerConnectionError``.
* Der Frame wurde geschrieben, swarmq wartet auf das Publisher-Confirm:
  die Zustellung ist unbestimmt, ``PublishIndeterminate`` ohne erneutes
  Senden.

Der Publish laeuft durch den echten aio-pika/aiormq-Code
(``tests/support/aiormq_transport.py``); nur Writer und Broker spielt der Test.

Test-Liste:
- [x] AMQPConnectionError, bevor der Frame geschrieben ist -> Wiederholung, Publish gelingt
- [x] AMQPConnectionError beim Warten auf das Confirm -> PublishIndeterminate, kein zweiter Kanal
- [x] ebenso CancelledError (Kanal/Verbindung von aio-pika geschlossen, etwa
      ``reconnect()``) und ChannelClosed (Broker schliesst den Kanal)
- [x] ebenso eine Ausnahme ausserhalb der Kanalfehler (pamqp-Framefehler, mit
      dem aiormqs Reader endet) -> PublishIndeterminate statt roh
- [x] dead_letter: Kopie bestaetigt, Ack des Originals scheitert mit
      AMQPConnectionError -> SettleFailedAfterPublish statt roh
- [x] dead_letter: Abriss beim Confirm der Kopie -> PublishIndeterminate,
      Original unbestaetigt
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, Mock

import pamqp.exceptions
import pytest
from aiormq.abc import ChannelFrame
from aiormq.exceptions import AMQPConnectionError, ChannelClosed

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.contracts import Delivery
from swarmq.exceptions import PublishIndeterminate, SettleFailedAfterPublish
from tests.support.aiormq_transport import FakePublishChannel, FakeTransport

pytestmark = [pytest.mark.timeout(5)]

_LOST = "Server connection unexpectedly closed. Read 0 bytes but 1 bytes expected"


@pytest.fixture(autouse=True)
def _no_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    # Die Wiederholungen warten 0.05 * 2**n s; im Unit-Test ohne Wartezeit.
    original = asyncio.sleep

    async def _instant(_delay: float, *args: object) -> None:
        await original(0)

    monkeypatch.setattr(asyncio, "sleep", _instant)


def _broker(*transports: FakeTransport) -> RabbitMQBroker:
    """Broker, dessen Publish-Kanaele nacheinander ueber *transports* laufen."""
    broker = RabbitMQBroker("amqp://guest:guest@localhost/")
    broker._connection = Mock(is_closed=False, transport=None)
    broker._connection.ready = AsyncMock()
    channels = [FakePublishChannel(transport) for transport in transports]
    broker._publish_channel_factory = AsyncMock(side_effect=channels)
    return broker


async def _frame_or_outcome(transport: FakeTransport, publish: asyncio.Task[Any]) -> ChannelFrame:
    """Der naechste Frame auf *transport*; endet der Publish vorher, dessen
    Ergebnis (meist die Ausnahme) statt eines Haengers bis zum Timeout."""
    frame = asyncio.ensure_future(transport.next_frame())
    await asyncio.wait({frame, publish}, return_when=asyncio.FIRST_COMPLETED)
    if not frame.done():
        frame.cancel()
        publish.result()
        pytest.fail("publish finished without sending on the expected channel")
    return frame.result()


async def test_connection_lost_before_the_frame_was_written_retries_on_a_new_channel() -> None:
    first, second = FakeTransport(), FakeTransport()
    broker = _broker(first, second)
    publish = asyncio.create_task(broker.publish("tasks", b"{}"))

    await first.next_frame()  # in der Schreib-Queue, nie geschrieben
    first.lose_connection(AMQPConnectionError(_LOST))
    frame = await _frame_or_outcome(second, publish)
    frame.drain()
    second.confirm_all()

    receipt = await publish
    assert receipt.queue == "tasks"


async def test_connection_lost_while_waiting_for_the_confirm_is_indeterminate_and_not_resent() -> None:
    """Der Kern von B24: Die Nachricht kann schon in der Queue liegen. Ein
    erneutes Senden fuehrte den Task doppelt aus, ein ``BrokerConnectionError``
    verspraeche dem Aufrufer, dass Wiederholen sicher ist."""
    transport = FakeTransport()
    broker = _broker(transport)
    publish = asyncio.create_task(broker.publish("tasks", b"{}"))

    frame = await transport.next_frame()
    frame.drain()  # auf dem Socket; jetzt fehlt nur das Confirm
    lost = AMQPConnectionError(_LOST)
    transport.lose_connection(lost)

    with pytest.raises(PublishIndeterminate) as caught:
        await publish
    assert caught.value.__cause__ is lost
    assert broker._publish_channel_factory.await_count == 1  # type: ignore[union-attr]


@pytest.mark.parametrize(
    "lost",
    [
        pytest.param(asyncio.CancelledError(), id="aio-pika-closed-the-channel"),
        pytest.param(ChannelClosed(406, "PRECONDITION_FAILED"), id="broker-closed-the-channel"),
    ],
)
async def test_other_losses_while_waiting_for_the_confirm_are_indeterminate_too(lost: BaseException) -> None:
    """Dieselbe Phase, andere Ausnahme: aiormq lehnt die offenen Futures mit
    der Ursache des Schliessens ab. Bisher wiederholte swarmq diese Typen."""
    transport = FakeTransport()
    broker = _broker(transport)
    publish = asyncio.create_task(broker.publish("tasks", b"{}"))

    frame = await transport.next_frame()
    frame.drain()
    transport.lose_connection(lost)

    with pytest.raises(PublishIndeterminate) as caught:
        await publish
    assert caught.value.__cause__ is lost
    assert broker._publish_channel_factory.await_count == 1  # type: ignore[union-attr]


async def test_a_reader_failure_while_waiting_for_the_confirm_is_indeterminate_not_raw() -> None:
    """Endet aiormqs Reader an einem kaputten Frame, lehnt er die offenen
    Futures mit dem pamqp-Fehler ab -- keiner der bekannten Kanalfehler."""
    transport = FakeTransport()
    broker = _broker(transport)
    publish = asyncio.create_task(broker.publish("tasks", b"{}"))

    frame = await transport.next_frame()
    frame.drain()
    lost = pamqp.exceptions.AMQPFrameError(b"\x00")
    transport.lose_connection(lost)

    with pytest.raises(PublishIndeterminate) as caught:
        await publish
    assert caught.value.__cause__ is lost


def _delivery() -> Delivery:
    return Delivery(body=b"{}", headers={}, queue="tasks", handle=AsyncMock(consumer_tag=None))


async def test_dead_letter_whose_ack_loses_the_connection_reports_the_copy_as_published() -> None:
    transport = FakeTransport()
    broker = _broker(transport)
    delivery = _delivery()
    delivery.handle.ack.side_effect = AMQPConnectionError(_LOST)
    dead_letter = asyncio.create_task(broker.dead_letter(delivery, headers={}))

    frame = await transport.next_frame()
    frame.drain()
    transport.confirm_all()

    with pytest.raises(SettleFailedAfterPublish):
        await dead_letter


async def test_dead_letter_losing_the_copy_confirm_is_indeterminate_and_keeps_the_original() -> None:
    transport = FakeTransport()
    broker = _broker(transport)
    delivery = _delivery()
    dead_letter = asyncio.create_task(broker.dead_letter(delivery, headers={}))

    frame = await transport.next_frame()
    frame.drain()
    transport.lose_connection(AMQPConnectionError(_LOST))

    with pytest.raises(PublishIndeterminate):
        await dead_letter
    delivery.handle.ack.assert_not_awaited()
