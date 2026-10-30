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
- [x] pamqp-Framefehler, bevor der Frame geschrieben ist -> Wiederholung wie
      bei AMQPConnectionError (nichts gesendet), nicht roh
- [x] ebenso ``InvalidFrameError``, mit dem aiormq 7.2 den pamqp-Fehler des
      Readers meldet (KTD4, Plan 2026-10-29-1945)
- [x] ``InvalidFrameError`` beim Warten auf das Confirm -> PublishIndeterminate
- [x] Fehler unter der Kanal-Sperre vor dem Einreihen (Marshalling) -> nicht
      gesendet: der Fehler geht roh durch, nicht als PublishIndeterminate (KTD3)
- [x] Abriss vor dem Schreiben: das verwaiste Confirm-Future wird abgeholt,
      asyncio meldet kein "Future exception was never retrieved"
- [x] Aufrufer bricht ab, bevor der Frame geschrieben ist (``wait_for``), der
      Abriss kommt danach: auch dieses Confirm wird abgeholt
- [x] Versionsschutz: aiormqs ``FutureStore`` holt die Ausnahme eines
      abgelehnten Futures selbst ab, auf das niemand wartet (aiormq #253;
      deshalb holt swarmq das Confirm nicht mehr selbst ab, KTD3)
"""

from __future__ import annotations

import asyncio
import gc
from collections.abc import AsyncIterator
from typing import Any
from unittest.mock import AsyncMock, Mock

import aiormq.base
import pamqp.exceptions
import pytest
from aiormq.abc import ChannelFrame
from aiormq.exceptions import AMQPConnectionError, ChannelClosed, InvalidFrameError

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


@pytest.mark.entscheidung("E-042")
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


@pytest.mark.entscheidung("E-042")
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


def _delivery() -> tuple[Delivery, AsyncMock]:
    handle = AsyncMock(consumer_tag=None)
    return Delivery(body=b"{}", headers={}, queue="tasks", handle=handle), handle


async def test_dead_letter_whose_ack_loses_the_connection_reports_the_copy_as_published() -> None:
    transport = FakeTransport()
    broker = _broker(transport)
    delivery, handle = _delivery()
    handle.ack.side_effect = AMQPConnectionError(_LOST)
    dead_letter = asyncio.create_task(broker.dead_letter(delivery, headers={}))

    frame = await transport.next_frame()
    frame.drain()
    transport.confirm_all()

    with pytest.raises(SettleFailedAfterPublish):
        await dead_letter


@pytest.mark.entscheidung("E-041")
async def test_dead_letter_losing_the_copy_confirm_is_indeterminate_and_keeps_the_original() -> None:
    transport = FakeTransport()
    broker = _broker(transport)
    delivery, handle = _delivery()
    dead_letter = asyncio.create_task(broker.dead_letter(delivery, headers={}))

    frame = await transport.next_frame()
    frame.drain()
    transport.lose_connection(AMQPConnectionError(_LOST))

    with pytest.raises(PublishIndeterminate):
        await dead_letter
    handle.ack.assert_not_awaited()


@pytest.mark.entscheidung("E-042")
async def test_a_reader_failure_before_the_frame_was_written_retries_on_a_new_channel() -> None:
    """Endet aiormqs Reader an einem kaputten Frame, ist die Verbindung tot
    wie bei ``AMQPConnectionError``; gesendet wurde nichts."""
    first, second = FakeTransport(), FakeTransport()
    broker = _broker(first, second)
    publish = asyncio.create_task(broker.publish("tasks", b"{}"))

    await first.next_frame()
    first.lose_connection(pamqp.exceptions.AMQPFrameError(b"\x00"))
    frame = await _frame_or_outcome(second, publish)
    frame.drain()
    second.confirm_all()

    receipt = await publish
    assert receipt.queue == "tasks"


async def test_an_invalid_frame_before_the_frame_was_written_retries_on_a_new_channel() -> None:
    """KTD4 (R6): aiormq 7.2 meldet einen kaputten Frame des Readers als
    ``InvalidFrameError``; gesendet wurde nichts, also wie B24 wiederholen."""
    first, second = FakeTransport(), FakeTransport()
    broker = _broker(first, second)
    publish = asyncio.create_task(broker.publish("tasks", b"{}"))

    await first.next_frame()
    first.lose_connection(InvalidFrameError(b"\x00"))
    frame = await _frame_or_outcome(second, publish)
    frame.drain()
    second.confirm_all()

    receipt = await publish
    assert receipt.queue == "tasks"


async def test_an_invalid_frame_while_waiting_for_the_confirm_is_indeterminate() -> None:
    transport = FakeTransport()
    broker = _broker(transport)
    publish = asyncio.create_task(broker.publish("tasks", b"{}"))

    frame = await transport.next_frame()
    frame.drain()
    lost = InvalidFrameError(b"\x00")
    transport.lose_connection(lost)

    with pytest.raises(PublishIndeterminate) as caught:
        await publish
    assert caught.value.__cause__ is lost
    assert broker._publish_channel_factory.await_count == 1  # type: ignore[union-attr]


async def test_a_failure_before_the_frames_are_enqueued_is_not_indeterminate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """KTD3: aiormq 7.2 bindet ``drain_future = None`` schon vor dem Einreihen;
    erst ``sent`` sagt, ob die Frames in der ``write_queue`` liegen. Ein Fehler
    davor hat nichts gesendet."""
    transport = FakeTransport()
    broker = _broker(transport)
    failure = TypeError("marshalling failed")

    def _marshall(*_args: object, **_kwargs: object) -> ChannelFrame:
        raise failure

    monkeypatch.setattr(ChannelFrame, "marshall", _marshall)

    with pytest.raises(TypeError) as caught:
        await broker.publish("tasks", b"{}")

    assert caught.value is failure
    assert transport.write_queue.empty()


async def _callbacks_ran() -> None:
    """Wartet, bis die bis jetzt eingeplanten Loop-Callbacks gelaufen sind
    (etwa Done-Callbacks eines eben abgelehnten Futures); FIFO-Reihenfolge."""
    ran = asyncio.Event()
    asyncio.get_running_loop().call_soon(ran.set)
    await ran.wait()


@pytest.fixture
async def unretrieved() -> AsyncIterator[list[str]]:
    """Meldungen "Future exception was never retrieved" des Loops. asyncio
    meldet sie, wenn ein Future mit unabgeholter Ausnahme eingesammelt wird."""
    loop = asyncio.get_running_loop()
    previous = loop.get_exception_handler()
    messages: list[str] = []

    def handler(_loop: asyncio.AbstractEventLoop, context: dict[str, Any]) -> None:
        if "never retrieved" in str(context.get("message", "")):
            messages.append(repr(context.get("exception")))

    loop.set_exception_handler(handler)
    try:
        yield messages
    finally:
        loop.set_exception_handler(previous)


async def test_the_orphaned_confirm_of_a_failed_drain_is_retrieved(unretrieved: list[str]) -> None:
    """aiormq legt das Confirm-Future vor dem Schreiben an. Scheitert schon
    das Schreiben, wartet niemand mehr darauf; seine Ausnahme aus dem
    Verbindungsabriss blieb unabgeholt (Loadsuite v0.12.0rc1)."""
    first, second = FakeTransport(), FakeTransport()
    broker = _broker(first, second)
    publish = asyncio.create_task(broker.publish("tasks", b"{}"))

    await first.next_frame()
    first.lose_connection(AMQPConnectionError(_LOST))
    frame = await _frame_or_outcome(second, publish)
    frame.drain()
    second.confirm_all()
    await publish
    await _callbacks_ran()
    first.forget_futures()
    gc.collect()

    assert unretrieved == []


async def test_the_orphaned_confirm_of_a_cancelled_publish_is_retrieved(unretrieved: list[str]) -> None:
    """Die Loadsuite brach einen Dispatch per ``wait_for`` ab, waehrend der
    Publish noch schrieb; der spaetere Abriss lehnte das Confirm ab."""
    transport = FakeTransport()
    broker = _broker(transport)
    publish = asyncio.create_task(broker.publish("tasks", b"{}"))

    await transport.next_frame()
    publish.cancel()
    with pytest.raises(asyncio.CancelledError):
        await publish
    transport.lose_connection(AMQPConnectionError(_LOST))
    await _callbacks_ran()
    transport.forget_futures()
    gc.collect()

    assert unretrieved == []


async def test_aiormq_retrieves_a_rejected_future_nobody_waits_for(unretrieved: list[str]) -> None:
    """Gegen das echte aiormq (#253): ``basic_publish`` legt Confirm- und
    Drain-Future ueber ``FutureStore.create_future`` an. Scheitert der Drain,
    wartet niemand mehr auf das Confirm; der Abriss lehnt es trotzdem ab, ohne
    dass asyncio "never retrieved" meldet."""
    store = aiormq.base.FutureStore(asyncio.get_running_loop())
    _ = store.create_future()

    await store.reject_all(AMQPConnectionError(_LOST))
    await _callbacks_ran()
    gc.collect()

    assert unretrieved == []
