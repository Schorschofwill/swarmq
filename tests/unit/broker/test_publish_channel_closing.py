"""Publish gegen ``closing`` des aiormq-Kanals (FS-08, KTD9, Ergaenzung a3).

Jeder Versuch laeuft als Kind-Task gegen ``closing`` seines Kanals. Wird der
Kanal zuerst geschlossen, wird das Kind abgebrochen: ``drain_future`` erfuellt
heisst gesendet (``PublishIndeterminate``, E-042), sonst naechster Versuch auf
einem neuen Kanal. Reisst die Publish-Frist, bevor gesendet wurde, folgt
``BrokerConnectionError`` statt ``PublishIndeterminate`` (a3).

Der Publish laeuft durch den echten aio-pika/aiormq-Code
(``tests/support/aiormq_transport.py``, mit ``closing``-Future); Writer und
Broker spielt der Test. Den Haenger in der Sperren-Warteschlange selbst weist
``test_befund_fs08_publish_lock_waiter.py`` nach, die Kombination mit einem
Drain-Abbruch ``tests/unit/worker/test_fs08_drain_cancel_publish.py``.

Test-Liste:
- [x] Kind in der Sperren-Warteschlange abgebrochen, Kanal zu, bevor die
      Sperre frei wird -> nicht gesendet, beide gelingen auf neuem Kanal
- [x] Kind hat die Sperre bekommen und den Frame eingereiht, dann Kanal zu
      -> nicht gesendet, neuer Kanal (zweite Wakeup-Reihenfolge)
- [x] ``drain_future`` erfuellt, dann Kanal zu -> PublishIndeterminate, kein
      zweiter Kanal
- [x] Broker schliesst nur den Kanal (ChannelClosed) beim Warten auf den Drain
      -> neuer Kanal, Publish gelingt
- [x] Frist reisst waehrend des Wartens nach dem Einreihen -> PublishIndeterminate
      (E-041), Kind abgebrochen und abgeholt, kein "never retrieved"
- [x] eigener Abbruch waehrend des Wartens geht als CancelledError durch, Kind
      abgebrochen
- [x] Kanal schon zu beim Versuchsbeginn -> nichts eingereiht, neuer Kanal
- [x] Frist reisst vor dem Senden (Warten auf ``ready()``) -> BrokerConnectionError
      (a3), nicht PublishIndeterminate
- [x] Frist reisst beim Kanalaufbau nach einem nicht gesendeten Versuch ->
      BrokerConnectionError (a3)
- [x] Versuch 1 negativ bestaetigt (Basic.Nack), Versuch 2 nicht gesendet,
      Frist reisst beim Kanalaufbau -> PublishIndeterminate (E-041, Review U8)
- [x] Frist reisst, waehrend das Kind in der Sperren-Warteschlange steht
      (Verbindung offen) -> BrokerConnectionError (E-042, Review U8)
- [x] viele parallele Publishes auf einem Kanal -> ein Callback auf
      ``closing`` statt einem je Publish (O(N^2), Review U8)
- [x] beendetes Kind haengt nicht am ``closing`` des Kanals (kein Speicherleck)
- [x] aio-pika-Kanal ohne ``closing``-Future -> Rueckfall mit einer WARNING je Broker
- [x] Versionsschutz: aiormq traegt ``closing``-Future, aio-pika ``get_underlay_channel``
- [x] eigener Abbruch in der Sperren-Warteschlange -> ``PublishCancelledUnsent``
      (U4, E-042-Kriterium ``maybe_sent``)
- [x] eigener Abbruch im Backoff nach einem nicht gesendeten Versuch -> markiert
- [x] eigener Abbruch beim Kanalaufbau (Warten auf ``ready()``) -> markiert
- [x] eigener Abbruch nach dem Einreihen -> unmarkierter ``CancelledError``
- [x] eigener Abbruch nach einem negativen Confirm -> unmarkiert (E-041)
- [x] fremder ``CancelledError`` vor dem Senden -> unmarkiert
- [x] Handler, den der Drain schon abgebrochen hat (``cancelling() > 0``),
      verliert den Kanal vor dem Senden -> kein markierter Abbruch
"""

from __future__ import annotations

import asyncio
import contextlib
import gc
import weakref
from collections.abc import Iterator
from typing import Any, cast, override
from unittest.mock import AsyncMock, Mock

import aio_pika
import aiormq
import aiormq.base
import pytest
from aiormq.abc import ExceptionType
from aiormq.exceptions import AMQPConnectionError, ChannelClosed

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.contracts import PublishReceipt
from swarmq.exceptions import BrokerConnectionError, PublishCancelledUnsent, PublishIndeterminate
from tests.support.aiormq_transport import FakePublishChannel, FakeTransport
from tests.support.virtual_time import on_virtual_clock

# Alle Tests laufen auf der virtuellen Loop-Uhr (``on_virtual_clock``, 0.14):
# Die Publish-Frist reisst genau dann, wenn die Loop sonst nur noch warten
# wuerde, nie schon waehrend der Schritte davor. Vorher rissen die engen
# Fristen gegen die echte Uhr unter paralleler Last (CI und Stresslauf
# 2026-10-16); ein serieller Durchgang (``timing``) ist nicht mehr noetig.
pytestmark = [pytest.mark.timeout(5)]

_LOST = "Server connection unexpectedly closed. Read 0 bytes but 1 bytes expected"


@pytest.fixture(autouse=True)
def _no_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    # Die Wiederholungen warten 0.05 * 2**n s; im Unit-Test ohne Wartezeit.
    original = asyncio.sleep

    async def _instant(_delay: float, *args: object) -> None:
        await original(0)

    monkeypatch.setattr(asyncio, "sleep", _instant)


def _broker(*transports: FakeTransport, ready: AsyncMock | None = None) -> RabbitMQBroker:
    """Broker, dessen Publish-Kanaele nacheinander ueber *transports* laufen;
    *ready* ist ``connection.ready`` (Default: sofort bereit)."""
    broker = RabbitMQBroker("amqp://guest:guest@localhost/")
    broker._connection = Mock(is_closed=False, transport=None)
    broker._connection.ready = ready or AsyncMock()
    channels = [FakePublishChannel(transport) for transport in transports]
    broker._publish_channel_factory = AsyncMock(side_effect=channels)
    return broker


@contextlib.contextmanager
def _unretrieved() -> Iterator[list[str]]:
    """Meldungen "never retrieved" des laufenden Loops (wie in
    test_publish_connection_loss.py). Kein Fixture: Der Test laeuft auf der
    virtuellen Loop, nicht auf der von pytest-asyncio."""
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


async def _callbacks_ran() -> None:
    """Laesst bereits eingeplante Callbacks laufen; FIFO-Reihenfolge."""
    ran = asyncio.Event()
    asyncio.get_running_loop().call_soon(ran.set)
    await ran.wait()


async def _healthy_wire(transport: FakeTransport) -> None:
    """Writer und Broker eines gesunden Kanals: schreiben und bestaetigen."""
    while True:
        frame = await transport.next_frame()
        frame.drain()
        transport.confirm_all()


@on_virtual_clock
async def test_a_publish_cancelled_in_the_lock_queue_is_not_sent_and_retried() -> None:
    dead, healthy = FakeTransport(closing=True), FakeTransport()
    broker = _broker(dead, healthy)
    wire = asyncio.create_task(_healthy_wire(healthy))
    try:
        holder = asyncio.create_task(broker.publish("tasks", b'{"n": 1}'))
        await dead.next_frame()  # eingereiht, nie geschrieben; haelt die Sperre
        waiter = asyncio.create_task(broker.publish("tasks", b'{"n": 2}'))
        await dead.lock.waiter_arrived.wait()

        dead.close_channel(AMQPConnectionError(_LOST))  # Sperre bleibt belegt
        results = await asyncio.gather(holder, waiter)
    finally:
        wire.cancel()
        await asyncio.gather(wire, return_exceptions=True)

    assert all(isinstance(result, PublishReceipt) for result in results)
    assert dead.write_queue.empty(), "der Wartende hat nichts in den toten Kanal gelegt"


@on_virtual_clock
async def test_a_publish_that_got_the_lock_after_the_cut_is_not_sent_and_retried() -> None:
    """Zweite Wakeup-Reihenfolge: Der Halter scheitert zuerst und gibt die
    Sperre frei, der Wartende reiht seinen Frame in den toten Kanal ein; erst
    dann ist der Kanal zu."""
    dead, healthy = FakeTransport(closing=True), FakeTransport()
    broker = _broker(dead, healthy)
    wire = asyncio.create_task(_healthy_wire(healthy))
    try:
        holder = asyncio.create_task(broker.publish("tasks", b'{"n": 1}'))
        await dead.next_frame()
        waiter = asyncio.create_task(broker.publish("tasks", b'{"n": 2}'))
        await dead.lock.waiter_arrived.wait()

        lost = AMQPConnectionError(_LOST)
        dead.reject_pending(lost)  # nur die Futures des Halters, Kanal noch offen
        await dead.next_frame()  # der Wartende hat die Sperre und reiht ein
        dead.close_channel(lost)
        results = await asyncio.gather(holder, waiter)
    finally:
        wire.cancel()
        await asyncio.gather(wire, return_exceptions=True)

    assert all(isinstance(result, PublishReceipt) for result in results)


@on_virtual_clock
async def test_a_drained_publish_whose_channel_closes_is_indeterminate_and_not_resent() -> None:
    transport = FakeTransport(closing=True)
    broker = _broker(transport)
    publish = asyncio.create_task(broker.publish("tasks", b"{}"))

    frame = await transport.next_frame()
    frame.drain()  # auf dem Socket; jetzt fehlt nur das Confirm
    transport.close_channel(AMQPConnectionError(_LOST))

    with pytest.raises(PublishIndeterminate):
        await publish
    assert cast("AsyncMock", broker._publish_channel_factory).await_count == 1


@on_virtual_clock
async def test_a_channel_closed_by_the_broker_while_waiting_for_the_drain_retries_on_a_new_channel() -> None:
    closed, healthy = FakeTransport(closing=True), FakeTransport()
    broker = _broker(closed, healthy)
    wire = asyncio.create_task(_healthy_wire(healthy))
    try:
        publish = asyncio.create_task(broker.publish("tasks", b"{}"))
        await closed.next_frame()  # eingereiht, der Writer kommt nicht nach
        closed.lose_connection(ChannelClosed(406, "PRECONDITION_FAILED"))
        receipt = await publish
    finally:
        wire.cancel()
        await asyncio.gather(wire, return_exceptions=True)

    assert receipt.queue == "tasks"


@on_virtual_clock
async def test_a_channel_closed_before_the_attempt_sends_nothing_and_retries() -> None:
    closed, healthy = FakeTransport(closing=True), FakeTransport()
    closed.close_channel(AMQPConnectionError(_LOST))
    broker = _broker(closed, healthy)
    wire = asyncio.create_task(_healthy_wire(healthy))
    try:
        receipt = await broker.publish("tasks", b"{}")
    finally:
        wire.cancel()
        await asyncio.gather(wire, return_exceptions=True)

    assert receipt.queue == "tasks"
    assert closed.write_queue.empty()


@on_virtual_clock
async def test_an_own_cancel_while_waiting_passes_through_and_cancels_the_child() -> None:
    transport = FakeTransport(closing=True)
    broker = _broker(transport)
    publish = asyncio.create_task(broker.publish("tasks", b"{}"))

    await transport.next_frame()
    publish.cancel()

    with pytest.raises(asyncio.CancelledError):
        await publish
    assert asyncio.all_tasks() == {asyncio.current_task()}, "das Kind laeuft nicht weiter"


async def _deadline_while_waiting() -> tuple[type[BaseException] | None, set[asyncio.Task[object]]]:
    """Frist reisst, waehrend das Kind auf den Drain wartet; danach reisst die
    Verbindung. Liefert nur den Ausgangstyp und die noch laufenden Tasks:
    Ausnahme, Broker und Transport halten ueber Tracebacks die Rahmen des
    Versuchs und damit seine Futures; sie enden mit diesem Aufruf."""
    transport = FakeTransport(closing=True, retrieve_rejected=False)
    broker = _broker(transport)
    # Die Frist laeuft von selbst ab: Ein per ``set_publish_deadline``
    # verschobener Deckel liesse seinen alten Timer-Handle im Loop stehen.
    broker.publish_timeout_s = 0.02
    publish = asyncio.create_task(broker.publish("tasks", b"{}"))
    await transport.next_frame()
    outcome: type[BaseException] | None = None
    try:
        await publish
    except BaseException as exc:  # nur der Typ zaehlt
        outcome = type(exc)
    running = asyncio.all_tasks() - {asyncio.current_task()}
    transport.lose_connection(AMQPConnectionError(_LOST))
    await _callbacks_ran()
    transport.forget_futures()
    return outcome, running


@on_virtual_clock
async def test_a_deadline_while_waiting_cancels_and_collects_the_child() -> None:
    """E-041: Die Frist reisst mitten im Versuch (Frame eingereiht) ->
    unbestimmt. Das Kind endet mit dem Vorgang; sein Confirm, das der spaetere
    Abriss ablehnt, ist abgeholt."""
    with _unretrieved() as unretrieved:
        outcome, running = await _deadline_while_waiting()
        gc.collect()

    assert outcome is PublishIndeterminate
    assert running == set(), "das Kind laeuft nicht weiter"
    assert unretrieved == []


#: Frist fuer Tests, in denen sie erst am zweiten, nie bereiten Kanalaufbau
#: reissen soll. Auf der echten Uhr rissen 20 ms unter CI-Last schon vorher
#: und verfehlten die Voraussetzung; auf der virtuellen Uhr kosten die 0.25 s
#: keine Laufzeit mehr.
_DEADLINE_AT_SECOND_READY_S = 0.25


async def _never_ready() -> None:
    """``connection.ready()`` einer Verbindung, die nicht wiederkommt."""
    await asyncio.Event().wait()


@on_virtual_clock
async def test_a_deadline_before_anything_was_sent_is_not_indeterminate() -> None:
    """a3: Die Frist reisst beim Warten auf die Verbindung; gesendet wurde
    nichts, der Aufrufer darf sicher wiederholen."""
    broker = _broker(FakeTransport(closing=True), ready=AsyncMock(side_effect=_never_ready))
    broker.publish_timeout_s = 0.02

    with pytest.raises(BrokerConnectionError) as caught:
        await broker.publish("tasks", b"{}")

    assert not isinstance(caught.value, PublishIndeterminate)


@on_virtual_clock
async def test_a_deadline_while_reconnecting_after_an_unsent_attempt_is_not_indeterminate() -> None:
    """a3: Der erste Versuch wurde nachweislich nicht gesendet (Abriss vor
    dem Schreiben); die Frist reisst beim Kanalaufbau des naechsten."""
    dead = FakeTransport(closing=True)
    ready_calls = 0

    async def _ready_once() -> None:
        nonlocal ready_calls
        ready_calls += 1
        if ready_calls > 1:
            await _never_ready()

    broker = _broker(dead, ready=AsyncMock(side_effect=_ready_once))
    # Die Frist reisst ohnehin am haengenden zweiten Kanalaufbau; 0.25 s
    # laesst den Schritten davor Luft (20 ms rissen unter CI-Last vorher).
    broker.publish_timeout_s = _DEADLINE_AT_SECOND_READY_S
    publish = asyncio.create_task(broker.publish("tasks", b"{}"))
    await dead.next_frame()  # eingereiht, nie geschrieben
    dead.lose_connection(AMQPConnectionError(_LOST))

    with pytest.raises(BrokerConnectionError) as caught:
        await publish

    assert not isinstance(caught.value, PublishIndeterminate)
    assert ready_calls == 2, "Voraussetzung: die Frist riss beim zweiten Kanalaufbau"


@on_virtual_clock
async def test_a_deadline_after_a_nacked_then_an_unsent_attempt_stays_indeterminate() -> None:
    """E-041: Ein negatives Confirm heisst "womoeglich gesendet". Ein spaeterer,
    nachweislich nicht gesendeter Versuch loescht das nicht; reisst die Frist
    danach beim Kanalaufbau, bleibt der Ausgang unbestimmt."""
    transport = FakeTransport(closing=True)
    ready_calls = 0

    async def _ready_once() -> None:
        nonlocal ready_calls
        ready_calls += 1
        if ready_calls > 1:
            await _never_ready()

    broker = _broker(transport, ready=AsyncMock(side_effect=_ready_once))
    # Die Frist reisst ohnehin am haengenden zweiten Kanalaufbau; 0.25 s
    # laesst den Schritten davor Luft (20 ms rissen unter CI-Last vorher).
    broker.publish_timeout_s = _DEADLINE_AT_SECOND_READY_S
    publish = asyncio.create_task(broker.publish("tasks", b"{}"))
    frame = await transport.next_frame()
    frame.drain()
    transport.nack_all()  # Versuch 1: Basic.Nack -> DeliveryError
    transport.close_channel(AMQPConnectionError(_LOST))  # Versuch 2 sendet nichts

    with pytest.raises(PublishIndeterminate):
        await publish

    assert ready_calls == 2, "Voraussetzung: die Frist riss beim Kanalaufbau nach dem nicht gesendeten Versuch"
    assert transport.write_queue.empty(), "Voraussetzung: Versuch 2 hat nichts eingereiht"


@on_virtual_clock
async def test_a_deadline_while_waiting_for_the_channel_lock_is_not_indeterminate() -> None:
    """E-042: Das Kind steht noch in der Warteschlange der Kanal-Sperre, die
    Verbindung bleibt offen. Reisst die Frist, hat es nichts eingereiht; der
    Aufrufer darf sicher wiederholen."""
    transport = FakeTransport(closing=True)
    broker = _broker(transport)
    broker.publish_timeout_s = 0.02
    await transport.lock.acquire()  # ein anderer Publish haelt die Sperre
    try:
        publish = asyncio.create_task(broker.publish("tasks", b"{}"))
        await transport.lock.waiter_arrived.wait()

        with pytest.raises(BrokerConnectionError) as caught:
            await publish
    finally:
        transport.lock.release()

    assert not isinstance(caught.value, PublishIndeterminate)
    assert transport.write_queue.empty()


async def _cancel_and_catch(publish: asyncio.Task[Any]) -> asyncio.CancelledError:
    """Bricht *publish* ab und liefert den ``CancelledError``, mit dem es endet."""
    publish.cancel()
    with pytest.raises(asyncio.CancelledError) as caught:
        await publish
    assert publish.cancelled()
    return caught.value


@pytest.mark.entscheidung("E-041")
@on_virtual_clock
async def test_an_own_cancel_while_waiting_for_the_channel_lock_is_marked_unsent() -> None:
    """U4 (E-042-Kriterium): In der Warteschlange der Kanal-Sperre hat der
    Versuch nichts eingereiht; der Abbruch ist als nicht gesendet markiert."""
    transport = FakeTransport(closing=True)
    broker = _broker(transport)
    await transport.lock.acquire()  # ein anderer Publish haelt die Sperre
    try:
        publish = asyncio.create_task(broker.publish("tasks", b"{}"))
        await transport.lock.waiter_arrived.wait()
        cancelled = await _cancel_and_catch(publish)
    finally:
        transport.lock.release()

    assert isinstance(cancelled, PublishCancelledUnsent)
    assert transport.write_queue.empty()


@pytest.fixture
def hanging_backoff(monkeypatch: pytest.MonkeyPatch) -> asyncio.Event:
    """Der Backoff zwischen zwei Versuchen haengt; das Event meldet seinen Beginn."""
    original = asyncio.sleep
    entered = asyncio.Event()

    async def _sleep(delay: float, *args: object) -> None:
        if delay <= 0:
            await original(0)
            return
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(asyncio, "sleep", _sleep)
    return entered


@pytest.mark.entscheidung("E-041")
@on_virtual_clock
async def test_an_own_cancel_in_the_backoff_after_an_unsent_attempt_is_marked_unsent(
    hanging_backoff: asyncio.Event,
) -> None:
    dead = FakeTransport(closing=True)
    broker = _broker(dead)
    publish = asyncio.create_task(broker.publish("tasks", b"{}"))
    await dead.next_frame()  # eingereiht, nie geschrieben
    dead.lose_connection(AMQPConnectionError(_LOST))
    await hanging_backoff.wait()

    cancelled = await _cancel_and_catch(publish)

    assert isinstance(cancelled, PublishCancelledUnsent)


@pytest.mark.entscheidung("E-041")
@on_virtual_clock
async def test_an_own_cancel_in_the_backoff_after_a_nacked_attempt_stays_unmarked(
    hanging_backoff: asyncio.Event,
) -> None:
    """Ein negatives Confirm heisst "womoeglich gesendet" (E-041): Der
    Abbruch im Backoff danach bleibt unmarkiert, der Claim bleibt."""
    transport = FakeTransport(closing=True)
    broker = _broker(transport)
    publish = asyncio.create_task(broker.publish("tasks", b"{}"))
    frame = await transport.next_frame()
    frame.drain()
    transport.nack_all()
    await hanging_backoff.wait()

    cancelled = await _cancel_and_catch(publish)

    assert not isinstance(cancelled, PublishCancelledUnsent)


@pytest.mark.entscheidung("E-041")
@on_virtual_clock
async def test_an_own_cancel_while_the_channel_opens_is_marked_unsent() -> None:
    waiting = asyncio.Event()

    async def _ready() -> None:
        waiting.set()
        await asyncio.Event().wait()

    broker = _broker(FakeTransport(closing=True), ready=AsyncMock(side_effect=_ready))
    publish = asyncio.create_task(broker.publish("tasks", b"{}"))
    await waiting.wait()

    cancelled = await _cancel_and_catch(publish)

    assert isinstance(cancelled, PublishCancelledUnsent)


@pytest.mark.entscheidung("E-041")
@on_virtual_clock
async def test_an_own_cancel_after_the_frame_was_enqueued_stays_unmarked() -> None:
    """Eingereiht heisst: Der Writer schreibt womoeglich noch (E-042)."""
    transport = FakeTransport(closing=True)
    broker = _broker(transport)
    publish = asyncio.create_task(broker.publish("tasks", b"{}"))
    await transport.next_frame()

    cancelled = await _cancel_and_catch(publish)

    assert not isinstance(cancelled, PublishCancelledUnsent)


@on_virtual_clock
async def test_a_foreign_cancel_before_sending_stays_unmarked() -> None:
    """Ein ``CancelledError`` aus aio-pika (kein Anstieg von ``cancelling()``)
    ist kein Abbruch des Aufrufers und wird nicht markiert."""
    broker = _broker(FakeTransport(closing=True), ready=AsyncMock(side_effect=asyncio.CancelledError()))

    with pytest.raises(asyncio.CancelledError) as caught:
        await broker.publish("tasks", b"{}")

    assert not isinstance(caught.value, PublishCancelledUnsent)


@on_virtual_clock
async def test_a_drain_cancelled_handler_losing_the_channel_before_sending_stays_unmarked() -> None:
    """KTD9: Ein vom Drain abgebrochener Handler laeuft mit ``cancelling() > 0``
    weiter (E-001); ein aio-pika-interner Abbruch ist nicht sein eigener."""
    broker = _broker(FakeTransport(closing=True), ready=AsyncMock(side_effect=asyncio.CancelledError()))

    async def _handler() -> BaseException:
        current = asyncio.current_task()
        assert current is not None
        current.cancel()
        try:
            await asyncio.sleep(0)
        except asyncio.CancelledError:
            pass  # der Drain-Abbruch; der Handler laeuft weiter (E-001)
        try:
            await broker.publish("tasks", b"{}")
        except asyncio.CancelledError as exc:
            return exc
        raise AssertionError("publish should have failed")

    outcome = await asyncio.create_task(_handler())

    assert not isinstance(outcome, PublishCancelledUnsent)


class _CountingClosing(asyncio.Future[Any]):
    """``closing``-Future, das die Callback-Registrierungen zaehlt."""

    def __init__(self) -> None:
        super().__init__()
        self.registered = 0

    @override
    def add_done_callback(self, fn: Any, *, context: Any = None) -> None:
        self.registered += 1
        super().add_done_callback(fn, context=context)


@on_virtual_clock
async def test_many_concurrent_publishes_register_once_on_the_shared_closing() -> None:
    """Alle Publishes teilen den einen Publish-Kanal und sein ``closing``. Je
    Publish ein Callback darauf (und sein lineares Entfernen) machte N
    parallele Publishes quadratisch teuer; registriert wird je Kanal einmal."""
    transport = FakeTransport(closing=True)
    closing = _CountingClosing()
    transport.closing = closing
    broker = _broker(transport)
    wire = asyncio.create_task(_healthy_wire(transport))
    try:
        receipts = await asyncio.gather(*(broker.publish("tasks", b"{}") for _ in range(200)))
    finally:
        wire.cancel()
        await asyncio.gather(wire, return_exceptions=True)

    assert len(receipts) == 200
    assert closing.registered == 1


@on_virtual_clock
async def test_a_finished_publish_child_is_not_kept_alive_by_its_channel() -> None:
    """Der Kanal lebt lange; ein beendetes Publish-Kind darf nicht an seinem
    ``closing`` haengen bleiben (sonst waechst der Speicher je Publish)."""
    transport = FakeTransport(closing=True)
    broker = _broker(transport)
    publish = asyncio.create_task(broker.publish("tasks", b"{}"))
    frame = await transport.next_frame()
    (child,) = asyncio.all_tasks() - {asyncio.current_task(), publish}
    child_ref = weakref.ref(child)
    del child
    frame.drain()
    transport.confirm_all()
    await publish
    gc.collect()

    assert child_ref() is None


class _AioPikaChannel(FakePublishChannel):
    """Gibt sich als echter aio-pika-Kanal aus (virtuelle Unterklasse): Nur
    dort heisst ein fehlendes ``closing``, dass sich aio-pika/aiormq
    geaendert haben; Test-Doubles bleiben still."""


aio_pika.abc.AbstractChannel.register(_AioPikaChannel)


@pytest.mark.entscheidung("E-042")
@on_virtual_clock
async def test_an_unreadable_closing_falls_back_with_one_warning_per_broker(
    swarmq_logs: list[dict[str, Any]],
) -> None:
    """Ein aio-pika-Kanal ohne lesbares ``closing``-Future (aiormq geaendert):
    Der Publish laeuft wie vor FS-08, das meldet je Broker einmal."""
    transport = FakeTransport()  # ohne ``closing``
    broker = _broker(transport)
    cast("AsyncMock", broker._publish_channel_factory).side_effect = [_AioPikaChannel(transport)]
    wire = asyncio.create_task(_healthy_wire(transport))
    try:
        await broker.publish("tasks", b"{}")
        await broker.publish("tasks", b"{}")
    finally:
        wire.cancel()
        await asyncio.gather(wire, return_exceptions=True)

    warnings = [e["event"] for e in swarmq_logs if e["log_level"] == "warning"]
    assert warnings == ["publish_channel_closing_unreadable"]


class _AiormqBase(aiormq.base.Base):
    @override
    async def _on_close(self, exc: ExceptionType | None = None) -> None:
        return None

    @override
    def __str__(self) -> str:
        return "base"


@on_virtual_clock
async def test_the_installed_aio_pika_and_aiormq_still_provide_a_closing_future() -> None:
    """Versionsschutz: Der FS-08-Waechter braucht ``get_underlay_channel`` an
    aio-pikas Kanal und ein ``closing``-Future an aiormqs Kanal. Fehlt eins
    nach einem Upgrade, faellt der Publish still in den Haenger zurueck."""
    base = _AiormqBase(loop=asyncio.get_running_loop())

    assert isinstance(base.closing, asyncio.Future)
    assert issubclass(aiormq.Channel, aiormq.base.Base)
    assert callable(getattr(aio_pika.RobustChannel, "get_underlay_channel", None))
    assert issubclass(aio_pika.RobustChannel, aio_pika.abc.AbstractChannel)


@on_virtual_clock
async def test_a_confirm_that_arrives_with_the_channel_close_completes_the_publish() -> None:
    """Das Ack und das Schliessen kommen im selben Moment (Schliessen zuerst).
    Der Waechter bricht das Kind erst nach dessen eigenem Weiterlaufen ab;
    ein schon bestaetigter Publish ist zugestellt, nicht unbestimmt."""
    transport = FakeTransport(closing=True)
    broker = _broker(transport)
    publish = asyncio.create_task(broker.publish("tasks", b"{}"))
    frame = await transport.next_frame()
    frame.drain()
    await _callbacks_ran()  # das Kind wartet jetzt auf das Confirm

    transport.close_channel(AMQPConnectionError(_LOST))
    transport.confirm_all()

    receipt = await publish
    assert receipt.queue == "tasks"
