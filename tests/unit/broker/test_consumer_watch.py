"""Consumer-Wächter (Plan 2026-10-08-1541, U6, KTD3).

Der Wächter vergleicht jeden angemeldeten Consumer-Tag mit ``consumers`` des
aktuellen aiormq-Channels und meldet verlorene Consumer selbst neu an. Die
Fakes bilden genau die Teile von aio-pika nach, die der Wächter benutzt
(``RobustQueue.cancel``/``consume`` und ``channel.get_underlay_channel``);
das Verhalten der echten Broker steht in
``tests/integration/broker/test_consumer_timeout.py``.

Test-Liste:
1. Offener, unveränderter Channel ohne den Tag → sofortiger eigener Cancel,
   danach Neuanmeldung mit neuem Tag über dieselbe Queue.
2. Neuanmeldung erzeugt Log ``consumer_reregistered`` mit Grund und ruft den
   Metrik-Hook.
3. Eigener Cancel scheitert → keine Neuanmeldung, nächster Takt versucht erneut.
4. Channel ersetzt, Tag fehlt im ersten Takt, im zweiten wieder da → keine
   Neuanmeldung.
5. Channel ersetzt, Tag fehlt zwei Takte → Cancel des alten Tags, Neuanmeldung.
6. Derselbe, aber geschlossene Channel zählt wie ein ersetzter.
7. ``get_underlay_channel()`` hängt → Takt bricht nach Timeout ab, Wächter
   lebt weiter.
8. Alle Consumer gleichzeitig weg → alle werden neu angemeldet.
9. Cancel gelingt, Neuanmeldung scheitert → nächster Takt meldet ohne
   erneuten Cancel an.
10. Lücken: wie lange eine Queue ohne aktiven Consumer ist (injizierte Uhr).
11. Selbst abgemeldete Tags sind abrufbar; auf den Cancel eines Tags lässt
    sich warten; ``is_watched`` folgt dem aktuellen Tag (Schnittstelle für U7).
12. Laufender Wächter: ``request_check`` stößt sofort einen Takt an; ein
    Fehler im Takt beendet ihn nicht.
13. Shutdown: nach ``stop`` keine Neuanmeldung, auch nicht aus einem
    laufenden Takt.
14. Broker: ``consume`` meldet beim Wächter an, ``consumer_gaps`` vorher leer.
15. Broker: ``cancel_consume`` stoppt den Wächter und meldet den aktuellen
    (neu angemeldeten) Tag ab.
16. Broker: ``start_consumer_watch`` startet den Wächter mit Takt und Hook.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock

import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.broker.rabbitmq.consumer_watch import ConsumerWatch
from tests.support.consumer_fakes import FakeChannel, FakeQueue, FakeUnderlay

pytestmark = pytest.mark.timeout(5)


def _watched(name: str = "q1", tag: str = "tag-1") -> tuple[ConsumerWatch, FakeQueue, Any]:
    channel = FakeChannel(FakeUnderlay(tag))
    queue = FakeQueue(name, channel)
    callback = object()
    watch = ConsumerWatch()
    watch.add(name, queue, callback, tag)
    return watch, queue, callback


class TestBrokerCancelOnOpenChannel:
    async def test_missing_tag_on_unchanged_channel_is_cancelled_and_reregistered(self):
        watch, queue, callback = _watched()
        await watch.tick()
        assert queue.cancelled == [] and queue.consumed == []

        queue.channel.underlay.consumers.pop("tag-1")  # basic.cancel vom Broker
        await watch.tick()

        assert queue.cancelled == ["tag-1"]
        assert queue.consumed == [callback]
        assert watch.tags() == [("q1", "q1-new-1")]

    async def test_reregistration_is_logged_with_reason_and_counted(self, swarmq_logs):
        watch, queue, _ = _watched()
        counted: list[tuple[str, str]] = []
        watch.on_reregistered = lambda queue_name, reason: counted.append((queue_name, reason))
        await watch.tick()
        queue.channel.underlay.consumers.pop("tag-1")

        await watch.tick()

        [entry] = [e for e in swarmq_logs if e["event"] == "consumer_reregistered"]
        assert entry["log_level"] == "warning"
        assert entry["logger"] == "swarmq.broker.rabbitmq.consumer_watch"
        assert entry["queue"] == "q1"
        assert entry["reason"] == "broker_cancel"
        assert entry["old_consumer_tag"] == "tag-1"
        assert entry["consumer_tag"] == "q1-new-1"
        assert counted == [("q1", "broker_cancel")]

    async def test_failed_own_cancel_skips_reregistration_and_retries_next_tick(self, swarmq_logs):
        watch, queue, callback = _watched()
        await watch.tick()
        queue.channel.underlay.consumers.pop("tag-1")
        queue.cancel_error = RuntimeError("cancel timed out")

        await watch.tick()

        assert queue.consumed == []
        assert watch.tags() == [("q1", "tag-1")]
        [failed] = [e for e in swarmq_logs if e["event"] == "consumer_cancel_failed"]
        assert failed["queue"] == "q1" and failed["error_type"] == "RuntimeError"

        queue.cancel_error = None
        await watch.tick()

        assert queue.cancelled == ["tag-1"]
        assert queue.consumed == [callback]


class TestReplacedChannel:
    async def test_tag_restored_on_replaced_channel_within_two_ticks_is_left_alone(self):
        watch, queue, _ = _watched()
        await watch.tick()
        queue.channel.underlay = FakeUnderlay()  # Channel-Close, Restore läuft noch

        await watch.tick()
        queue.channel.underlay.consumers["tag-1"] = object()  # Restore mit gleichem Tag
        await watch.tick()

        assert queue.cancelled == [] and queue.consumed == []
        assert watch.tags() == [("q1", "tag-1")]

    async def test_tag_missing_two_ticks_on_replaced_channel_is_reregistered(self, swarmq_logs):
        watch, queue, callback = _watched()
        await watch.tick()
        queue.channel.underlay = FakeUnderlay()

        await watch.tick()
        assert queue.cancelled == [] and queue.consumed == []
        await watch.tick()

        # Der alte Tag muss auch aus ``RobustQueue._consumers``, sonst legt der
        # nächste Restore ihn neben dem neuen wieder an.
        assert queue.cancelled == ["tag-1"]
        assert queue.consumed == [callback]
        assert watch.tags() == [("q1", "q1-new-1")]
        [entry] = [e for e in swarmq_logs if e["event"] == "consumer_reregistered"]
        assert entry["reason"] == "channel_replaced"

    async def test_closed_channel_counts_as_replaced(self):
        watch, queue, _ = _watched()
        await watch.tick()
        queue.channel.underlay.consumers.pop("tag-1")
        queue.channel.underlay.is_closed = True  # Restore hat noch nicht getauscht

        await watch.tick()

        assert queue.cancelled == [] and queue.consumed == []


class HangingChannel(FakeChannel):
    """``get_underlay_channel`` wartet auf ``connection.ready()`` — hier ewig."""

    def __init__(self, underlay: FakeUnderlay) -> None:
        super().__init__(underlay)
        self.hanging = True

    async def get_underlay_channel(self) -> FakeUnderlay:
        if self.hanging:
            await asyncio.Event().wait()
        return self.underlay


class TestHangingChannel:
    async def test_tick_gives_up_after_timeout_and_the_next_tick_works(self, swarmq_logs):
        channel = HangingChannel(FakeUnderlay("tag-1"))
        queue = FakeQueue("q1", channel)
        watch = ConsumerWatch(timeout_s=0.01)
        watch.add("q1", queue, object(), "tag-1")

        await watch.tick()

        [entry] = [e for e in swarmq_logs if e["event"] == "consumer_watch_channel_timeout"]
        assert entry["queue"] == "q1" and entry["timeout_s"] == 0.01
        channel.hanging = False
        await watch.tick()
        channel.underlay.consumers.pop("tag-1")
        await watch.tick()
        assert queue.cancelled == ["tag-1"]


class TestAllConsumersGone:
    async def test_every_lost_consumer_on_the_channel_is_reregistered(self):
        """Charakterisierung (ohne eigenes Rot): folgt aus der Prüfung je Tag."""
        channel = FakeChannel(FakeUnderlay("t-high", "t-normal", "t-low"))
        queues = [FakeQueue(name, channel) for name in ("q.high", "q", "q.low")]
        watch = ConsumerWatch()
        for queue, tag in zip(queues, ("t-high", "t-normal", "t-low"), strict=True):
            watch.add(queue.name, queue, object(), tag)
        await watch.tick()
        channel.underlay.consumers.clear()

        await watch.tick()

        assert [q.cancelled for q in queues] == [["t-high"], ["t-normal"], ["t-low"]]
        assert watch.tags() == [("q.high", "q.high-new-1"), ("q", "q-new-1"), ("q.low", "q.low-new-1")]


class TestFailedConsume:
    async def test_failed_consume_after_cancel_is_retried_without_cancelling_again(self, swarmq_logs):
        watch, queue, callback = _watched()
        await watch.tick()
        queue.channel.underlay.consumers.pop("tag-1")
        queue.consume_error = RuntimeError("consume timed out")

        await watch.tick()

        [failed] = [e for e in swarmq_logs if e["event"] == "consumer_reregister_failed"]
        assert failed["queue"] == "q1" and failed["error_type"] == "RuntimeError"
        queue.consume_error = None
        await watch.tick()

        assert queue.cancelled == ["tag-1"]
        assert queue.consumed == [callback]
        assert watch.tags() == [("q1", "q1-new-1")]


class TestGaps:
    async def test_gap_grows_while_the_consumer_is_missing_and_closes_on_return(self):
        now = [100.0]
        channel = FakeChannel(FakeUnderlay("tag-1"))
        queue = FakeQueue("q1", channel)
        watch = ConsumerWatch(clock=lambda: now[0])
        watch.add("q1", queue, object(), "tag-1")
        assert watch.gaps() == {"q1": 0.0}

        await watch.tick()
        channel.underlay = FakeUnderlay()
        now[0] = 105.0
        await watch.tick()  # erstmals vermisst
        now[0] = 108.0
        assert watch.gaps() == {"q1": 3.0}

        channel.underlay.consumers["tag-1"] = object()
        await watch.tick()
        assert watch.gaps() == {"q1": 0.0}

    async def test_gap_stays_open_until_reregistration_succeeds(self):
        """Charakterisierung (ohne eigenes Rot) der Lücke bei scheiterndem Cancel."""
        now = [0.0]
        channel = FakeChannel(FakeUnderlay("tag-1"))
        queue = FakeQueue("q1", channel)
        watch = ConsumerWatch(clock=lambda: now[0])
        watch.add("q1", queue, object(), "tag-1")
        await watch.tick()
        channel.underlay.consumers.pop("tag-1")
        queue.cancel_error = RuntimeError("boom")

        await watch.tick()
        now[0] = 40.0
        assert watch.gaps() == {"q1": 40.0}

        queue.cancel_error = None
        await watch.tick()
        assert watch.gaps() == {"q1": 0.0}


class TestSelfCancelledTags:
    """Schnittstelle für die Settle-Regeln (U7, KTD4)."""

    async def test_own_cancel_is_recorded(self):
        watch, queue, _ = _watched()
        await watch.tick()
        assert watch.self_cancelled_tags == frozenset()
        queue.channel.underlay.consumers.pop("tag-1")

        await watch.tick()

        assert watch.self_cancelled_tags == frozenset({"tag-1"})

    async def test_waiting_for_the_cancel_of_a_tag(self):
        watch, queue, _ = _watched()
        await watch.tick()
        queue.channel.underlay.consumers.pop("tag-1")
        waiter = asyncio.create_task(watch.wait_cancelled("tag-1", timeout=1.0))
        await asyncio.sleep(0)  # Waiter wartet bereits

        await watch.tick()

        assert await waiter is True
        assert await watch.wait_cancelled("tag-1", timeout=1.0) is True
        assert await watch.wait_cancelled("never", timeout=0.01) is False

    async def test_is_watched_follows_the_current_tag(self):
        watch, queue, _ = _watched()
        assert watch.is_watched("tag-1") is True
        await watch.tick()
        queue.channel.underlay.consumers.pop("tag-1")

        await watch.tick()

        assert watch.is_watched("tag-1") is False
        assert watch.is_watched("q1-new-1") is True


class TestRunningWatch:
    async def test_request_check_runs_a_tick_at_once(self):
        watch, queue, _ = _watched()
        await watch.tick()
        watch.start(interval_s=3600.0)
        try:
            queue.channel.underlay.consumers.pop("tag-1")

            watch.request_check()

            assert await watch.wait_cancelled("tag-1", timeout=1.0) is True
        finally:
            await watch.stop()

    async def test_a_failing_tick_does_not_end_the_watch(self, swarmq_logs):
        class ExplodingOnce(FakeChannel):
            def __init__(self, underlay: FakeUnderlay) -> None:
                super().__init__(underlay)
                self.exploded = asyncio.Event()

            async def get_underlay_channel(self) -> FakeUnderlay:
                if not self.exploded.is_set():
                    self.exploded.set()
                    raise RuntimeError("channel invalid state")
                return self.underlay

        channel = ExplodingOnce(FakeUnderlay("tag-1"))
        queue = FakeQueue("q1", channel)
        watch = ConsumerWatch()
        watch.add("q1", queue, object(), "tag-1")
        watch.start(interval_s=3600.0)
        try:
            watch.request_check()
            await asyncio.wait_for(channel.exploded.wait(), 1.0)
            await watch.tick()  # Referenz-Channel festhalten
            channel.underlay.consumers.pop("tag-1")

            watch.request_check()

            assert await watch.wait_cancelled("tag-1", timeout=1.0) is True
        finally:
            await watch.stop()
        [failed] = [e for e in swarmq_logs if e["event"] == "consumer_watch_tick_failed"]
        assert failed["error_type"] == "RuntimeError"


class TestShutdown:
    async def test_stop_lets_a_running_own_cancel_finish_but_skips_the_reregistration(self):
        watch, queue, _ = _watched()
        await watch.tick()
        entered, release = asyncio.Event(), asyncio.Event()
        plain_cancel = queue.cancel

        async def slow_cancel(tag: str, timeout: float | None = None) -> None:
            entered.set()
            await release.wait()
            await plain_cancel(tag, timeout)

        queue.cancel = slow_cancel  # type: ignore[method-assign]
        watch.start(interval_s=3600.0)
        queue.channel.underlay.consumers.pop("tag-1")
        watch.request_check()
        await asyncio.wait_for(entered.wait(), 1.0)

        stopping = asyncio.create_task(watch.stop())
        await asyncio.sleep(0)
        release.set()
        await stopping

        # Der Cancel ist am Broker durch und bekannt — kein halber Zustand.
        assert watch.self_cancelled_tags == frozenset({"tag-1"})
        assert queue.consumed == []

    async def test_watch_can_be_started_again_after_stop(self):
        watch, queue, _ = _watched()
        await watch.tick()
        watch.start(interval_s=3600.0)
        await watch.stop()

        watch.start(interval_s=3600.0)
        try:
            queue.channel.underlay.consumers.pop("tag-1")
            watch.request_check()
            assert await watch.wait_cancelled("tag-1", timeout=1.0) is True
        finally:
            await watch.stop()
        assert len(queue.consumed) == 1


class TestHangingRpc:
    async def test_hanging_own_cancel_gives_up_after_timeout(self, swarmq_logs):
        channel = FakeChannel(FakeUnderlay("tag-1"))
        queue = FakeQueue("q1", channel)
        watch = ConsumerWatch(timeout_s=0.01)
        watch.add("q1", queue, object(), "tag-1")
        await watch.tick()

        async def hanging_cancel(tag: str, timeout: float | None = None) -> None:
            await asyncio.Event().wait()

        queue.cancel = hanging_cancel  # type: ignore[method-assign]
        channel.underlay.consumers.pop("tag-1")

        await watch.tick()

        [failed] = [e for e in swarmq_logs if e["event"] == "consumer_cancel_failed"]
        assert failed["error_type"] == "TimeoutError"
        assert queue.consumed == []

    async def test_hanging_reregistration_gives_up_after_timeout(self, swarmq_logs):
        channel = FakeChannel(FakeUnderlay("tag-1"))
        queue = FakeQueue("q1", channel)
        watch = ConsumerWatch(timeout_s=0.01)
        watch.add("q1", queue, object(), "tag-1")
        await watch.tick()

        async def hanging_consume(callback: Any, timeout: float | None = None) -> str:
            await asyncio.Event().wait()
            return "never"

        queue.consume = hanging_consume  # type: ignore[method-assign]
        channel.underlay.consumers.pop("tag-1")

        await watch.tick()

        [failed] = [e for e in swarmq_logs if e["event"] == "consumer_reregister_failed"]
        assert failed["error_type"] == "TimeoutError"


def _broker_with(*queues: FakeQueue) -> RabbitMQBroker:
    broker = RabbitMQBroker("amqp://guest:guest@localhost/")
    broker._connection = AsyncMock()
    broker._consume_channel = AsyncMock(is_closed=False)
    broker._consume_channel.get_queue.side_effect = list(queues)
    return broker


class TestBrokerWiring:
    async def test_consume_registers_the_consumer_with_the_watch(self):
        queue = FakeQueue("q1", FakeChannel(FakeUnderlay()))
        broker = _broker_with(queue)
        assert broker.consumer_gaps() == {}

        await broker.consume("q1", AsyncMock(), concurrency=4)

        assert broker.consumer_gaps() == {"q1": 0.0}

    async def test_cancel_consume_cancels_the_reregistered_tag_and_forgets_the_queue(self):
        queue = FakeQueue("q1", FakeChannel(FakeUnderlay()))
        broker = _broker_with(queue)
        await broker.consume("q1", AsyncMock(), concurrency=4)
        await broker.consumer_watch.tick()
        queue.channel.underlay.consumers.clear()
        await broker.consumer_watch.tick()

        await broker.cancel_consume()

        assert queue.cancelled == ["q1-new-1", "q1-new-2"]
        assert broker.consumer_gaps() == {}
        assert broker.consumer_watch.self_cancelled_tags == frozenset({"q1-new-1", "q1-new-2"})

    async def test_close_stops_the_watch(self):
        queue = FakeQueue("q1", FakeChannel(FakeUnderlay()))
        broker = _broker_with(queue)
        before = asyncio.all_tasks()
        await broker.consume("q1", AsyncMock(), concurrency=4)
        broker.start_consumer_watch(interval_s=3600.0)

        await broker.close()

        assert asyncio.all_tasks() == before

    async def test_started_watch_reports_reregistrations_to_the_hook(self):
        queue = FakeQueue("q1", FakeChannel(FakeUnderlay()))
        broker = _broker_with(queue)
        await broker.consume("q1", AsyncMock(), concurrency=4)
        await broker.consumer_watch.tick()
        counted: list[tuple[str, str]] = []
        broker.start_consumer_watch(interval_s=3600.0, on_reregistered=lambda q, r: counted.append((q, r)))
        try:
            queue.channel.underlay.consumers.clear()
            broker.consumer_watch.request_check()
            assert await broker.consumer_watch.wait_cancelled("q1-new-1", timeout=1.0)
        finally:
            await broker.close()
        assert counted == [("q1", "broker_cancel")]
