"""Schedfire-Dedup-Gate (Plan U7): aus ``test_worker.py`` verschobene Fälle."""

from __future__ import annotations

from unittest.mock import AsyncMock

import structlog

from swarmq.registry import TaskRegistry
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker
from swarmq.worker.gates import GateContext
from swarmq.worker.outcome import Continue, Redelivered, Terminal
from tests.support.messages import make_message
from tests.unit.worker._helpers import (
    AddTask,  # noqa: F401
    _default_payload,
)


class TestSchedfireDedup:
    """Scheduled-fire messages carry an ``x-swarmq-schedfire`` header.

    The worker SETNXs a key derived from that header before entering
    the normal pipeline. Duplicates (two deliveries of the same fire)
    get ack'd and skipped silently.

    Dedup runs BEFORE cancel-check so a duplicate fire never even
    enters the cancel-scan. Both stages run BEFORE the semaphore acquire.
    """

    async def test_no_header_skips_dedup(self, broker, backend, config):

        class NoopTask(Task):
            task_info = TaskInfo(name="noop", queues=["default"])
            async def run(self):
                return "ok"

        registry = TaskRegistry()
        registry.register(NoopTask)
        w = Worker(broker=broker, backend=backend, registry=registry, config=config)

        msg = make_message(
            _default_payload(name="noop", args=[], kwargs={}),
            headers={},  # no x-swarmq-schedfire
        )
        await w._handle_message(msg)

        backend.schedules.mark_fire_completed.assert_not_awaited()
        backend.store_result.assert_awaited_once()

    async def test_first_delivery_runs_task(self, broker, backend, config):
        """Unit 1 two-marker flow: completed=False, inflight-SETNX wins,
        task runs, finally block writes the completed marker with
        long TTL."""

        class NoopTask(Task):
            task_info = TaskInfo(name="noop", queues=["default"])
            async def run(self):
                return "ok"

        registry = TaskRegistry()
        registry.register(NoopTask)
        w = Worker(broker=broker, backend=backend, registry=registry, config=config)

        backend.schedules.is_fire_completed.return_value = False
        backend.schedules.claim_fire.return_value = True  # claim wins
        msg = make_message(
            _default_payload(name="noop", args=[], kwargs={}),
            headers={"x-swarmq-schedfire": "cron_a:1700000000000"},
        )
        await w._handle_message(msg)

        backend.schedules.is_fire_completed.assert_awaited_once_with(
            "cron_a:1700000000000"
        )
        # Inflight TTL = scheduler_inflight_ttl * 1000 = 300 * 1000
        # (no hard_timeout on NoopTask → config value applies).
        inflight_call = backend.schedules.claim_fire.await_args
        assert inflight_call.args[0] == "cron_a:1700000000000"
        assert inflight_call.args[1] == 300_000
        # Task ran
        backend.store_result.assert_awaited_once()
        # finally-block wrote the long-TTL completed marker
        dedup_call = backend.schedules.mark_fire_completed.await_args
        assert dedup_call.args[0] == "cron_a:1700000000000"
        # dedup_ttl * 1000 = 3900 * 1000
        assert dedup_call.args[1] == 3_900_000

    async def test_inflight_ttl_covers_long_hard_timeout(
        self, broker, backend, config,
    ):
        """Härtung R2 P1-6: die fixe 300s-inflight-TTL war der einzige
        Split-Brain-Schutz — lief sie WÄHREND eines langen Laufs ab
        (hard_timeout > 300s ist erlaubt), gewann eine Stale-Leader-
        Kopie das frei gewordene SETNX → Doppelausführung desselben
        Fires. Die TTL muss die maximal erlaubte Laufzeit überdecken:
        max(scheduler_inflight_ttl, hard_timeout + 30s) — dasselbe
        Muster wie compute_lock_ttl_ms (P0-A-Fix 2026-06-02)."""

        class LongTask(Task):
            task_info = TaskInfo(
                name="long", queues=["default"], hard_timeout=600,
            )

            async def run(self):
                return "ok"

        registry = TaskRegistry()
        registry.register(LongTask)
        w = Worker(broker=broker, backend=backend, registry=registry, config=config)

        backend.schedules.is_fire_completed.return_value = False
        backend.schedules.claim_fire.return_value = True
        msg = make_message(
            _default_payload(name="long", args=[], kwargs={}),
            headers={"x-swarmq-schedfire": "long_cron:1700000000000"},
        )
        await w._handle_message(msg)

        inflight_call = backend.schedules.claim_fire.await_args
        # (600 + 30) * 1000 — outlives the watchdog-bounded runtime.
        assert inflight_call.args[1] == 630_000

    async def test_duplicate_when_completed_marker_set(
        self, broker, backend, config
    ):
        """Redelivery after a prior successful fire sees the completed
        marker and ack-skips without claiming in-flight."""

        class NoopTask(Task):
            task_info = TaskInfo(name="noop", queues=["default"])
            async def run(self):
                raise AssertionError("should not run on duplicate")

        registry = TaskRegistry()
        registry.register(NoopTask)
        w = Worker(broker=broker, backend=backend, registry=registry, config=config)

        backend.schedules.is_fire_completed.return_value = True  # prior run
        msg = make_message(
            _default_payload(name="noop", args=[], kwargs={}),
            headers={"x-swarmq-schedfire": "cron_a:1700000000000"},
        )
        await w._handle_message(msg)

        broker.ack.assert_awaited_once_with(msg)
        backend.schedules.claim_fire.assert_not_awaited()
        backend.store_result.assert_not_awaited()

    async def test_duplicate_when_inflight_claim_lost(
        self, broker, backend, config
    ):
        """Another worker is running the same fire right now. Ack+skip
        without running. When that worker completes, the completed
        marker blocks future redeliveries; if it crashes, the in-flight
        TTL expires and the next redelivery can re-run."""

        class NoopTask(Task):
            task_info = TaskInfo(name="noop", queues=["default"])
            async def run(self):
                raise AssertionError("should not run on inflight-claim-lost")

        registry = TaskRegistry()
        registry.register(NoopTask)
        w = Worker(broker=broker, backend=backend, registry=registry, config=config)

        backend.schedules.is_fire_completed.return_value = False
        backend.schedules.claim_fire.return_value = False  # someone else has it
        msg = make_message(
            _default_payload(name="noop", args=[], kwargs={}),
            headers={"x-swarmq-schedfire": "cron_a:1700000000000"},
        )
        await w._handle_message(msg)

        broker.ack.assert_awaited_once_with(msg)
        backend.store_result.assert_not_awaited()
        # No long-TTL write because we didn't own the run.
        backend.schedules.mark_fire_completed.assert_not_awaited()

    async def test_dedup_backend_error_nacks_for_requeue(
        self, broker, backend, config
    ):
        """A Valkey failure during dedup-check must requeue the message
        — we cannot assume either new or duplicate."""

        class NoopTask(Task):
            task_info = TaskInfo(name="noop", queues=["default"])
            async def run(self):
                raise AssertionError("must not run on backend error")

        registry = TaskRegistry()
        registry.register(NoopTask)
        w = Worker(broker=broker, backend=backend, registry=registry, config=config)

        from swarmq.exceptions import BackendConnectionError as BCE
        # First stage in the two-marker flow is completed-check;
        # fail it to exercise the nack path.
        backend.schedules.is_fire_completed.side_effect = BCE("valkey down")
        msg = make_message(
            _default_payload(name="noop", args=[], kwargs={}),
            headers={"x-swarmq-schedfire": "cron_a:1700000000000"},
        )
        await w._handle_message(msg)

        # Zurueckstellung ueber die Delay-Queue statt Requeue an derselben
        # Stelle; die Kopie traegt den Schedfire-Schluessel weiter.
        broker.retry_later.assert_awaited_once()
        assert broker.retry_later.await_args.kwargs["headers"]["x-swarmq-schedfire"] == "cron_a:1700000000000"
        broker.redeliver.assert_not_awaited()

        # Unit 9 / Fork A2: no header mutation on the nack path — AMQP
        # nack-with-requeue does not transmit in-memory header changes
        # back to the broker, so a counter header would be broken by
        # construction. Observability is via the structured
        # schedfire_dedup_backend_error log event (visible in stderr
        # capture above) — not via headers.
        assert "x-swarmq-schedfire-dedup-count" not in msg.headers

    async def test_dedup_backend_error_does_not_touch_existing_headers(
        self, broker, backend, config
    ):
        """Unit 9 guard: pre-existing headers on the message (e.g. an
        operator-injected tracking header) pass through unchanged — we
        don't mutate the message object on the nack path at all."""

        class NoopTask(Task):
            task_info = TaskInfo(name="noop_pass", queues=["default"])
            async def run(self):
                raise AssertionError("must not run on backend error")

        registry = TaskRegistry()
        registry.register(NoopTask)
        w = Worker(broker=broker, backend=backend, registry=registry, config=config)

        from swarmq.exceptions import BackendConnectionError as BCE
        backend.schedules.mark_fire_completed.side_effect = BCE("valkey down")
        original_headers = {
            "x-swarmq-schedfire": "cron_a:1700000000000",
            "x-tracking-id": "abc-123",
        }
        msg = make_message(
            _default_payload(name="noop_pass", args=[], kwargs={}),
            headers=dict(original_headers),
        )
        await w._handle_message(msg)

        # Headers unchanged — no mutation, no counter addition
        assert msg.headers == original_headers

    async def test_worker_start_instantiates_scheduler(
        self, broker, backend, config, monkeypatch
    ):
        """Worker.start() must construct a SchedulerLoop after the
        backend + lock-manager are ready and call scheduler.start()."""
        from swarmq.worker import lifecycle as worker_mod  # U9: SchedulerLoop lebt im Lifecycle-Modul

        class NoopTask(Task):
            task_info = TaskInfo(name="noop_sched", queues=["default"])
            async def run(self):
                return None

        registry = TaskRegistry()
        registry.register(NoopTask)

        # Patch SchedulerLoop so we can observe start() was called
        created = []
        class _FakeSched:
            def __init__(self, **kwargs):
                created.append(kwargs)
                self.started = False
                self.stopped = False
            async def start(self):
                self.started = True
            async def stop(self):
                self.stopped = True

        monkeypatch.setattr(worker_mod, "SchedulerLoop", _FakeSched)
        # Patch broker.consume so start() returns from the consume loop
        # — we shut down immediately after start() assembles the pipeline.
        async def _noop_consume(*a, **kw): return None
        broker.consume = AsyncMock(side_effect=_noop_consume)

        w = Worker(broker=broker, backend=backend, registry=registry, config=config)
        # Fire shutdown before start() blocks on _shutdown_event
        w._shutdown_event.set()

        await w.start()

        assert len(created) == 1
        assert w._scheduler is not None
        assert w._scheduler.started is True
        assert w._scheduler.stopped is True

    async def test_dedup_runs_before_cancel_check(
        self, broker, backend, config
    ):
        """Duplicate schedfire must ack+skip BEFORE cancel-check is reached.
        This guarantees the pipeline-ordering-discipline documented in
        docs/solutions/logic-errors/worker-pipeline-stage-ack-ordering-*."""

        class NoopTask(Task):
            task_info = TaskInfo(name="noop", queues=["default"])
            async def run(self):
                return "ok"

        registry = TaskRegistry()
        registry.register(NoopTask)
        w = Worker(broker=broker, backend=backend, registry=registry, config=config)

        backend.schedules.is_fire_completed.return_value = True  # prior fire
        backend.is_cancelled.side_effect = AssertionError(
            "cancel-check must not run for a duplicate schedfire"
        )
        msg = make_message(
            _default_payload(name="noop", args=[], kwargs={}),
            headers={"x-swarmq-schedfire": "cron_a:1700000000000"},
        )
        await w._handle_message(msg)

        broker.ack.assert_awaited_once_with(msg)


# ---------------------------------------------------------------------------
# R3.5 Unique Tasks — Worker release (Units 10 + 11)
# ---------------------------------------------------------------------------


class TestSchedfireGateOutcomes:
    """Direkte Gate-Tests (Plan U7)."""

    def _ctx(self):
        return GateContext(task_id="abc-123", task_name="add", queue="default")

    async def test_without_header_is_continue(self, worker):
        assert await worker._schedfire_gate.apply(make_message(_default_payload()), self._ctx()) is Continue

    async def test_completed_marker_is_terminal_without_execution(self, worker, broker, backend):
        backend.schedules.is_fire_completed = AsyncMock(return_value=True)
        msg = make_message(_default_payload(), headers={"x-swarmq-schedfire": "s:1"})
        assert await worker._schedfire_gate.apply(msg, self._ctx()) == Terminal.never_release()
        broker.ack.assert_awaited_once_with(msg)
        backend.schedules.claim_fire.assert_not_awaited()

    async def test_won_claim_is_continue_and_records_claim(self, worker, backend):
        msg = make_message(_default_payload(), headers={"x-swarmq-schedfire": "s:1"})
        assert await worker._schedfire_gate.apply(msg, self._ctx()) is Continue
        assert worker._schedfire_inflight_claims[id(msg)] == "s:1"

    async def test_inflight_ttl_grows_with_hard_timeout(self, worker, backend, config):
        worker._registry.register(LongTask)
        msg = make_message(_default_payload(name="long"), headers={"x-swarmq-schedfire": "s:2"})
        ctx = GateContext(task_id="abc-123", task_name="long", queue="default")
        await worker._schedfire_gate.apply(msg, ctx)
        ttl_ms = backend.schedules.claim_fire.await_args.args[1]
        assert ttl_ms == max(config.scheduler_inflight_ttl, 900 + 30) * 1000

    async def test_inflight_claim_backend_error_is_deferred(self, worker, broker, backend):
        from swarmq.exceptions import BackendConnectionError

        backend.schedules.claim_fire = AsyncMock(side_effect=BackendConnectionError("valkey down"))
        msg = make_message(_default_payload(), headers={"x-swarmq-schedfire": "s:1"})
        assert await worker._schedfire_gate.apply(msg, self._ctx()) is Redelivered
        broker.retry_later.assert_awaited_once()
        broker.redeliver.assert_not_awaited()
        assert id(msg) not in worker._schedfire_inflight_claims

    async def test_completed_check_backend_error_is_a_warning_with_error_fields(self, worker, backend, swarmq_logs):
        """Plan 2026-10-06-0831, U9 (R17, KTD11): Redelivery erholt sich selbst."""
        from swarmq.exceptions import BackendConnectionError

        backend.schedules.is_fire_completed = AsyncMock(side_effect=BackendConnectionError("valkey down"))
        msg = make_message(_default_payload(), headers={"x-swarmq-schedfire": "s:1"})
        await worker._schedfire_gate.apply(msg, self._ctx())

        [entry] = [e for e in swarmq_logs if e["event"] == "schedfire_dedup_backend_error"]
        assert entry["logger"] == "swarmq.worker.gates.schedfire"
        assert entry["log_level"] == "warning" and entry["stage"] == "completed_check"
        assert entry["error_type"] == "BackendConnectionError" and entry["error"] == "valkey down"

    async def test_inflight_claim_backend_error_is_a_warning_with_error_fields(self, worker, backend, swarmq_logs):
        from swarmq.exceptions import BackendConnectionError

        backend.schedules.claim_fire = AsyncMock(side_effect=BackendConnectionError("valkey down"))
        msg = make_message(_default_payload(), headers={"x-swarmq-schedfire": "s:1"})
        await worker._schedfire_gate.apply(msg, self._ctx())

        [entry] = [e for e in swarmq_logs if e["event"] == "schedfire_dedup_backend_error"]
        assert entry["log_level"] == "warning" and entry["stage"] == "inflight_claim"
        assert entry["error_type"] == "BackendConnectionError" and entry["error"] == "valkey down"


class LongTask(Task):
    task_info = TaskInfo(name="long", queues=["default"], hard_timeout=900)

    async def run(self) -> None:
        return None


class TestSchedfireGateLogContext:
    """Plan 2026-10-06-0831, U10 (KTD13): frueh endende Pfade hinterlassen
    keinen Task-Kontext und lassen Variablen der App stehen."""

    async def test_completed_duplicate_keeps_the_app_context(self, worker, backend, app_log_context):
        backend.schedules.is_fire_completed = AsyncMock(return_value=True)
        msg = make_message(_default_payload(), headers={"x-swarmq-schedfire": "cron_a:1700000000000"})

        await worker._handle_message(msg)

        assert structlog.contextvars.get_contextvars() == app_log_context
