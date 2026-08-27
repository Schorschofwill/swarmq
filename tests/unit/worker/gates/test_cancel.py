"""Pre-Start-Cancel-Gate (Plan U7): aus ``test_worker.py`` verschobene Fälle."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

import structlog

from swarmq.exceptions import BackendConnectionError
from swarmq.registry import TaskRegistry
from swarmq.serialization import decode_result
from swarmq.signals import Signal
from swarmq.task import DeliveryMode, Task, TaskInfo
from swarmq.worker import Worker
from swarmq.worker.gates import GateContext
from swarmq.worker.outcome import Continue, Redelivered, Terminal
from tests.support.messages import make_message
from tests.unit.worker._helpers import (
    AddTask,
    _default_payload,
)


class TestWorkerPreStartCancelCheck:
    """Pre-start cancel check: runs BEFORE semaphore/rate-limit/lock.

    Unit-level scenarios with mocked backend. Verifies control flow,
    ordering, DeliveryMode branching, signal dispatch, structlog cleanup,
    and error-path behaviour. Integration and E2E live in
    tests/integration/test_worker_cancellation.py and
    tests/e2e/test_cancellation.py.
    """

    async def test_cancel_flag_set_skips_run(self, worker, broker, backend):
        """When is_cancelled=True, task.run() must never be invoked."""
        backend.is_cancelled = AsyncMock(return_value=True)

        msg = make_message(_default_payload())
        with patch.object(AddTask, "run", new=AsyncMock()) as mock_run:
            await worker._handle_message(msg)

        mock_run.assert_not_awaited()

    async def test_cancel_flag_set_stores_is_cancelled_result(
        self, worker, broker, backend,
    ):
        """AT_LEAST_ONCE cancel → result_data has is_cancelled=True."""
        backend.is_cancelled = AsyncMock(return_value=True)
        msg = make_message(_default_payload())

        await worker._handle_message(msg)

        backend.store_result.assert_awaited_once()
        stored_bytes = backend.store_result.await_args.args[1]
        decoded = decode_result(stored_bytes)
        assert decoded["is_cancelled"] is True
        assert decoded["is_error"] is False
        assert decoded["result"] is None
        assert decoded["task"]["id"] == "abc-123"
        assert decoded["task"]["name"] == "add"

    async def test_cancel_flag_set_clears_flag_after_store(
        self, worker, broker, backend,
    ):
        """Flag is cleared via backend.clear_cancel_flag."""
        backend.is_cancelled = AsyncMock(return_value=True)
        msg = make_message(_default_payload())

        await worker._handle_message(msg)

        backend.clear_cancel_flag.assert_awaited_once_with("abc-123", op_timeout=5.0)

    async def test_cancel_flag_set_acks_message(self, worker, broker, backend):
        backend.is_cancelled = AsyncMock(return_value=True)
        msg = make_message(_default_payload())

        await worker._handle_message(msg)

        broker.ack.assert_awaited_once_with(msg)
        broker.nack.assert_not_awaited()

    async def test_cancel_flag_set_dispatches_task_cancelled_signal(
        self, worker, broker, backend,
    ):
        """Signal.TASK_CANCELLED is emitted with task_id + task_name."""
        backend.is_cancelled = AsyncMock(return_value=True)
        captured: list[tuple] = []

        from swarmq.signals import Signal

        async def handler(**kwargs):
            captured.append(kwargs)

        worker._signal_dispatcher.connect(Signal.TASK_CANCELLED, handler)
        msg = make_message(_default_payload())

        await worker._handle_message(msg)

        assert len(captured) == 1
        # ce-review COR-M-01 / PR fix: queue label now included so
        # metrics collectors can attribute cancellations per queue.
        assert captured[0] == {
            "task_id": "abc-123", "task_name": "add", "queue": "default",
        }

    async def test_cancel_flag_set_does_not_acquire_semaphore(
        self, worker, broker, backend, config,
    ):
        """Pipeline-position invariant: semaphore must NOT be acquired.

        Observable assertion instead of probing ``_semaphore._value``
        (REV-P2-12): try to exhaust the semaphore AFTER the cancelled
        message is handled. If the prior message had consumed a slot
        and failed to release, this would deadlock.
        """
        backend.is_cancelled = AsyncMock(return_value=True)

        msg = make_message(_default_payload())
        await worker._handle_message(msg)

        # If the cancel branch wrongly acquired-without-release, the
        # semaphore has concurrency-1 slots left. Acquiring "concurrency"
        # times without release would hang — wait_for forces the
        # deadlock to fail fast.
        for _ in range(config.concurrency):
            await asyncio.wait_for(worker._semaphore.acquire(), timeout=0.5)
        for _ in range(config.concurrency):
            worker._semaphore.release()

    async def test_cancel_flag_set_clears_structlog_context(
        self, worker, broker, backend,
    ):
        """Contextvars must be cleared after the cancel branch."""
        backend.is_cancelled = AsyncMock(return_value=True)
        msg = make_message(_default_payload())

        # Sanity: context empty before
        assert structlog.contextvars.get_contextvars() == {}

        await worker._handle_message(msg)

        # Context must be empty after — otherwise the next handler on
        # the same event loop would leak task_id from this one.
        assert structlog.contextvars.get_contextvars() == {}

    async def test_cancel_flag_not_set_proceeds_to_normal_flow(
        self, worker, broker, backend,
    ):
        """Regression: backend.is_cancelled returns False → full pipeline."""
        backend.is_cancelled = AsyncMock(return_value=False)
        msg = make_message(_default_payload())

        await worker._handle_message(msg)

        # Task ran: store_result was called with a SUCCESS (not cancelled) result.
        backend.store_result.assert_awaited_once()
        stored_bytes = backend.store_result.await_args.args[1]
        decoded = decode_result(stored_bytes)
        assert decoded.get("is_cancelled", False) is False
        assert decoded["result"] == 3  # 1 + 2

    async def test_cancel_faf_with_result_ttl_zero_skips_store_result(
        self, broker, backend, config,
    ):
        """FAF + result_ttl=0 (default): cancel emits signal, no store_result."""
        registry = TaskRegistry()

        class FafTask(Task):
            task_info = TaskInfo(
                name="faf", queues=["default"],
                delivery=DeliveryMode.FIRE_AND_FORGET,
                # result_ttl not set → defaults to None → becomes 0 for FAF
            )
            async def run(self):  # pragma: no cover - should not be called
                pass

        registry.register(FafTask)
        w = Worker(broker=broker, backend=backend, registry=registry, config=config)

        backend.is_cancelled = AsyncMock(return_value=True)
        msg = make_message(_default_payload(name="faf", args=[], kwargs={}))

        await w._handle_message(msg)

        backend.store_result.assert_not_awaited()
        backend.clear_cancel_flag.assert_awaited_once_with("abc-123", op_timeout=5.0)
        broker.ack.assert_awaited_once_with(msg)

    async def test_cancel_faf_with_explicit_result_ttl_stores_result(
        self, broker, backend, config,
    ):
        """FAF + result_ttl>0 (opt-in): store_result IS called."""
        registry = TaskRegistry()

        class FafTaskWithTtl(Task):
            task_info = TaskInfo(
                name="faf_ttl", queues=["default"],
                delivery=DeliveryMode.FIRE_AND_FORGET,
                result_ttl=60,
            )
            async def run(self):  # pragma: no cover
                pass

        registry.register(FafTaskWithTtl)
        w = Worker(broker=broker, backend=backend, registry=registry, config=config)

        backend.is_cancelled = AsyncMock(return_value=True)
        msg = make_message(_default_payload(name="faf_ttl", args=[], kwargs={}))

        await w._handle_message(msg)

        backend.store_result.assert_awaited_once()
        backend.clear_cancel_flag.assert_awaited_once_with("abc-123", op_timeout=5.0)

    async def test_cancel_unknown_task_falls_back_to_at_least_once(
        self, broker, backend, config,
    ):
        """Unknown task + cancel flag: store_result is called (ALO fallback)."""
        registry = TaskRegistry()  # empty
        w = Worker(broker=broker, backend=backend, registry=registry, config=config)

        backend.is_cancelled = AsyncMock(return_value=True)
        msg = make_message(_default_payload(name="unknown_task"))

        await w._handle_message(msg)

        # ALO fallback: store_result gets called with config.result_ttl.
        backend.store_result.assert_awaited_once()
        broker.ack.assert_awaited_once_with(msg)

    async def test_cancel_is_cancelled_raises_nacks_with_requeue(
        self, worker, broker, backend,
    ):
        """BackendConnectionError during is_cancelled → nack(requeue=True)."""
        backend.is_cancelled = AsyncMock(
            side_effect=BackendConnectionError("valkey down")
        )
        msg = make_message(_default_payload())

        await worker._handle_message(msg)

        broker.nack.assert_awaited_once_with(msg, requeue=True)
        broker.ack.assert_not_awaited()
        backend.store_result.assert_not_awaited()
        # Contextvars cleared even on error path.
        assert structlog.contextvars.get_contextvars() == {}

    async def test_cancel_store_result_failure_nacks_and_keeps_flag(
        self, worker, broker, backend,
    ):
        """store_result wirft → nack(requeue=True), flag stays for retry."""
        backend.is_cancelled = AsyncMock(return_value=True)
        backend.store_result = AsyncMock(
            side_effect=BackendConnectionError("write failed")
        )
        msg = make_message(_default_payload())

        await worker._handle_message(msg)

        broker.nack.assert_awaited_once_with(msg, requeue=True)
        broker.ack.assert_not_awaited()
        # Flag was NOT cleared — redelivery will re-trigger the cancel branch.
        backend.clear_cancel_flag.assert_not_awaited()

    async def test_cancel_publish_notification_failure_still_acks(
        self, worker, broker, backend,
    ):
        """publish_notification wirft → log+continue, message is still acked."""
        backend.is_cancelled = AsyncMock(return_value=True)
        backend.publish_notification = AsyncMock(
            side_effect=Exception("pubsub down")
        )
        msg = make_message(_default_payload())

        await worker._handle_message(msg)

        broker.ack.assert_awaited_once_with(msg)
        backend.clear_cancel_flag.assert_awaited_once()

    async def test_cancel_does_not_invoke_rate_limiter(
        self, worker, broker, backend,
    ):
        """Pipeline-position: rate-limiter must not be called."""
        backend.is_cancelled = AsyncMock(return_value=True)
        # Replace rate-limiter with a mock so any invocation is observable.
        worker._rate_limiter = AsyncMock()
        worker._rate_limiter.check = AsyncMock(return_value=(True, 100, 0))

        # Add rate-limit to TaskInfo so the stage would normally fire.
        class RateLimitedTask(Task):
            task_info = TaskInfo(
                name="rate_add", queues=["default"], rate_limit="10/s"
            )
            async def run(self, a: int, b: int) -> int:  # pragma: no cover
                return a + b

        registry = TaskRegistry()
        registry.register(RateLimitedTask)
        w = Worker(broker=broker, backend=backend, registry=registry, config=worker._config)
        w._rate_limiter = worker._rate_limiter

        msg = make_message(_default_payload(name="rate_add"))
        await w._handle_message(msg)

        w._rate_limiter.check.assert_not_awaited()


# ===========================================================================
# Phase 3b.3 / Unit 7 — Cancel-Monitor Coroutine (isolated)
# ===========================================================================



class TestWorkflowCancelPickup:
    """``handle.cancel()`` setzte nur das Workflow-Flag; der Worker-
    Pickup prüfte ausschließlich das per-Task-Flag. Bereits eingereihte
    group-/chord-Member liefen nach dem Cancel ungebremst durch. Der
    Pickup muss bei gesetztem Workflow-Header auch das Workflow-Flag
    prüfen und den Step als cancelled abschließen."""

    WF_ID = "0190b6f2-1234-7abc-8def-0123456789ab"

    def _wf_message(self):
        return make_message(
            _default_payload(name="add", args=[1, 2], kwargs={}),
            headers={"x-swarmq-workflow-id": self.WF_ID},
        )

    async def test_cancelled_workflow_step_does_not_run(
        self, worker, broker, backend,
    ):
        backend.is_cancelled = AsyncMock(return_value=False)
        backend.is_workflow_cancelled = AsyncMock(return_value=True)
        # No prior result for this step → the cancel marker is written.
        backend.get_result = AsyncMock(return_value=None)

        events: list[dict] = []

        @worker._signal_dispatcher.on_signal(Signal.TASK_CANCELLED)
        async def _c(**kw):
            events.append(kw)

        msg = self._wf_message()
        await worker._handle_message(msg)

        backend.is_workflow_cancelled.assert_awaited_once()
        assert backend.is_workflow_cancelled.await_args.args[0] == self.WF_ID
        # The step never executed: no success result was stored — the
        # only store is the cancelled-result marker.
        stored = backend.store_result.await_args
        from swarmq.serialization import decode_result
        result = decode_result(stored.args[1])
        assert result["is_cancelled"] is True
        broker.ack.assert_awaited_once_with(msg)
        assert len(events) == 1

    async def test_live_workflow_step_runs_normally(
        self, worker, broker, backend,
    ):
        backend.is_cancelled = AsyncMock(return_value=False)
        backend.is_workflow_cancelled = AsyncMock(return_value=False)

        msg = self._wf_message()
        await worker._handle_message(msg)

        backend.is_workflow_cancelled.assert_awaited_once()
        # Task ran: a success result was stored.
        from swarmq.serialization import decode_result
        result = decode_result(backend.store_result.await_args.args[1])
        assert result.get("is_cancelled") is False
        assert result.get("result") == 3

    async def test_non_workflow_message_skips_workflow_check(
        self, worker, broker, backend,
    ):
        """Kein Workflow-Header → kein zusätzlicher Valkey-Roundtrip."""
        backend.is_workflow_cancelled = AsyncMock(return_value=False)
        msg = make_message(_default_payload(name="add", args=[1, 2], kwargs={}))
        await worker._handle_message(msg)
        backend.is_workflow_cancelled.assert_not_awaited()

    async def test_workflow_check_backend_error_nacks_requeue(
        self, worker, broker, backend,
    ):
        from swarmq.exceptions import BackendConnectionError

        backend.is_cancelled = AsyncMock(return_value=False)
        backend.is_workflow_cancelled = AsyncMock(
            side_effect=BackendConnectionError("valkey down"),
        )

        msg = self._wf_message()
        await worker._handle_message(msg)

        broker.nack.assert_awaited_once_with(msg, requeue=True)
        backend.store_result.assert_not_awaited()


# ---------------------------------------------------------------------------
# Härtung R2 P2: Body-Size-Cap vor dem JSON-Decode
# ---------------------------------------------------------------------------




class TestWorkflowCancelDoesNotOverwriteResult:
    """Review R2: Redelivery eines bereits ERFOLGREICH gelaufenen Steps
    (Result gespeichert, Ack verloren) nach Workflow-Cancel darf das
    Original-Result nicht mit einem Cancel-Marker überschreiben."""

    WF_ID = "0190b6f2-1234-7abc-8def-0123456789ab"

    async def test_existing_result_preserved(self, worker, broker, backend):
        backend.is_cancelled = AsyncMock(return_value=False)
        backend.is_workflow_cancelled = AsyncMock(return_value=True)
        # The step already completed once: a success result exists.
        backend.get_result = AsyncMock(return_value=b"original-success")

        msg = make_message(
            _default_payload(name="add", args=[1, 2], kwargs={}),
            headers={"x-swarmq-workflow-id": self.WF_ID},
        )
        await worker._handle_message(msg)

        # No overwrite — and the redelivery is still settled (acked).
        backend.store_result.assert_not_awaited()
        broker.ack.assert_awaited_once_with(msg)

    async def test_per_task_cancel_keeps_unconditional_write(
        self, worker, broker, backend,
    ):
        """Bestandssemantik des per-Task-Cancels unverändert: dort wird
        ohne Existenz-Check geschrieben (Flag existiert nur pre-/mid-run)."""
        backend.is_cancelled = AsyncMock(return_value=True)
        backend.get_result = AsyncMock(return_value=b"should-not-be-read")

        msg = make_message(_default_payload(name="add", args=[1, 2], kwargs={}))
        await worker._handle_message(msg)

        backend.store_result.assert_awaited_once()
        backend.get_result.assert_not_awaited()
        broker.ack.assert_awaited_once_with(msg)


class TestCancelGateOutcomes:
    """Direkte Gate-Tests (Plan U7)."""

    def _ctx(self):
        return GateContext(task_id="abc-123", task_name="add", queue="default")

    async def test_no_flag_is_continue(self, worker, broker):
        assert await worker._cancel_gate.apply(make_message(_default_payload()), self._ctx()) is Continue
        broker.ack.assert_not_awaited()

    async def test_flag_set_is_terminal_already_released(self, worker, broker, backend):
        backend.is_cancelled = AsyncMock(return_value=True)
        msg = make_message(_default_payload())
        assert await worker._cancel_gate.apply(msg, self._ctx()) == Terminal.already_released()
        backend.clear_cancel_flag.assert_awaited_once()
        broker.ack.assert_awaited_once_with(msg)

    async def test_backend_error_is_redelivered(self, worker, broker, backend):
        backend.is_cancelled = AsyncMock(side_effect=BackendConnectionError("down"))
        msg = make_message(_default_payload())
        assert await worker._cancel_gate.apply(msg, self._ctx()) is Redelivered
        broker.nack.assert_awaited_once_with(msg, requeue=True)
