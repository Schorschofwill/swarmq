"""Lock-Gate (Plan U7): aus ``test_worker.py`` verschobene Fälle plus direkte Outcome-Tests."""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from swarmq.contracts.coordination import LockToken
from swarmq.exceptions import BackendConnectionError, BrokerConnectionError
from swarmq.registry import TaskRegistry
from swarmq.signals import Signal
from swarmq.task import DeliveryMode, Task, TaskInfo
from swarmq.worker import Worker
from swarmq.worker.gates import GateContext
from swarmq.worker.gates.lock import LOCK_CONFLICT_DLQ_CAP, render_lock_name
from swarmq.worker.outcome import Continue, Redelivered
from tests.support.messages import make_message
from tests.unit.worker._helpers import (
    AddTask,
    _default_payload,
)


class LockedTask(Task):
    task_info = TaskInfo(
        name="locked", queues=["default"], lock="resource", hard_timeout=60
    )


class FireAndForgetLockedTask(Task):
    task_info = TaskInfo(
        name="ff_locked",
        queues=["default"],
        delivery=DeliveryMode.FIRE_AND_FORGET,
        lock="resource",
        hard_timeout=60,
    )


class TestWorkerLocking:
    """Unit-level coverage of the lock branch in _handle_message.

    A real Worker is built but ``_lock_manager`` is replaced with a
    mock so no Valkey is needed.
    """

    @pytest.mark.asyncio
    async def test_acquired_path_runs_the_task(self, broker, backend, config):
        reg = TaskRegistry()
        reg.register(LockedTask)

        w = Worker(broker=broker, backend=backend, registry=reg, config=config)
        w._lock_manager = MagicMock()
        w._lock_manager.acquire = AsyncMock(
            side_effect=lambda name, *, owner, ttl_ms, max_concurrency: LockToken(
                name=name, owner=owner, max_concurrency=max_concurrency
            )
        )
        w._lock_manager.release = AsyncMock(return_value=True)

        msg = make_message(_default_payload(name="locked", args=[], kwargs={}))

        async def _run(self):
            return None

        with patch.object(LockedTask, "run", _run):
            await w._handle_message(msg)

        w._lock_manager.acquire.assert_awaited_once()
        w._lock_manager.release.assert_awaited_once()
        broker.retry_later.assert_not_called()
        broker.ack.assert_awaited_once_with(msg)

    @pytest.mark.asyncio
    async def test_conflict_path_requeues_with_delay_and_skips_execution(
        self, broker, backend, config
    ):
        from swarmq.signals import SignalDispatcher

        dispatcher = SignalDispatcher()
        seen: list[dict] = []

        @dispatcher.on_signal(Signal.TASK_LOCKED)
        async def _handler(**kwargs: object) -> None:
            seen.append(dict(kwargs))

        reg = TaskRegistry()
        reg.register(LockedTask)

        w = Worker(
            broker=broker, backend=backend, registry=reg, config=config,
            signal_dispatcher=dispatcher,
        )
        w._lock_manager = MagicMock()
        w._lock_manager.acquire = AsyncMock(return_value=None)
        w._lock_manager.release = AsyncMock(return_value=True)

        ran = False

        async def _run(self):
            nonlocal ran
            ran = True

        with patch.object(LockedTask, "run", _run):
            msg = make_message(_default_payload(name="locked", args=[], kwargs={}))
            await w._handle_message(msg)

        assert ran is False, "task.run must NOT run when lock conflicted"
        broker.retry_later.assert_awaited_once()
        # U8: retry_later/dead_letter bestätigen das Original selbst
        broker.ack.assert_not_awaited()
        broker.dead_letter.assert_not_called()
        # Release is NOT called when acquire returned False
        w._lock_manager.release.assert_not_awaited()
        assert len(seen) == 1
        assert seen[0]["task_name"] == "locked"
        assert seen[0]["lock_name"] == "resource"
        assert seen[0]["lock_conflict_count"] == 1

    @pytest.mark.asyncio
    async def test_no_lock_skips_lock_manager_entirely(
        self, broker, backend, config
    ):
        reg = TaskRegistry()
        reg.register(AddTask)

        w = Worker(broker=broker, backend=backend, registry=reg, config=config)
        w._lock_manager = MagicMock()
        w._lock_manager.acquire = AsyncMock()

        msg = make_message(_default_payload())
        await w._handle_message(msg)

        w._lock_manager.acquire.assert_not_called()

    # --- Mandatory regression #1 ---------------------------------------------

    @pytest.mark.asyncio
    async def test_lock_set_but_lock_manager_none_falls_through_to_execution(
        self, broker, backend, config
    ):
        """Defensive guard: ``_lock_manager is None`` (Worker.start() was
        never called) must not crash. The task simply runs unprotected —
        better than silently dropping it. Same shape as
        test_rate_limit_set_but_limiter_none_falls_through_to_execution.
        """
        reg = TaskRegistry()
        reg.register(LockedTask)

        w = Worker(broker=broker, backend=backend, registry=reg, config=config)
        # _lock_manager intentionally stays None.

        ran = False

        async def _run(self):
            nonlocal ran
            ran = True

        with patch.object(LockedTask, "run", _run):
            msg = make_message(_default_payload(name="locked", args=[], kwargs={}))
            await w._handle_message(msg)

        assert ran is True
        broker.ack.assert_awaited_once_with(msg)
        broker.retry_later.assert_not_called()

    # --- Mandatory regression #2 ---------------------------------------------

    @pytest.mark.asyncio
    async def test_lock_check_connection_error_nacks_with_requeue(
        self, broker, backend, config
    ):
        """Valkey unreachable mid-acquire must nack(requeue=True), NOT
        drop the message. Same shape as
        test_limiter_connection_error_nacks_with_requeue."""
        reg = TaskRegistry()
        reg.register(LockedTask)

        w = Worker(broker=broker, backend=backend, registry=reg, config=config)
        w._lock_manager = MagicMock()
        w._lock_manager.acquire = AsyncMock(
            side_effect=BackendConnectionError("valkey down")
        )

        msg = make_message(_default_payload(name="locked", args=[], kwargs={}))
        await w._handle_message(msg)

        broker.redeliver.assert_awaited_once_with(msg)
        broker.ack.assert_not_awaited()
        broker.retry_later.assert_not_called()


class TestFireAndForgetLock:
    """Mandatory regression #3 — FIRE_AND_FORGET + lock-stage failure must
    NOT drop the message. Same shape as TestFireAndForgetRateLimit."""

    @pytest.mark.asyncio
    async def test_fire_and_forget_lock_failure_does_not_drop_message(
        self, broker, backend, config
    ):
        reg = TaskRegistry()
        reg.register(FireAndForgetLockedTask)

        w = Worker(broker=broker, backend=backend, registry=reg, config=config)
        w._lock_manager = MagicMock()
        w._lock_manager.acquire = AsyncMock(
            side_effect=BackendConnectionError("simulated valkey hiccup")
        )

        msg = make_message(_default_payload(name="ff_locked", args=[], kwargs={}))
        await w._handle_message(msg)

        broker.redeliver.assert_awaited_once_with(msg)
        # Crucially: NO ack happened. Message survives for redelivery.
        broker.ack.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_fire_and_forget_lock_conflict_acks_once(
        self, broker, backend, config
    ):
        reg = TaskRegistry()
        reg.register(FireAndForgetLockedTask)

        w = Worker(broker=broker, backend=backend, registry=reg, config=config)
        w._lock_manager = MagicMock()
        w._lock_manager.acquire = AsyncMock(return_value=None)

        msg = make_message(_default_payload(name="ff_locked", args=[], kwargs={}))
        await w._handle_message(msg)

        broker.retry_later.assert_awaited_once()
        # U8: retry_later bestätigt selbst; das FAF-Ack wird nie erreicht.
        broker.ack.assert_not_awaited()


class TestLockRequeueCounter:
    """Mandatory regression #4 — x-swarmq-lock-conflict-count header
    increments on every conflict so operators can see a wedged lock.
    Same shape as TestRateLimitRequeueCounter."""

    @pytest.mark.asyncio
    async def test_first_lock_conflict_sets_counter_to_one(
        self, broker, backend, config
    ):
        reg = TaskRegistry()
        reg.register(LockedTask)

        w = Worker(broker=broker, backend=backend, registry=reg, config=config)
        w._lock_manager = MagicMock()
        w._lock_manager.acquire = AsyncMock(return_value=None)

        msg = make_message(_default_payload(name="locked", args=[], kwargs={}))
        msg.headers = {}

        await w._handle_message(msg)

        _, kwargs = broker.retry_later.call_args
        assert kwargs["headers"]["x-swarmq-lock-conflict-count"] == 1
        # Wedged-lock recovery: DLQ (and requeue) messages must carry the
        # rendered lock name so operators can `valkey-cli DEL sq:lock:<name>`.
        assert kwargs["headers"]["x-swarmq-lock-name"] == "resource"

    @pytest.mark.asyncio
    async def test_existing_counter_is_incremented(
        self, broker, backend, config
    ):
        reg = TaskRegistry()
        reg.register(LockedTask)

        w = Worker(broker=broker, backend=backend, registry=reg, config=config)
        w._lock_manager = MagicMock()
        w._lock_manager.acquire = AsyncMock(return_value=None)

        msg = make_message(_default_payload(name="locked", args=[], kwargs={}))
        msg.headers = {"x-swarmq-lock-conflict-count": 5}

        await w._handle_message(msg)

        _, kwargs = broker.retry_later.call_args
        assert kwargs["headers"]["x-swarmq-lock-conflict-count"] == 6

    @pytest.mark.asyncio
    async def test_counter_reaching_cap_routes_to_dlq(
        self, broker, backend, config
    ):
        """When new_count reaches the cap (10), the message routes to DLQ
        instead of being requeued. Cap is INCLUSIVE — the 10th conflict
        is the one that DLQs, not the 11th."""
        reg = TaskRegistry()
        reg.register(LockedTask)

        w = Worker(broker=broker, backend=backend, registry=reg, config=config)
        w._lock_manager = MagicMock()
        w._lock_manager.acquire = AsyncMock(return_value=None)

        msg = make_message(_default_payload(name="locked", args=[], kwargs={}))
        # Prior count = 9; new_count = 10 → exactly at cap → DLQ
        msg.headers = {"x-swarmq-lock-conflict-count": 9}

        await w._handle_message(msg)

        # No more requeue
        broker.retry_later.assert_not_called()
        # DLQ instead, with diagnostic header
        broker.dead_letter.assert_awaited_once()
        _, kwargs = broker.dead_letter.call_args
        assert kwargs["headers"]["x-swarmq-lock-conflict-count"] == 10
        assert kwargs["headers"]["x-swarmq-lock-name"] == "resource"
        # U8: retry_later/dead_letter bestätigen das Original selbst
        broker.ack.assert_not_awaited()

    async def test_counter_just_below_cap_still_requeues(
        self, broker, backend, config
    ):
        """Boundary check: count=8 (becomes new_count=9) is just below the
        cap and still requeues with backoff. Confirms the cap is
        inclusive on the DLQ side, exclusive on the requeue side."""
        reg = TaskRegistry()
        reg.register(LockedTask)

        w = Worker(broker=broker, backend=backend, registry=reg, config=config)
        w._lock_manager = MagicMock()
        w._lock_manager.acquire = AsyncMock(return_value=None)

        msg = make_message(_default_payload(name="locked", args=[], kwargs={}))
        msg.headers = {"x-swarmq-lock-conflict-count": 8}

        await w._handle_message(msg)

        broker.retry_later.assert_awaited_once()
        broker.dead_letter.assert_not_called()
        _, kwargs = broker.retry_later.call_args
        assert kwargs["headers"]["x-swarmq-lock-conflict-count"] == 9

    async def test_dlq_failure_at_cap_drops_message_no_infinite_loop(
        self, broker, backend, config
    ):
        """Cascade-fix regression: when dead_letter fails
        AT the cap, falling back to nack(requeue=True) would let the
        message bounce back at count >= cap, hit the same DLQ failure,
        and loop forever without backoff (the requeue branch is
        unreachable past the cap). Honest data loss with a loud ERROR
        log naming the lock is the right trade-off."""
        reg = TaskRegistry()
        reg.register(LockedTask)

        w = Worker(broker=broker, backend=backend, registry=reg, config=config)
        w._lock_manager = MagicMock()
        w._lock_manager.acquire = AsyncMock(return_value=None)
        broker.dead_letter = AsyncMock(
            side_effect=ConnectionError("broker down")
        )

        msg = make_message(_default_payload(name="locked", args=[], kwargs={}))
        msg.headers = {"x-swarmq-lock-conflict-count": 9}

        await w._handle_message(msg)

        # nack(requeue=False) — drop the message, do not requeue into the
        # bounce loop.
        broker.route_via_dlx.assert_awaited_once_with(msg)
        # No ack happened.
        broker.ack.assert_not_awaited()
        # No requeue either — the requeue branch is unreachable past the cap.
        broker.retry_later.assert_not_called()

    async def test_dlq_failure_at_cap_emits_task_rejected(
        self, broker, backend, config
    ):
        """U6 (R12): Der Datenverlust bleibt, aber er ist nicht mehr stumm --
        vorher sah ein Betreiber nur eine ERROR-Zeile und einen Gauge."""
        from swarmq.signals import Signal

        reg = TaskRegistry()
        reg.register(LockedTask)

        w = Worker(broker=broker, backend=backend, registry=reg, config=config)
        w._lock_manager = MagicMock()
        w._lock_manager.acquire = AsyncMock(return_value=None)
        broker.dead_letter = AsyncMock(side_effect=ConnectionError("broker down"))
        events: list[dict] = []

        async def _dispatch(signal, **kwargs):
            events.append({"signal": signal, **kwargs})

        w._signal_dispatcher.dispatch = AsyncMock(side_effect=_dispatch)

        msg = make_message(_default_payload(name="locked", args=[], kwargs={}))
        msg.headers = {"x-swarmq-lock-conflict-count": 9}

        await w._handle_message(msg)

        rejected = [e for e in events if e["signal"] is Signal.TASK_REJECTED]
        assert len(rejected) == 1
        assert rejected[0]["reason"] == "dlq_publish_failed"
        assert rejected[0]["task_name"] == "locked"
        assert "broker down" in rejected[0]["error"]


class FormatStringLockTask(Task):
    task_info = TaskInfo(
        name="user_charge",
        queues=["default"],
        lock="user-{user_id}",
        hard_timeout=60,
    )

    async def run(self, user_id: int, amount: int) -> None:
        return None


class TestFormatStringLock:
    """Lock names with format placeholders are rendered at acquire-time
    via inspect.signature.bind, so positional and keyword args both
    fill the template."""

    @pytest.mark.asyncio
    async def test_keyword_args_render_lock_name(self, broker, backend, config):
        reg = TaskRegistry()
        reg.register(FormatStringLockTask)

        w = Worker(broker=broker, backend=backend, registry=reg, config=config)
        w._lock_manager = MagicMock()
        w._lock_manager.acquire = AsyncMock(
            side_effect=lambda name, *, owner, ttl_ms, max_concurrency: LockToken(
                name=name, owner=owner, max_concurrency=max_concurrency
            )
        )
        w._lock_manager.release = AsyncMock(return_value=True)

        msg = make_message(
            _default_payload(name="user_charge", args=[], kwargs={"user_id": 42, "amount": 100})
        )
        await w._handle_message(msg)

        args, _ = w._lock_manager.acquire.call_args
        assert args[0] == "user-42"

    @pytest.mark.asyncio
    async def test_positional_args_render_lock_name(self, broker, backend, config):
        reg = TaskRegistry()
        reg.register(FormatStringLockTask)

        w = Worker(broker=broker, backend=backend, registry=reg, config=config)
        w._lock_manager = MagicMock()
        w._lock_manager.acquire = AsyncMock(
            side_effect=lambda name, *, owner, ttl_ms, max_concurrency: LockToken(
                name=name, owner=owner, max_concurrency=max_concurrency
            )
        )
        w._lock_manager.release = AsyncMock(return_value=True)

        msg = make_message(
            _default_payload(name="user_charge", args=[7, 100], kwargs={})
        )
        await w._handle_message(msg)

        args, _ = w._lock_manager.acquire.call_args
        assert args[0] == "user-7"

    @pytest.mark.asyncio
    async def test_missing_arg_routes_to_dlq_with_render_error_header(
        self, broker, backend, config
    ):
        reg = TaskRegistry()
        reg.register(FormatStringLockTask)

        w = Worker(broker=broker, backend=backend, registry=reg, config=config)
        w._lock_manager = MagicMock()
        w._lock_manager.acquire = AsyncMock()

        # Missing user_id entirely
        msg = make_message(
            _default_payload(name="user_charge", args=[], kwargs={"amount": 100})
        )
        await w._handle_message(msg)

        w._lock_manager.acquire.assert_not_called()
        broker.dead_letter.assert_awaited_once()
        _, kwargs = broker.dead_letter.call_args
        assert "x-swarmq-lock-render-error" in kwargs["headers"]

    @pytest.mark.asyncio
    async def test_render_error_with_dlq_publish_failure_nacks_no_requeue(
        self, broker, backend, config
    ):
        """Regression: lock-render DLQ path must wrap publish + ack in
        try/except, AND on broker fault must nack(requeue=False) so the
        message routes via the broker DLX to {queue}.XQ instead of
        bouncing back into the consume loop. A render error is permanent
        (deterministic output of inspect.signature.bind on this payload)
        — requeueing would just hit the same render error again, then
        the same DLQ failure, then loop forever (same shape as ADV-002
        at the conflict cap)."""
        reg = TaskRegistry()
        reg.register(FormatStringLockTask)

        w = Worker(broker=broker, backend=backend, registry=reg, config=config)
        w._lock_manager = MagicMock()
        w._lock_manager.acquire = AsyncMock()
        broker.dead_letter = AsyncMock(
            side_effect=ConnectionError("broker down")
        )

        msg = make_message(
            _default_payload(name="user_charge", args=[], kwargs={"amount": 100})
        )
        await w._handle_message(msg)

        # Render failed, DLQ publish failed → nack(requeue=False) routes
        # via DLX to {queue}.XQ (not lost), no ack.
        broker.route_via_dlx.assert_awaited_once_with(msg)
        broker.ack.assert_not_awaited()


class EmptyRenderingLockTask(Task):
    task_info = TaskInfo(
        name="empty_render",
        queues=["default"],
        lock="{tag}",
        hard_timeout=60,
    )

    async def run(self, tag: str) -> str:
        return tag


class TestEmptyRenderedLock:
    """Lock template that renders to "" or whitespace must be rejected
    at acquire-time. TaskInfo blocks literal `lock=""` at construction,
    but a format-string template that renders to empty (e.g. lock="{var}"
    with var="") would slip past — without this guard, lock_key("")
    returns "sq:lock:" and silently collides with every other empty-
    rendered lock in the system."""

    @pytest.mark.asyncio
    async def test_empty_render_routes_to_dlq_with_diagnostic_header(
        self, broker, backend, config
    ):
        reg = TaskRegistry()
        reg.register(EmptyRenderingLockTask)

        w = Worker(broker=broker, backend=backend, registry=reg, config=config)
        w._lock_manager = MagicMock()
        w._lock_manager.acquire = AsyncMock()

        msg = make_message(
            _default_payload(name="empty_render", args=[], kwargs={"tag": ""})
        )
        await w._handle_message(msg)

        # Acquire never called — guard fires before LockManager
        w._lock_manager.acquire.assert_not_called()
        broker.dead_letter.assert_awaited_once()
        _, kwargs = broker.dead_letter.call_args
        assert "rendered to empty string" in kwargs["headers"][
            "x-swarmq-lock-render-error"
        ]
        # U8: retry_later/dead_letter bestätigen das Original selbst
        broker.ack.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_whitespace_render_also_rejected(
        self, broker, backend, config
    ):
        reg = TaskRegistry()
        reg.register(EmptyRenderingLockTask)

        w = Worker(broker=broker, backend=backend, registry=reg, config=config)
        w._lock_manager = MagicMock()
        w._lock_manager.acquire = AsyncMock()

        msg = make_message(
            _default_payload(name="empty_render", args=[], kwargs={"tag": "   "})
        )
        await w._handle_message(msg)

        w._lock_manager.acquire.assert_not_called()
        broker.dead_letter.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_empty_render_with_dlq_failure_nacks_no_requeue(
        self, broker, backend, config
    ):
        """Same hazard pattern as render-fail: empty render is permanent
        for this payload, so DLQ publish failure must route via DLX
        rather than bouncing the message back into the consume loop."""
        reg = TaskRegistry()
        reg.register(EmptyRenderingLockTask)

        w = Worker(broker=broker, backend=backend, registry=reg, config=config)
        w._lock_manager = MagicMock()
        w._lock_manager.acquire = AsyncMock()
        broker.dead_letter = AsyncMock(
            side_effect=ConnectionError("broker down")
        )

        msg = make_message(
            _default_payload(name="empty_render", args=[], kwargs={"tag": ""})
        )
        await w._handle_message(msg)

        broker.route_via_dlx.assert_awaited_once_with(msg)
        broker.ack.assert_not_awaited()


# ===========================================================================
# Phase 3b.3 / Unit 6 — Worker Pre-Start Cancel Check
# ===========================================================================


class TestLockGateOutcomes:
    """Direkte Gate-Tests (Plan U7): Outcome, Registrierung, Fallbacks."""

    def _ctx(self, task_class=LockedTask, **overrides):
        base = dict(
            task_id="t1", task_name=task_class.task_info.name, queue="default",
            task_info=task_class.task_info, task_class=task_class, args=[], kwargs={}, handler_id=uuid.uuid4(),
        )
        base.update(overrides)
        return GateContext(**base)

    async def test_acquire_is_continue_and_registers_entry(self, worker):
        worker._lock_manager = MagicMock()
        worker._lock_manager.acquire = AsyncMock(
            side_effect=lambda name, *, owner, ttl_ms, max_concurrency: LockToken(
                name=name, owner=owner, max_concurrency=max_concurrency
            )
        )
        ctx = self._ctx()
        assert await worker._lock_gate.apply(make_message(_default_payload()), ctx) is Continue
        assert len(ctx.acquired_locks) == 1 and ctx.acquired_locks[0].name == "resource"
        assert worker._active_handler_locks[ctx.handler_id] == ctx.acquired_locks

    async def test_conflict_under_cap_is_redelivered_with_exponential_delay(self, worker, broker):
        worker._lock_manager = MagicMock()
        worker._lock_manager.acquire = AsyncMock(return_value=None)
        msg = make_message(_default_payload(), headers={"x-swarmq-lock-conflict-count": 2})
        assert await worker._lock_gate.apply(msg, self._ctx()) is Redelivered
        kwargs = broker.retry_later.await_args.kwargs
        assert kwargs["delay_ms"] == 400  # 100 * 2**(3-1)
        assert kwargs["headers"]["x-swarmq-lock-name"] == "resource"

    async def test_conflict_at_cap_dead_letters_with_diagnostics(self, worker, broker):
        worker._lock_manager = MagicMock()
        worker._lock_manager.acquire = AsyncMock(return_value=None)
        msg = make_message(_default_payload(), headers={"x-swarmq-lock-conflict-count": LOCK_CONFLICT_DLQ_CAP - 1})
        assert await worker._lock_gate.apply(msg, self._ctx()) is Redelivered
        headers = broker.dead_letter.await_args.kwargs["headers"]
        assert headers["x-swarmq-lock-conflict-count"] == LOCK_CONFLICT_DLQ_CAP
        broker.ack.assert_not_awaited()  # dead_letter bestätigt selbst (U8)

    async def test_render_error_dead_letters_and_falls_back_to_dlx(self, worker, broker):
        worker._lock_manager = MagicMock()
        broker.dead_letter = AsyncMock(side_effect=BrokerConnectionError("down"))
        msg = make_message(_default_payload())
        outcome = await worker._lock_gate.apply(msg, self._ctx(FormatStringLockTask, args=[], kwargs={}))
        assert outcome is Redelivered
        broker.route_via_dlx.assert_awaited_once_with(msg)

    def test_render_lock_name_binds_run_signature(self):
        assert render_lock_name("user-{user_id}", FormatStringLockTask, [7, 1], {}) == "user-7"
        assert render_lock_name("plain", FormatStringLockTask, [], {}) == "plain"
        with pytest.raises(ValueError, match="failed to render lock name"):
            render_lock_name("user-{missing}", FormatStringLockTask, [7, 1], {})


class TestLockGateLogging:
    """Plan 2026-10-06-0831, U9 (R17, R18): feste Event-Namen, Fehler mit
    ``error_type``/``error``."""

    def _ctx(self, task_class=LockedTask, **overrides):
        base = dict(
            task_id="t1", task_name=task_class.task_info.name, queue="default",
            task_info=task_class.task_info, task_class=task_class, args=[], kwargs={}, handler_id=uuid.uuid4(),
        )
        base.update(overrides)
        return GateContext(**base)

    @staticmethod
    def _entry(logs, event):
        [entry] = [e for e in logs if e["logger"] == "swarmq.worker.gates.lock" and e["event"] == event]
        return entry

    async def test_render_dlq_failure_logs_fixed_event_with_error_fields(self, worker, broker, swarmq_logs):
        worker._lock_manager = MagicMock()
        broker.dead_letter = AsyncMock(side_effect=BrokerConnectionError("down"))
        await worker._lock_gate.apply(
            make_message(_default_payload()), self._ctx(FormatStringLockTask, args=[], kwargs={})
        )

        entry = self._entry(swarmq_logs, "lock_render_dlq_failed")
        assert entry["log_level"] == "error" and entry["lock_template"] == "user-{user_id}"
        assert entry["error_type"] == "BrokerConnectionError" and entry["error"] == "down"

    async def test_empty_render_dlq_failure_logs_its_own_fixed_event(self, worker, broker, swarmq_logs):
        worker._lock_manager = MagicMock()
        broker.dead_letter = AsyncMock(side_effect=BrokerConnectionError("down"))
        await worker._lock_gate.apply(
            make_message(_default_payload()), self._ctx(EmptyRenderingLockTask, args=[], kwargs={"tag": ""})
        )

        entry = self._entry(swarmq_logs, "lock_empty_render_dlq_failed")
        assert entry["log_level"] == "error" and entry["lock_template"] == "{tag}"
        assert entry["error_type"] == "BrokerConnectionError" and entry["error"] == "down"

    async def test_render_failure_logs_error_fields(self, worker, swarmq_logs):
        worker._lock_manager = MagicMock()
        await worker._lock_gate.apply(
            make_message(_default_payload()), self._ctx(FormatStringLockTask, args=[], kwargs={})
        )

        entry = self._entry(swarmq_logs, "lock_name_render_failed")
        assert entry["log_level"] == "error" and entry["lock_template"] == "user-{user_id}"
        assert entry["error_type"] == "ValueError" and "failed to render lock name" in entry["error"]

    async def test_acquire_failure_logs_error_fields(self, worker, swarmq_logs):
        worker._lock_manager = MagicMock()
        worker._lock_manager.acquire = AsyncMock(side_effect=BackendConnectionError("valkey down"))
        await worker._lock_gate.apply(make_message(_default_payload()), self._ctx())

        entry = self._entry(swarmq_logs, "lock_acquire_failed")
        assert entry["log_level"] == "error" and entry["lock_name"] == "resource"
        assert entry["error_type"] == "BackendConnectionError" and entry["error"] == "valkey down"

    async def test_conflict_dlq_failure_at_cap_logs_error_fields(self, worker, broker, swarmq_logs):
        worker._lock_manager = MagicMock()
        worker._lock_manager.acquire = AsyncMock(return_value=None)
        broker.dead_letter = AsyncMock(side_effect=BrokerConnectionError("down"))
        msg = make_message(_default_payload(), headers={"x-swarmq-lock-conflict-count": LOCK_CONFLICT_DLQ_CAP - 1})
        await worker._lock_gate.apply(msg, self._ctx())

        entry = self._entry(swarmq_logs, "lock_conflict_dlq_failed")
        assert entry["log_level"] == "error" and entry["lock_conflict_count"] == LOCK_CONFLICT_DLQ_CAP
        assert entry["error_type"] == "BrokerConnectionError" and entry["error"] == "down"
        assert "note" in entry

    async def test_conflict_requeue_failure_logs_error_fields(self, worker, broker, swarmq_logs):
        worker._lock_manager = MagicMock()
        worker._lock_manager.acquire = AsyncMock(return_value=None)
        broker.retry_later = AsyncMock(side_effect=BrokerConnectionError("down"))
        await worker._lock_gate.apply(make_message(_default_payload()), self._ctx())

        entry = self._entry(swarmq_logs, "lock_conflict_requeue_failed")
        assert entry["log_level"] == "error" and entry["lock_name"] == "resource"
        assert entry["error_type"] == "BrokerConnectionError" and entry["error"] == "down"
