"""Rate-Limit-Gate (Plan U7): aus ``test_worker.py`` verschobene Fälle plus direkte Outcome-Tests."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from swarmq.exceptions import BackendConnectionError
from swarmq.registry import TaskRegistry
from swarmq.signals import Signal
from swarmq.task import DeliveryMode, Task, TaskInfo
from swarmq.worker import Worker
from swarmq.worker.gates import GateContext
from swarmq.worker.outcome import Continue, Redelivered
from tests.support.messages import make_message
from tests.unit.worker._helpers import (
    AddTask,
    _default_payload,
)


class RateLimitedTask(Task):
    task_info = TaskInfo(name="rate_limited", queues=["default"], rate_limit="10/m")


class TestWorkerRateLimiting:
    """Unit-level coverage of the rate-limit branch in _handle_message.

    A real Worker is built but the ``_rate_limiter`` attribute is
    replaced with an ``AsyncMock`` so no Valkey is needed.
    """

    @pytest.mark.asyncio
    async def test_allowed_path_runs_the_task(self, broker, backend, config):
        """allowed=True -> pre_execute + run fire, no delay-queue publish."""
        reg = TaskRegistry()
        reg.register(RateLimitedTask)

        w = Worker(broker=broker, backend=backend, registry=reg, config=config)
        # Simulate start()'s lifecycle step without connecting.
        w._rate_limiter = MagicMock()
        w._rate_limiter.check = AsyncMock(return_value=(True, 9, 0))

        msg = make_message(_default_payload(name="rate_limited", args=[], kwargs={}))

        async def _run(self):
            return None

        with patch.object(RateLimitedTask, "run", _run):
            await w._handle_message(msg)

        w._rate_limiter.check.assert_awaited_once()
        broker.publish_to_delay_queue.assert_not_called()
        # Normal ack after successful execution (AT_LEAST_ONCE).
        broker.ack.assert_awaited_once_with(msg)

    @pytest.mark.asyncio
    async def test_rate_limited_path_requeues_with_delay_and_skips_execution(
        self, broker, backend, config
    ):
        """allowed=False -> publish_to_delay_queue + ack, no pre_execute,
        no DLQ, TASK_RATE_LIMITED signal fired with retry_after + remaining.
        """
        from swarmq.signals import SignalDispatcher

        dispatcher = SignalDispatcher()
        seen: list[dict] = []

        @dispatcher.on_signal(Signal.TASK_RATE_LIMITED)
        async def _handler(**kwargs: object) -> None:
            seen.append(dict(kwargs))

        reg = TaskRegistry()
        reg.register(RateLimitedTask)

        w = Worker(
            broker=broker,
            backend=backend,
            registry=reg,
            config=config,
            signal_dispatcher=dispatcher,
        )
        w._rate_limiter = MagicMock()
        w._rate_limiter.check = AsyncMock(return_value=(False, 0, 2500))

        ran = False

        async def _run(self):
            nonlocal ran
            ran = True

        with patch.object(RateLimitedTask, "run", _run):
            msg = make_message(
                _default_payload(name="rate_limited", args=[], kwargs={})
            )
            await w._handle_message(msg)

        assert ran is False, "task.run must NOT run when rate-limited"
        broker.publish_to_delay_queue.assert_awaited_once()
        _, kwargs = broker.publish_to_delay_queue.call_args
        assert kwargs["delay_ms"] == 2500
        broker.ack.assert_awaited_once_with(msg)
        broker.publish_to_dead_letter_queue.assert_not_called()
        assert len(seen) == 1
        assert seen[0]["retry_after_ms"] == 2500
        assert seen[0]["remaining"] == 0
        assert seen[0]["task_name"] == "rate_limited"

    @pytest.mark.asyncio
    async def test_no_rate_limit_skips_limiter_entirely(
        self, broker, backend, config
    ):
        """Task without ``rate_limit`` must not call the limiter at all."""
        reg = TaskRegistry()
        reg.register(AddTask)

        w = Worker(broker=broker, backend=backend, registry=reg, config=config)
        w._rate_limiter = MagicMock()
        w._rate_limiter.check = AsyncMock()

        msg = make_message(_default_payload())
        await w._handle_message(msg)

        w._rate_limiter.check.assert_not_called()

    @pytest.mark.asyncio
    async def test_rate_limit_set_but_limiter_none_falls_through_to_execution(
        self, broker, backend, config
    ):
        """Defensive guard: ``_rate_limiter is None`` (Worker.start() was
        never called) must not silently skip enforcement AND crash. The
        message simply executes — better than dropping it."""
        reg = TaskRegistry()
        reg.register(RateLimitedTask)

        w = Worker(broker=broker, backend=backend, registry=reg, config=config)
        # _rate_limiter intentionally stays None.

        ran = False

        async def _run(self):
            nonlocal ran
            ran = True

        with patch.object(RateLimitedTask, "run", _run):
            msg = make_message(
                _default_payload(name="rate_limited", args=[], kwargs={})
            )
            await w._handle_message(msg)

        assert ran is True
        broker.ack.assert_awaited_once_with(msg)
        broker.publish_to_delay_queue.assert_not_called()

    @pytest.mark.asyncio
    async def test_limiter_connection_error_nacks_with_requeue(
        self, broker, backend, config
    ):
        """Valkey unreachable mid-check must nack(requeue=True), NOT drop
        the message. Critical for FIRE_AND_FORGET tasks where the early
        ack used to silently swallow Valkey failures."""
        reg = TaskRegistry()
        reg.register(RateLimitedTask)

        w = Worker(broker=broker, backend=backend, registry=reg, config=config)
        w._rate_limiter = MagicMock()
        w._rate_limiter.check = AsyncMock(
            side_effect=ConnectionError("valkey down")
        )

        msg = make_message(
            _default_payload(name="rate_limited", args=[], kwargs={})
        )
        await w._handle_message(msg)

        broker.nack.assert_awaited_once_with(msg, requeue=True)
        broker.ack.assert_not_awaited()
        broker.publish_to_delay_queue.assert_not_called()


class FireAndForgetRateLimitedTask(Task):
    task_info = TaskInfo(
        name="ff_rate_limited",
        queues=["default"],
        delivery=DeliveryMode.FIRE_AND_FORGET,
        rate_limit="10/m",
    )


class TestFireAndForgetRateLimit:
    """Regression suite for the silent-drop bug surfaced by review:
    FIRE_AND_FORGET acked BEFORE the rate-limit check, so any failure
    in the limiter or broker would drop the message permanently."""

    @pytest.mark.asyncio
    async def test_rate_limited_path_does_not_double_ack_fire_and_forget(
        self, broker, backend, config
    ):
        """Rate-limit handler now runs BEFORE the FIRE_AND_FORGET ack.
        On rate-limit it ack's exactly once (not twice)."""
        reg = TaskRegistry()
        reg.register(FireAndForgetRateLimitedTask)

        w = Worker(broker=broker, backend=backend, registry=reg, config=config)
        w._rate_limiter = MagicMock()
        w._rate_limiter.check = AsyncMock(return_value=(False, 0, 1500))

        msg = make_message(
            _default_payload(name="ff_rate_limited", args=[], kwargs={})
        )
        await w._handle_message(msg)

        broker.publish_to_delay_queue.assert_awaited_once()
        # Single ack — not the old "FIRE_AND_FORGET ack + rate-limit ack" double.
        assert broker.ack.await_count == 1

    @pytest.mark.asyncio
    async def test_fire_and_forget_limiter_failure_does_not_drop_message(
        self, broker, backend, config
    ):
        """The bug we are guarding against: Valkey error after the
        legacy early-ack would have left the message gone forever.
        Now: nack(requeue=True), original delivery survives."""
        reg = TaskRegistry()
        reg.register(FireAndForgetRateLimitedTask)

        w = Worker(broker=broker, backend=backend, registry=reg, config=config)
        w._rate_limiter = MagicMock()
        w._rate_limiter.check = AsyncMock(
            side_effect=RuntimeError("simulated valkey hiccup")
        )

        msg = make_message(
            _default_payload(name="ff_rate_limited", args=[], kwargs={})
        )
        await w._handle_message(msg)

        broker.nack.assert_awaited_once_with(msg, requeue=True)
        # Crucially: NO ack happened. Message survives for redelivery.
        broker.ack.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_fire_and_forget_allowed_acks_then_runs(
        self, broker, backend, config
    ):
        """Allowed FIRE_AND_FORGET still acks early (the original
        intent of FIRE_AND_FORGET), then runs the task body."""
        reg = TaskRegistry()
        reg.register(FireAndForgetRateLimitedTask)

        w = Worker(broker=broker, backend=backend, registry=reg, config=config)
        w._rate_limiter = MagicMock()
        w._rate_limiter.check = AsyncMock(return_value=(True, 9, 0))

        ran = False

        async def _run(self):
            nonlocal ran
            ran = True

        with patch.object(FireAndForgetRateLimitedTask, "run", _run):
            msg = make_message(
                _default_payload(name="ff_rate_limited", args=[], kwargs={})
            )
            await w._handle_message(msg)

        assert ran is True
        broker.ack.assert_awaited_once_with(msg)


class TestRateLimitRequeueCounter:
    """The new x-swarmq-rate-limit-count header increments on every
    rate-limit hit so operators can see when a task is stuck in the
    limiter loop."""

    @pytest.mark.asyncio
    async def test_first_rate_limit_hit_sets_counter_to_one(
        self, broker, backend, config
    ):
        reg = TaskRegistry()
        reg.register(RateLimitedTask)

        w = Worker(broker=broker, backend=backend, registry=reg, config=config)
        w._rate_limiter = MagicMock()
        w._rate_limiter.check = AsyncMock(return_value=(False, 0, 1000))

        msg = make_message(
            _default_payload(name="rate_limited", args=[], kwargs={})
        )
        # No prior counter on the headers.
        msg.headers = {}

        await w._handle_message(msg)

        _, kwargs = broker.publish_to_delay_queue.call_args
        assert kwargs["headers"]["x-swarmq-rate-limit-count"] == 1

    @pytest.mark.asyncio
    async def test_existing_counter_is_incremented(
        self, broker, backend, config
    ):
        reg = TaskRegistry()
        reg.register(RateLimitedTask)

        w = Worker(broker=broker, backend=backend, registry=reg, config=config)
        w._rate_limiter = MagicMock()
        w._rate_limiter.check = AsyncMock(return_value=(False, 0, 1000))

        msg = make_message(
            _default_payload(name="rate_limited", args=[], kwargs={})
        )
        msg.headers = {"x-swarmq-rate-limit-count": 5}

        await w._handle_message(msg)

        _, kwargs = broker.publish_to_delay_queue.call_args
        assert kwargs["headers"]["x-swarmq-rate-limit-count"] == 6


# ===========================================================================
# Phase 3b.2 — Task Locking pipeline-stage tests
#
# Mirrors the rate-limit suite above. Same four-test mandatory regression
# template from docs/solutions/.../worker-pipeline-stage-ack-ordering-...md
# applied to _try_apply_lock instead of _try_apply_rate_limit.
# ===========================================================================


class TestRateLimitGateOutcomes:
    """Direkte Gate-Tests (Plan U7): Outcome statt bool."""

    def _ctx(self):
        return GateContext(task_id="t1", task_name="rate_limited", queue="default", task_info=RateLimitedTask.task_info)

    async def test_no_limit_configured_is_continue(self, worker, broker):
        worker._rate_limiter = MagicMock()
        ctx = GateContext(task_id="t1", task_name="add", queue="default", task_info=AddTask.task_info)
        assert await worker._rate_limit_gate.apply(make_message(_default_payload()), ctx) is Continue
        broker.ack.assert_not_awaited()

    async def test_allowed_is_continue(self, worker):
        worker._rate_limiter = MagicMock()
        worker._rate_limiter.check = AsyncMock(return_value=(True, 9, 0))
        assert await worker._rate_limit_gate.apply(make_message(_default_payload()), self._ctx()) is Continue

    async def test_limited_is_redelivered_with_counter_header(self, worker, broker):
        worker._rate_limiter = MagicMock()
        worker._rate_limiter.check = AsyncMock(return_value=(False, 0, 1500))
        msg = make_message(_default_payload(), headers={"x-swarmq-rate-limit-count": 2})
        assert await worker._rate_limit_gate.apply(msg, self._ctx()) is Redelivered
        kwargs = broker.publish_to_delay_queue.await_args.kwargs
        assert kwargs["delay_ms"] == 1500 and kwargs["headers"]["x-swarmq-rate-limit-count"] == 3
        broker.ack.assert_awaited_once_with(msg)

    async def test_backend_error_is_redelivered_via_nack_without_ack(self, worker, broker):
        worker._rate_limiter = MagicMock()
        worker._rate_limiter.check = AsyncMock(side_effect=BackendConnectionError("down"))
        msg = make_message(_default_payload())
        assert await worker._rate_limit_gate.apply(msg, self._ctx()) is Redelivered
        broker.nack.assert_awaited_once_with(msg, requeue=True)
        broker.ack.assert_not_awaited()
