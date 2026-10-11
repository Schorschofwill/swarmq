"""Unit tests for Worker soft/hard timeout enforcement (Unit 2.4)."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.config import SwarmQConfig
from swarmq.exceptions import SoftTimeoutError, SwarmQError
from swarmq.registry import TaskRegistry
from swarmq.serialization import decode_result
from swarmq.task import DeliveryMode, Task, TaskInfo
from swarmq.worker import Worker
from tests.support.messages import make_message

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _default_payload(
    task_id: str = "abc-123",
    queue: str = "default",
    name: str = "add",
    args: list | None = None,
    kwargs: dict | None = None,
) -> dict:
    return {
        "task": {"id": task_id, "queue": queue, "name": name},
        "args": args if args is not None else [1, 2],
        "kwargs": kwargs if kwargs is not None else {},
    }


@pytest.fixture
def broker():
    b = MagicMock(spec=RabbitMQBroker)
    b.connect = AsyncMock()
    b.declare_queue = AsyncMock()
    b.consume = AsyncMock()
    b.ack = AsyncMock()
    b.nack = AsyncMock()
    b.close = AsyncMock()
    b.retry_later = AsyncMock()
    b.dead_letter = AsyncMock()
    return b


@pytest.fixture
def backend():
    be = MagicMock(spec=ValkeyBackend)
    be.connect = AsyncMock()
    be.store_result = AsyncMock()
    be.close = AsyncMock()
    # Cancel-flag primitives (Phase 3b.3). Default: no flag set, so the
    # pre-start cancel check is transparent for timeout tests.
    be.is_cancelled = AsyncMock(return_value=False)
    be.set_cancel_flag = AsyncMock(return_value=True)
    be.clear_cancel_flag = AsyncMock()
    return be


@pytest.fixture
def config():
    return SwarmQConfig(
        broker_url="amqp://localhost",
        backend_url="valkey://localhost",
        concurrency=10,
        queues=["default"],
    )


# ---------------------------------------------------------------------------
# 1. SoftTimeoutError is a SwarmQError subclass
# ---------------------------------------------------------------------------


def test_soft_timeout_error_is_swarmq_error_subclass():
    assert issubclass(SoftTimeoutError, SwarmQError)
    exc = SoftTimeoutError("timed out")
    assert isinstance(exc, SwarmQError)
    assert str(exc) == "timed out"


# ---------------------------------------------------------------------------
# 2. Soft-timeout fires when run() exceeds soft_timeout
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_soft_timeout_fires_when_run_exceeds_limit(broker, backend, config):
    """Task with soft_timeout=0.05s, run() sleeps 1s -> error result with SoftTimeoutError."""

    class SlowTask(Task):
        task_info = TaskInfo(name="slow_soft", queues=["default"], soft_timeout=0.05)

        async def run(self) -> str:
            await asyncio.sleep(1)
            return "never"

    reg = TaskRegistry()
    reg.register(SlowTask)
    w = Worker(broker=broker, backend=backend, registry=reg, config=config)

    msg = make_message(_default_payload(name="slow_soft", args=[], kwargs={}))
    await w._handle_message(msg)

    # Should store error result with SoftTimeoutError message
    stored = decode_result(backend.store_result.call_args[0][1])
    assert stored["is_error"] is True
    assert "SoftTimeoutError" in stored["error_message"] or "soft timeout" in stored["error_message"].lower()


# ---------------------------------------------------------------------------
# 3. SoftTimeoutError triggers retry logic (retried via delay queue)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_soft_timeout_triggers_retry(broker, backend, config):
    """SoftTimeoutError with max_retries > 0 -> message published to delay queue."""

    class SlowRetryTask(Task):
        task_info = TaskInfo(
            name="slow_retry",
            queues=["default"],
            soft_timeout=0.05,
            max_retries=3,
            retry_base_delay=0.1,
        )

        async def run(self) -> str:
            await asyncio.sleep(1)
            return "never"

    reg = TaskRegistry()
    reg.register(SlowRetryTask)
    w = Worker(broker=broker, backend=backend, registry=reg, config=config)

    msg = make_message(
        _default_payload(name="slow_retry", args=[], kwargs={}),
        headers={},
    )
    await w._handle_message(msg)

    # Should publish to delay queue (retry), NOT store error result
    broker.retry_later.assert_awaited_once()
    call_kwargs = broker.retry_later.call_args
    headers = call_kwargs.kwargs["headers"]
    assert headers["x-swarmq-retry-count"] == 1
    # U8: retry_later/dead_letter bestätigen das Original selbst
    broker.ack.assert_not_awaited()
    backend.store_result.assert_not_awaited()


# ---------------------------------------------------------------------------
# 4. Hard-timeout fires -> CancelledError caught -> retry path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_hard_timeout_fires_and_retries(broker, backend, config):
    """Task with hard_timeout=0.05s, run() sleeps 1s -> retried via delay queue."""

    class SlowHardTask(Task):
        task_info = TaskInfo(
            name="slow_hard",
            queues=["default"],
            hard_timeout=0.05,
            max_retries=3,
            retry_base_delay=0.1,
        )

        async def run(self) -> str:
            await asyncio.sleep(1)
            return "never"

    reg = TaskRegistry()
    reg.register(SlowHardTask)
    w = Worker(broker=broker, backend=backend, registry=reg, config=config)

    msg = make_message(
        _default_payload(name="slow_hard", args=[], kwargs={}),
        headers={},
    )
    await w._handle_message(msg)

    # Should publish to delay queue (retry)
    broker.retry_later.assert_awaited_once()
    call_kwargs = broker.retry_later.call_args
    headers = call_kwargs.kwargs["headers"]
    assert headers["x-swarmq-retry-count"] == 1
    # U8: retry_later/dead_letter bestätigen das Original selbst
    broker.ack.assert_not_awaited()
    backend.store_result.assert_not_awaited()


# ---------------------------------------------------------------------------
# 5. No timeouts -> normal behavior (backwards compatible)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_timeouts_normal_behavior(broker, backend, config):
    """Task without timeouts executes normally, no timeout machinery engaged."""

    class NormalTask(Task):
        task_info = TaskInfo(name="normal", queues=["default"])

        async def run(self, a: int, b: int) -> int:
            return a + b

    reg = TaskRegistry()
    reg.register(NormalTask)
    w = Worker(broker=broker, backend=backend, registry=reg, config=config)

    msg = make_message(_default_payload(name="normal", args=[10, 20], kwargs={}))
    await w._handle_message(msg)

    stored = decode_result(backend.store_result.call_args[0][1])
    assert stored["is_error"] is False
    assert stored["result"] == 30
    broker.ack.assert_awaited_once_with(msg)


# ---------------------------------------------------------------------------
# 6. Soft-timeout: post_execution is NOT called
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_soft_timeout_skips_post_execution(broker, backend, config):
    """When soft timeout fires, post_execution must NOT be called."""
    post_called = False

    class SoftTimeoutPostTask(Task):
        task_info = TaskInfo(name="soft_post", queues=["default"], soft_timeout=0.05)

        async def run(self) -> str:
            await asyncio.sleep(1)
            return "never"

        async def post_execution(self) -> None:
            nonlocal post_called
            post_called = True

    reg = TaskRegistry()
    reg.register(SoftTimeoutPostTask)
    w = Worker(broker=broker, backend=backend, registry=reg, config=config)

    msg = make_message(_default_payload(name="soft_post", args=[], kwargs={}))
    await w._handle_message(msg)

    assert post_called is False


# ---------------------------------------------------------------------------
# 7. Hard-timeout after max_retries exhausted -> DLQ
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_hard_timeout_exhausted_retries_sends_to_dlq(broker, backend, config):
    """Hard timeout on final retry -> message sent to DLQ."""

    class SlowDlqTask(Task):
        task_info = TaskInfo(
            name="slow_dlq",
            queues=["default"],
            hard_timeout=0.05,
            max_retries=2,
            retry_base_delay=0.1,
        )

        async def run(self) -> str:
            await asyncio.sleep(1)
            return "never"

    reg = TaskRegistry()
    reg.register(SlowDlqTask)
    w = Worker(broker=broker, backend=backend, registry=reg, config=config)

    # Simulate retry_count=2 (== max_retries) -> exhausted
    msg = make_message(
        _default_payload(name="slow_dlq", args=[], kwargs={}),
        headers={"x-swarmq-retry-count": 2},
    )
    await w._handle_message(msg)

    # Should go to DLQ
    broker.dead_letter.assert_awaited_once()
    # Should NOT retry
    broker.retry_later.assert_not_awaited()
    # U8: retry_later/dead_letter bestätigen das Original selbst
    broker.ack.assert_not_awaited()


# ---------------------------------------------------------------------------
# 8. Only hard_timeout set (no soft) -> only hard-timeout active
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_only_hard_timeout_no_soft(broker, backend, config):
    """Only hard_timeout=0.05, no soft_timeout -> hard timeout cancels task."""

    class HardOnlyTask(Task):
        task_info = TaskInfo(
            name="hard_only",
            queues=["default"],
            hard_timeout=0.05,
            max_retries=1,
            retry_base_delay=0.1,
        )

        async def run(self) -> str:
            await asyncio.sleep(1)
            return "never"

    reg = TaskRegistry()
    reg.register(HardOnlyTask)
    w = Worker(broker=broker, backend=backend, registry=reg, config=config)

    msg = make_message(
        _default_payload(name="hard_only", args=[], kwargs={}),
        headers={},
    )
    await w._handle_message(msg)

    # Should retry via delay queue (hard timeout triggers retry)
    broker.retry_later.assert_awaited_once()
    call_kwargs = broker.retry_later.call_args
    headers = call_kwargs.kwargs["headers"]
    assert headers["x-swarmq-retry-count"] == 1
    assert "hard timeout" in headers["x-swarmq-traceback"]
    # U8: retry_later/dead_letter bestätigen das Original selbst
    broker.ack.assert_not_awaited()


# ---------------------------------------------------------------------------
# 9. FIRE_AND_FORGET + hard-timeout -> cancelled, logged, no nack
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fire_and_forget_hard_timeout_logged_no_nack(broker, backend, config):
    """FIRE_AND_FORGET + hard_timeout -> task cancelled, error logged, no nack or retry."""

    class FnfHardTask(Task):
        task_info = TaskInfo(
            name="fnf_hard",
            queues=["default"],
            delivery=DeliveryMode.FIRE_AND_FORGET,
            hard_timeout=0.05,
        )

        async def run(self) -> str:
            await asyncio.sleep(1)
            return "never"

    reg = TaskRegistry()
    reg.register(FnfHardTask)
    w = Worker(broker=broker, backend=backend, registry=reg, config=config)

    msg = make_message(
        _default_payload(name="fnf_hard", args=[], kwargs={}),
        headers={},
    )

    with patch("swarmq.worker.execution.logger") as mock_logger:
        await w._handle_message(msg)

        # Error must be logged
        mock_logger.error.assert_called()

    # nack must NOT be called
    broker.redeliver.assert_not_awaited()
    # No retry, no DLQ
    broker.retry_later.assert_not_awaited()
    broker.dead_letter.assert_not_awaited()
    # ack was called exactly once (the early FIRE_AND_FORGET ack)
    broker.ack.assert_awaited_once_with(msg)
    # No error result stored
    backend.store_result.assert_not_awaited()


# ---------------------------------------------------------------------------
# 10. Watchdog cleanup: hard-timeout watchdog is always cancelled, even when
#     run_task raises an arbitrary exception (not just TimeoutError /
#     CancelledError). Regression test for the watchdog-leak bug where the
#     cancel lived only in the `else` branch of the try/except.
# ---------------------------------------------------------------------------


def _install_watchdog_tracker(worker: Worker) -> list[asyncio.Task]:
    """Patch worker._executor._hard_timeout_watchdog to track spawned watchdog tasks.

    Returns a list that the test can inspect after _handle_message completes.
    Each element is the actual asyncio.Task created for the watchdog coroutine.
    """
    created: list[asyncio.Task] = []
    original = worker._executor._hard_timeout_watchdog

    async def tracking_watchdog(run_task, timeout, set_reason_if_unset=None):
        # Record the *current* task (the watchdog task) on first yield so the
        # test can assert on its state after _handle_message returns.
        current = asyncio.current_task()
        if current is not None:
            created.append(current)
        await original(run_task, timeout, set_reason_if_unset)

    worker._executor._hard_timeout_watchdog = tracking_watchdog  # type: ignore[assignment]
    return created


@pytest.mark.asyncio
async def test_watchdog_cancelled_when_task_raises_exception(broker, backend, config):
    """Task with hard_timeout raises ValueError immediately -> watchdog must be cancelled."""

    class RaisingTask(Task):
        task_info = TaskInfo(
            name="raising",
            queues=["default"],
            hard_timeout=5,
            max_retries=0,
        )

        async def run(self) -> str:
            raise ValueError("boom")

    reg = TaskRegistry()
    reg.register(RaisingTask)
    w = Worker(broker=broker, backend=backend, registry=reg, config=config)

    tracked = _install_watchdog_tracker(w)

    # Snapshot tasks before so we can compute orphans added by _handle_message.
    tasks_before = asyncio.all_tasks()

    msg = make_message(_default_payload(name="raising", args=[], kwargs={}))
    await w._handle_message(msg)

    # Let any pending cancellations flush through the loop.
    await asyncio.sleep(0)

    # The watchdog must have been created and must be done() after handler
    # returns (cancelled, not still sleeping for hard_timeout seconds).
    assert len(tracked) == 1, "watchdog task should have been spawned exactly once"
    watchdog_task = tracked[0]
    assert watchdog_task.done(), "watchdog task must be done after _handle_message returns"

    # No new tasks (other than the current test task) should remain.
    tasks_after = asyncio.all_tasks()
    orphans = (tasks_after - tasks_before) - {asyncio.current_task()}
    # All orphans (if any) should be done — specifically, no pending watchdog.
    assert all(t.done() for t in orphans), f"orphan pending tasks survived _handle_message: {orphans}"

    # Error should still have been surfaced as an error result.
    stored = decode_result(backend.store_result.call_args[0][1])
    assert stored["is_error"] is True
    assert "boom" in stored["error_message"]


@pytest.mark.asyncio
async def test_watchdog_cancelled_when_task_soft_timeouts(broker, backend, config):
    """Soft timeout fires while hard_timeout watchdog is sleeping -> watchdog must be cancelled."""

    class SoftAndHardTask(Task):
        task_info = TaskInfo(
            name="soft_and_hard",
            queues=["default"],
            soft_timeout=0.05,
            hard_timeout=5,
            max_retries=0,
        )

        async def run(self) -> str:
            await asyncio.sleep(1)
            return "never"

    reg = TaskRegistry()
    reg.register(SoftAndHardTask)
    w = Worker(broker=broker, backend=backend, registry=reg, config=config)

    tracked = _install_watchdog_tracker(w)

    tasks_before = asyncio.all_tasks()

    msg = make_message(_default_payload(name="soft_and_hard", args=[], kwargs={}))
    await w._handle_message(msg)

    await asyncio.sleep(0)

    assert len(tracked) == 1
    watchdog_task = tracked[0]
    assert watchdog_task.done(), "watchdog must be cancelled when soft timeout fires first"

    tasks_after = asyncio.all_tasks()
    orphans = (tasks_after - tasks_before) - {asyncio.current_task()}
    assert all(t.done() for t in orphans), f"orphan pending tasks survived _handle_message: {orphans}"

    # Soft-timeout error result should have been stored.
    stored = decode_result(backend.store_result.call_args[0][1])
    assert stored["is_error"] is True
    assert "SoftTimeoutError" in stored["error_message"] or "soft timeout" in stored["error_message"].lower()


@pytest.mark.asyncio
async def test_watchdog_cancelled_on_success(broker, backend, config):
    """Baseline: watchdog is cleaned up when run_task completes successfully."""

    class FastTask(Task):
        task_info = TaskInfo(
            name="fast",
            queues=["default"],
            hard_timeout=5,
        )

        async def run(self, a: int, b: int) -> int:
            return a + b

    reg = TaskRegistry()
    reg.register(FastTask)
    w = Worker(broker=broker, backend=backend, registry=reg, config=config)

    tracked = _install_watchdog_tracker(w)

    tasks_before = asyncio.all_tasks()

    msg = make_message(_default_payload(name="fast", args=[7, 35], kwargs={}))
    await w._handle_message(msg)

    await asyncio.sleep(0)

    assert len(tracked) == 1
    watchdog_task = tracked[0]
    assert watchdog_task.done(), "watchdog must be cancelled after successful completion"

    tasks_after = asyncio.all_tasks()
    orphans = (tasks_after - tasks_before) - {asyncio.current_task()}
    assert all(t.done() for t in orphans), f"orphan pending tasks survived _handle_message: {orphans}"

    stored = decode_result(backend.store_result.call_args[0][1])
    assert stored["is_error"] is False
    assert stored["result"] == 42
