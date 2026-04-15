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
from swarmq.serialization import decode_result, encode_json
from swarmq.task import DeliveryMode, Task, TaskInfo
from swarmq.worker import Worker


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_message(payload: dict, headers: dict | None = None) -> MagicMock:
    """Create a mock AMQP message with JSON-encoded body."""
    msg = MagicMock()
    msg.body = encode_json(payload)
    msg.headers = headers or {}
    msg.ack = AsyncMock()
    msg.nack = AsyncMock()
    return msg


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


@pytest.fixture()
def broker():
    b = MagicMock(spec=RabbitMQBroker)
    b.connect = AsyncMock()
    b.declare_queue = AsyncMock()
    b.consume = AsyncMock()
    b.ack = AsyncMock()
    b.nack = AsyncMock()
    b.close = AsyncMock()
    b.publish_to_delay_queue = AsyncMock()
    b.publish_to_dead_letter_queue = AsyncMock()
    return b


@pytest.fixture()
def backend():
    be = MagicMock(spec=ValkeyBackend)
    be.connect = AsyncMock()
    be.store_result = AsyncMock()
    be.close = AsyncMock()
    return be


@pytest.fixture()
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

    msg = _make_message(_default_payload(name="slow_soft", args=[], kwargs={}))
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

    msg = _make_message(
        _default_payload(name="slow_retry", args=[], kwargs={}),
        headers={},
    )
    await w._handle_message(msg)

    # Should publish to delay queue (retry), NOT store error result
    broker.publish_to_delay_queue.assert_awaited_once()
    call_kwargs = broker.publish_to_delay_queue.call_args
    headers = call_kwargs.kwargs["headers"]
    assert headers["x-swarmq-retry-count"] == 1
    broker.ack.assert_awaited_once_with(msg)
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

    msg = _make_message(
        _default_payload(name="slow_hard", args=[], kwargs={}),
        headers={},
    )
    await w._handle_message(msg)

    # Should publish to delay queue (retry)
    broker.publish_to_delay_queue.assert_awaited_once()
    call_kwargs = broker.publish_to_delay_queue.call_args
    headers = call_kwargs.kwargs["headers"]
    assert headers["x-swarmq-retry-count"] == 1
    broker.ack.assert_awaited_once_with(msg)
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

    msg = _make_message(_default_payload(name="normal", args=[10, 20], kwargs={}))
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

    msg = _make_message(_default_payload(name="soft_post", args=[], kwargs={}))
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
    msg = _make_message(
        _default_payload(name="slow_dlq", args=[], kwargs={}),
        headers={"x-swarmq-retry-count": 2},
    )
    await w._handle_message(msg)

    # Should go to DLQ
    broker.publish_to_dead_letter_queue.assert_awaited_once()
    # Should NOT retry
    broker.publish_to_delay_queue.assert_not_awaited()
    broker.ack.assert_awaited_once_with(msg)


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

    msg = _make_message(
        _default_payload(name="hard_only", args=[], kwargs={}),
        headers={},
    )
    await w._handle_message(msg)

    # Should retry via delay queue (hard timeout triggers retry)
    broker.publish_to_delay_queue.assert_awaited_once()
    call_kwargs = broker.publish_to_delay_queue.call_args
    headers = call_kwargs.kwargs["headers"]
    assert headers["x-swarmq-retry-count"] == 1
    assert "hard timeout" in headers["x-swarmq-traceback"]
    broker.ack.assert_awaited_once_with(msg)


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

    msg = _make_message(
        _default_payload(name="fnf_hard", args=[], kwargs={}),
        headers={},
    )

    with patch("swarmq.worker.logger") as mock_logger:
        await w._handle_message(msg)

        # Error must be logged
        mock_logger.error.assert_called()

    # nack must NOT be called
    broker.nack.assert_not_awaited()
    # No retry, no DLQ
    broker.publish_to_delay_queue.assert_not_awaited()
    broker.publish_to_dead_letter_queue.assert_not_awaited()
    # ack was called exactly once (the early FIRE_AND_FORGET ack)
    broker.ack.assert_awaited_once_with(msg)
    # No error result stored
    backend.store_result.assert_not_awaited()
