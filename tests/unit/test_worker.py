"""Unit tests for Worker consumer loop."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import structlog

from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.config import SwarmQConfig
from swarmq.exceptions import (
    BackendConnectionError,
    NoRetry,
    ResultSerializationError,
    Retry,
    TaskNotFoundError,
)
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


def _make_raw_message(body: bytes) -> MagicMock:
    """Create a mock AMQP message with raw bytes body."""
    msg = MagicMock()
    msg.body = body
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
    b.cancel_consume = AsyncMock()
    b.publish_to_delay_queue = AsyncMock()
    b.publish_to_dead_letter_queue = AsyncMock()
    return b


@pytest.fixture()
def backend():
    be = MagicMock(spec=ValkeyBackend)
    be.connect = AsyncMock()
    be.store_result = AsyncMock()
    be.publish_notification = AsyncMock()
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


@pytest.fixture()
def registry():
    return TaskRegistry()


class AddTask(Task):
    task_info = TaskInfo(name="add", queues=["default"])

    async def run(self, a: int, b: int) -> int:
        return a + b


@pytest.fixture()
def worker(broker, backend, registry, config):
    registry.register(AddTask)
    return Worker(broker=broker, backend=backend, registry=registry, config=config)


# ---------------------------------------------------------------------------
# 1. Happy path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_happy_path_message_processed_and_acked(worker, broker, backend):
    msg = _make_message(_default_payload())

    await worker._handle_message(msg)

    broker.ack.assert_awaited_once_with(msg)
    assert backend.store_result.await_count == 1
    call_args = backend.store_result.call_args
    assert call_args[0][0] == "abc-123"  # task_id
    stored = decode_result(call_args[0][1])
    assert stored["is_error"] is False
    assert stored["result"] == 3
    assert stored["task"]["id"] == "abc-123"


# ---------------------------------------------------------------------------
# 2. Task with return value serialized in result dict
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_return_value_serialized_in_result_dict(worker, backend):
    msg = _make_message(_default_payload(args=[10, 20]))

    await worker._handle_message(msg)

    stored = decode_result(backend.store_result.call_args[0][1])
    assert stored["result"] == 30
    assert stored["is_error"] is False
    assert stored["error_message"] == ""


# ---------------------------------------------------------------------------
# 3. pre_execution and post_execution are called
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pre_and_post_execution_called(broker, backend, config):
    call_order: list[str] = []

    class LifecycleTask(Task):
        task_info = TaskInfo(name="lifecycle", queues=["default"])

        async def pre_execution(self) -> None:
            call_order.append("pre")

        async def run(self) -> str:
            call_order.append("run")
            return "ok"

        async def post_execution(self) -> None:
            call_order.append("post")

    reg = TaskRegistry()
    reg.register(LifecycleTask)
    w = Worker(broker=broker, backend=backend, registry=reg, config=config)

    msg = _make_message(_default_payload(name="lifecycle", args=[], kwargs={}))
    await w._handle_message(msg)

    assert call_order == ["pre", "run", "post"]


# ---------------------------------------------------------------------------
# 4. Concurrency semaphore limits parallel executions
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_semaphore_limits_concurrency(broker, backend, config):
    concurrency = 2
    config_limited = SwarmQConfig(
        broker_url="amqp://localhost",
        backend_url="valkey://localhost",
        concurrency=concurrency,
        queues=["default"],
    )
    max_concurrent = 0
    current = 0
    lock = asyncio.Lock()

    class SlowTask(Task):
        task_info = TaskInfo(name="slow", queues=["default"])

        async def run(self) -> None:
            nonlocal max_concurrent, current
            async with lock:
                current += 1
                if current > max_concurrent:
                    max_concurrent = current
            await asyncio.sleep(0.05)
            async with lock:
                current -= 1

    reg = TaskRegistry()
    reg.register(SlowTask)
    w = Worker(broker=broker, backend=backend, registry=reg, config=config_limited)

    messages = [
        _make_message(_default_payload(task_id=f"t-{i}", name="slow", args=[], kwargs={}))
        for i in range(6)
    ]
    await asyncio.gather(*(w._handle_message(m) for m in messages))

    assert max_concurrent <= concurrency


# ---------------------------------------------------------------------------
# 5. result_ttl=0 skips store_result
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_result_ttl_zero_skips_store(broker, backend, config):
    class NoStoreTask(Task):
        task_info = TaskInfo(name="nostore", queues=["default"], result_ttl=0)

        async def run(self) -> str:
            return "ignored"

    reg = TaskRegistry()
    reg.register(NoStoreTask)
    w = Worker(broker=broker, backend=backend, registry=reg, config=config)

    msg = _make_message(_default_payload(name="nostore", args=[], kwargs={}))
    await w._handle_message(msg)

    backend.store_result.assert_not_awaited()
    broker.ack.assert_awaited_once_with(msg)


# ---------------------------------------------------------------------------
# 6. Task raises exception -> error result stored, acked
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_task_exception_stores_error_result(broker, backend, config):
    class FailTask(Task):
        task_info = TaskInfo(name="fail", queues=["default"])

        async def run(self) -> None:
            raise ValueError("boom")

    reg = TaskRegistry()
    reg.register(FailTask)
    w = Worker(broker=broker, backend=backend, registry=reg, config=config)

    msg = _make_message(_default_payload(name="fail", args=[], kwargs={}))
    await w._handle_message(msg)

    stored = decode_result(backend.store_result.call_args[0][1])
    assert stored["is_error"] is True
    assert "boom" in stored["error_message"]
    assert stored["result"] is None
    broker.ack.assert_awaited_once_with(msg)


# ---------------------------------------------------------------------------
# 7. pre_execution raises -> error stored, post NOT called
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pre_execution_raises_skips_post(broker, backend, config):
    post_called = False

    class PreFailTask(Task):
        task_info = TaskInfo(name="prefail", queues=["default"])

        async def pre_execution(self) -> None:
            raise RuntimeError("pre failed")

        async def run(self) -> str:
            return "never"

        async def post_execution(self) -> None:
            nonlocal post_called
            post_called = True

    reg = TaskRegistry()
    reg.register(PreFailTask)
    w = Worker(broker=broker, backend=backend, registry=reg, config=config)

    msg = _make_message(_default_payload(name="prefail", args=[], kwargs={}))
    await w._handle_message(msg)

    assert post_called is False
    stored = decode_result(backend.store_result.call_args[0][1])
    assert stored["is_error"] is True
    assert "pre failed" in stored["error_message"]
    broker.ack.assert_awaited_once_with(msg)


# ---------------------------------------------------------------------------
# 8. post_execution raises -> error overwrites success
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_post_execution_raises_overwrites_success(broker, backend, config):
    class PostFailTask(Task):
        task_info = TaskInfo(name="postfail", queues=["default"])

        async def run(self) -> str:
            return "ok"

        async def post_execution(self) -> None:
            raise RuntimeError("post failed")

    reg = TaskRegistry()
    reg.register(PostFailTask)
    w = Worker(broker=broker, backend=backend, registry=reg, config=config)

    msg = _make_message(_default_payload(name="postfail", args=[], kwargs={}))
    await w._handle_message(msg)

    stored = decode_result(backend.store_result.call_args[0][1])
    assert stored["is_error"] is True
    assert "post failed" in stored["error_message"]
    broker.ack.assert_awaited_once_with(msg)


# ---------------------------------------------------------------------------
# 9. Result serialization fails -> error result stored, acked
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_result_serialization_error_stores_error(broker, backend, config):
    class BadResultTask(Task):
        task_info = TaskInfo(name="badresult", queues=["default"])

        async def run(self) -> object:
            return object()  # not serializable by msgpack

    reg = TaskRegistry()
    reg.register(BadResultTask)
    w = Worker(broker=broker, backend=backend, registry=reg, config=config)

    msg = _make_message(_default_payload(name="badresult", args=[], kwargs={}))
    await w._handle_message(msg)

    stored = decode_result(backend.store_result.call_args[0][1])
    assert stored["is_error"] is True
    assert stored["result"] is None
    broker.ack.assert_awaited_once_with(msg)
    broker.nack.assert_not_awaited()


# ---------------------------------------------------------------------------
# 10. Task not in registry -> nack(requeue=False)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_unknown_task_nacked_no_requeue(worker, broker, backend):
    msg = _make_message(_default_payload(name="nonexistent"))

    await worker._handle_message(msg)

    broker.nack.assert_awaited_once_with(msg, requeue=False)
    broker.ack.assert_not_awaited()
    backend.store_result.assert_not_awaited()


# ---------------------------------------------------------------------------
# 11. Backend connection error -> nack(requeue=True)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_backend_error_nacks_with_requeue(worker, broker, backend):
    backend.store_result = AsyncMock(
        side_effect=BackendConnectionError("conn lost")
    )
    msg = _make_message(_default_payload())

    await worker._handle_message(msg)

    broker.nack.assert_awaited_once_with(msg, requeue=True)
    broker.ack.assert_not_awaited()


# ---------------------------------------------------------------------------
# 12. Invalid JSON -> nack(requeue=False)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_invalid_json_nacked_no_requeue(worker, broker, backend):
    msg = _make_raw_message(b"not valid json {{{")

    await worker._handle_message(msg)

    broker.nack.assert_awaited_once_with(msg, requeue=False)
    broker.ack.assert_not_awaited()
    backend.store_result.assert_not_awaited()


# ---------------------------------------------------------------------------
# 13. Missing task.name field -> nack(requeue=False)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_missing_task_name_nacked_no_requeue(worker, broker, backend):
    payload = {"task": {"id": "abc", "queue": "default"}, "args": [], "kwargs": {}}
    msg = _make_message(payload)

    await worker._handle_message(msg)

    broker.nack.assert_awaited_once_with(msg, requeue=False)
    broker.ack.assert_not_awaited()
    backend.store_result.assert_not_awaited()


# ---------------------------------------------------------------------------
# 14. Task with None return -> result stored with None
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_none_return_stored(broker, backend, config):
    class NoneTask(Task):
        task_info = TaskInfo(name="nonetask", queues=["default"])

        async def run(self) -> None:
            return None

    reg = TaskRegistry()
    reg.register(NoneTask)
    w = Worker(broker=broker, backend=backend, registry=reg, config=config)

    msg = _make_message(_default_payload(name="nonetask", args=[], kwargs={}))
    await w._handle_message(msg)

    stored = decode_result(backend.store_result.call_args[0][1])
    assert stored["result"] is None
    assert stored["is_error"] is False


# ---------------------------------------------------------------------------
# 15. Semaphore acquired after deserialization, released in finally
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_semaphore_released_on_error(broker, backend, config):
    """Semaphore is released even when the task raises."""

    class ExplodeTask(Task):
        task_info = TaskInfo(name="explode", queues=["default"])

        async def run(self) -> None:
            raise RuntimeError("kaboom")

    reg = TaskRegistry()
    reg.register(ExplodeTask)
    config_one = SwarmQConfig(
        broker_url="amqp://localhost",
        backend_url="valkey://localhost",
        concurrency=1,
        queues=["default"],
    )
    w = Worker(broker=broker, backend=backend, registry=reg, config=config_one)

    msg1 = _make_message(_default_payload(name="explode", args=[], kwargs={}))
    msg2 = _make_message(_default_payload(name="explode", args=[], kwargs={}))

    # If semaphore leaks, second message would hang forever
    await asyncio.wait_for(
        asyncio.gather(w._handle_message(msg1), w._handle_message(msg2)),
        timeout=2.0,
    )

    assert broker.ack.await_count == 2


# ---------------------------------------------------------------------------
# 16. _on_message spawns a task instead of blocking
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_on_message_creates_task(worker, broker, backend):
    """_on_message should return immediately, spawning _handle_message as a task."""
    msg = _make_message(_default_payload())

    await worker._on_message(msg)

    # The spawned task hasn't run yet; give the event loop enough ticks
    # to complete (including internal create_task for run coroutine)
    await asyncio.sleep(0.01)

    broker.ack.assert_awaited_once_with(msg)
    assert backend.store_result.await_count == 1


# ---------------------------------------------------------------------------
# start / stop
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_start_connects_and_declares_queues(worker, broker, backend):
    # Trigger shutdown immediately so start() returns
    worker._shutdown_event.set()
    await worker.start()

    broker.connect.assert_awaited_once()
    backend.connect.assert_awaited_once()
    broker.declare_queue.assert_awaited_once_with("default")
    broker.consume.assert_awaited_once()


@pytest.mark.asyncio
async def test_stop_closes_connections(worker, broker, backend):
    await worker.stop()

    broker.close.assert_awaited_once()
    backend.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_start_declares_all_config_queues(broker, backend):
    cfg = SwarmQConfig(
        broker_url="amqp://localhost",
        backend_url="valkey://localhost",
        queues=["q1", "q2", "q3"],
    )
    reg = TaskRegistry()

    class MultiTask(Task):
        task_info = TaskInfo(name="multi", queues=["q1", "q2", "q3"])

        async def run(self) -> None:
            pass

    reg.register(MultiTask)
    w = Worker(broker=broker, backend=backend, registry=reg, config=cfg)

    # Trigger shutdown immediately so start() returns
    w._shutdown_event.set()
    await w.start()

    declared = {call.args[0] for call in broker.declare_queue.call_args_list}
    assert declared == {"q1", "q2", "q3"}
    assert broker.consume.await_count == 3


# ---------------------------------------------------------------------------
# result_ttl resolution
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_result_ttl_from_task_info(broker, backend, config):
    class TtlTask(Task):
        task_info = TaskInfo(name="ttltask", queues=["default"], result_ttl=300)

        async def run(self) -> str:
            return "ttl"

    reg = TaskRegistry()
    reg.register(TtlTask)
    w = Worker(broker=broker, backend=backend, registry=reg, config=config)

    msg = _make_message(_default_payload(name="ttltask", args=[], kwargs={}))
    await w._handle_message(msg)

    call_args = backend.store_result.call_args
    assert call_args[0][2] == 300_000  # ttl_ms = result_ttl * 1000


@pytest.mark.asyncio
async def test_result_ttl_falls_back_to_config(worker, backend):
    msg = _make_message(_default_payload())

    await worker._handle_message(msg)

    # AddTask has result_ttl=None, config default is 86400
    call_args = backend.store_result.call_args
    assert call_args[0][2] == 86_400_000


# ---------------------------------------------------------------------------
# structlog integration
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_handle_message_binds_contextvars(broker, backend, config):
    """_handle_message binds task_id, task_name, queue via structlog.contextvars."""
    captured_ctx = {}

    class CapTask(Task):
        task_info = TaskInfo(name="captask", queues=["default"])

        async def run(self) -> str:
            captured_ctx.update(structlog.contextvars.get_contextvars())
            return "ok"

    reg = TaskRegistry()
    reg.register(CapTask)
    w = Worker(broker=broker, backend=backend, registry=reg, config=config)

    structlog.contextvars.clear_contextvars()

    msg = _make_message(
        _default_payload(task_id="ctx-123", name="captask", queue="myqueue", args=[], kwargs={})
    )
    await w._handle_message(msg)

    assert captured_ctx["task_id"] == "ctx-123"
    assert captured_ctx["task_name"] == "captask"
    assert captured_ctx["queue"] == "myqueue"

    # After finally block, contextvars should be cleared
    ctx = structlog.contextvars.get_contextvars()
    assert "task_id" not in ctx


@pytest.mark.asyncio
async def test_start_logs_startup_report(broker, backend, config):
    """Worker.start() logs a startup report with INFO level."""
    reg = TaskRegistry()
    reg.register(AddTask)
    w = Worker(broker=broker, backend=backend, registry=reg, config=config)

    with patch("swarmq.worker.logger") as mock_logger:
        # Trigger shutdown immediately so start() returns
        w._shutdown_event.set()
        await w.start()

        mock_logger.info.assert_called_once()
        call_args, call_kwargs = mock_logger.info.call_args
        assert call_args[0] == "worker_started"
        assert "pid" in call_kwargs
        assert "python_version" in call_kwargs
        assert "queues" in call_kwargs
        assert "concurrency" in call_kwargs
        assert "registered_tasks" in call_kwargs
        assert "broker_url" in call_kwargs
        assert "backend_url" in call_kwargs


# ---------------------------------------------------------------------------
# Delivery Modes (Unit 2.1)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fire_and_forget_acks_before_execution(broker, backend, config):
    """FIRE_AND_FORGET acks the message BEFORE task execution begins."""
    ack_happened_before_run = False

    class FnfTask(Task):
        task_info = TaskInfo(
            name="fnf", queues=["default"], delivery=DeliveryMode.FIRE_AND_FORGET
        )

        async def run(self) -> str:
            nonlocal ack_happened_before_run
            # At the time run() executes, ack should already have been called
            ack_happened_before_run = broker.ack.await_count > 0
            return "done"

    reg = TaskRegistry()
    reg.register(FnfTask)
    w = Worker(broker=broker, backend=backend, registry=reg, config=config)

    msg = _make_message(_default_payload(name="fnf", args=[], kwargs={}))
    await w._handle_message(msg)

    assert ack_happened_before_run is True
    # ack should have been called exactly once
    broker.ack.assert_awaited_once_with(msg)


@pytest.mark.asyncio
async def test_at_least_once_acks_after_execution(broker, backend, config):
    """AT_LEAST_ONCE acks the message AFTER execution + result-write."""
    ack_count_during_run = -1

    class AloTask(Task):
        task_info = TaskInfo(
            name="alo", queues=["default"], delivery=DeliveryMode.AT_LEAST_ONCE
        )

        async def run(self) -> str:
            nonlocal ack_count_during_run
            ack_count_during_run = broker.ack.await_count
            return "ok"

    reg = TaskRegistry()
    reg.register(AloTask)
    w = Worker(broker=broker, backend=backend, registry=reg, config=config)

    msg = _make_message(_default_payload(name="alo", args=[], kwargs={}))
    await w._handle_message(msg)

    # ack must NOT have been called during run()
    assert ack_count_during_run == 0
    # ack must be called after result stored
    broker.ack.assert_awaited_once_with(msg)
    assert backend.store_result.await_count == 1


@pytest.mark.asyncio
async def test_fire_and_forget_with_explicit_result_ttl_stores_result(broker, backend, config):
    """FIRE_AND_FORGET with explicit result_ttl=3600 stores the result."""

    class FnfWithTtlTask(Task):
        task_info = TaskInfo(
            name="fnf_ttl",
            queues=["default"],
            delivery=DeliveryMode.FIRE_AND_FORGET,
            result_ttl=3600,
        )

        async def run(self) -> str:
            return "stored"

    reg = TaskRegistry()
    reg.register(FnfWithTtlTask)
    w = Worker(broker=broker, backend=backend, registry=reg, config=config)

    msg = _make_message(_default_payload(name="fnf_ttl", args=[], kwargs={}))
    await w._handle_message(msg)

    # Result MUST be stored with the explicit TTL
    assert backend.store_result.await_count == 1
    call_args = backend.store_result.call_args
    assert call_args[0][2] == 3_600_000  # ttl_ms = 3600 * 1000
    stored = decode_result(call_args[0][1])
    assert stored["result"] == "stored"
    assert stored["is_error"] is False


@pytest.mark.asyncio
async def test_fire_and_forget_without_result_ttl_skips_storage(broker, backend, config):
    """FIRE_AND_FORGET with result_ttl=None defaults to result_ttl=0, skipping storage."""

    class FnfNoTtlTask(Task):
        task_info = TaskInfo(
            name="fnf_no_ttl",
            queues=["default"],
            delivery=DeliveryMode.FIRE_AND_FORGET,
            # result_ttl defaults to None
        )

        async def run(self) -> str:
            return "not stored"

    reg = TaskRegistry()
    reg.register(FnfNoTtlTask)
    w = Worker(broker=broker, backend=backend, registry=reg, config=config)

    msg = _make_message(_default_payload(name="fnf_no_ttl", args=[], kwargs={}))
    await w._handle_message(msg)

    # Result must NOT be stored
    backend.store_result.assert_not_awaited()
    # Ack already happened early (FIRE_AND_FORGET)
    broker.ack.assert_awaited_once_with(msg)


@pytest.mark.asyncio
async def test_fire_and_forget_exception_logged_no_nack(broker, backend, config):
    """FIRE_AND_FORGET task that throws: exception is logged, nack is NOT called."""

    class FnfFailTask(Task):
        task_info = TaskInfo(
            name="fnf_fail",
            queues=["default"],
            delivery=DeliveryMode.FIRE_AND_FORGET,
        )

        async def run(self) -> None:
            raise RuntimeError("fnf boom")

    reg = TaskRegistry()
    reg.register(FnfFailTask)
    w = Worker(broker=broker, backend=backend, registry=reg, config=config)

    msg = _make_message(_default_payload(name="fnf_fail", args=[], kwargs={}))

    with patch("swarmq.worker.logger") as mock_logger:
        await w._handle_message(msg)

        # Error must be logged
        mock_logger.error.assert_called()

    # nack must NOT be called (message was already acked)
    broker.nack.assert_not_awaited()
    # ack was called exactly once (the early ack)
    broker.ack.assert_awaited_once_with(msg)


@pytest.mark.asyncio
async def test_delivery_mode_default_is_at_least_once(broker, backend, config):
    """TaskInfo without explicit delivery defaults to AT_LEAST_ONCE (backwards compatible)."""

    class DefaultDeliveryTask(Task):
        task_info = TaskInfo(name="default_delivery", queues=["default"])

        async def run(self) -> str:
            return "ok"

    # Verify the default at the TaskInfo level
    assert DefaultDeliveryTask.task_info.delivery is DeliveryMode.AT_LEAST_ONCE

    # Verify runtime behavior: ack happens AFTER result storage (AT_LEAST_ONCE)
    ack_count_during_run = -1

    class DefaultDeliveryTask2(Task):
        task_info = TaskInfo(name="default_delivery2", queues=["default"])

        async def run(self) -> str:
            nonlocal ack_count_during_run
            ack_count_during_run = broker.ack.await_count
            return "ok"

    reg = TaskRegistry()
    reg.register(DefaultDeliveryTask2)
    w = Worker(broker=broker, backend=backend, registry=reg, config=config)

    msg = _make_message(_default_payload(name="default_delivery2", args=[], kwargs={}))
    await w._handle_message(msg)

    assert ack_count_during_run == 0  # not acked during execution
    broker.ack.assert_awaited_once_with(msg)  # acked after


# ---------------------------------------------------------------------------
# Retry System (Unit 2.2 + 2.3)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_retry_first_failure_publishes_to_delay_queue(broker, backend, config):
    """Task with max_retries=3, first failure -> message published to .DQ with correct TTL."""

    class RetryTask(Task):
        task_info = TaskInfo(
            name="retry_task", queues=["default"], max_retries=3, retry_base_delay=1.0
        )

        async def run(self) -> None:
            raise RuntimeError("transient")

    reg = TaskRegistry()
    reg.register(RetryTask)
    w = Worker(broker=broker, backend=backend, registry=reg, config=config)

    msg = _make_message(
        _default_payload(name="retry_task", args=[], kwargs={}),
        headers={},
    )
    await w._handle_message(msg)

    # Should publish to delay queue, not store error result
    broker.publish_to_delay_queue.assert_awaited_once()
    call_kwargs = broker.publish_to_delay_queue.call_args
    queue_name = call_kwargs[0][0]
    assert queue_name == "default"
    # delay_ms should be within jitter bounds: base=1.0, attempt=0
    # exponential: 1.0 * 2^0 * jitter(1,2) = [1.0, 2.0] seconds = [1000, 2000] ms
    delay_ms = call_kwargs.kwargs["delay_ms"]
    assert 1000 <= delay_ms <= 2000
    # Original message acked
    broker.ack.assert_awaited_once_with(msg)
    # No error result stored
    backend.store_result.assert_not_awaited()


@pytest.mark.asyncio
async def test_retry_count_incremented_in_headers(broker, backend, config):
    """x-swarmq-retry-count header is incremented from 0 to 1 on first retry."""

    class RetryTask(Task):
        task_info = TaskInfo(
            name="retry_inc", queues=["default"], max_retries=3, retry_base_delay=1.0
        )

        async def run(self) -> None:
            raise RuntimeError("fail")

    reg = TaskRegistry()
    reg.register(RetryTask)
    w = Worker(broker=broker, backend=backend, registry=reg, config=config)

    # First attempt (retry_count=0 in headers)
    msg = _make_message(
        _default_payload(name="retry_inc", args=[], kwargs={}),
        headers={"x-swarmq-retry-count": 0},
    )
    await w._handle_message(msg)

    call_kwargs = broker.publish_to_delay_queue.call_args
    headers = call_kwargs.kwargs["headers"]
    assert headers["x-swarmq-retry-count"] == 1

    # Second attempt (retry_count=1 in headers)
    broker.publish_to_delay_queue.reset_mock()
    broker.ack.reset_mock()
    msg2 = _make_message(
        _default_payload(name="retry_inc", args=[], kwargs={}),
        headers={"x-swarmq-retry-count": 1},
    )
    await w._handle_message(msg2)

    call_kwargs2 = broker.publish_to_delay_queue.call_args
    headers2 = call_kwargs2.kwargs["headers"]
    assert headers2["x-swarmq-retry-count"] == 2


@pytest.mark.asyncio
async def test_retries_exhausted_sends_to_dlq(broker, backend, config):
    """After max_retries exhausted -> published to .XQ, original acked."""

    class RetryTask(Task):
        task_info = TaskInfo(
            name="retry_exhaust", queues=["default"], max_retries=2
        )

        async def run(self) -> None:
            raise RuntimeError("still broken")

    reg = TaskRegistry()
    reg.register(RetryTask)
    w = Worker(broker=broker, backend=backend, registry=reg, config=config)

    # Simulate retry_count=2 (== max_retries) -> exhausted
    msg = _make_message(
        _default_payload(name="retry_exhaust", args=[], kwargs={}),
        headers={"x-swarmq-retry-count": 2},
    )
    await w._handle_message(msg)

    # Should publish to DLQ
    broker.publish_to_dead_letter_queue.assert_awaited_once()
    dlq_call = broker.publish_to_dead_letter_queue.call_args
    assert dlq_call[0][0] == "default"  # queue_name
    # Should NOT publish to delay queue
    broker.publish_to_delay_queue.assert_not_awaited()
    # Original message acked
    broker.ack.assert_awaited_once_with(msg)


@pytest.mark.asyncio
async def test_retry_exception_overrides_backoff_delay(broker, backend, config):
    """raise Retry(delay=60) -> retry with 60s delay, overriding backoff calculation."""

    class RetryDelayTask(Task):
        task_info = TaskInfo(
            name="retry_delay", queues=["default"], max_retries=3, retry_base_delay=1.0
        )

        async def run(self) -> None:
            raise Retry(delay=60)

    reg = TaskRegistry()
    reg.register(RetryDelayTask)
    w = Worker(broker=broker, backend=backend, registry=reg, config=config)

    msg = _make_message(
        _default_payload(name="retry_delay", args=[], kwargs={}),
        headers={},
    )
    await w._handle_message(msg)

    broker.publish_to_delay_queue.assert_awaited_once()
    call_kwargs = broker.publish_to_delay_queue.call_args
    delay_ms = call_kwargs.kwargs["delay_ms"]
    # Should be exactly 60 seconds = 60000 ms (no jitter)
    assert delay_ms == 60000
    broker.ack.assert_awaited_once_with(msg)


@pytest.mark.asyncio
async def test_noretry_exception_sends_to_dlq_directly(broker, backend, config):
    """raise NoRetry('bad input') -> direct DLQ, no retry regardless of max_retries."""

    class NoRetryTask(Task):
        task_info = TaskInfo(
            name="noretry_task", queues=["default"], max_retries=10
        )

        async def run(self) -> None:
            raise NoRetry("bad input")

    reg = TaskRegistry()
    reg.register(NoRetryTask)
    w = Worker(broker=broker, backend=backend, registry=reg, config=config)

    msg = _make_message(
        _default_payload(name="noretry_task", args=[], kwargs={}),
        headers={},
    )
    await w._handle_message(msg)

    # Should go to DLQ directly
    broker.publish_to_dead_letter_queue.assert_awaited_once()
    # Should NOT retry
    broker.publish_to_delay_queue.assert_not_awaited()
    broker.ack.assert_awaited_once()


@pytest.mark.asyncio
async def test_retry_exceptions_mismatch_sends_to_dlq(broker, backend, config):
    """retry_exceptions=[ConnectionError], ValueError raised -> DLQ, no retry."""

    class FilteredRetryTask(Task):
        task_info = TaskInfo(
            name="filtered_retry",
            queues=["default"],
            max_retries=5,
            retry_exceptions=[ConnectionError],
        )

        async def run(self) -> None:
            raise ValueError("not retryable")

    reg = TaskRegistry()
    reg.register(FilteredRetryTask)
    w = Worker(broker=broker, backend=backend, registry=reg, config=config)

    msg = _make_message(
        _default_payload(name="filtered_retry", args=[], kwargs={}),
        headers={},
    )
    await w._handle_message(msg)

    # ValueError is not in retry_exceptions -> DLQ
    broker.publish_to_dead_letter_queue.assert_awaited_once()
    broker.publish_to_delay_queue.assert_not_awaited()
    broker.ack.assert_awaited_once()


@pytest.mark.asyncio
async def test_retry_exceptions_match_retries(broker, backend, config):
    """retry_exceptions=[ConnectionError], ConnectionError raised -> retry."""

    class FilteredRetryTask(Task):
        task_info = TaskInfo(
            name="filtered_retry_match",
            queues=["default"],
            max_retries=5,
            retry_exceptions=[ConnectionError],
        )

        async def run(self) -> None:
            raise ConnectionError("connection lost")

    reg = TaskRegistry()
    reg.register(FilteredRetryTask)
    w = Worker(broker=broker, backend=backend, registry=reg, config=config)

    msg = _make_message(
        _default_payload(name="filtered_retry_match", args=[], kwargs={}),
        headers={},
    )
    await w._handle_message(msg)

    # ConnectionError matches retry_exceptions -> retry
    broker.publish_to_delay_queue.assert_awaited_once()
    broker.publish_to_dead_letter_queue.assert_not_awaited()
    broker.ack.assert_awaited_once()


@pytest.mark.asyncio
async def test_max_retries_zero_no_retry_stores_error(broker, backend, config):
    """max_retries=0 (default) -> no retry, error-result stored and acked."""

    class NoRetryDefaultTask(Task):
        task_info = TaskInfo(name="no_retry_default", queues=["default"])
        # max_retries defaults to 0

        async def run(self) -> None:
            raise RuntimeError("error without retry")

    reg = TaskRegistry()
    reg.register(NoRetryDefaultTask)
    w = Worker(broker=broker, backend=backend, registry=reg, config=config)

    msg = _make_message(
        _default_payload(name="no_retry_default", args=[], kwargs={}),
        headers={},
    )
    await w._handle_message(msg)

    # No retry, no DLQ — just store error result and ack
    broker.publish_to_delay_queue.assert_not_awaited()
    broker.publish_to_dead_letter_queue.assert_not_awaited()
    backend.store_result.assert_awaited_once()
    stored = decode_result(backend.store_result.call_args[0][1])
    assert stored["is_error"] is True
    assert "error without retry" in stored["error_message"]
    broker.ack.assert_awaited_once()


@pytest.mark.asyncio
async def test_fire_and_forget_not_retried(broker, backend, config):
    """FIRE_AND_FORGET tasks are never retried, even with max_retries > 0."""

    class FnfRetryTask(Task):
        task_info = TaskInfo(
            name="fnf_retry",
            queues=["default"],
            delivery=DeliveryMode.FIRE_AND_FORGET,
            max_retries=5,
        )

        async def run(self) -> None:
            raise RuntimeError("fnf failure")

    reg = TaskRegistry()
    reg.register(FnfRetryTask)
    w = Worker(broker=broker, backend=backend, registry=reg, config=config)

    msg = _make_message(
        _default_payload(name="fnf_retry", args=[], kwargs={}),
        headers={},
    )
    await w._handle_message(msg)

    # FIRE_AND_FORGET: no retry, no DLQ publish, no error result
    broker.publish_to_delay_queue.assert_not_awaited()
    broker.publish_to_dead_letter_queue.assert_not_awaited()
    backend.store_result.assert_not_awaited()
    # Ack happened once (the early FIRE_AND_FORGET ack)
    broker.ack.assert_awaited_once()


# ---------------------------------------------------------------------------
# Pub/Sub Notification after store_result (Unit 2)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_publish_notification_after_success(worker, broker, backend):
    """After successful task -> publish_notification(task_id) called."""
    msg = _make_message(_default_payload())

    await worker._handle_message(msg)

    backend.publish_notification.assert_awaited_once_with("abc-123")


@pytest.mark.asyncio
async def test_publish_notification_after_error(broker, backend, config):
    """After error task -> publish_notification(task_id) called."""

    class ErrorTask(Task):
        task_info = TaskInfo(name="error_notify", queues=["default"])

        async def run(self) -> None:
            raise ValueError("task failed")

    reg = TaskRegistry()
    reg.register(ErrorTask)
    w = Worker(broker=broker, backend=backend, registry=reg, config=config)

    msg = _make_message(_default_payload(name="error_notify", args=[], kwargs={}))
    await w._handle_message(msg)

    backend.publish_notification.assert_awaited_once_with("abc-123")


@pytest.mark.asyncio
async def test_publish_notification_after_dlq(broker, backend, config):
    """After DLQ routing -> publish_notification(task_id) called."""

    class DlqTask(Task):
        task_info = TaskInfo(
            name="dlq_notify", queues=["default"], max_retries=1
        )

        async def run(self) -> None:
            raise RuntimeError("permanent failure")

    reg = TaskRegistry()
    reg.register(DlqTask)
    w = Worker(broker=broker, backend=backend, registry=reg, config=config)

    # retry_count == max_retries -> exhausted -> DLQ
    msg = _make_message(
        _default_payload(name="dlq_notify", args=[], kwargs={}),
        headers={"x-swarmq-retry-count": 1},
    )
    await w._handle_message(msg)

    broker.publish_to_dead_letter_queue.assert_awaited_once()
    backend.publish_notification.assert_awaited_once_with("abc-123")


@pytest.mark.asyncio
async def test_publish_notification_skipped_when_result_ttl_zero(broker, backend, config):
    """result_ttl=0 -> publish_notification NOT called."""

    class NoTtlTask(Task):
        task_info = TaskInfo(name="no_ttl_notify", queues=["default"], result_ttl=0)

        async def run(self) -> str:
            return "ignored"

    reg = TaskRegistry()
    reg.register(NoTtlTask)
    w = Worker(broker=broker, backend=backend, registry=reg, config=config)

    msg = _make_message(_default_payload(name="no_ttl_notify", args=[], kwargs={}))
    await w._handle_message(msg)

    backend.store_result.assert_not_awaited()
    backend.publish_notification.assert_not_awaited()


@pytest.mark.asyncio
async def test_publish_notification_failure_does_not_block_ack(broker, backend, config):
    """publish_notification failure -> task ack still succeeds (fire-and-forget)."""
    backend.publish_notification = AsyncMock(side_effect=Exception("pubsub down"))

    class NotifyFailTask(Task):
        task_info = TaskInfo(name="notify_fail", queues=["default"])

        async def run(self) -> str:
            return "ok"

    reg = TaskRegistry()
    reg.register(NotifyFailTask)
    w = Worker(broker=broker, backend=backend, registry=reg, config=config)

    msg = _make_message(_default_payload(name="notify_fail", args=[], kwargs={}))
    await w._handle_message(msg)

    # publish_notification was called (and failed)
    backend.publish_notification.assert_awaited_once_with("abc-123")
    # But ack still happened
    broker.ack.assert_awaited_once_with(msg)
    # And the result was stored
    backend.store_result.assert_awaited_once()


# ---------------------------------------------------------------------------
# Unit 4: Worker Middleware + Signal integration
# ---------------------------------------------------------------------------


def _make_middleware_chain_mock() -> MagicMock:
    """Build a MagicMock MiddlewareChain with all 6 run_* methods as AsyncMock."""
    from swarmq.middleware import MiddlewareChain

    chain = MagicMock(spec=MiddlewareChain)
    chain.run_pre_enqueue = AsyncMock()
    chain.run_post_enqueue = AsyncMock()
    chain.run_pre_execute = AsyncMock()
    chain.run_post_execute = AsyncMock()
    chain.run_on_error = AsyncMock()
    chain.run_on_retry = AsyncMock()
    return chain


def _make_signal_dispatcher_mock() -> MagicMock:
    """Build a MagicMock SignalDispatcher with dispatch as AsyncMock."""
    from swarmq.signals import SignalDispatcher

    dispatcher = MagicMock(spec=SignalDispatcher)
    dispatcher.dispatch = AsyncMock()
    return dispatcher


class TestWorkerMiddlewareAndSignals:
    """Unit 4: Worker integration for MiddlewareChain and SignalDispatcher."""

    @pytest.mark.asyncio
    async def test_pre_execute_called_before_run(self, broker, backend, config):
        """pre_execute runs before task.run()."""
        call_order: list[str] = []

        mw_chain = _make_middleware_chain_mock()
        mw_chain.run_pre_execute.side_effect = (
            lambda *a, **kw: call_order.append("pre_execute") or None
        )

        class OrderTask(Task):
            task_info = TaskInfo(name="order_pre", queues=["default"])

            async def run(self) -> str:
                call_order.append("run")
                return "ok"

        reg = TaskRegistry()
        reg.register(OrderTask)
        w = Worker(
            broker=broker,
            backend=backend,
            registry=reg,
            config=config,
            middleware_chain=mw_chain,
        )

        msg = _make_message(_default_payload(name="order_pre", args=[], kwargs={}))
        await w._handle_message(msg)

        assert call_order == ["pre_execute", "run"]
        mw_chain.run_pre_execute.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_post_execute_called_after_successful_run(
        self, broker, backend, config
    ):
        """post_execute runs after a successful task.run() completes (including post_execution)."""

        class GoodTask(Task):
            task_info = TaskInfo(name="good_post", queues=["default"])

            async def run(self) -> int:
                return 42

        mw_chain = _make_middleware_chain_mock()

        reg = TaskRegistry()
        reg.register(GoodTask)
        w = Worker(
            broker=broker,
            backend=backend,
            registry=reg,
            config=config,
            middleware_chain=mw_chain,
        )

        msg = _make_message(_default_payload(name="good_post", args=[], kwargs={}))
        await w._handle_message(msg)

        mw_chain.run_post_execute.assert_awaited_once()
        call_kwargs = mw_chain.run_post_execute.call_args.kwargs
        assert call_kwargs["result"] == 42
        # on_error must NOT fire on success
        mw_chain.run_on_error.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_post_execute_not_called_when_post_execution_fails(
        self, broker, backend, config
    ):
        """post_execute hook is NOT called when the task's post_execution() raises."""

        class PostFailTask(Task):
            task_info = TaskInfo(name="post_fail_hook", queues=["default"])

            async def run(self) -> str:
                return "ok"

            async def post_execution(self) -> None:
                raise RuntimeError("post failed")

        mw_chain = _make_middleware_chain_mock()

        reg = TaskRegistry()
        reg.register(PostFailTask)
        w = Worker(
            broker=broker,
            backend=backend,
            registry=reg,
            config=config,
            middleware_chain=mw_chain,
        )

        msg = _make_message(_default_payload(name="post_fail_hook", args=[], kwargs={}))
        await w._handle_message(msg)

        # post_execute (middleware) was NOT called because the lifecycle errored
        mw_chain.run_post_execute.assert_not_awaited()
        # on_error was called instead
        mw_chain.run_on_error.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_on_error_called_on_task_exception(self, broker, backend, config):
        """on_error hook fires when the task raises."""

        class BoomTask(Task):
            task_info = TaskInfo(name="boom_hook", queues=["default"])

            async def run(self) -> None:
                raise ValueError("boom")

        mw_chain = _make_middleware_chain_mock()

        reg = TaskRegistry()
        reg.register(BoomTask)
        w = Worker(
            broker=broker,
            backend=backend,
            registry=reg,
            config=config,
            middleware_chain=mw_chain,
        )

        msg = _make_message(_default_payload(name="boom_hook", args=[], kwargs={}))
        await w._handle_message(msg)

        mw_chain.run_on_error.assert_awaited_once()
        call_kwargs = mw_chain.run_on_error.call_args.kwargs
        assert isinstance(call_kwargs["exception"], ValueError)
        mw_chain.run_post_execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_on_retry_called_when_retrying(self, broker, backend, config):
        """on_retry hook fires when _evaluate_retry chooses the retry path."""

        class RetryingTask(Task):
            task_info = TaskInfo(
                name="retry_hook",
                queues=["default"],
                max_retries=3,
                retry_base_delay=1.0,
            )

            async def run(self) -> None:
                raise RuntimeError("transient")

        mw_chain = _make_middleware_chain_mock()

        reg = TaskRegistry()
        reg.register(RetryingTask)
        w = Worker(
            broker=broker,
            backend=backend,
            registry=reg,
            config=config,
            middleware_chain=mw_chain,
        )

        msg = _make_message(
            _default_payload(name="retry_hook", args=[], kwargs={}),
            headers={},
        )
        await w._handle_message(msg)

        mw_chain.run_on_retry.assert_awaited_once()
        call_kwargs = mw_chain.run_on_retry.call_args.kwargs
        assert call_kwargs["retry_count"] == 1
        assert call_kwargs["delay"] > 0
        assert isinstance(call_kwargs["exception"], RuntimeError)
        # retry path hit the delay queue
        broker.publish_to_delay_queue.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_on_error_and_on_retry_both_fire_on_retried_failure(
        self, broker, backend, config
    ):
        """Retried failure: on_error fires first, then on_retry."""

        class RetryBothTask(Task):
            task_info = TaskInfo(
                name="retry_both",
                queues=["default"],
                max_retries=3,
                retry_base_delay=1.0,
            )

            async def run(self) -> None:
                raise RuntimeError("boom")

        mw_chain = _make_middleware_chain_mock()
        call_order: list[str] = []
        mw_chain.run_on_error.side_effect = (
            lambda *a, **kw: call_order.append("on_error") or None
        )
        mw_chain.run_on_retry.side_effect = (
            lambda *a, **kw: call_order.append("on_retry") or None
        )

        reg = TaskRegistry()
        reg.register(RetryBothTask)
        w = Worker(
            broker=broker,
            backend=backend,
            registry=reg,
            config=config,
            middleware_chain=mw_chain,
        )

        msg = _make_message(
            _default_payload(name="retry_both", args=[], kwargs={}),
            headers={},
        )
        await w._handle_message(msg)

        assert call_order == ["on_error", "on_retry"]

    @pytest.mark.asyncio
    async def test_on_retry_not_called_when_sent_to_dlq(
        self, broker, backend, config
    ):
        """When retries exhausted and task goes to DLQ, on_error fires but on_retry does NOT."""

        class ExhaustedTask(Task):
            task_info = TaskInfo(
                name="exhausted_hook",
                queues=["default"],
                max_retries=2,
            )

            async def run(self) -> None:
                raise RuntimeError("still broken")

        mw_chain = _make_middleware_chain_mock()

        reg = TaskRegistry()
        reg.register(ExhaustedTask)
        w = Worker(
            broker=broker,
            backend=backend,
            registry=reg,
            config=config,
            middleware_chain=mw_chain,
        )

        # retry_count == max_retries (2) -> DLQ, no retry
        msg = _make_message(
            _default_payload(name="exhausted_hook", args=[], kwargs={}),
            headers={"x-swarmq-retry-count": 2},
        )
        await w._handle_message(msg)

        mw_chain.run_on_error.assert_awaited_once()
        mw_chain.run_on_retry.assert_not_awaited()
        broker.publish_to_dead_letter_queue.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_task_started_signal_dispatched(self, broker, backend, config):
        """TASK_STARTED is dispatched with task_id + task_name before execution."""
        from swarmq.signals import Signal

        dispatcher = _make_signal_dispatcher_mock()

        reg = TaskRegistry()
        reg.register(AddTask)
        w = Worker(
            broker=broker,
            backend=backend,
            registry=reg,
            config=config,
            signal_dispatcher=dispatcher,
        )

        msg = _make_message(_default_payload())
        await w._handle_message(msg)

        started_calls = [
            c for c in dispatcher.dispatch.call_args_list
            if c.args and c.args[0] is Signal.TASK_STARTED
        ]
        assert len(started_calls) == 1
        assert started_calls[0].kwargs["task_id"] == "abc-123"
        assert started_calls[0].kwargs["task_name"] == "add"

    @pytest.mark.asyncio
    async def test_task_completed_signal_dispatched_with_duration(
        self, broker, backend, config
    ):
        """TASK_COMPLETED dispatched with duration_ms after success."""
        from swarmq.signals import Signal

        dispatcher = _make_signal_dispatcher_mock()

        reg = TaskRegistry()
        reg.register(AddTask)
        w = Worker(
            broker=broker,
            backend=backend,
            registry=reg,
            config=config,
            signal_dispatcher=dispatcher,
        )

        msg = _make_message(_default_payload())
        await w._handle_message(msg)

        completed_calls = [
            c for c in dispatcher.dispatch.call_args_list
            if c.args and c.args[0] is Signal.TASK_COMPLETED
        ]
        assert len(completed_calls) == 1
        kw = completed_calls[0].kwargs
        assert kw["task_id"] == "abc-123"
        assert kw["task_name"] == "add"
        assert "duration_ms" in kw
        assert isinstance(kw["duration_ms"], float)
        assert kw["duration_ms"] >= 0

    @pytest.mark.asyncio
    async def test_task_failed_signal_dispatched_on_error(
        self, broker, backend, config
    ):
        """TASK_FAILED dispatched with exception string on task error."""
        from swarmq.signals import Signal

        class FailSig(Task):
            task_info = TaskInfo(name="fail_sig", queues=["default"])

            async def run(self) -> None:
                raise ValueError("bad thing")

        dispatcher = _make_signal_dispatcher_mock()

        reg = TaskRegistry()
        reg.register(FailSig)
        w = Worker(
            broker=broker,
            backend=backend,
            registry=reg,
            config=config,
            signal_dispatcher=dispatcher,
        )

        msg = _make_message(_default_payload(name="fail_sig", args=[], kwargs={}))
        await w._handle_message(msg)

        failed_calls = [
            c for c in dispatcher.dispatch.call_args_list
            if c.args and c.args[0] is Signal.TASK_FAILED
        ]
        assert len(failed_calls) == 1
        kw = failed_calls[0].kwargs
        assert kw["task_id"] == "abc-123"
        assert kw["task_name"] == "fail_sig"
        assert "bad thing" in kw["exception"]

    @pytest.mark.asyncio
    async def test_task_retrying_signal_dispatched_on_retry(
        self, broker, backend, config
    ):
        """TASK_RETRYING dispatched on retry path with retry_count and delay."""
        from swarmq.signals import Signal

        class RetrySig(Task):
            task_info = TaskInfo(
                name="retry_sig",
                queues=["default"],
                max_retries=3,
                retry_base_delay=1.0,
            )

            async def run(self) -> None:
                raise RuntimeError("transient")

        dispatcher = _make_signal_dispatcher_mock()

        reg = TaskRegistry()
        reg.register(RetrySig)
        w = Worker(
            broker=broker,
            backend=backend,
            registry=reg,
            config=config,
            signal_dispatcher=dispatcher,
        )

        msg = _make_message(
            _default_payload(name="retry_sig", args=[], kwargs={}),
            headers={},
        )
        await w._handle_message(msg)

        retrying_calls = [
            c for c in dispatcher.dispatch.call_args_list
            if c.args and c.args[0] is Signal.TASK_RETRYING
        ]
        assert len(retrying_calls) == 1
        kw = retrying_calls[0].kwargs
        assert kw["task_id"] == "abc-123"
        assert kw["retry_count"] == 1
        assert kw["delay"] > 0

    @pytest.mark.asyncio
    async def test_worker_online_and_shutdown_signals_dispatched(
        self, broker, backend, config
    ):
        """WORKER_ONLINE dispatched after connect; WORKER_SHUTDOWN dispatched on shutdown."""
        from swarmq.signals import Signal

        dispatcher = _make_signal_dispatcher_mock()

        reg = TaskRegistry()
        reg.register(AddTask)
        w = Worker(
            broker=broker,
            backend=backend,
            registry=reg,
            config=config,
            signal_dispatcher=dispatcher,
        )

        # Trigger shutdown immediately so start() returns
        w._shutdown_event.set()
        await w.start()

        signals_seen = [
            c.args[0] for c in dispatcher.dispatch.call_args_list if c.args
        ]
        assert Signal.WORKER_ONLINE in signals_seen
        assert Signal.WORKER_SHUTDOWN in signals_seen

        # Check worker_id kwarg present for both
        online_call = next(
            c for c in dispatcher.dispatch.call_args_list
            if c.args and c.args[0] is Signal.WORKER_ONLINE
        )
        assert "worker_id" in online_call.kwargs
        shutdown_call = next(
            c for c in dispatcher.dispatch.call_args_list
            if c.args and c.args[0] is Signal.WORKER_SHUTDOWN
        )
        assert "worker_id" in shutdown_call.kwargs

    @pytest.mark.asyncio
    async def test_message_dict_contains_canonical_fields(
        self, broker, backend, config
    ):
        """message_dict passed to middleware hooks contains all canonical fields."""

        class KwTask(Task):
            task_info = TaskInfo(name="kw_task", queues=["default"])

            async def run(self, *args, **kwargs) -> str:
                return "ok"

        mw_chain = _make_middleware_chain_mock()

        reg = TaskRegistry()
        reg.register(KwTask)
        w = Worker(
            broker=broker,
            backend=backend,
            registry=reg,
            config=config,
            middleware_chain=mw_chain,
        )

        msg = _make_message(
            _default_payload(
                task_id="mid-1",
                queue="myq",
                name="kw_task",
                args=[7, 8],
                kwargs={"extra": "k"},
            ),
            headers={"x-swarmq-retry-count": 2},
        )
        await w._handle_message(msg)

        mw_chain.run_pre_execute.assert_awaited_once()
        message_dict = mw_chain.run_pre_execute.call_args.args[0]

        assert message_dict["task_id"] == "mid-1"
        assert message_dict["task_name"] == "kw_task"
        assert message_dict["queue"] == "myq"
        assert message_dict["args"] == [7, 8]
        assert message_dict["kwargs"] == {"extra": "k"}
        assert message_dict["delivery_mode"] == DeliveryMode.AT_LEAST_ONCE.value
        assert message_dict["retry_count"] == 2

    @pytest.mark.asyncio
    async def test_backward_compatible_without_middleware_or_signals(
        self, broker, backend, config
    ):
        """Without middleware_chain/signal_dispatcher, Worker works exactly as before."""
        reg = TaskRegistry()
        reg.register(AddTask)
        # Neither middleware_chain nor signal_dispatcher provided
        w = Worker(broker=broker, backend=backend, registry=reg, config=config)

        msg = _make_message(_default_payload())
        await w._handle_message(msg)

        # Happy-path invariants hold
        broker.ack.assert_awaited_once_with(msg)
        backend.store_result.assert_awaited_once()
        stored = decode_result(backend.store_result.call_args[0][1])
        assert stored["is_error"] is False
        assert stored["result"] == 3
