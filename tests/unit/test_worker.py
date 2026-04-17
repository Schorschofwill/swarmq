"""Unit tests for Worker consumer loop."""

from __future__ import annotations

import asyncio
import contextlib
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
# SwarmQConfig is used in fixtures for the Unit 7 monitor tests.
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
    # Cancel-flag primitives (Phase 3b.3). Default: no flag set, so
    # existing happy-path tests continue to pass the pre-start check.
    be.is_cancelled = AsyncMock(return_value=False)
    be.set_cancel_flag = AsyncMock(return_value=True)
    be.clear_cancel_flag = AsyncMock()
    # Schedfire-dedup (Phase 3c Unit 8 + Unit 1 pre/post markers).
    # Defaults represent "first delivery, no prior completion":
    # is_schedfire_completed=False, inflight-SETNX wins, dedup-write OK.
    be.is_schedfire_completed = AsyncMock(return_value=False)
    be.set_schedfire_inflight = AsyncMock(return_value=True)
    be.set_schedfire_dedup = AsyncMock(return_value=True)
    # _client is needed by Worker.start() to construct RateLimiter. We
    # provide a MagicMock whose register_script returns another MagicMock
    # — concrete rate-limit behaviour is covered by the integration suite.
    be._client = MagicMock()
    be._client.register_script = MagicMock(return_value=AsyncMock())
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
    async def test_on_retry_message_dict_retry_count_matches_parameter(
        self, broker, backend, config
    ):
        """message['retry_count'] in on_retry call equals the explicit retry_count parameter."""

        class RetryingTask(Task):
            task_info = TaskInfo(
                name="retry_sync",
                queues=["default"],
                max_retries=5,
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

        # Start with retry_count=2 in headers -> next attempt is 3
        msg = _make_message(
            _default_payload(name="retry_sync", args=[], kwargs={}),
            headers={"x-swarmq-retry-count": 2},
        )
        await w._handle_message(msg)

        mw_chain.run_on_retry.assert_awaited_once()
        call = mw_chain.run_on_retry.call_args
        message_dict = call.args[0]
        retry_count_param = call.kwargs["retry_count"]

        # Both the dict and the explicit parameter should agree.
        assert message_dict["retry_count"] == retry_count_param
        assert retry_count_param == 3

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

    @pytest.mark.asyncio
    async def test_factory_is_invoked_per_message(
        self, broker, backend, config
    ):
        """Worker built with a factory callable must invoke it on every message.

        Regression guard for F1+F2: the worker used to accept a static
        MiddlewareChain only, so middleware added AFTER Worker construction
        (via SwarmQ.add_middleware) was invisible. The factory is invoked on
        every message so the chain reflects the current state.
        """
        reg = TaskRegistry()
        reg.register(AddTask)

        chain_a = _make_middleware_chain_mock()
        chain_b = _make_middleware_chain_mock()
        # A plain MagicMock (no spec=MiddlewareChain) fails the isinstance
        # check in Worker.__init__ and is classified as a factory callable.
        # side_effect=[chain_a, chain_b] returns chain_a first, chain_b next —
        # mimics add_middleware() producing a new chain after the first message.
        factory = MagicMock(side_effect=[chain_a, chain_b])

        w = Worker(
            broker=broker,
            backend=backend,
            registry=reg,
            config=config,
            middleware_chain=factory,
        )

        msg1 = _make_message(_default_payload())
        await w._handle_message(msg1)
        msg2 = _make_message(_default_payload())
        await w._handle_message(msg2)

        assert factory.call_count == 2
        chain_a.run_pre_execute.assert_awaited_once()
        chain_b.run_pre_execute.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_factory_rejects_invalid_type(self, broker, backend, config):
        """Worker.__init__ rejects non-chain, non-callable, non-None inputs."""
        reg = TaskRegistry()
        with pytest.raises(TypeError, match=r"middleware_chain must be.*got int"):
            Worker(
                broker=broker,
                backend=backend,
                registry=reg,
                config=config,
                middleware_chain=42,  # type: ignore[arg-type]
            )

    @pytest.mark.asyncio
    async def test_same_chain_across_hooks_within_one_message(
        self, broker, backend, config
    ):
        """Factory is called exactly once per message; chain identity is stable
        across pre_execute, on_error, and on_retry.

        Failing task goes through pre_execute + on_error + on_retry (retry
        path) — all three must see the chain factory returned first, not
        call the factory again mid-message.
        """
        from swarmq.task import DeliveryMode, TaskInfo

        class FailingTask(Task):
            task_info = TaskInfo(
                name="mw_retry_chain_identity",
                queues=["default"],
                delivery=DeliveryMode.AT_LEAST_ONCE,
                max_retries=2,
            )

            async def run(self) -> None:
                raise ValueError("boom")

        reg = TaskRegistry()
        reg.register(FailingTask)

        chain_a = _make_middleware_chain_mock()
        # A second chain we should NEVER see — if factory is called again
        # mid-message, chain_b's run_on_error/run_on_retry would fire.
        chain_b = _make_middleware_chain_mock()
        factory = MagicMock(side_effect=[chain_a, chain_b])

        w = Worker(
            broker=broker,
            backend=backend,
            registry=reg,
            config=config,
            middleware_chain=factory,
        )

        msg = _make_message(
            _default_payload(name="mw_retry_chain_identity", args=[], kwargs={})
        )
        await w._handle_message(msg)

        assert factory.call_count == 1
        chain_a.run_pre_execute.assert_awaited_once()
        chain_a.run_on_error.assert_awaited_once()
        chain_a.run_on_retry.assert_awaited_once()
        chain_b.run_pre_execute.assert_not_awaited()
        chain_b.run_on_error.assert_not_awaited()
        chain_b.run_on_retry.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_accepts_static_middleware_chain_instance(
        self, broker, backend, config
    ):
        """Worker with a real MiddlewareChain instance runs its hooks."""
        from swarmq.middleware import Middleware, MiddlewareChain

        captured: list[tuple[str, dict]] = []

        class Spy(Middleware):
            async def pre_execute(self, message: dict) -> None:
                captured.append(("pre", dict(message)))

            async def post_execute(
                self, message: dict, result: object = None
            ) -> None:
                captured.append(("post", dict(message)))

        chain = MiddlewareChain([Spy()])
        reg = TaskRegistry()
        reg.register(AddTask)

        w = Worker(
            broker=broker,
            backend=backend,
            registry=reg,
            config=config,
            middleware_chain=chain,
        )

        await w._handle_message(_make_message(_default_payload()))

        assert [name for name, _ in captured] == ["pre", "post"]

    @pytest.mark.asyncio
    async def test_factory_exception_nacks_without_requeue(
        self, broker, backend, config
    ):
        """A broken factory (raises on call) must nack(requeue=False), not crash."""
        reg = TaskRegistry()
        reg.register(AddTask)

        def bad_factory() -> object:
            raise RuntimeError("factory broken")

        w = Worker(
            broker=broker,
            backend=backend,
            registry=reg,
            config=config,
            middleware_chain=bad_factory,
        )

        msg = _make_message(_default_payload())
        await w._handle_message(msg)

        broker.nack.assert_awaited_once_with(msg, requeue=False)
        broker.ack.assert_not_awaited()


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

        msg = _make_message(_default_payload(name="rate_limited", args=[], kwargs={}))

        async def _run(self):  # noqa: ANN001
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
        from swarmq.signals import Signal, SignalDispatcher

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

        async def _run(self):  # noqa: ANN001
            nonlocal ran
            ran = True

        with patch.object(RateLimitedTask, "run", _run):
            msg = _make_message(
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

        msg = _make_message(_default_payload())
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

        async def _run(self):  # noqa: ANN001
            nonlocal ran
            ran = True

        with patch.object(RateLimitedTask, "run", _run):
            msg = _make_message(
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

        msg = _make_message(
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

        msg = _make_message(
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

        msg = _make_message(
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

        async def _run(self):  # noqa: ANN001
            nonlocal ran
            ran = True

        with patch.object(FireAndForgetRateLimitedTask, "run", _run):
            msg = _make_message(
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

        msg = _make_message(
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

        msg = _make_message(
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
        w._lock_manager.acquire = AsyncMock(return_value=True)
        w._lock_manager.release = AsyncMock(return_value=True)

        msg = _make_message(_default_payload(name="locked", args=[], kwargs={}))

        async def _run(self):  # noqa: ANN001
            return None

        with patch.object(LockedTask, "run", _run):
            await w._handle_message(msg)

        w._lock_manager.acquire.assert_awaited_once()
        w._lock_manager.release.assert_awaited_once()
        broker.publish_to_delay_queue.assert_not_called()
        broker.ack.assert_awaited_once_with(msg)

    @pytest.mark.asyncio
    async def test_conflict_path_requeues_with_delay_and_skips_execution(
        self, broker, backend, config
    ):
        from swarmq.signals import Signal, SignalDispatcher

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
        w._lock_manager.acquire = AsyncMock(return_value=False)
        w._lock_manager.release = AsyncMock(return_value=True)

        ran = False

        async def _run(self):  # noqa: ANN001
            nonlocal ran
            ran = True

        with patch.object(LockedTask, "run", _run):
            msg = _make_message(_default_payload(name="locked", args=[], kwargs={}))
            await w._handle_message(msg)

        assert ran is False, "task.run must NOT run when lock conflicted"
        broker.publish_to_delay_queue.assert_awaited_once()
        broker.ack.assert_awaited_once_with(msg)
        broker.publish_to_dead_letter_queue.assert_not_called()
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

        msg = _make_message(_default_payload())
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

        async def _run(self):  # noqa: ANN001
            nonlocal ran
            ran = True

        with patch.object(LockedTask, "run", _run):
            msg = _make_message(_default_payload(name="locked", args=[], kwargs={}))
            await w._handle_message(msg)

        assert ran is True
        broker.ack.assert_awaited_once_with(msg)
        broker.publish_to_delay_queue.assert_not_called()

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

        msg = _make_message(_default_payload(name="locked", args=[], kwargs={}))
        await w._handle_message(msg)

        broker.nack.assert_awaited_once_with(msg, requeue=True)
        broker.ack.assert_not_awaited()
        broker.publish_to_delay_queue.assert_not_called()


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

        msg = _make_message(_default_payload(name="ff_locked", args=[], kwargs={}))
        await w._handle_message(msg)

        broker.nack.assert_awaited_once_with(msg, requeue=True)
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
        w._lock_manager.acquire = AsyncMock(return_value=False)

        msg = _make_message(_default_payload(name="ff_locked", args=[], kwargs={}))
        await w._handle_message(msg)

        broker.publish_to_delay_queue.assert_awaited_once()
        # Single ack — not the old "FIRE_AND_FORGET ack + lock-conflict ack" double.
        assert broker.ack.await_count == 1


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
        w._lock_manager.acquire = AsyncMock(return_value=False)

        msg = _make_message(_default_payload(name="locked", args=[], kwargs={}))
        msg.headers = {}

        await w._handle_message(msg)

        _, kwargs = broker.publish_to_delay_queue.call_args
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
        w._lock_manager.acquire = AsyncMock(return_value=False)

        msg = _make_message(_default_payload(name="locked", args=[], kwargs={}))
        msg.headers = {"x-swarmq-lock-conflict-count": 5}

        await w._handle_message(msg)

        _, kwargs = broker.publish_to_delay_queue.call_args
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
        w._lock_manager.acquire = AsyncMock(return_value=False)

        msg = _make_message(_default_payload(name="locked", args=[], kwargs={}))
        # Prior count = 9; new_count = 10 → exactly at cap → DLQ
        msg.headers = {"x-swarmq-lock-conflict-count": 9}

        await w._handle_message(msg)

        # No more requeue
        broker.publish_to_delay_queue.assert_not_called()
        # DLQ instead, with diagnostic header
        broker.publish_to_dead_letter_queue.assert_awaited_once()
        _, kwargs = broker.publish_to_dead_letter_queue.call_args
        assert kwargs["headers"]["x-swarmq-lock-conflict-count"] == 10
        assert kwargs["headers"]["x-swarmq-lock-name"] == "resource"
        broker.ack.assert_awaited_once_with(msg)

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
        w._lock_manager.acquire = AsyncMock(return_value=False)

        msg = _make_message(_default_payload(name="locked", args=[], kwargs={}))
        msg.headers = {"x-swarmq-lock-conflict-count": 8}

        await w._handle_message(msg)

        broker.publish_to_delay_queue.assert_awaited_once()
        broker.publish_to_dead_letter_queue.assert_not_called()
        _, kwargs = broker.publish_to_delay_queue.call_args
        assert kwargs["headers"]["x-swarmq-lock-conflict-count"] == 9

    async def test_dlq_failure_at_cap_drops_message_no_infinite_loop(
        self, broker, backend, config
    ):
        """Cascade-fix regression: when publish_to_dead_letter_queue fails
        AT the cap, falling back to nack(requeue=True) would let the
        message bounce back at count >= cap, hit the same DLQ failure,
        and loop forever without backoff (the requeue branch is
        unreachable past the cap). Honest data loss with a loud ERROR
        log naming the lock is the right trade-off."""
        reg = TaskRegistry()
        reg.register(LockedTask)

        w = Worker(broker=broker, backend=backend, registry=reg, config=config)
        w._lock_manager = MagicMock()
        w._lock_manager.acquire = AsyncMock(return_value=False)
        broker.publish_to_dead_letter_queue = AsyncMock(
            side_effect=ConnectionError("broker down")
        )

        msg = _make_message(_default_payload(name="locked", args=[], kwargs={}))
        msg.headers = {"x-swarmq-lock-conflict-count": 9}

        await w._handle_message(msg)

        # nack(requeue=False) — drop the message, do not requeue into the
        # bounce loop.
        broker.nack.assert_awaited_once_with(msg, requeue=False)
        # No ack happened.
        broker.ack.assert_not_awaited()
        # No requeue either — the requeue branch is unreachable past the cap.
        broker.publish_to_delay_queue.assert_not_called()


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
        w._lock_manager.acquire = AsyncMock(return_value=True)
        w._lock_manager.release = AsyncMock(return_value=True)

        msg = _make_message(
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
        w._lock_manager.acquire = AsyncMock(return_value=True)
        w._lock_manager.release = AsyncMock(return_value=True)

        msg = _make_message(
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
        msg = _make_message(
            _default_payload(name="user_charge", args=[], kwargs={"amount": 100})
        )
        await w._handle_message(msg)

        w._lock_manager.acquire.assert_not_called()
        broker.publish_to_dead_letter_queue.assert_awaited_once()
        _, kwargs = broker.publish_to_dead_letter_queue.call_args
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
        broker.publish_to_dead_letter_queue = AsyncMock(
            side_effect=ConnectionError("broker down")
        )

        msg = _make_message(
            _default_payload(name="user_charge", args=[], kwargs={"amount": 100})
        )
        await w._handle_message(msg)

        # Render failed, DLQ publish failed → nack(requeue=False) routes
        # via DLX to {queue}.XQ (not lost), no ack.
        broker.nack.assert_awaited_once_with(msg, requeue=False)
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

        msg = _make_message(
            _default_payload(name="empty_render", args=[], kwargs={"tag": ""})
        )
        await w._handle_message(msg)

        # Acquire never called — guard fires before LockManager
        w._lock_manager.acquire.assert_not_called()
        broker.publish_to_dead_letter_queue.assert_awaited_once()
        _, kwargs = broker.publish_to_dead_letter_queue.call_args
        assert "rendered to empty string" in kwargs["headers"][
            "x-swarmq-lock-render-error"
        ]
        broker.ack.assert_awaited_once_with(msg)

    @pytest.mark.asyncio
    async def test_whitespace_render_also_rejected(
        self, broker, backend, config
    ):
        reg = TaskRegistry()
        reg.register(EmptyRenderingLockTask)

        w = Worker(broker=broker, backend=backend, registry=reg, config=config)
        w._lock_manager = MagicMock()
        w._lock_manager.acquire = AsyncMock()

        msg = _make_message(
            _default_payload(name="empty_render", args=[], kwargs={"tag": "   "})
        )
        await w._handle_message(msg)

        w._lock_manager.acquire.assert_not_called()
        broker.publish_to_dead_letter_queue.assert_awaited_once()

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
        broker.publish_to_dead_letter_queue = AsyncMock(
            side_effect=ConnectionError("broker down")
        )

        msg = _make_message(
            _default_payload(name="empty_render", args=[], kwargs={"tag": ""})
        )
        await w._handle_message(msg)

        broker.nack.assert_awaited_once_with(msg, requeue=False)
        broker.ack.assert_not_awaited()


# ===========================================================================
# Phase 3b.3 / Unit 6 — Worker Pre-Start Cancel Check
# ===========================================================================

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

        msg = _make_message(_default_payload())
        with patch.object(AddTask, "run", new=AsyncMock()) as mock_run:
            await worker._handle_message(msg)

        mock_run.assert_not_awaited()

    async def test_cancel_flag_set_stores_is_cancelled_result(
        self, worker, broker, backend,
    ):
        """AT_LEAST_ONCE cancel → result_data has is_cancelled=True."""
        backend.is_cancelled = AsyncMock(return_value=True)
        msg = _make_message(_default_payload())

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
        msg = _make_message(_default_payload())

        await worker._handle_message(msg)

        backend.clear_cancel_flag.assert_awaited_once_with("abc-123", op_timeout=5.0)

    async def test_cancel_flag_set_acks_message(self, worker, broker, backend):
        backend.is_cancelled = AsyncMock(return_value=True)
        msg = _make_message(_default_payload())

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
        msg = _make_message(_default_payload())

        await worker._handle_message(msg)

        assert len(captured) == 1
        assert captured[0] == {"task_id": "abc-123", "task_name": "add"}

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

        msg = _make_message(_default_payload())
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
        msg = _make_message(_default_payload())

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
        msg = _make_message(_default_payload())

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
        msg = _make_message(_default_payload(name="faf", args=[], kwargs={}))

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
        msg = _make_message(_default_payload(name="faf_ttl", args=[], kwargs={}))

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
        msg = _make_message(_default_payload(name="unknown_task"))

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
        msg = _make_message(_default_payload())

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
        msg = _make_message(_default_payload())

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
        msg = _make_message(_default_payload())

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

        msg = _make_message(_default_payload(name="rate_add"))
        await w._handle_message(msg)

        w._rate_limiter.check.assert_not_awaited()


# ===========================================================================
# Phase 3b.3 / Unit 7 — Cancel-Monitor Coroutine (isolated)
# ===========================================================================

class TestCancelMonitorEdgeCases:
    """Isolated tests for two _cancel_monitor edge cases that are
    hard to reproduce via the public _handle_message path.

    AGENTS.md §Verboten discourages private-method tests; REV-P2-12
    proposed removing this whole class. We keep only the cases that
    would be genuinely lost otherwise — the happy-path flag-set and
    already-done branches are covered by
    TestRunningCancelInHandleMessage and the integration suite.
    Keeping two targeted edge-case tests reflects a deliberate
    trade-off: test value over strict private-method discipline.
    """

    async def test_monitor_swallows_is_cancelled_exceptions(
        self, worker, backend,
    ):
        """Transient BackendConnectionError → log + continue loop."""
        # First call raises, second returns False, third True.
        backend.is_cancelled = AsyncMock(
            side_effect=[
                BackendConnectionError("blip"),
                False,
                True,
            ],
        )
        # Shorten the poll interval so this test completes fast.
        worker._config.cancel_poll_interval = 0.05

        run_task = asyncio.create_task(asyncio.sleep(10))
        reasons: list[str | None] = [None]

        try:
            await worker._cancel_monitor(
                "task-abc", run_task,
                lambda r: reasons.__setitem__(0, r) if reasons[0] is None else None,
            )
        finally:
            if not run_task.done():
                run_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await run_task

        # Eventually set reason=cancel after surviving the first blip.
        assert reasons[0] == "cancel"

    async def test_monitor_exits_when_run_task_completes_during_sleep(
        self, worker, backend,
    ):
        """asyncio.wait(timeout) lets the monitor detect run_task.done() quickly."""
        backend.is_cancelled = AsyncMock(return_value=False)
        worker._config.cancel_poll_interval = 1.0  # long, to verify early-exit

        async def short_run():
            await asyncio.sleep(0.1)
            return "ok"

        run_task = asyncio.create_task(short_run())
        reasons: list[str | None] = [None]

        # Wrap in wait_for with a tight timeout: if the monitor blocks
        # for the full 1.0 s poll interval it will time out here.
        await asyncio.wait_for(
            worker._cancel_monitor(
                "task-abc", run_task,
                lambda r: reasons.__setitem__(0, r),
            ),
            timeout=0.5,
        )

        assert run_task.done()
        assert reasons[0] is None  # never set — task completed normally


class TestSetReasonIfUnset:
    """The compare-and-set helper lives in _handle_message as a closure.

    This test runs the pattern directly to prove first-writer-wins
    semantics — the identical pattern will be used inline in
    _handle_message once Unit 7 wires it up.
    """

    def test_first_call_sets_value(self):
        reasons: list[str | None] = [None]

        def set_reason_if_unset(r: str) -> None:
            if reasons[0] is None:
                reasons[0] = r

        set_reason_if_unset("cancel")
        assert reasons[0] == "cancel"

    def test_second_call_does_not_overwrite(self):
        reasons: list[str | None] = [None]

        def set_reason_if_unset(r: str) -> None:
            if reasons[0] is None:
                reasons[0] = r

        set_reason_if_unset("cancel")
        set_reason_if_unset("timeout")
        assert reasons[0] == "cancel"  # first writer wins

    def test_second_call_with_same_value_is_idempotent(self):
        reasons: list[str | None] = [None]

        def set_reason_if_unset(r: str) -> None:
            if reasons[0] is None:
                reasons[0] = r

        set_reason_if_unset("cancel")
        set_reason_if_unset("cancel")
        assert reasons[0] == "cancel"


# Module-level contextlib.suppress helper — pytest-asyncio has a quirk
# where contextlib.suppress used inside async tests occasionally masks
# the test's own CancelledError on teardown. Inline try/except avoids it.
# ===========================================================================
# Phase 3b.3 / Unit 7 — Monitor in the full _handle_message pipeline
# ===========================================================================

class _SlowTask(Task):
    """Task whose run() takes long enough that the monitor can intervene."""
    task_info = TaskInfo(name="slow_run", queues=["default"])

    async def run(self) -> str:
        await asyncio.sleep(5)
        return "completed"


class TestRunningCancelInHandleMessage:
    """Unit-level integration: cancel during _handle_message execution."""

    @pytest.fixture()
    def slow_worker(self, broker, backend, config):
        registry = TaskRegistry()
        registry.register(_SlowTask)
        cfg = SwarmQConfig(
            broker_url="amqp://localhost",
            backend_url="valkey://localhost",
            concurrency=2,
            queues=["default"],
            cancel_poll_interval=0.05,  # fast polling for unit tests
        )
        return Worker(broker=broker, backend=backend, registry=registry, config=cfg)

    async def test_cancel_during_run_raises_task_cancelled_error(
        self, slow_worker, broker, backend,
    ):
        """Flag flips to True during run() → CancelledError → is_cancelled=True."""
        call_count = [0]

        async def is_cancelled_impl(task_id, **_kw):
            call_count[0] += 1
            # First poll returns False, second returns True.
            return call_count[0] >= 2

        backend.is_cancelled = AsyncMock(side_effect=is_cancelled_impl)
        msg = _make_message(_default_payload(name="slow_run", args=[], kwargs={}))

        await slow_worker._handle_message(msg)

        # Result was stored with is_cancelled=True.
        backend.store_result.assert_awaited_once()
        stored_bytes = backend.store_result.await_args.args[1]
        decoded = decode_result(stored_bytes)
        assert decoded["is_cancelled"] is True
        assert decoded["is_error"] is False
        assert decoded["result"] is None

        # Flag was cleared after result-write.
        backend.clear_cancel_flag.assert_awaited_once_with("abc-123", op_timeout=5.0)
        broker.ack.assert_awaited_once()
        broker.nack.assert_not_awaited()

    async def test_cancel_during_run_dispatches_task_cancelled_signal(
        self, slow_worker, backend,
    ):
        """is_cancelled=True path dispatches TASK_CANCELLED, NOT TASK_COMPLETED/FAILED."""
        call_count = [0]

        async def is_cancelled_impl(task_id, **_kw):
            call_count[0] += 1
            return call_count[0] >= 2

        backend.is_cancelled = AsyncMock(side_effect=is_cancelled_impl)

        captured: list[tuple] = []
        async def cancelled_handler(**kwargs):
            captured.append(("cancelled", kwargs))
        async def completed_handler(**kwargs):
            captured.append(("completed", kwargs))
        async def failed_handler(**kwargs):
            captured.append(("failed", kwargs))

        from swarmq.signals import Signal
        slow_worker._signal_dispatcher.connect(Signal.TASK_CANCELLED, cancelled_handler)
        slow_worker._signal_dispatcher.connect(Signal.TASK_COMPLETED, completed_handler)
        slow_worker._signal_dispatcher.connect(Signal.TASK_FAILED, failed_handler)

        msg = _make_message(_default_payload(name="slow_run", args=[], kwargs={}))
        await slow_worker._handle_message(msg)

        kinds = [c[0] for c in captured]
        assert "cancelled" in kinds
        assert "completed" not in kinds
        assert "failed" not in kinds

    async def test_cancel_during_run_bypasses_retry(
        self, broker, backend,
    ):
        """Task with max_retries>0, cancel during run → NO retry, is_cancelled result."""
        class RetryableSlowTask(Task):
            task_info = TaskInfo(
                name="retry_slow", queues=["default"],
                max_retries=3, retry_base_delay=1.0,
            )
            async def run(self) -> str:
                await asyncio.sleep(5)
                return "x"

        registry = TaskRegistry()
        registry.register(RetryableSlowTask)
        cfg = SwarmQConfig(
            broker_url="amqp://localhost",
            backend_url="valkey://localhost",
            concurrency=2,
            queues=["default"],
            cancel_poll_interval=0.05,
        )
        w = Worker(broker=broker, backend=backend, registry=registry, config=cfg)

        call_count = [0]
        async def is_cancelled_impl(task_id, **_kw):
            call_count[0] += 1
            return call_count[0] >= 2
        backend.is_cancelled = AsyncMock(side_effect=is_cancelled_impl)

        msg = _make_message(_default_payload(name="retry_slow", args=[], kwargs={}))
        await w._handle_message(msg)

        # Publish to delay queue (retry) must NOT happen.
        broker.publish_to_delay_queue.assert_not_awaited()
        broker.publish_to_dead_letter_queue.assert_not_awaited()
        # Cancel result stored.
        backend.store_result.assert_awaited_once()
        stored_bytes = backend.store_result.await_args.args[1]
        decoded = decode_result(stored_bytes)
        assert decoded["is_cancelled"] is True

    async def test_cancel_during_run_clears_flag_before_ack(
        self, slow_worker, broker, backend,
    ):
        """clear_cancel_flag is called BEFORE broker.ack (Flag-lifecycle rule)."""
        call_count = [0]
        async def is_cancelled_impl(task_id, **_kw):
            call_count[0] += 1
            return call_count[0] >= 2
        backend.is_cancelled = AsyncMock(side_effect=is_cancelled_impl)

        call_order: list[str] = []
        backend.clear_cancel_flag = AsyncMock(
            side_effect=lambda tid, **_kw: call_order.append("clear"),
        )
        broker.ack = AsyncMock(
            side_effect=lambda msg: call_order.append("ack"),
        )

        msg = _make_message(_default_payload(name="slow_run", args=[], kwargs={}))
        await slow_worker._handle_message(msg)

        assert call_order == ["clear", "ack"]

    async def test_cancel_during_run_faf_emits_signal_no_result(
        self, broker, backend, config,
    ):
        """FAF + cancel during run: Signal fires, no store_result."""
        class FafSlow(Task):
            task_info = TaskInfo(
                name="faf_slow", queues=["default"],
                delivery=DeliveryMode.FIRE_AND_FORGET,
            )
            async def run(self) -> None:
                await asyncio.sleep(5)

        registry = TaskRegistry()
        registry.register(FafSlow)
        cfg = SwarmQConfig(
            broker_url="amqp://localhost",
            backend_url="valkey://localhost",
            concurrency=2,
            queues=["default"],
            cancel_poll_interval=0.05,
        )
        w = Worker(broker=broker, backend=backend, registry=registry, config=cfg)

        call_count = [0]
        async def is_cancelled_impl(task_id, **_kw):
            call_count[0] += 1
            return call_count[0] >= 2
        backend.is_cancelled = AsyncMock(side_effect=is_cancelled_impl)

        captured = []
        async def h(**kwargs):
            captured.append(kwargs)
        from swarmq.signals import Signal
        w._signal_dispatcher.connect(Signal.TASK_CANCELLED, h)

        msg = _make_message(_default_payload(name="faf_slow", args=[], kwargs={}))
        await w._handle_message(msg)

        backend.store_result.assert_not_awaited()
        backend.clear_cancel_flag.assert_awaited_once_with("abc-123", op_timeout=5.0)
        assert len(captured) == 1


class TestHardTimeoutVsCancelRace:
    """Race between hard-timeout watchdog and cancel monitor.

    Both write to cancel_reason via set_reason_if_unset — first writer
    wins. The CancelledError branch in _handle_message reads the box.
    """

    async def test_cancel_wins_race_user_sees_task_cancelled(
        self, broker, backend,
    ):
        """Cancel-flag fires BEFORE hard_timeout → user sees TaskCancelledError."""
        class TaskWithTimeout(Task):
            task_info = TaskInfo(
                name="race_task", queues=["default"], hard_timeout=2,
            )
            async def run(self) -> str:
                await asyncio.sleep(10)
                return "x"

        registry = TaskRegistry()
        registry.register(TaskWithTimeout)
        cfg = SwarmQConfig(
            broker_url="amqp://localhost",
            backend_url="valkey://localhost",
            concurrency=1,
            queues=["default"],
            cancel_poll_interval=0.05,
        )
        w = Worker(broker=broker, backend=backend, registry=registry, config=cfg)

        # Cancel fires after 100ms (well before hard_timeout=2s).
        call_count = [0]
        async def is_cancelled_impl(task_id, **_kw):
            call_count[0] += 1
            return call_count[0] >= 2
        backend.is_cancelled = AsyncMock(side_effect=is_cancelled_impl)

        msg = _make_message(_default_payload(name="race_task", args=[], kwargs={}))
        await w._handle_message(msg)

        backend.store_result.assert_awaited_once()
        stored_bytes = backend.store_result.await_args.args[1]
        decoded = decode_result(stored_bytes)
        assert decoded["is_cancelled"] is True
        assert decoded["is_error"] is False
        assert "hard timeout" not in decoded.get("error_message", "")

    async def test_hard_timeout_wins_when_cancel_never_fires(
        self, broker, backend,
    ):
        """Hard-timeout with no cancel flag → existing hard-timeout path."""
        class TimeoutTask(Task):
            task_info = TaskInfo(
                name="timeout_only", queues=["default"], hard_timeout=1,
            )
            async def run(self) -> str:
                await asyncio.sleep(10)
                return "x"

        registry = TaskRegistry()
        registry.register(TimeoutTask)
        cfg = SwarmQConfig(
            broker_url="amqp://localhost",
            backend_url="valkey://localhost",
            concurrency=1,
            queues=["default"],
            cancel_poll_interval=0.1,
        )
        w = Worker(broker=broker, backend=backend, registry=registry, config=cfg)

        # Cancel flag stays False throughout — regression for hard-timeout.
        backend.is_cancelled = AsyncMock(return_value=False)

        msg = _make_message(_default_payload(name="timeout_only", args=[], kwargs={}))
        await w._handle_message(msg)

        backend.store_result.assert_awaited_once()
        stored_bytes = backend.store_result.await_args.args[1]
        decoded = decode_result(stored_bytes)
        assert decoded["is_cancelled"] is False
        assert decoded["is_error"] is True
        assert "hard timeout" in decoded["error_message"]


# ---------------------------------------------------------------------------
# Consumer-side schedfire dedup (Unit 8)
# ---------------------------------------------------------------------------


class TestSchedfireDedup:
    """Scheduled-fire messages carry an ``x-swarmq-schedfire`` header.

    The worker SETNXs a key derived from that header before entering
    the normal pipeline. Duplicates (two deliveries of the same fire)
    get ack'd and skipped silently.

    Dedup runs BEFORE cancel-check so a duplicate fire never even
    enters the cancel-scan. Both stages run BEFORE the semaphore acquire.
    """

    async def test_no_header_skips_dedup(self, broker, backend, config):
        from swarmq.task import Task, TaskInfo
        from swarmq.registry import TaskRegistry

        class NoopTask(Task):
            task_info = TaskInfo(name="noop", queues=["default"])
            async def run(self):
                return "ok"

        registry = TaskRegistry()
        registry.register(NoopTask)
        w = Worker(broker=broker, backend=backend, registry=registry, config=config)

        msg = _make_message(
            _default_payload(name="noop", args=[], kwargs={}),
            headers={},  # no x-swarmq-schedfire
        )
        await w._handle_message(msg)

        backend.set_schedfire_dedup.assert_not_awaited()
        backend.store_result.assert_awaited_once()

    async def test_first_delivery_runs_task(self, broker, backend, config):
        """Unit 1 two-marker flow: completed=False, inflight-SETNX wins,
        task runs, finally block writes the completed marker with
        long TTL."""
        from swarmq.task import Task, TaskInfo
        from swarmq.registry import TaskRegistry

        class NoopTask(Task):
            task_info = TaskInfo(name="noop", queues=["default"])
            async def run(self):
                return "ok"

        registry = TaskRegistry()
        registry.register(NoopTask)
        w = Worker(broker=broker, backend=backend, registry=registry, config=config)

        backend.is_schedfire_completed.return_value = False
        backend.set_schedfire_inflight.return_value = True  # claim wins
        msg = _make_message(
            _default_payload(name="noop", args=[], kwargs={}),
            headers={"x-swarmq-schedfire": "cron_a:1700000000000"},
        )
        await w._handle_message(msg)

        backend.is_schedfire_completed.assert_awaited_once_with(
            "cron_a:1700000000000"
        )
        # Inflight TTL = scheduler_inflight_ttl * 1000 = 300 * 1000
        inflight_call = backend.set_schedfire_inflight.await_args
        assert inflight_call.args[0] == "cron_a:1700000000000"
        assert inflight_call.args[1] == 300_000
        # Task ran
        backend.store_result.assert_awaited_once()
        # finally-block wrote the long-TTL completed marker
        dedup_call = backend.set_schedfire_dedup.await_args
        assert dedup_call.args[0] == "cron_a:1700000000000"
        # dedup_ttl * 1000 = 3900 * 1000
        assert dedup_call.args[1] == 3_900_000

    async def test_duplicate_when_completed_marker_set(
        self, broker, backend, config
    ):
        """Redelivery after a prior successful fire sees the completed
        marker and ack-skips without claiming in-flight."""
        from swarmq.task import Task, TaskInfo
        from swarmq.registry import TaskRegistry

        class NoopTask(Task):
            task_info = TaskInfo(name="noop", queues=["default"])
            async def run(self):
                raise AssertionError("should not run on duplicate")

        registry = TaskRegistry()
        registry.register(NoopTask)
        w = Worker(broker=broker, backend=backend, registry=registry, config=config)

        backend.is_schedfire_completed.return_value = True  # prior run
        msg = _make_message(
            _default_payload(name="noop", args=[], kwargs={}),
            headers={"x-swarmq-schedfire": "cron_a:1700000000000"},
        )
        await w._handle_message(msg)

        broker.ack.assert_awaited_once_with(msg)
        backend.set_schedfire_inflight.assert_not_awaited()
        backend.store_result.assert_not_awaited()

    async def test_duplicate_when_inflight_claim_lost(
        self, broker, backend, config
    ):
        """Another worker is running the same fire right now. Ack+skip
        without running. When that worker completes, the completed
        marker blocks future redeliveries; if it crashes, the in-flight
        TTL expires and the next redelivery can re-run."""
        from swarmq.task import Task, TaskInfo
        from swarmq.registry import TaskRegistry

        class NoopTask(Task):
            task_info = TaskInfo(name="noop", queues=["default"])
            async def run(self):
                raise AssertionError("should not run on inflight-claim-lost")

        registry = TaskRegistry()
        registry.register(NoopTask)
        w = Worker(broker=broker, backend=backend, registry=registry, config=config)

        backend.is_schedfire_completed.return_value = False
        backend.set_schedfire_inflight.return_value = False  # someone else has it
        msg = _make_message(
            _default_payload(name="noop", args=[], kwargs={}),
            headers={"x-swarmq-schedfire": "cron_a:1700000000000"},
        )
        await w._handle_message(msg)

        broker.ack.assert_awaited_once_with(msg)
        backend.store_result.assert_not_awaited()
        # No long-TTL write because we didn't own the run.
        backend.set_schedfire_dedup.assert_not_awaited()

    async def test_dedup_backend_error_nacks_for_requeue(
        self, broker, backend, config
    ):
        """A Valkey failure during dedup-check must requeue the message
        — we cannot assume either new or duplicate."""
        from swarmq.task import Task, TaskInfo
        from swarmq.registry import TaskRegistry

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
        backend.is_schedfire_completed.side_effect = BCE("valkey down")
        msg = _make_message(
            _default_payload(name="noop", args=[], kwargs={}),
            headers={"x-swarmq-schedfire": "cron_a:1700000000000"},
        )
        await w._handle_message(msg)

        broker.nack.assert_awaited_once()
        nack_call = broker.nack.await_args
        # requeue=True so the message comes back and a healthy worker
        # can try again
        assert nack_call.kwargs.get("requeue") is True

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
        from swarmq.task import Task, TaskInfo
        from swarmq.registry import TaskRegistry

        class NoopTask(Task):
            task_info = TaskInfo(name="noop_pass", queues=["default"])
            async def run(self):
                raise AssertionError("must not run on backend error")

        registry = TaskRegistry()
        registry.register(NoopTask)
        w = Worker(broker=broker, backend=backend, registry=registry, config=config)

        from swarmq.exceptions import BackendConnectionError as BCE
        backend.set_schedfire_dedup.side_effect = BCE("valkey down")
        original_headers = {
            "x-swarmq-schedfire": "cron_a:1700000000000",
            "x-tracking-id": "abc-123",
        }
        msg = _make_message(
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
        from swarmq.task import Task, TaskInfo
        from swarmq.registry import TaskRegistry
        from swarmq import worker as worker_mod

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
        from swarmq.task import Task, TaskInfo
        from swarmq.registry import TaskRegistry

        class NoopTask(Task):
            task_info = TaskInfo(name="noop", queues=["default"])
            async def run(self):
                return "ok"

        registry = TaskRegistry()
        registry.register(NoopTask)
        w = Worker(broker=broker, backend=backend, registry=registry, config=config)

        backend.is_schedfire_completed.return_value = True  # prior fire
        backend.is_cancelled.side_effect = AssertionError(
            "cancel-check must not run for a duplicate schedfire"
        )
        msg = _make_message(
            _default_payload(name="noop", args=[], kwargs={}),
            headers={"x-swarmq-schedfire": "cron_a:1700000000000"},
        )
        await w._handle_message(msg)

        broker.ack.assert_awaited_once_with(msg)


# ---------------------------------------------------------------------------
# R3.5 Unique Tasks — Worker release (Units 10 + 11)
# ---------------------------------------------------------------------------


class TestWorkerUniqueRelease:
    """Start-mode pickup release (Unit 10) and completion-mode terminal
    release (Unit 11). Focus on behavioural contracts, not line-by-line
    coverage of every terminal path — each path is exercised by one
    targeted test.

    Header values MUST be 64-char lowercase hex (worker validates the
    format; invalid strings are treated as "no flag" to defend against
    adversarial producers).
    """

    # Fixed 64-char hex digests for deterministic assertions.
    HASH_DEFAULT = "a" * 64
    HASH_FAIL = "b" * 64
    HASH_RETRY = "c" * 64
    HASH_DLQ = "d" * 64
    HASH_RATE_LIMIT = "e" * 64
    HASH_OVERRIDE = "f" * 64
    HASH_TNF = "1" * 64  # task-not-found test
    HASH_MW = "2" * 64  # middleware-factory-fail test

    @pytest.fixture
    def release_backend(self, backend):
        backend.release_unique = AsyncMock(return_value=True)
        return backend

    def _unique_message(self, *, unique_until="completion", task_name="add"):
        return _make_message(
            _default_payload(name=task_name),
            headers={
                "x-swarmq-unique-hash": self.HASH_DEFAULT,
                "x-swarmq-unique-until": unique_until,
            },
        )

    async def test_no_header_skips_release_entirely(
        self, worker, broker, release_backend,
    ):
        """Baseline: messages without the header never call release_unique."""
        msg = _make_message(_default_payload())
        await worker._handle_message(msg)
        release_backend.release_unique.assert_not_awaited()

    async def test_start_mode_releases_at_pickup(
        self, worker, broker, release_backend,
    ):
        """unique_until=start → release BEFORE execution."""
        call_order: list[str] = []

        async def track_release(*_a, **_kw):
            call_order.append("release")
            return True

        async def track_store(*_a, **_kw):
            call_order.append("store_result")

        release_backend.release_unique = AsyncMock(side_effect=track_release)
        release_backend.store_result = AsyncMock(side_effect=track_store)

        msg = self._unique_message(unique_until="start")
        await worker._handle_message(msg)

        # Release fires before the task's result is stored (start-mode).
        assert call_order[0] == "release"
        assert "store_result" in call_order
        # Only one release call in start-mode (no completion-release).
        assert call_order.count("release") == 1

    async def test_completion_mode_releases_at_success(
        self, worker, broker, release_backend,
    ):
        """unique_until=completion → release during terminal success path."""
        msg = self._unique_message(unique_until="completion")
        await worker._handle_message(msg)

        release_backend.release_unique.assert_awaited_once()
        call_args = release_backend.release_unique.await_args.args
        assert call_args[0] == self.HASH_DEFAULT
        broker.ack.assert_awaited_once_with(msg)

    async def test_completion_mode_releases_on_task_exception(
        self, broker, backend, config,
    ):
        """Task raises → release still fires in finally."""
        class FailTask(Task):
            task_info = TaskInfo(name="fail", queues=["default"])
            async def run(self):
                raise RuntimeError("boom")

        registry = TaskRegistry()
        registry.register(FailTask)
        backend.release_unique = AsyncMock(return_value=True)
        w = Worker(broker=broker, backend=backend, registry=registry,
                   config=config)
        msg = _make_message(
            _default_payload(name="fail", args=[], kwargs={}),
            headers={
                "x-swarmq-unique-hash": self.HASH_FAIL,
                "x-swarmq-unique-until": "completion",
            },
        )
        await w._handle_message(msg)
        backend.release_unique.assert_awaited_once()

    async def test_retry_path_preserves_unique_header_and_skips_release(
        self, broker, backend, config,
    ):
        """Retry → publish_to_delay_queue must carry x-swarmq-unique-hash
        forward AND the finally must NOT release (redelivery still owns
        the flag)."""
        class RetryTask(Task):
            task_info = TaskInfo(
                name="retry_task", queues=["default"],
                max_retries=2,
            )
            async def run(self):
                raise RuntimeError("try again")

        registry = TaskRegistry()
        registry.register(RetryTask)
        backend.release_unique = AsyncMock(return_value=True)
        w = Worker(broker=broker, backend=backend, registry=registry,
                   config=config)
        msg = _make_message(
            _default_payload(name="retry_task", args=[], kwargs={}),
            headers={
                "x-swarmq-unique-hash": self.HASH_RETRY,
                "x-swarmq-unique-until": "completion",
            },
        )
        await w._handle_message(msg)

        broker.publish_to_delay_queue.assert_awaited_once()
        forwarded_headers = broker.publish_to_delay_queue.await_args.kwargs[
            "headers"
        ]
        # Header merge preserves the original dedup identity.
        assert forwarded_headers["x-swarmq-unique-hash"] == self.HASH_RETRY
        assert forwarded_headers["x-swarmq-unique-until"] == "completion"
        assert forwarded_headers["x-swarmq-retry-count"] == 1
        # Release must NOT fire — redelivery still owns the flag.
        backend.release_unique.assert_not_awaited()

    async def test_dlq_noretry_releases_unique(
        self, broker, backend, config,
    ):
        """NoRetry exception → direct DLQ path releases inline."""
        class NoRetryTask(Task):
            task_info = TaskInfo(
                name="noretry_task", queues=["default"],
                max_retries=3,
            )
            async def run(self):
                raise NoRetry("do not retry")

        registry = TaskRegistry()
        registry.register(NoRetryTask)
        backend.release_unique = AsyncMock(return_value=True)
        w = Worker(broker=broker, backend=backend, registry=registry,
                   config=config)
        msg = _make_message(
            _default_payload(name="noretry_task", args=[], kwargs={}),
            headers={
                "x-swarmq-unique-hash": self.HASH_DLQ,
                "x-swarmq-unique-until": "completion",
            },
        )
        await w._handle_message(msg)

        broker.publish_to_dead_letter_queue.assert_awaited_once()
        backend.release_unique.assert_awaited()
        dlq_headers = broker.publish_to_dead_letter_queue.await_args.kwargs[
            "headers"
        ]
        assert dlq_headers["x-swarmq-unique-hash"] == self.HASH_DLQ

    async def test_rate_limit_requeue_preserves_header_skips_release(
        self, broker, backend, config,
    ):
        """Rate-limited tasks requeue with header intact; no release."""
        class RateTask(Task):
            task_info = TaskInfo(
                name="rl_task", queues=["default"],
                rate_limit="1/s",
            )
            async def run(self):
                return None

        registry = TaskRegistry()
        registry.register(RateTask)

        from swarmq.limits import RateLimiter
        w = Worker(broker=broker, backend=backend, registry=registry,
                   config=config)
        rl = MagicMock(spec=RateLimiter)
        rl.check = AsyncMock(return_value=(False, 0, 500))
        w._rate_limiter = rl
        backend.release_unique = AsyncMock(return_value=True)

        msg = _make_message(
            _default_payload(name="rl_task", args=[], kwargs={}),
            headers={
                "x-swarmq-unique-hash": self.HASH_RATE_LIMIT,
                "x-swarmq-unique-until": "completion",
            },
        )
        await w._handle_message(msg)

        broker.publish_to_delay_queue.assert_awaited_once()
        headers = broker.publish_to_delay_queue.await_args.kwargs["headers"]
        assert headers["x-swarmq-unique-hash"] == self.HASH_RATE_LIMIT
        backend.release_unique.assert_not_awaited()

    async def test_shutdown_nack_requeue_skips_release(
        self, worker, broker, release_backend,
    ):
        """Shutdown-in-progress nack(requeue=True) leaves the flag."""
        msg = self._unique_message(unique_until="completion")
        worker._shutdown_event.set()
        await worker._handle_message(msg)
        broker.nack.assert_awaited_once_with(msg, requeue=True)
        release_backend.release_unique.assert_not_awaited()

    async def test_pre_start_cancel_releases_unique(
        self, worker, broker, release_backend,
    ):
        """Pre-start cancel → release flag, ack message."""
        release_backend.is_cancelled = AsyncMock(return_value=True)
        msg = self._unique_message(unique_until="completion")
        await worker._handle_message(msg)
        release_backend.release_unique.assert_awaited_once()
        broker.ack.assert_awaited_once_with(msg)

    async def test_header_unique_until_wins_over_taskinfo_default(
        self, broker, backend, config,
    ):
        """Per-call override via header → worker honours start even when
        TaskInfo defaults to completion."""
        class AddTaskCompletion(Task):
            task_info = TaskInfo(
                name="add_c", queues=["default"],
                unique_for=60, unique_until="completion",
            )
            async def run(self, a: int, b: int):
                return a + b

        registry = TaskRegistry()
        registry.register(AddTaskCompletion)
        backend.release_unique = AsyncMock(return_value=True)
        w = Worker(broker=broker, backend=backend, registry=registry,
                   config=config)

        msg = _make_message(
            _default_payload(name="add_c"),
            headers={
                "x-swarmq-unique-hash": self.HASH_OVERRIDE,
                "x-swarmq-unique-until": "start",  # per-call override
            },
        )
        await w._handle_message(msg)

        # In start mode release happens at pickup. Exactly one release
        # call — the completion-mode release in finally is a no-op.
        assert backend.release_unique.await_count == 1

    # --- R3.5 review follow-up: missing terminal-path coverage (TEST-001, TEST-002) ---

    async def test_task_not_found_with_unique_header_releases_flag(
        self, worker, broker, release_backend,
    ):
        """Unregistered task + unique header → nack(requeue=False), release fires in finally."""
        msg = _make_message(
            _default_payload(name="unknown_task"),
            headers={
                "x-swarmq-unique-hash": self.HASH_TNF,
                "x-swarmq-unique-until": "completion",
            },
        )
        await worker._handle_message(msg)

        broker.nack.assert_awaited_once_with(msg, requeue=False)
        release_backend.release_unique.assert_awaited_once()
        call_args = release_backend.release_unique.await_args.args
        assert call_args[0] == self.HASH_TNF

    async def test_middleware_factory_failure_with_unique_header_releases_flag(
        self, broker, backend, config,
    ):
        """Bad middleware chain factory → nack(requeue=False), release fires."""
        registry = TaskRegistry()
        registry.register(AddTask)
        backend.release_unique = AsyncMock(return_value=True)

        def broken_factory():
            raise RuntimeError("middleware factory is broken")

        w = Worker(
            broker=broker, backend=backend, registry=registry,
            config=config, middleware_chain=broken_factory,
        )
        msg = _make_message(
            _default_payload(),
            headers={
                "x-swarmq-unique-hash": self.HASH_MW,
                "x-swarmq-unique-until": "completion",
            },
        )
        await w._handle_message(msg)

        broker.nack.assert_awaited_once_with(msg, requeue=False)
        backend.release_unique.assert_awaited_once()

    async def test_malformed_unique_hash_header_is_ignored(
        self, worker, broker, release_backend,
    ):
        """Adversarial producer: 100KB header value. Worker must reject
        (format validation) and never forward to Valkey."""
        msg = _make_message(
            _default_payload(),
            headers={
                "x-swarmq-unique-hash": "x" * 100_000,  # not 64-char hex
                "x-swarmq-unique-until": "completion",
            },
        )
        await worker._handle_message(msg)
        release_backend.release_unique.assert_not_awaited()

    async def test_lock_conflict_cap_dlq_releases_unique(
        self, broker, backend, config,
    ):
        """Lock-conflict at DLQ cap terminal → release_unique fires AFTER
        ack (P1 #3 ordering)."""
        reg = TaskRegistry()
        reg.register(LockedTask)
        backend.release_unique = AsyncMock(return_value=True)
        w = Worker(broker=broker, backend=backend, registry=reg, config=config)
        w._lock_manager = MagicMock()
        w._lock_manager.acquire = AsyncMock(return_value=False)

        msg = _make_message(
            _default_payload(name="locked", args=[], kwargs={}),
            headers={
                "x-swarmq-unique-hash": "4" * 64,
                "x-swarmq-unique-until": "completion",
                "x-swarmq-lock-conflict-count": 9,  # 9+1 = cap
            },
        )
        await w._handle_message(msg)

        broker.publish_to_dead_letter_queue.assert_awaited_once()
        broker.ack.assert_awaited_once_with(msg)
        backend.release_unique.assert_awaited()

    async def test_lock_conflict_requeue_preserves_header_skips_release(
        self, broker, backend, config,
    ):
        """Lock-conflict below cap → requeue with unique-hash header; no release."""
        reg = TaskRegistry()
        reg.register(LockedTask)
        backend.release_unique = AsyncMock(return_value=True)
        w = Worker(broker=broker, backend=backend, registry=reg, config=config)
        w._lock_manager = MagicMock()
        w._lock_manager.acquire = AsyncMock(return_value=False)

        msg = _make_message(
            _default_payload(name="locked", args=[], kwargs={}),
            headers={
                "x-swarmq-unique-hash": "5" * 64,
                "x-swarmq-unique-until": "completion",
            },
        )
        await w._handle_message(msg)

        broker.publish_to_delay_queue.assert_awaited_once()
        headers = broker.publish_to_delay_queue.await_args.kwargs["headers"]
        assert headers["x-swarmq-unique-hash"] == "5" * 64
        backend.release_unique.assert_not_awaited()

    async def test_cancel_observed_post_execution_releases_unique(
        self, broker, backend, config,
    ):
        """Late cancel path (flag set while run() was executing) falls
        through to the is_cancelled branch — release fires normally in
        the finally block via _unique_release_pending=True default."""
        class CancellableTask(Task):
            task_info = TaskInfo(
                name="cancellable", queues=["default"],
                max_retries=1,
            )

            async def run(self):
                raise RuntimeError("will retry unless late cancel fires")

        reg = TaskRegistry()
        reg.register(CancellableTask)
        # is_cancelled returns True at the post-execution re-check
        # (normal pre-start check path is not hit because we set the
        # flag only after run() started).
        backend.is_cancelled = AsyncMock(side_effect=[False, True])
        backend.release_unique = AsyncMock(return_value=True)
        backend.clear_cancel_flag = AsyncMock()

        w = Worker(broker=broker, backend=backend, registry=reg, config=config)
        msg = _make_message(
            _default_payload(name="cancellable", args=[], kwargs={}),
            headers={
                "x-swarmq-unique-hash": "6" * 64,
                "x-swarmq-unique-until": "completion",
            },
        )
        await w._handle_message(msg)

        # late_cancel flipped to cancel path: stores cancel result, acks,
        # clears cancel flag, finally releases unique flag.
        broker.ack.assert_awaited_once_with(msg)
        backend.clear_cancel_flag.assert_awaited()
        backend.release_unique.assert_awaited_once()

    async def test_start_mode_rate_limited_keeps_flag_for_redelivery(
        self, broker, backend, config,
    ):
        """P1 #4 / ADV-002 fix: start-mode release must NOT fire when the
        message is rate-limited and requeued. Otherwise a concurrent
        schedule() during the requeue window would bypass dedup."""
        class RateStartTask(Task):
            task_info = TaskInfo(
                name="rl_start_task", queues=["default"],
                rate_limit="1/s",
                unique_for=60, unique_until="start",
            )
            async def run(self):
                return None

        registry = TaskRegistry()
        registry.register(RateStartTask)

        from swarmq.limits import RateLimiter
        w = Worker(broker=broker, backend=backend, registry=registry,
                   config=config)
        rl = MagicMock(spec=RateLimiter)
        rl.check = AsyncMock(return_value=(False, 0, 500))
        w._rate_limiter = rl
        backend.release_unique = AsyncMock(return_value=True)

        msg = _make_message(
            _default_payload(name="rl_start_task", args=[], kwargs={}),
            headers={
                "x-swarmq-unique-hash": "3" * 64,
                "x-swarmq-unique-until": "start",
            },
        )
        await w._handle_message(msg)

        # Rate-limit path re-published the message — no release at all.
        broker.publish_to_delay_queue.assert_awaited_once()
        backend.release_unique.assert_not_awaited()
        headers = broker.publish_to_delay_queue.await_args.kwargs["headers"]
        assert headers["x-swarmq-unique-hash"] == "3" * 64
        assert headers["x-swarmq-unique-until"] == "start"
