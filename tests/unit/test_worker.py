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
    ResultSerializationError,
    TaskNotFoundError,
)
from swarmq.registry import TaskRegistry
from swarmq.serialization import decode_result, encode_json
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_message(payload: dict) -> MagicMock:
    """Create a mock AMQP message with JSON-encoded body."""
    msg = MagicMock()
    msg.body = encode_json(payload)
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

    # The spawned task hasn't run yet; give the event loop a tick
    await asyncio.sleep(0)

    broker.ack.assert_awaited_once_with(msg)
    assert backend.store_result.await_count == 1


# ---------------------------------------------------------------------------
# start / stop
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_start_connects_and_declares_queues(worker, broker, backend):
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
