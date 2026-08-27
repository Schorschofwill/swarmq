"""Unit tests for Worker consumer loop."""

from __future__ import annotations

import asyncio
import contextlib
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import structlog

from swarmq.config import SwarmQConfig
from swarmq.exceptions import (
    BackendConnectionError,
    NoRetry,
    Retry,
)

# SwarmQConfig is used in fixtures for the Unit 7 monitor tests.
from swarmq.registry import TaskRegistry
from swarmq.serialization import decode_result
from swarmq.signals import Signal
from swarmq.task import DeliveryMode, Task, TaskInfo
from swarmq.worker import Worker
from tests.support.messages import make_message
from tests.unit.worker._helpers import AddTask, _default_payload  # Re-Export (test_app)
from tests.unit.worker.gates.test_lock import LockedTask

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------







# ---------------------------------------------------------------------------
# 1. Happy path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_happy_path_message_processed_and_acked(worker, broker, backend):
    msg = make_message(_default_payload())

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
    msg = make_message(_default_payload(args=[10, 20]))

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

    msg = make_message(_default_payload(name="lifecycle", args=[], kwargs={}))
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
        make_message(_default_payload(task_id=f"t-{i}", name="slow", args=[], kwargs={}))
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

    msg = make_message(_default_payload(name="nostore", args=[], kwargs={}))
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

    msg = make_message(_default_payload(name="fail", args=[], kwargs={}))
    await w._handle_message(msg)

    stored = decode_result(backend.store_result.call_args[0][1])
    assert stored["is_error"] is True
    assert "boom" in stored["error_message"]
    assert stored["result"] is None
    # U8: der Dead-Letter-Pfad bestätigt das Original selbst
    broker.dead_letter.assert_awaited_once()
    broker.ack.assert_not_awaited()


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

    msg = make_message(_default_payload(name="prefail", args=[], kwargs={}))
    await w._handle_message(msg)

    assert post_called is False
    stored = decode_result(backend.store_result.call_args[0][1])
    assert stored["is_error"] is True
    assert "pre failed" in stored["error_message"]
    # U8: der Dead-Letter-Pfad bestätigt das Original selbst
    broker.dead_letter.assert_awaited_once()
    broker.ack.assert_not_awaited()


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

    msg = make_message(_default_payload(name="postfail", args=[], kwargs={}))
    await w._handle_message(msg)

    stored = decode_result(backend.store_result.call_args[0][1])
    assert stored["is_error"] is True
    assert "post failed" in stored["error_message"]
    # U8: der Dead-Letter-Pfad bestätigt das Original selbst
    broker.dead_letter.assert_awaited_once()
    broker.ack.assert_not_awaited()


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

    msg = make_message(_default_payload(name="badresult", args=[], kwargs={}))
    await w._handle_message(msg)

    stored = decode_result(backend.store_result.call_args[0][1])
    assert stored["is_error"] is True
    assert stored["result"] is None
    broker.ack.assert_awaited_once_with(msg)
    broker.redeliver.assert_not_awaited()


# ---------------------------------------------------------------------------
# 10. Task not in registry -> nack(requeue=False)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_unknown_task_nacked_no_requeue(worker, broker, backend):
    msg = make_message(_default_payload(name="nonexistent"))

    await worker._handle_message(msg)

    broker.route_via_dlx.assert_awaited_once_with(msg)
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
    msg = make_message(_default_payload())

    await worker._handle_message(msg)

    broker.redeliver.assert_awaited_once_with(msg)
    broker.ack.assert_not_awaited()


# ---------------------------------------------------------------------------
# 12. Invalid JSON -> nack(requeue=False)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_invalid_json_nacked_no_requeue(worker, broker, backend):
    msg = make_message(body=b"not valid json {{{")

    await worker._handle_message(msg)

    broker.route_via_dlx.assert_awaited_once_with(msg)
    broker.ack.assert_not_awaited()
    backend.store_result.assert_not_awaited()


# ---------------------------------------------------------------------------
# 13. Missing task.name field -> nack(requeue=False)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_missing_task_name_nacked_no_requeue(worker, broker, backend):
    payload = {"task": {"id": "abc", "queue": "default"}, "args": [], "kwargs": {}}
    msg = make_message(payload)

    await worker._handle_message(msg)

    broker.route_via_dlx.assert_awaited_once_with(msg)
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

    msg = make_message(_default_payload(name="nonetask", args=[], kwargs={}))
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

    msg1 = make_message(_default_payload(name="explode", args=[], kwargs={}))
    msg2 = make_message(_default_payload(name="explode", args=[], kwargs={}))

    # If semaphore leaks, second message would hang forever
    await asyncio.wait_for(
        asyncio.gather(w._handle_message(msg1), w._handle_message(msg2)),
        timeout=2.0,
    )

    # U8: beide Fehler gehen über dead_letter (bestätigt selbst)
    assert broker.dead_letter.await_count == 2
    broker.ack.assert_not_awaited()


# ---------------------------------------------------------------------------
# 16. _on_message spawns a task instead of blocking
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_on_message_creates_task(worker, broker, backend):
    """_on_message should return immediately, spawning _handle_message as a task."""
    msg = make_message(_default_payload())

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

    msg = make_message(_default_payload(name="ttltask", args=[], kwargs={}))
    await w._handle_message(msg)

    call_args = backend.store_result.call_args
    assert call_args[0][2] == 300_000  # ttl_ms = result_ttl * 1000


@pytest.mark.asyncio
async def test_result_ttl_falls_back_to_config(worker, backend):
    msg = make_message(_default_payload())

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

    msg = make_message(
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

    with patch("swarmq.worker.pipeline.logger") as mock_logger:
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

    msg = make_message(_default_payload(name="fnf", args=[], kwargs={}))
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

    msg = make_message(_default_payload(name="alo", args=[], kwargs={}))
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

    msg = make_message(_default_payload(name="fnf_ttl", args=[], kwargs={}))
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

    msg = make_message(_default_payload(name="fnf_no_ttl", args=[], kwargs={}))
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

    msg = make_message(_default_payload(name="fnf_fail", args=[], kwargs={}))

    with patch("swarmq.worker.execution.logger") as mock_logger:
        await w._handle_message(msg)

        # Error must be logged
        mock_logger.error.assert_called()

    # nack must NOT be called (message was already acked)
    broker.redeliver.assert_not_awaited()
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

    msg = make_message(_default_payload(name="default_delivery2", args=[], kwargs={}))
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

    msg = make_message(
        _default_payload(name="retry_task", args=[], kwargs={}),
        headers={},
    )
    await w._handle_message(msg)

    # Should publish to delay queue, not store error result
    broker.retry_later.assert_awaited_once()
    call_kwargs = broker.retry_later.call_args
    # U8 (KTD2): retry_later erhält die Zustellung selbst — das Ziel ist das
    # Verzögerungsziel der Zustellungs-Queue, nicht mehr die Body-Queue.
    assert call_kwargs[0][0] is msg
    # delay_ms should be within jitter bounds: base=1.0, attempt=0
    # exponential: 1.0 * 2^0 * jitter(1,2) = [1.0, 2.0] seconds = [1000, 2000] ms
    delay_ms = call_kwargs.kwargs["delay_ms"]
    assert 1000 <= delay_ms <= 2000
    # U8: retry_later bestätigt das Original selbst
    broker.ack.assert_not_awaited()
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
    msg = make_message(
        _default_payload(name="retry_inc", args=[], kwargs={}),
        headers={"x-swarmq-retry-count": 0},
    )
    await w._handle_message(msg)

    call_kwargs = broker.retry_later.call_args
    headers = call_kwargs.kwargs["headers"]
    assert headers["x-swarmq-retry-count"] == 1

    # Second attempt (retry_count=1 in headers)
    broker.retry_later.reset_mock()
    broker.ack.reset_mock()
    msg2 = make_message(
        _default_payload(name="retry_inc", args=[], kwargs={}),
        headers={"x-swarmq-retry-count": 1},
    )
    await w._handle_message(msg2)

    call_kwargs2 = broker.retry_later.call_args
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
    msg = make_message(
        _default_payload(name="retry_exhaust", args=[], kwargs={}),
        headers={"x-swarmq-retry-count": 2},
    )
    await w._handle_message(msg)

    # Should publish to DLQ
    broker.dead_letter.assert_awaited_once()
    dlq_call = broker.dead_letter.call_args
    assert dlq_call[0][0] is msg  # U8: dead_letter erhält die Zustellung selbst
    # Should NOT publish to delay queue
    broker.retry_later.assert_not_awaited()
    # Original message acked
    # U8: retry_later/dead_letter bestätigen das Original selbst
    broker.ack.assert_not_awaited()


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

    msg = make_message(
        _default_payload(name="retry_delay", args=[], kwargs={}),
        headers={},
    )
    await w._handle_message(msg)

    broker.retry_later.assert_awaited_once()
    call_kwargs = broker.retry_later.call_args
    delay_ms = call_kwargs.kwargs["delay_ms"]
    # Should be exactly 60 seconds = 60000 ms (no jitter)
    assert delay_ms == 60000
    # U8: retry_later/dead_letter bestätigen das Original selbst
    broker.ack.assert_not_awaited()


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

    msg = make_message(
        _default_payload(name="noretry_task", args=[], kwargs={}),
        headers={},
    )
    await w._handle_message(msg)

    # Should go to DLQ directly
    broker.dead_letter.assert_awaited_once()
    # Should NOT retry
    broker.retry_later.assert_not_awaited()
    # U8: retry_later/dead_letter bestätigen das Original selbst
    broker.ack.assert_not_awaited()


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

    msg = make_message(
        _default_payload(name="filtered_retry", args=[], kwargs={}),
        headers={},
    )
    await w._handle_message(msg)

    # ValueError is not in retry_exceptions -> DLQ
    broker.dead_letter.assert_awaited_once()
    broker.retry_later.assert_not_awaited()
    # U8: retry_later/dead_letter bestätigen das Original selbst
    broker.ack.assert_not_awaited()


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

    msg = make_message(
        _default_payload(name="filtered_retry_match", args=[], kwargs={}),
        headers={},
    )
    await w._handle_message(msg)

    # ConnectionError matches retry_exceptions -> retry
    broker.retry_later.assert_awaited_once()
    broker.dead_letter.assert_not_awaited()
    # U8: retry_later/dead_letter bestätigen das Original selbst
    broker.ack.assert_not_awaited()


@pytest.mark.asyncio
async def test_max_retries_zero_error_routes_to_dlq(broker, backend, config):
    """max_retries=0 (default) -> no retry, but the terminal failure
    routes through the DLQ pipeline like every other terminal failure.

    Audit 2026-06-02 P2-2 (deliberate behaviour change, user-approved
    in Härtungs-Runde 2 follow-up): previously a max_retries=0 error
    stored the result + acked WITHOUT the DLQ copy — operators got
    uniform DLQ inspection only for tasks that opted into retries, and
    TASK_DLQ never fired for the most common "no recovery" config.
    Now: error result stored (get_result contract unchanged), message
    copied to {queue}.XQ, TASK_DLQ dispatched, original acked."""
    from swarmq.signals import Signal as _Signal

    class NoRetryDefaultTask(Task):
        task_info = TaskInfo(name="no_retry_default", queues=["default"])
        # max_retries defaults to 0

        async def run(self) -> None:
            raise RuntimeError("error without retry")

    reg = TaskRegistry()
    reg.register(NoRetryDefaultTask)
    w = Worker(broker=broker, backend=backend, registry=reg, config=config)

    dlq_signals: list[dict] = []

    @w._signal_dispatcher.on_signal(_Signal.TASK_DLQ)
    async def _on_dlq(**kw):
        dlq_signals.append(kw)

    msg = make_message(
        _default_payload(name="no_retry_default", args=[], kwargs={}),
        headers={},
    )
    await w._handle_message(msg)

    # No retry — straight to terminal DLQ handling.
    broker.retry_later.assert_not_awaited()
    broker.dead_letter.assert_awaited_once()
    backend.store_result.assert_awaited_once()
    stored = decode_result(backend.store_result.call_args[0][1])
    assert stored["is_error"] is True
    assert "error without retry" in stored["error_message"]
    # U8: retry_later/dead_letter bestätigen das Original selbst
    broker.ack.assert_not_awaited()
    assert len(dlq_signals) == 1
    assert dlq_signals[0]["task_name"] == "no_retry_default"


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

    msg = make_message(
        _default_payload(name="fnf_retry", args=[], kwargs={}),
        headers={},
    )
    await w._handle_message(msg)

    # FIRE_AND_FORGET: no retry, no DLQ publish, no error result
    broker.retry_later.assert_not_awaited()
    broker.dead_letter.assert_not_awaited()
    backend.store_result.assert_not_awaited()
    # Ack happened once (the early FIRE_AND_FORGET ack)
    broker.ack.assert_awaited_once()


# ---------------------------------------------------------------------------
# Pub/Sub Notification after store_result (Unit 2)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_publish_notification_after_success(worker, broker, backend):
    """After successful task -> publish_notification(task_id) called."""
    msg = make_message(_default_payload())

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

    msg = make_message(_default_payload(name="error_notify", args=[], kwargs={}))
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
    msg = make_message(
        _default_payload(name="dlq_notify", args=[], kwargs={}),
        headers={"x-swarmq-retry-count": 1},
    )
    await w._handle_message(msg)

    broker.dead_letter.assert_awaited_once()
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

    msg = make_message(_default_payload(name="no_ttl_notify", args=[], kwargs={}))
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

    msg = make_message(_default_payload(name="notify_fail", args=[], kwargs={}))
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

        msg = make_message(_default_payload(name="order_pre", args=[], kwargs={}))
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

        msg = make_message(_default_payload(name="good_post", args=[], kwargs={}))
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

        msg = make_message(_default_payload(name="post_fail_hook", args=[], kwargs={}))
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

        msg = make_message(_default_payload(name="boom_hook", args=[], kwargs={}))
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

        msg = make_message(
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
        broker.retry_later.assert_awaited_once()

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

        msg = make_message(
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
        msg = make_message(
            _default_payload(name="exhausted_hook", args=[], kwargs={}),
            headers={"x-swarmq-retry-count": 2},
        )
        await w._handle_message(msg)

        mw_chain.run_on_error.assert_awaited_once()
        mw_chain.run_on_retry.assert_not_awaited()
        broker.dead_letter.assert_awaited_once()

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
        msg = make_message(
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

        msg = make_message(_default_payload())
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

        msg = make_message(_default_payload())
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

        msg = make_message(_default_payload(name="fail_sig", args=[], kwargs={}))
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

        msg = make_message(
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

        msg = make_message(
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

        msg = make_message(_default_payload())
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

        msg1 = make_message(_default_payload())
        await w._handle_message(msg1)
        msg2 = make_message(_default_payload())
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

        msg = make_message(
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

        await w._handle_message(make_message(_default_payload()))

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

        msg = make_message(_default_payload())
        await w._handle_message(msg)

        broker.route_via_dlx.assert_awaited_once_with(msg)
        broker.ack.assert_not_awaited()


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
            await worker._executor._cancel_monitor(
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
            worker._executor._cancel_monitor(
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
        msg = make_message(_default_payload(name="slow_run", args=[], kwargs={}))

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
        broker.redeliver.assert_not_awaited()

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

        slow_worker._signal_dispatcher.connect(Signal.TASK_CANCELLED, cancelled_handler)
        slow_worker._signal_dispatcher.connect(Signal.TASK_COMPLETED, completed_handler)
        slow_worker._signal_dispatcher.connect(Signal.TASK_FAILED, failed_handler)

        msg = make_message(_default_payload(name="slow_run", args=[], kwargs={}))
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

        msg = make_message(_default_payload(name="retry_slow", args=[], kwargs={}))
        await w._handle_message(msg)

        # Publish to delay queue (retry) must NOT happen.
        broker.retry_later.assert_not_awaited()
        broker.dead_letter.assert_not_awaited()
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

        msg = make_message(_default_payload(name="slow_run", args=[], kwargs={}))
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
        w._signal_dispatcher.connect(Signal.TASK_CANCELLED, h)

        msg = make_message(_default_payload(name="faf_slow", args=[], kwargs={}))
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

        msg = make_message(_default_payload(name="race_task", args=[], kwargs={}))
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

        msg = make_message(_default_payload(name="timeout_only", args=[], kwargs={}))
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
        return make_message(
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
        msg = make_message(_default_payload())
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
        msg = make_message(
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
        """Retry → retry_later must carry x-swarmq-unique-hash
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
        msg = make_message(
            _default_payload(name="retry_task", args=[], kwargs={}),
            headers={
                "x-swarmq-unique-hash": self.HASH_RETRY,
                "x-swarmq-unique-until": "completion",
            },
        )
        await w._handle_message(msg)

        broker.retry_later.assert_awaited_once()
        forwarded_headers = broker.retry_later.await_args.kwargs[
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
        msg = make_message(
            _default_payload(name="noretry_task", args=[], kwargs={}),
            headers={
                "x-swarmq-unique-hash": self.HASH_DLQ,
                "x-swarmq-unique-until": "completion",
            },
        )
        await w._handle_message(msg)

        broker.dead_letter.assert_awaited_once()
        backend.release_unique.assert_awaited()
        dlq_headers = broker.dead_letter.await_args.kwargs[
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

        msg = make_message(
            _default_payload(name="rl_task", args=[], kwargs={}),
            headers={
                "x-swarmq-unique-hash": self.HASH_RATE_LIMIT,
                "x-swarmq-unique-until": "completion",
            },
        )
        await w._handle_message(msg)

        broker.retry_later.assert_awaited_once()
        headers = broker.retry_later.await_args.kwargs["headers"]
        assert headers["x-swarmq-unique-hash"] == self.HASH_RATE_LIMIT
        backend.release_unique.assert_not_awaited()

    async def test_shutdown_nack_requeue_skips_release(
        self, worker, broker, release_backend,
    ):
        """Shutdown-in-progress nack(requeue=True) leaves the flag."""
        msg = self._unique_message(unique_until="completion")
        worker._shutdown_event.set()
        await worker._handle_message(msg)
        broker.redeliver.assert_awaited_once_with(msg)
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

        msg = make_message(
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
        msg = make_message(
            _default_payload(name="unknown_task"),
            headers={
                "x-swarmq-unique-hash": self.HASH_TNF,
                "x-swarmq-unique-until": "completion",
            },
        )
        await worker._handle_message(msg)

        broker.route_via_dlx.assert_awaited_once_with(msg)
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
        msg = make_message(
            _default_payload(),
            headers={
                "x-swarmq-unique-hash": self.HASH_MW,
                "x-swarmq-unique-until": "completion",
            },
        )
        await w._handle_message(msg)

        broker.route_via_dlx.assert_awaited_once_with(msg)
        backend.release_unique.assert_awaited_once()

    async def test_malformed_unique_hash_header_is_ignored(
        self, worker, broker, release_backend,
    ):
        """Adversarial producer: 100KB header value. Worker must reject
        (format validation) and never forward to Valkey."""
        msg = make_message(
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

        msg = make_message(
            _default_payload(name="locked", args=[], kwargs={}),
            headers={
                "x-swarmq-unique-hash": "4" * 64,
                "x-swarmq-unique-until": "completion",
                "x-swarmq-lock-conflict-count": 9,  # 9+1 = cap
            },
        )
        await w._handle_message(msg)

        broker.dead_letter.assert_awaited_once()
        # U8: retry_later/dead_letter bestätigen das Original selbst
        broker.ack.assert_not_awaited()
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

        msg = make_message(
            _default_payload(name="locked", args=[], kwargs={}),
            headers={
                "x-swarmq-unique-hash": "5" * 64,
                "x-swarmq-unique-until": "completion",
            },
        )
        await w._handle_message(msg)

        broker.retry_later.assert_awaited_once()
        headers = broker.retry_later.await_args.kwargs["headers"]
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
        msg = make_message(
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

        msg = make_message(
            _default_payload(name="rl_start_task", args=[], kwargs={}),
            headers={
                "x-swarmq-unique-hash": "3" * 64,
                "x-swarmq-unique-until": "start",
            },
        )
        await w._handle_message(msg)

        # Rate-limit path re-published the message — no release at all.
        broker.retry_later.assert_awaited_once()
        backend.release_unique.assert_not_awaited()
        headers = broker.retry_later.await_args.kwargs["headers"]
        assert headers["x-swarmq-unique-hash"] == "3" * 64
        assert headers["x-swarmq-unique-until"] == "start"


# ---------------------------------------------------------------------------
# Härtung R2 P1-10: Workflow-Cancel stoppt eingereihte Steps am Pickup
# ---------------------------------------------------------------------------


class TestBodySizeCap:
    """Ein kompromittierter/fehlkonfigurierter Producer kann beliebig
    große Bodies liefern; msgspec dekodiert sie vollständig in den
    Speicher, BEVOR irgendeine Validierung greift (Tiefenschutz hat
    msgspec eingebaut, Größenschutz nicht). Oversized → DLQ-Route
    (nack requeue=False), kein Decode-Versuch."""

    async def test_oversized_body_nacked_without_decode(
        self, worker, broker, backend,
    ):
        cap = worker._config.max_message_body_bytes
        msg = MagicMock()
        msg.body = b"x" * (cap + 1)
        msg.headers = {}

        with patch("swarmq.worker.pipeline.decode_json") as mock_decode:
            await worker._handle_message(msg)

        mock_decode.assert_not_called()
        broker.route_via_dlx.assert_awaited_once_with(msg)

    async def test_body_at_cap_still_processed(self, worker, broker, backend):
        """Genau am Cap: normal verarbeiten (Grenze ist exklusiv)."""
        payload = _default_payload(name="add", args=[1, 2], kwargs={})
        msg = make_message(payload)
        assert len(msg.body) <= worker._config.max_message_body_bytes
        await worker._handle_message(msg)
        broker.ack.assert_awaited()

    def test_default_cap_is_16_mib(self):
        cfg = SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x")
        assert cfg.max_message_body_bytes == 16 * 1024 * 1024

    def test_env_override(self, monkeypatch):
        monkeypatch.setenv("SWARMQ_MAX_MESSAGE_BODY_BYTES", "1024")
        cfg = SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x")
        assert cfg.max_message_body_bytes == 1024
