"""E2E tests for Middleware and Signals — real RabbitMQ + Valkey.

These tests exercise the full worker/client round-trip to verify that
middleware hooks fire and signal handlers are invoked with the expected
payloads when tasks are enqueued, executed, and fail.
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from typing import Any, override

import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.middleware import Middleware
from swarmq.signals import Signal
from swarmq.task import Task, TaskInfo


def _unique_queue() -> str:
    """Return a queue name unique to this test run."""
    return f"e2e-{uuid.uuid4().hex[:12]}"


# ---------------------------------------------------------------------------
# Test 1: Custom Middleware receives pre_execute + post_execute
# ---------------------------------------------------------------------------


async def test_middleware_pre_and_post_execute_fire(rabbitmq_url: str, valkey_url: str) -> None:
    queue = _unique_queue()
    task_name = f"e2e_mw_ok_{uuid.uuid4().hex[:8]}"

    completed = asyncio.Event()
    events: list[tuple[str, dict[str, Any], Any]] = []

    class EchoTask(Task):
        task_info = TaskInfo(name=task_name, queues=[queue])

        @override
        async def run(self, value: int) -> int:
            return value * 2

    class TrackingMiddleware(Middleware):
        @override
        async def pre_execute(self, message: dict) -> None:
            events.append(("pre_execute", dict(message), None))

        @override
        async def post_execute(self, message: dict, result: Any = None) -> None:
            events.append(("post_execute", dict(message), result))
            completed.set()

    broker = RabbitMQBroker(rabbitmq_url)
    backend = ValkeyBackend(valkey_url)
    app = SwarmQ(broker, backend, queues=[queue])

    async with app:
        await broker.declare_queue(queue)

        worker_broker = RabbitMQBroker(rabbitmq_url)
        worker_backend = ValkeyBackend(valkey_url)
        worker_app = SwarmQ(worker_broker, worker_backend, queues=[queue])
        worker_app.add_middleware(TrackingMiddleware())
        worker_task = asyncio.create_task(worker_app.start_worker())

        try:
            await asyncio.sleep(0.5)

            task_id = await app.schedule(task_name, 21)
            result = await app.get_result(task_id, timeout=10)
            assert result == 42

            await asyncio.wait_for(completed.wait(), timeout=5)
        finally:
            worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await worker_task
            await worker_broker.close()
            await worker_backend.close()

    hooks = [name for name, _msg, _res in events]
    assert hooks == ["pre_execute", "post_execute"], hooks

    pre_name, pre_msg, _ = events[0]
    assert pre_name == "pre_execute"
    assert pre_msg["task_id"] == task_id
    assert pre_msg["task_name"] == task_name
    assert pre_msg["queue"] == queue
    assert pre_msg["args"] == [21]
    assert pre_msg["kwargs"] == {}
    assert "delivery_mode" in pre_msg
    assert "retry_count" in pre_msg

    post_name, post_msg, post_result = events[1]
    assert post_name == "post_execute"
    assert post_msg["task_id"] == task_id
    assert post_result == 42


# ---------------------------------------------------------------------------
# Test 2: on_error fires for failing task
# ---------------------------------------------------------------------------


async def test_middleware_on_error_fires_for_failing_task(rabbitmq_url: str, valkey_url: str) -> None:
    queue = _unique_queue()
    task_name = f"e2e_mw_err_{uuid.uuid4().hex[:8]}"

    on_error_called = asyncio.Event()
    captured: list[tuple[dict[str, Any], BaseException]] = []

    class BoomTask(Task):
        task_info = TaskInfo(name=task_name, queues=[queue])

        @override
        async def run(self) -> None:
            raise ValueError("boom")

    class ErrorTrackingMiddleware(Middleware):
        @override
        async def on_error(self, message: dict, exception: BaseException) -> None:
            captured.append((dict(message), exception))
            on_error_called.set()

    broker = RabbitMQBroker(rabbitmq_url)
    backend = ValkeyBackend(valkey_url)
    app = SwarmQ(broker, backend, queues=[queue])

    async with app:
        await broker.declare_queue(queue)

        worker_broker = RabbitMQBroker(rabbitmq_url)
        worker_backend = ValkeyBackend(valkey_url)
        worker_app = SwarmQ(worker_broker, worker_backend, queues=[queue])
        worker_app.add_middleware(ErrorTrackingMiddleware())
        worker_task = asyncio.create_task(worker_app.start_worker())

        try:
            await asyncio.sleep(0.5)

            task_id = await app.schedule(task_name)
            await asyncio.wait_for(on_error_called.wait(), timeout=5)
        finally:
            worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await worker_task
            await worker_broker.close()
            await worker_backend.close()

    assert len(captured) == 1
    msg, exc = captured[0]
    assert msg["task_id"] == task_id
    assert msg["task_name"] == task_name
    assert isinstance(exc, ValueError)
    assert "boom" in str(exc)


# ---------------------------------------------------------------------------
# Test 3: Signal TASK_COMPLETED handler receives task_id + duration_ms
# ---------------------------------------------------------------------------


async def test_signal_task_completed_handler_receives_kwargs(rabbitmq_url: str, valkey_url: str) -> None:
    queue = _unique_queue()
    task_name = f"e2e_sig_ok_{uuid.uuid4().hex[:8]}"

    signal_received = asyncio.Event()
    captured: dict[str, Any] = {}

    class PingTask(Task):
        task_info = TaskInfo(name=task_name, queues=[queue])

        @override
        async def run(self) -> str:
            return "pong"

    broker = RabbitMQBroker(rabbitmq_url)
    backend = ValkeyBackend(valkey_url)
    app = SwarmQ(broker, backend, queues=[queue])

    async with app:
        await broker.declare_queue(queue)

        worker_broker = RabbitMQBroker(rabbitmq_url)
        worker_backend = ValkeyBackend(valkey_url)
        worker_app = SwarmQ(worker_broker, worker_backend, queues=[queue])

        @worker_app.on_signal(Signal.TASK_COMPLETED)
        async def on_completed(**kwargs: Any) -> None:
            captured.update(kwargs)
            signal_received.set()

        worker_task = asyncio.create_task(worker_app.start_worker())

        try:
            await asyncio.sleep(0.5)

            task_id = await app.schedule(task_name)
            result = await app.get_result(task_id, timeout=10)
            assert result == "pong"

            await asyncio.wait_for(signal_received.wait(), timeout=5)
        finally:
            worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await worker_task
            await worker_broker.close()
            await worker_backend.close()

    assert captured["task_id"] == task_id
    assert captured["task_name"] == task_name
    assert "duration_ms" in captured
    assert isinstance(captured["duration_ms"], (int, float))
    assert captured["duration_ms"] > 0


# ---------------------------------------------------------------------------
# Test 4: pre_enqueue Exception prevents enqueue
# ---------------------------------------------------------------------------


async def test_pre_enqueue_exception_prevents_enqueue(rabbitmq_url: str, valkey_url: str) -> None:
    queue = _unique_queue()
    task_name = f"e2e_mw_block_{uuid.uuid4().hex[:8]}"

    worker_received = asyncio.Event()

    class NeverRunTask(Task):
        task_info = TaskInfo(name=task_name, queues=[queue])

        @override
        async def run(self) -> str:
            worker_received.set()
            return "should not happen"

    class BlockingMiddleware(Middleware):
        @override
        async def pre_enqueue(self, message: dict) -> None:
            raise ValueError("enqueue blocked by middleware")

    broker = RabbitMQBroker(rabbitmq_url)
    backend = ValkeyBackend(valkey_url)
    app = SwarmQ(broker, backend, queues=[queue])

    # SwarmQ wires a factory into the Client so middleware registered after
    # construction is live — no snapshot refresh required.
    app.add_middleware(BlockingMiddleware())

    async with app:
        await broker.declare_queue(queue)

        worker_broker = RabbitMQBroker(rabbitmq_url)
        worker_backend = ValkeyBackend(valkey_url)
        worker_app = SwarmQ(worker_broker, worker_backend, queues=[queue])
        worker_task = asyncio.create_task(worker_app.start_worker())

        try:
            await asyncio.sleep(0.5)

            with pytest.raises(ValueError, match="enqueue blocked"):
                await app.schedule(task_name)

            # If the enqueue was truly prevented, the worker never executes.
            # Wait a short moment and assert the run() method never fired.
            await asyncio.sleep(0.5)
            assert not worker_received.is_set(), "Task was enqueued despite pre_enqueue raising"
        finally:
            worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await worker_task
            await worker_broker.close()
            await worker_backend.close()
