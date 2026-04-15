"""E2E roundtrip tests — real RabbitMQ + Valkey."""

from __future__ import annotations

import asyncio
import contextlib
import uuid

import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import TaskError
from swarmq.task import Task, TaskInfo


def _unique_queue() -> str:
    """Return a queue name unique to this test run."""
    return f"e2e-{uuid.uuid4().hex[:12]}"


# ---------------------------------------------------------------------------
# Test 1: Successful roundtrip
# ---------------------------------------------------------------------------


async def test_successful_roundtrip(rabbitmq_url: str, valkey_url: str) -> None:
    queue = _unique_queue()

    task_name = f"e2e_add_{uuid.uuid4().hex[:8]}"

    class AddTask(Task):
        task_info = TaskInfo(name=task_name, queues=[queue])

        async def run(self, a: int, b: int) -> int:
            return a + b

    broker = RabbitMQBroker(rabbitmq_url)
    backend = ValkeyBackend(valkey_url)
    app = SwarmQ(broker, backend, queues=[queue])

    async with app:
        # Declare the queue so the worker can consume from it
        await broker.declare_queue(queue)

        # Start the worker in the background — it needs its own connections
        worker_broker = RabbitMQBroker(rabbitmq_url)
        worker_backend = ValkeyBackend(valkey_url)
        worker_app = SwarmQ(worker_broker, worker_backend, queues=[queue])
        worker_task = asyncio.create_task(worker_app.start_worker())

        try:
            # Allow the worker to connect and start consuming
            await asyncio.sleep(0.5)

            task_id = await app.schedule(task_name, 2, 3)
            result = await app.get_result(task_id, timeout=10)
            assert result == 5
        finally:
            worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await worker_task
            await worker_broker.close()
            await worker_backend.close()


# ---------------------------------------------------------------------------
# Test 2: Task with error
# ---------------------------------------------------------------------------


async def test_task_with_error(rabbitmq_url: str, valkey_url: str) -> None:
    queue = _unique_queue()

    task_name = f"e2e_fail_{uuid.uuid4().hex[:8]}"

    class FailTask(Task):
        task_info = TaskInfo(name=task_name, queues=[queue])

        async def run(self) -> None:
            raise ValueError("intentional error")

    broker = RabbitMQBroker(rabbitmq_url)
    backend = ValkeyBackend(valkey_url)
    app = SwarmQ(broker, backend, queues=[queue])

    async with app:
        await broker.declare_queue(queue)

        worker_broker = RabbitMQBroker(rabbitmq_url)
        worker_backend = ValkeyBackend(valkey_url)
        worker_app = SwarmQ(worker_broker, worker_backend, queues=[queue])
        worker_task = asyncio.create_task(worker_app.start_worker())

        try:
            await asyncio.sleep(0.5)

            task_id = await app.schedule(task_name)
            with pytest.raises(TaskError, match="intentional error"):
                await app.get_result(task_id, timeout=10)
        finally:
            worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await worker_task
            await worker_broker.close()
            await worker_backend.close()


# ---------------------------------------------------------------------------
# Test 3: Task with None return
# ---------------------------------------------------------------------------


async def test_task_with_none_return(rabbitmq_url: str, valkey_url: str) -> None:
    queue = _unique_queue()

    task_name = f"e2e_none_{uuid.uuid4().hex[:8]}"

    class NoneTask(Task):
        task_info = TaskInfo(name=task_name, queues=[queue])

        async def run(self) -> None:
            return None

    broker = RabbitMQBroker(rabbitmq_url)
    backend = ValkeyBackend(valkey_url)
    app = SwarmQ(broker, backend, queues=[queue])

    async with app:
        await broker.declare_queue(queue)

        worker_broker = RabbitMQBroker(rabbitmq_url)
        worker_backend = ValkeyBackend(valkey_url)
        worker_app = SwarmQ(worker_broker, worker_backend, queues=[queue])
        worker_task = asyncio.create_task(worker_app.start_worker())

        try:
            await asyncio.sleep(0.5)

            task_id = await app.schedule(task_name)
            result = await app.get_result(task_id, timeout=10)
            assert result is None
        finally:
            worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await worker_task
            await worker_broker.close()
            await worker_backend.close()
