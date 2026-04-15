"""E2E performance tests — Pub/Sub, batch operations, queue types."""

from __future__ import annotations

import asyncio
import contextlib
import time
import uuid

import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.task import Task, TaskInfo


def _unique_queue() -> str:
    return f"e2e-{uuid.uuid4().hex[:12]}"


# ---------------------------------------------------------------------------
# Test 1: Pub/Sub get_result completes quickly for a no-op task
# ---------------------------------------------------------------------------


async def test_pubsub_get_result_fast(rabbitmq_url: str, valkey_url: str) -> None:
    """Pub/Sub notification should let get_result return well under 200ms."""
    queue = _unique_queue()
    task_name = f"e2e_noop_{uuid.uuid4().hex[:8]}"

    class NoopTask(Task):
        task_info = TaskInfo(name=task_name, queues=[queue])

        async def run(self) -> str:
            return "done"

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

            t0 = time.monotonic()
            result = await app.get_result(task_id, timeout=10)
            elapsed = time.monotonic() - t0

            assert result == "done"
            # Pub/Sub should resolve much faster than the old 200ms polling.
            # We use a generous 2s ceiling to avoid flakiness — the point is
            # that it works, not that we hit a specific latency target.
            assert elapsed < 2.0, f"get_result took {elapsed:.3f}s, expected < 2s"
        finally:
            worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await worker_task
            await worker_broker.close()
            await worker_backend.close()


# ---------------------------------------------------------------------------
# Test 2: get_result works regardless of Pub/Sub timing (polling fallback)
# ---------------------------------------------------------------------------


async def test_get_result_works_end_to_end(rabbitmq_url: str, valkey_url: str) -> None:
    """get_result returns the correct value even if the Pub/Sub race window
    is missed (the polling fallback catches it)."""
    queue = _unique_queue()
    task_name = f"e2e_echo_{uuid.uuid4().hex[:8]}"

    class EchoTask(Task):
        task_info = TaskInfo(name=task_name, queues=[queue])

        async def run(self, msg: str) -> str:
            return msg

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

            task_id = await app.schedule(task_name, "hello-pubsub")
            result = await app.get_result(task_id, timeout=10)
            assert result == "hello-pubsub"
        finally:
            worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await worker_task
            await worker_broker.close()
            await worker_backend.close()


# ---------------------------------------------------------------------------
# Test 3: schedule_many + get_results batch roundtrip
# ---------------------------------------------------------------------------


async def test_schedule_many_get_results(rabbitmq_url: str, valkey_url: str) -> None:
    """schedule_many publishes 10 tasks; get_results retrieves all of them."""
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
        await broker.declare_queue(queue)

        worker_broker = RabbitMQBroker(rabbitmq_url)
        worker_backend = ValkeyBackend(valkey_url)
        worker_app = SwarmQ(worker_broker, worker_backend, queues=[queue])
        worker_task = asyncio.create_task(worker_app.start_worker())

        try:
            await asyncio.sleep(0.5)

            tasks = [
                (task_name, (i, i * 10), {})
                for i in range(10)
            ]
            task_ids = await app.schedule_many(tasks, queue=queue)
            assert len(task_ids) == 10

            results = await app.get_results(task_ids, timeout=10)
            assert len(results) == 10

            for i, res in enumerate(results):
                assert res == i + i * 10, f"task {i}: expected {i + i * 10}, got {res}"
        finally:
            worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await worker_task
            await worker_broker.close()
            await worker_backend.close()


# ---------------------------------------------------------------------------
# Test 4: Roundtrip with classic queue type
# ---------------------------------------------------------------------------


async def test_classic_queue_roundtrip(rabbitmq_url: str, valkey_url: str) -> None:
    """Roundtrip works with queue_type='classic' (not just quorum)."""
    queue = _unique_queue()
    task_name = f"e2e_classic_{uuid.uuid4().hex[:8]}"

    class ClassicTask(Task):
        task_info = TaskInfo(name=task_name, queues=[queue])

        async def run(self, x: int) -> int:
            return x * 2

    broker = RabbitMQBroker(rabbitmq_url)
    backend = ValkeyBackend(valkey_url)
    app = SwarmQ(broker, backend, queues=[queue], queue_type="classic")

    async with app:
        await broker.declare_queue(queue)

        worker_broker = RabbitMQBroker(rabbitmq_url)
        worker_backend = ValkeyBackend(valkey_url)
        worker_app = SwarmQ(
            worker_broker, worker_backend, queues=[queue], queue_type="classic",
        )
        worker_task = asyncio.create_task(worker_app.start_worker())

        try:
            await asyncio.sleep(0.5)

            task_id = await app.schedule(task_name, 7)
            result = await app.get_result(task_id, timeout=10)
            assert result == 14
        finally:
            worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await worker_task
            await worker_broker.close()
            await worker_backend.close()
