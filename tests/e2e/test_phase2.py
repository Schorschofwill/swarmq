"""E2E tests for Phase 2 features — delivery modes, retry, timeouts, graceful shutdown."""

from __future__ import annotations

import asyncio
import contextlib
import uuid

import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import ResultNotReadyError, TaskError
from swarmq.task import DeliveryMode, Task, TaskInfo
from swarmq.worker import Worker, WorkerState


def _unique_queue() -> str:
    return f"e2e-{uuid.uuid4().hex[:12]}"


# ---------------------------------------------------------------------------
# Test 1: FIRE_AND_FORGET task — runs but stores no result
# ---------------------------------------------------------------------------


async def test_fire_and_forget(rabbitmq_url: str, valkey_url: str) -> None:
    queue = _unique_queue()
    task_name = f"e2e_fnf_{uuid.uuid4().hex[:8]}"

    executed = asyncio.Event()

    class FireAndForgetTask(Task):
        task_info = TaskInfo(
            name=task_name,
            queues=[queue],
            delivery=DeliveryMode.FIRE_AND_FORGET,
        )

        async def run(self) -> str:
            executed.set()
            return "ignored"

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

            # The task should actually execute
            await asyncio.wait_for(executed.wait(), timeout=5)
            assert executed.is_set()

            # No result stored — get_result should raise ResultNotReadyError
            with pytest.raises(ResultNotReadyError):
                await app.get_result(task_id, timeout=0)
        finally:
            worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await worker_task
            await worker_broker.close()
            await worker_backend.close()


# ---------------------------------------------------------------------------
# Test 2: Retry exhaustion — task goes to DLQ after max_retries
# ---------------------------------------------------------------------------


async def test_retry_exhaustion_dlq(rabbitmq_url: str, valkey_url: str) -> None:
    queue = _unique_queue()
    task_name = f"e2e_dlq_{uuid.uuid4().hex[:8]}"

    attempt_count = 0

    class AlwaysFailTask(Task):
        task_info = TaskInfo(
            name=task_name,
            queues=[queue],
            max_retries=2,
            retry_backoff="fixed",
            retry_base_delay=0.3,
        )

        async def run(self) -> None:
            nonlocal attempt_count
            attempt_count += 1
            raise ValueError("always fails")

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

            # Wait enough time for 1 initial attempt + 2 retries with 0.3s fixed delay
            # Each retry: 0.3s delay in DQ + processing time. Total ~3s should be plenty.
            await asyncio.sleep(3.0)

            # After retries exhausted, message goes to DLQ — error result stored
            # so the caller gets a clear error instead of hanging
            with pytest.raises(TaskError, match="always fails"):
                await app.get_result(task_id, timeout=0)

            # Task ran 3 times total: 1 initial + 2 retries
            assert attempt_count == 3

            # Verify message ended up in .XQ queue
            import aio_pika

            dlq_conn = await aio_pika.connect_robust(rabbitmq_url)
            try:
                dlq_channel = await dlq_conn.channel()
                dlq_queue = await dlq_channel.get_queue(f"{queue}.XQ")
                dlq_msg = await dlq_queue.get(timeout=2, fail=False)
                assert dlq_msg is not None
                await dlq_msg.ack()
            finally:
                await dlq_conn.close()
        finally:
            worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await worker_task
            await worker_broker.close()
            await worker_backend.close()


# ---------------------------------------------------------------------------
# Test 3: Retry with backoff — fails first attempt, succeeds on second
# ---------------------------------------------------------------------------


async def test_retry_backoff_success(rabbitmq_url: str, valkey_url: str) -> None:
    queue = _unique_queue()
    task_name = f"e2e_retry_{uuid.uuid4().hex[:8]}"

    attempt_count = 0

    class RetryOnceTask(Task):
        task_info = TaskInfo(
            name=task_name,
            queues=[queue],
            max_retries=1,
            retry_backoff="fixed",
            retry_base_delay=0.3,
        )

        async def run(self) -> str:
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count == 1:
                raise ValueError("transient failure")
            return "recovered"

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

            assert result == "recovered"
            assert attempt_count == 2
        finally:
            worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await worker_task
            await worker_broker.close()
            await worker_backend.close()


# ---------------------------------------------------------------------------
# Test 4: Soft-timeout triggers retry — first attempt times out, second succeeds
# ---------------------------------------------------------------------------


async def test_soft_timeout_retry(rabbitmq_url: str, valkey_url: str) -> None:
    queue = _unique_queue()
    task_name = f"e2e_stimeout_{uuid.uuid4().hex[:8]}"

    attempt_count = 0

    class SlowThenFastTask(Task):
        task_info = TaskInfo(
            name=task_name,
            queues=[queue],
            soft_timeout=1,
            max_retries=1,
            retry_backoff="fixed",
            retry_base_delay=0.3,
        )

        async def run(self) -> str:
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count == 1:
                await asyncio.sleep(2)  # exceeds soft_timeout=1
            return "ok after timeout"

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

            assert result == "ok after timeout"
            assert attempt_count == 2
        finally:
            worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await worker_task
            await worker_broker.close()
            await worker_backend.close()


# ---------------------------------------------------------------------------
# Test 5: Graceful shutdown — running task completes before worker stops
# ---------------------------------------------------------------------------


async def test_graceful_shutdown(rabbitmq_url: str, valkey_url: str) -> None:
    queue = _unique_queue()
    task_name = f"e2e_shutdown_{uuid.uuid4().hex[:8]}"

    class SlowTask(Task):
        task_info = TaskInfo(name=task_name, queues=[queue])

        async def run(self) -> str:
            await asyncio.sleep(1)
            return "completed during shutdown"

    broker = RabbitMQBroker(rabbitmq_url)
    backend = ValkeyBackend(valkey_url)
    app = SwarmQ(broker, backend, queues=[queue])

    async with app:
        await broker.declare_queue(queue)

        worker_broker = RabbitMQBroker(rabbitmq_url)
        worker_backend = ValkeyBackend(valkey_url)
        # Build worker directly so we can call _request_shutdown()
        worker_app = SwarmQ(worker_broker, worker_backend, queues=[queue])
        worker = Worker(worker_broker, worker_backend, worker_app.registry, worker_app.config)
        worker_task = asyncio.create_task(worker.start())

        try:
            await asyncio.sleep(0.5)

            task_id = await app.schedule(task_name)

            # Wait for the task to start executing
            await asyncio.sleep(0.3)

            # Request graceful shutdown while the task is still running
            worker._request_shutdown()

            # Wait for worker to finish its shutdown sequence
            await asyncio.wait_for(worker_task, timeout=10)

            # The running task should have completed — result is stored
            result = await app.get_result(task_id, timeout=5)
            assert result == "completed during shutdown"

            # Worker reached STOPPED state
            assert worker._state is WorkerState.STOPPED
        finally:
            # Worker already shut down cleanly, just ensure connections are closed
            if not worker_task.done():
                worker_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await worker_task
            await worker_broker.close()
            await worker_backend.close()
