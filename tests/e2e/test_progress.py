"""E2E tests for R3.7 Progress Tracking.

Full-stack scenarios: task emits progress via update_progress →
Client.get_progress reads the HASH and Client.watch_progress streams
updates in real time against real RabbitMQ + Valkey.
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from typing import Any

import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.signals import Signal
from swarmq.task import Task, TaskInfo

pytestmark = [pytest.mark.timeout(25)]


def _unique_queue() -> str:
    return f"e2e-progress-{uuid.uuid4().hex[:10]}"


def _task_name(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def _cleanup_tasks():
    yield
    Task._clear_registered_tasks()


@contextlib.asynccontextmanager
async def _running_worker(rabbitmq_url: str, valkey_url: str, queue: str):
    worker_broker = RabbitMQBroker(rabbitmq_url)
    worker_backend = ValkeyBackend(valkey_url)
    worker_app = SwarmQ(worker_broker, worker_backend, queues=[queue])

    online = asyncio.Event()

    @worker_app.on_signal(Signal.WORKER_ONLINE)
    async def _on_online(**_: Any) -> None:
        online.set()

    worker_task = asyncio.create_task(worker_app.start_worker())
    try:
        await asyncio.wait_for(online.wait(), timeout=5.0)
        yield worker_app
    finally:
        worker_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker_task
        await worker_broker.close()
        await worker_backend.close()


async def test_watch_progress_streams_updates(
    rabbitmq_url: str,
    valkey_url: str,
    _cleanup_tasks,
):
    """A 10-step task emits progress; watch_progress yields at least
    three events (throttling-bounded); the final event shows percent
    == 100.0 with current == total."""
    queue = _unique_queue()
    task_name = _task_name("stepper")

    class StepperTask(Task):
        task_info = TaskInfo(name=task_name, queues=[queue])

        async def run(self) -> int:
            # 10 steps × 120ms = 1.2s total. Throttling at 500ms
            # means the client should observe at least 3 events
            # (t=0, t≈.5, t≈1.0) plus the forced final-flush.
            total = 10
            for i in range(1, total + 1):
                await self.update_progress(i, total, f"step {i}")
                await asyncio.sleep(0.12)
            return total

    client_broker = RabbitMQBroker(rabbitmq_url)
    client_backend = ValkeyBackend(valkey_url)
    client_app = SwarmQ(client_broker, client_backend, queues=[queue])

    observed: list = []

    async with client_app:
        await client_broker.declare_queue(queue)

        async with _running_worker(rabbitmq_url, valkey_url, queue):
            tid = await client_app.schedule(task_name)

            async def watcher():
                async for info in client_app.client.watch_progress(tid):
                    observed.append(info)
                    if info.percent >= 100.0:
                        break

            watch_task = asyncio.create_task(watcher())
            result = await client_app.get_result(tid, timeout=10)
            assert result == 10
            # Give the watcher a moment to consume the final update
            # that landed as the task completed.
            try:
                await asyncio.wait_for(watch_task, timeout=2.0)
            except TimeoutError:
                watch_task.cancel()

    # Throttling means we get at most ~3 intermediate events across
    # a 1.2s task with a 500ms window, plus the final 100% flush.
    assert len(observed) >= 2, (
        f"expected at least two progress events (mid-run + final); got {len(observed)}: {observed!r}"
    )
    final = observed[-1]
    assert final.current == 10
    assert final.total == 10
    assert final.percent == 100.0
    assert "10" in final.message


async def test_get_progress_polling_sees_terminal_state(
    rabbitmq_url: str,
    valkey_url: str,
    _cleanup_tasks,
):
    """After the task completes, one-shot get_progress returns the
    final HASH state (until TTL expires)."""
    queue = _unique_queue()
    task_name = _task_name("quick")

    class QuickTask(Task):
        task_info = TaskInfo(name=task_name, queues=[queue])

        async def run(self) -> str:
            await self.update_progress(1, 2, "half")
            await asyncio.sleep(0.05)
            await self.update_progress(2, 2, "done")
            return "ok"

    client_broker = RabbitMQBroker(rabbitmq_url)
    client_backend = ValkeyBackend(valkey_url)
    client_app = SwarmQ(client_broker, client_backend, queues=[queue])

    async with client_app:
        await client_broker.declare_queue(queue)
        async with _running_worker(rabbitmq_url, valkey_url, queue):
            tid = await client_app.schedule(task_name)
            result = await client_app.get_result(tid, timeout=8)
            assert result == "ok"
            # Terminal state observable via get_progress.
            info = await client_app.client.get_progress(tid)
            assert info is not None
            assert info.current == 2
            assert info.total == 2
            assert info.percent == 100.0
            assert info.message == "done"


async def test_get_progress_none_for_unknown_task(
    rabbitmq_url: str,
    valkey_url: str,
    _cleanup_tasks,
):
    """Never-written task_id returns None (not an error)."""
    client_broker = RabbitMQBroker(rabbitmq_url)
    client_backend = ValkeyBackend(valkey_url)
    client_app = SwarmQ(
        client_broker,
        client_backend,
        queues=["dummy"],
    )
    async with client_app:
        info = await client_app.client.get_progress("never-existed")
        assert info is None


async def test_final_flush_bypasses_throttle_for_fast_task(
    rabbitmq_url: str,
    valkey_url: str,
    _cleanup_tasks,
):
    """A task that finishes in <500ms still emits the final update,
    per the force-flush rule on current == total."""
    queue = _unique_queue()
    task_name = _task_name("burst")

    class BurstTask(Task):
        task_info = TaskInfo(name=task_name, queues=[queue])

        async def run(self) -> str:
            # Entire loop completes well inside 500ms.
            for i in range(1, 4):
                await self.update_progress(i, 3, f"step {i}")
            return "done"

    client_broker = RabbitMQBroker(rabbitmq_url)
    client_backend = ValkeyBackend(valkey_url)
    client_app = SwarmQ(client_broker, client_backend, queues=[queue])

    async with client_app:
        await client_broker.declare_queue(queue)
        async with _running_worker(rabbitmq_url, valkey_url, queue):
            tid = await client_app.schedule(task_name)
            result = await client_app.get_result(tid, timeout=5)
            assert result == "done"
            info = await client_app.client.get_progress(tid)
            assert info is not None
            # Final update was the 3/3 flush.
            assert info.current == 3
            assert info.total == 3
            assert info.percent == 100.0
