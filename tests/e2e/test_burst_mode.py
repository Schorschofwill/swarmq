"""E2E tests for R4.5 Burst-Mode (in-process, real RabbitMQ + Valkey).

Verifies:
- Empty-queue burst exits within `burst_idle_timeout` seconds.
- Populated-queue burst processes everything, then exits cleanly.
- In-flight tasks complete before exit (graceful drain semantics).
"""

from __future__ import annotations

import asyncio
import contextlib
import time
import uuid

import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.signals import Signal
from swarmq.task import Task, TaskInfo

pytestmark = [pytest.mark.timeout(30)]


def _unique_queue() -> str:
    return f"e2e-burst-{uuid.uuid4().hex[:10]}"


def _task_name(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def _cleanup_tasks():
    yield
    Task._clear_registered_tasks()


async def test_burst_exits_when_queue_empty(
    rabbitmq_url, valkey_url, _cleanup_tasks,
):
    """Worker started against an empty queue with --burst exits within
    the idle-timeout window."""
    queue = _unique_queue()
    task_name = _task_name("nop")

    class NopTask(Task):
        task_info = TaskInfo(name=task_name, queues=[queue], max_retries=0)

        async def run(self) -> str:
            return "ok"

    broker = RabbitMQBroker(rabbitmq_url)
    backend = ValkeyBackend(valkey_url)
    app = SwarmQ(
        broker, backend,
        queues=[queue],
        burst_mode=True,
        burst_idle_timeout=1.0,
    )
    async with app:
        await app.broker.declare_queue(queue)
        t0 = time.monotonic()
        # start_worker blocks until burst-monitor triggers shutdown.
        await asyncio.wait_for(app.start_worker(), timeout=10.0)
        elapsed = time.monotonic() - t0

    # Should exit within burst_idle_timeout + start-up overhead.
    assert 1.0 <= elapsed <= 5.0, (
        f"burst exit took {elapsed:.2f}s; expected ~1s + startup"
    )


async def test_burst_drains_queue_then_exits(
    rabbitmq_url, valkey_url, _cleanup_tasks,
):
    """Pre-populate a queue, start worker with --burst, verify all
    tasks completed and worker exited."""
    queue = _unique_queue()
    task_name = _task_name("drain")
    completed: list[int] = []
    completed_event = asyncio.Event()

    class DrainTask(Task):
        task_info = TaskInfo(name=task_name, queues=[queue], max_retries=0)

        async def run(self, n: int) -> int:
            completed.append(n)
            if len(completed) >= 5:
                completed_event.set()
            return n

    # Stage A: populate the queue with 5 tasks (no worker).
    client_app = SwarmQ(
        RabbitMQBroker(rabbitmq_url),
        ValkeyBackend(valkey_url),
        queues=[queue],
    )
    async with client_app:
        await client_app.broker.declare_queue(queue)
        for i in range(5):
            await client_app.schedule(task_name, i)

    # Stage B: burst worker drains and exits.
    worker_app = SwarmQ(
        RabbitMQBroker(rabbitmq_url),
        ValkeyBackend(valkey_url),
        queues=[queue],
        burst_mode=True,
        burst_idle_timeout=1.0,
    )
    async with worker_app:
        t0 = time.monotonic()
        await asyncio.wait_for(worker_app.start_worker(), timeout=15.0)
        elapsed = time.monotonic() - t0

    assert len(completed) == 5, f"expected 5 drained, got {completed}"
    # 5 tasks process + 1s idle window + start overhead. ≤ 7s plenty.
    assert elapsed <= 10.0, f"burst-drain took {elapsed:.2f}s"


async def test_burst_waits_for_inflight_before_exit(
    rabbitmq_url, valkey_url, _cleanup_tasks,
):
    """A slow task picked up just before the idle window must complete
    before the burst worker exits — graceful-drain semantics."""
    queue = _unique_queue()
    task_name = _task_name("slow")
    started = asyncio.Event()
    completed = asyncio.Event()

    class SlowTask(Task):
        task_info = TaskInfo(name=task_name, queues=[queue], max_retries=0)

        async def run(self) -> str:
            started.set()
            await asyncio.sleep(2.0)
            completed.set()
            return "done"

    # Stage A: schedule the slow task.
    client_app = SwarmQ(
        RabbitMQBroker(rabbitmq_url),
        ValkeyBackend(valkey_url),
        queues=[queue],
    )
    async with client_app:
        await client_app.broker.declare_queue(queue)
        await client_app.schedule(task_name)

    # Stage B: burst worker. Idle-timeout 0.5s would normally fire
    # before the 2s sleep finishes — but active-tasks set non-empty
    # means the monitor holds off.
    worker_app = SwarmQ(
        RabbitMQBroker(rabbitmq_url),
        ValkeyBackend(valkey_url),
        queues=[queue],
        burst_mode=True,
        burst_idle_timeout=0.5,
    )
    async with worker_app:
        await asyncio.wait_for(worker_app.start_worker(), timeout=10.0)

    assert completed.is_set(), "slow task did not complete before exit"
