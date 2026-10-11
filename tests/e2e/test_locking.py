"""E2E tests for Phase 3b.2 Task Locking — real RabbitMQ + Valkey.

Exercises the full client → worker → Valkey-locked-execute round-trip
to verify multi-task serialisation under realistic conditions. Same
shape as ``tests/e2e/test_rate_limiting.py``.

Note on chaos coverage: `kill -9` against the worker process itself
would require subprocess-based fixtures, which the existing e2e harness
does not have. Instead we cover the equivalent failure modes via the
Lua-level ``__force__`` release (simulating an operator wedge-recovery)
and via in-process ``asyncio.Task.cancel()`` mid-handler.
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import time
import uuid
from typing import Any

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.locking import lock_key
from swarmq.signals import Signal
from swarmq.task import Task, TaskInfo


def _unique_queue() -> str:
    return f"e2e-{uuid.uuid4().hex[:12]}"


@contextlib.asynccontextmanager
async def _running_worker(rabbitmq_url: str, valkey_url: str, queue: str):
    """Spawn a worker against the given queue and tear it down cleanly.

    Yields the worker's SwarmQ app so callers can attach signal handlers
    BEFORE start_worker() runs; the WORKER_ONLINE event is awaited so
    the caller knows the worker is consuming before it schedules tasks.
    """
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


# ---------------------------------------------------------------------------
# Test 1: Mutex serialises overlapping tasks
# ---------------------------------------------------------------------------


async def test_mutex_serialises_concurrent_tasks(rabbitmq_url: str, valkey_url: str) -> None:
    """Five tasks with the same lock name and 100ms work each — none of
    their execution intervals may overlap, total wall-clock >= 5 * 100ms.
    """
    queue = _unique_queue()
    task_name = f"e2e_mutex_{uuid.uuid4().hex[:8]}"
    intervals: list[tuple[float, float]] = []
    completed = asyncio.Event()
    total_messages = 5

    class MutexTask(Task):
        task_info = TaskInfo(
            name=task_name,
            queues=[queue],
            lock="resource",
            hard_timeout=60,
        )

        async def run(self, idx: int) -> int:
            t0 = time.monotonic()
            await asyncio.sleep(0.1)
            intervals.append((t0, time.monotonic()))
            if len(intervals) >= total_messages:
                completed.set()
            return idx

    broker = RabbitMQBroker(rabbitmq_url)
    backend = ValkeyBackend(valkey_url)
    app = SwarmQ(broker, backend, queues=[queue])

    async with app:
        await broker.declare_queue(queue)
        async with _running_worker(rabbitmq_url, valkey_url, queue):
            for i in range(total_messages):
                await app.schedule(task_name, i)
            await asyncio.wait_for(completed.wait(), timeout=15.0)

    assert len(intervals) == total_messages
    intervals.sort(key=lambda iv: iv[0])
    for prev, curr in itertools.pairwise(intervals):
        assert prev[1] <= curr[0] + 1e-3, f"mutex overlap detected: prev={prev}, curr={curr}"


# ---------------------------------------------------------------------------
# Test 2: Semaphore allows up to max_concurrency overlap
# ---------------------------------------------------------------------------


async def test_semaphore_admits_at_most_max_concurrency(rabbitmq_url: str, valkey_url: str) -> None:
    """max_concurrency=2 with 6 tasks @ 200ms each — at any wall-clock
    sample point, no more than 2 are running. Worker concurrency is set
    high enough that the limit must come from the lock, not the worker.
    """
    queue = _unique_queue()
    task_name = f"e2e_sem_{uuid.uuid4().hex[:8]}"
    in_flight = 0
    max_seen = 0
    completed = asyncio.Event()
    total_messages = 6
    max_concurrency = 2

    class SemaphoreTask(Task):
        task_info = TaskInfo(
            name=task_name,
            queues=[queue],
            lock="sem-resource",
            max_concurrency=max_concurrency,
            hard_timeout=60,
        )

        async def run(self, idx: int) -> int:
            nonlocal in_flight, max_seen
            in_flight += 1
            max_seen = max(max_seen, in_flight)
            try:
                await asyncio.sleep(0.2)
            finally:
                in_flight -= 1
            if idx == total_messages - 1:
                completed.set()
            return idx

    # Worker concurrency >> max_concurrency so the lock is the only gate.
    import os

    os.environ["SWARMQ_CONCURRENCY"] = "10"
    try:
        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(rabbitmq_url, valkey_url, queue):
                for i in range(total_messages):
                    await app.schedule(task_name, i)
                await asyncio.wait_for(completed.wait(), timeout=15.0)
    finally:
        os.environ.pop("SWARMQ_CONCURRENCY", None)

    assert max_seen == max_concurrency, f"expected max {max_concurrency} concurrent, saw {max_seen}"


# ---------------------------------------------------------------------------
# Test 3: Format-string lock partitions by user_id
# ---------------------------------------------------------------------------


async def test_format_string_lock_serialises_per_user(rabbitmq_url: str, valkey_url: str) -> None:
    """Two tasks for user_id=1 must serialise; tasks for user_id=2
    overlap freely with user_id=1's tasks (different lock names)."""
    queue = _unique_queue()
    task_name = f"e2e_user_{uuid.uuid4().hex[:8]}"
    intervals_per_user: dict[int, list[tuple[float, float]]] = {1: [], 2: []}
    completed = asyncio.Event()
    expected_total = 4  # 2 per user

    class UserChargeTask(Task):
        task_info = TaskInfo(
            name=task_name,
            queues=[queue],
            lock="user-{user_id}",
            hard_timeout=60,
        )

        async def run(self, user_id: int, idx: int) -> int:
            t0 = time.monotonic()
            await asyncio.sleep(0.1)
            intervals_per_user[user_id].append((t0, time.monotonic()))
            total = sum(len(v) for v in intervals_per_user.values())
            if total >= expected_total:
                completed.set()
            return idx

    broker = RabbitMQBroker(rabbitmq_url)
    backend = ValkeyBackend(valkey_url)
    app = SwarmQ(broker, backend, queues=[queue])

    async with app:
        await broker.declare_queue(queue)
        async with _running_worker(rabbitmq_url, valkey_url, queue):
            # Interleave so different-user tasks land in close succession
            await app.schedule(task_name, user_id=1, idx=0)
            await app.schedule(task_name, user_id=2, idx=1)
            await app.schedule(task_name, user_id=1, idx=2)
            await app.schedule(task_name, user_id=2, idx=3)
            await asyncio.wait_for(completed.wait(), timeout=15.0)

    # Within each user, intervals must not overlap
    for user_id, ivs in intervals_per_user.items():
        ivs.sort(key=lambda iv: iv[0])
        for prev, curr in itertools.pairwise(ivs):
            assert prev[1] <= curr[0] + 1e-3, f"user {user_id} mutex overlap: prev={prev}, curr={curr}"


# ---------------------------------------------------------------------------
# Test 4: Operator wedge-recovery via __force__
# ---------------------------------------------------------------------------


async def test_force_release_recovers_wedged_lock(rabbitmq_url: str, valkey_url: str) -> None:
    """Pre-populates the Valkey key as if a crashed worker held the lock,
    then verifies a new worker can acquire after `__force__` release —
    documenting the operator-recovery procedure mentioned in the plan's
    Operational Notes (`valkey-cli DEL sq:lock:<name>` is the
    operator-side equivalent).
    """
    import valkey.asyncio as valkey_asyncio

    from swarmq.backend.valkey.locks import LockManager

    queue = _unique_queue()
    task_name = f"e2e_wedge_{uuid.uuid4().hex[:8]}"
    completed = asyncio.Event()

    class WedgedLockTask(Task):
        task_info = TaskInfo(
            name=task_name,
            queues=[queue],
            lock="wedged-resource",
            hard_timeout=60,
        )

        async def run(self, idx: int) -> int:
            completed.set()
            return idx

    # Pre-wedge the lock with a foreign owner-token + huge TTL.
    client = valkey_asyncio.Valkey.from_url(valkey_url)
    try:
        await client.set(lock_key("wedged-resource"), "ghost-owner-token", px=600_000)

        # Force-release as an operator would (simulated via LockManager
        # to exercise the same Lua path as a future CLI).
        manager = LockManager(client)
        released = await manager.release("wedged-resource", "__force__", 1)
        assert released is True
    finally:
        await client.aclose()

    broker = RabbitMQBroker(rabbitmq_url)
    backend = ValkeyBackend(valkey_url)
    app = SwarmQ(broker, backend, queues=[queue])

    async with app:
        await broker.declare_queue(queue)
        async with _running_worker(rabbitmq_url, valkey_url, queue):
            await app.schedule(task_name, 1)
            await asyncio.wait_for(completed.wait(), timeout=10.0)


# ---------------------------------------------------------------------------
# Test 5: Lock released after task exception (no leak)
# ---------------------------------------------------------------------------


async def test_lock_released_after_task_exception(rabbitmq_url: str, valkey_url: str) -> None:
    """Task A throws → lock freed → Task B can run immediately. The
    finally-block discipline (Unit 5) is the load-bearing guarantee."""
    queue = _unique_queue()
    task_name = f"e2e_exc_{uuid.uuid4().hex[:8]}"
    second_completed = asyncio.Event()
    state = {"first_done": False}

    class ExcOrOkTask(Task):
        task_info = TaskInfo(
            name=task_name,
            queues=[queue],
            lock="exc-resource",
            hard_timeout=60,
        )

        async def run(self, should_throw: bool) -> int:
            if should_throw and not state["first_done"]:
                state["first_done"] = True
                raise RuntimeError("simulated task failure")
            second_completed.set()
            return 1

    broker = RabbitMQBroker(rabbitmq_url)
    backend = ValkeyBackend(valkey_url)
    app = SwarmQ(broker, backend, queues=[queue])

    async with app:
        await broker.declare_queue(queue)
        async with _running_worker(rabbitmq_url, valkey_url, queue):
            await app.schedule(task_name, True)
            await app.schedule(task_name, False)
            # Without finally-release this would block on the first task's
            # held lock until TTL (90s for hard_timeout=60); we expect
            # immediate progression via the cooperative release.
            await asyncio.wait_for(second_completed.wait(), timeout=10.0)
