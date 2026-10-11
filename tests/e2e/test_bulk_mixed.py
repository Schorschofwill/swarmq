"""E2E tests for bulk scheduling with mixed outcomes (U1.5).

Verifies that ``schedule_many`` correctly handles heterogeneous
outcome sets in a single batch:

- success + failure mixed: ``get_results`` surfaces each outcome
  verbatim (success value for winners, TaskError-wrapped exception
  for losers), no result is lost, none is mis-attributed.
- success + cancellation: cancelled task_ids resolve to
  ``TaskCancelledError``; non-cancelled ones complete normally.
- Unique-dedup: a batch containing 50 originals + 50 duplicates (same
  args, ``unique_for`` set) returns 50 distinct task_ids in 100 slots,
  with the duplicate slots mapping back to their winner.
- Broker-reconnect during schedule_many: Phase-1 channel-recreate
  (U1.1) must keep every publish alive through a reconnect window.
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from typing import Any, override

import aio_pika
import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import TaskCancelledError, TaskError
from swarmq.signals import Signal
from swarmq.task import Task, TaskInfo

pytestmark = [pytest.mark.timeout(40)]


def _unique_queue() -> str:
    return f"e2e-bulk-{uuid.uuid4().hex[:10]}"


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
    worker_app = SwarmQ(
        worker_broker,
        worker_backend,
        queues=[queue],
        concurrency=8,
    )
    online = asyncio.Event()

    @worker_app.on_signal(Signal.WORKER_ONLINE)
    async def _on_online(**_):
        online.set()

    worker_task = asyncio.create_task(worker_app.start_worker())
    try:
        await asyncio.wait_for(online.wait(), timeout=6)
        yield worker_app
    finally:
        worker_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker_task
        await worker_broker.close()
        await worker_backend.close()


@pytest.mark.usefixtures("_cleanup_tasks")
async def test_schedule_many_mixed_success_and_failure(
    rabbitmq_url,
    valkey_url,
):
    """50 tasks total: 30 succeed, 20 raise. get_results must surface
    both outcomes in the correct slots."""
    queue = _unique_queue()
    succ_name = _task_name("ok")
    fail_name = _task_name("fail")

    class SuccessTask(Task):
        task_info = TaskInfo(name=succ_name, queues=[queue], max_retries=0)

        @override
        async def run(self, n: int) -> int:
            return n * 2

    class FailingTask(Task):
        task_info = TaskInfo(name=fail_name, queues=[queue], max_retries=0)

        @override
        async def run(self, n: int) -> None:
            raise ValueError(f"boom-{n}")

    client_broker = RabbitMQBroker(rabbitmq_url)
    client_backend = ValkeyBackend(valkey_url)
    client_app = SwarmQ(client_broker, client_backend, queues=[queue])

    async with client_app:
        await client_app.broker.declare_queue(queue)
        async with _running_worker(rabbitmq_url, valkey_url, queue):
            # Interleave 30 success + 20 fail (50 total) so index
            # alignment is tested, not just sorted order.
            batch: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []
            success_indices: list[int] = []
            fail_indices: list[int] = []
            for i in range(50):
                if i % 5 == 0 or i % 5 == 2:  # 20 fail slots
                    batch.append((fail_name, (i,), {}))
                    fail_indices.append(i)
                else:
                    batch.append((succ_name, (i,), {}))
                    success_indices.append(i)

            task_ids = await client_app.schedule_many(batch)
            assert len(task_ids) == 50
            assert len(set(task_ids)) == 50, "expected all task_ids unique"

            # Fail outcomes surface as TaskError; success outcomes as
            # the task's return value. We iterate per-slot so a
            # single failure doesn't abort the collection.
            for idx, tid in enumerate(task_ids):
                if idx in fail_indices:
                    with pytest.raises(TaskError, match=f"boom-{idx}"):
                        await client_app.get_result(tid, timeout=8)
                else:
                    assert await client_app.get_result(tid, timeout=8) == idx * 2


@pytest.mark.usefixtures("_cleanup_tasks")
async def test_schedule_many_mixed_success_and_cancellation(
    rabbitmq_url,
    valkey_url,
):
    """Batch of slow tasks; cancel a subset before they run. Cancelled
    slots surface as TaskCancelledError, others complete normally."""
    queue = _unique_queue()
    task_name = _task_name("slow")
    run_started = asyncio.Event()

    class SlowTask(Task):
        task_info = TaskInfo(name=task_name, queues=[queue], max_retries=0)

        @override
        async def run(self, n: int) -> int:
            run_started.set()
            # Long enough that the cancel-monitor (poll every 200ms by
            # default) has multiple chances to observe the flag before
            # run() returns.
            await asyncio.sleep(2.5)
            return n

    client_broker = RabbitMQBroker(rabbitmq_url)
    client_backend = ValkeyBackend(valkey_url)
    client_app = SwarmQ(client_broker, client_backend, queues=[queue])

    async with client_app:
        await client_app.broker.declare_queue(queue)
        async with _running_worker(rabbitmq_url, valkey_url, queue):
            batch: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = [(task_name, (i,), {}) for i in range(10)]
            task_ids = await client_app.schedule_many(batch)

            # Cancel the first 4 task_ids BEFORE the worker picks
            # them up (worker concurrency=8, first 8 land in flight
            # near-simultaneously; we race the cancel against that,
            # but even if some started, the cancel-monitor kicks in).
            cancel_ids = task_ids[:4]
            keep_ids = task_ids[4:]
            cancel_results = await asyncio.gather(
                *(client_app.cancel(tid) for tid in cancel_ids),
            )
            assert all(cancel_results), (
                f"cancel returned False for some task_ids: {list(zip(cancel_ids, cancel_results))}"
            )

            # Keep slots must complete normally.
            for i, tid in enumerate(keep_ids, start=4):
                assert await client_app.get_result(tid, timeout=15) == i

            # Cancelled slots raise TaskCancelledError (may also be
            # TaskError when the cancel raced with a completed run —
            # accept either for the 4-slot window).
            cancelled_count = 0
            for tid in cancel_ids:
                try:
                    await client_app.get_result(tid, timeout=6)
                except TaskCancelledError:
                    cancelled_count += 1
                except TaskError:
                    # Raced with completion — tolerated when the
                    # worker picked it up before the cancel landed.
                    pass
            assert cancelled_count >= 1, "expected at least one of 4 cancels to land before execution completed"


@pytest.mark.usefixtures("_cleanup_tasks")
async def test_schedule_many_with_unique_dedup_returns_winner_ids(
    rabbitmq_url,
    valkey_url,
):
    """Batch contains 50 unique tasks + 50 duplicates of them (same
    args). Return value length is 100; duplicate slots point to the
    winner's task_id; only 50 distinct task_ids are in the list."""
    queue = _unique_queue()
    task_name = _task_name("uniq")

    class UniqueTask(Task):
        task_info = TaskInfo(
            name=task_name,
            queues=[queue],
            max_retries=0,
            unique_for=60,  # seconds
        )

        @override
        async def run(self, n: int) -> int:
            return n

    client_broker = RabbitMQBroker(rabbitmq_url)
    client_backend = ValkeyBackend(valkey_url)
    client_app = SwarmQ(client_broker, client_backend, queues=[queue])

    async with client_app:
        await client_app.broker.declare_queue(queue)
        async with _running_worker(rabbitmq_url, valkey_url, queue):
            # 50 originals interleaved with 50 duplicates (same args).
            originals: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = [(task_name, (i,), {}) for i in range(50)]
            dups: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = [(task_name, (i,), {}) for i in range(50)]
            batch = originals + dups

            task_ids = await client_app.schedule_many(batch)
            assert len(task_ids) == 100

            # For i < 50 the slot holds a freshly-minted task_id;
            # for i >= 50 the slot must point at the winner
            # (task_ids[i - 50]).
            for i in range(50):
                assert task_ids[i + 50] == task_ids[i], (
                    f"dup at slot {i + 50} did not dedupe to "
                    f"winner at slot {i}: "
                    f"dup={task_ids[i + 50]!r} winner={task_ids[i]!r}"
                )

            distinct = set(task_ids)
            assert len(distinct) == 50

            # Every winner should resolve to its arg value.
            for i in range(50):
                assert (
                    await client_app.get_result(
                        task_ids[i],
                        timeout=10,
                    )
                    == i
                )


@pytest.mark.usefixtures("_cleanup_tasks")
async def test_schedule_many_survives_broker_reconnect_midway(
    rabbitmq_url,
    valkey_url,
):
    """Force a broker reconnect mid-batch. Phase-1 U1.1 channel-
    recreate must ensure every publish lands — otherwise the batch
    return includes None-shaped failures or the broker crashes."""
    queue = _unique_queue()
    task_name = _task_name("survives")

    class FastTask(Task):
        task_info = TaskInfo(name=task_name, queues=[queue], max_retries=0)

        @override
        async def run(self, n: int) -> int:
            return n

    client_broker = RabbitMQBroker(rabbitmq_url)
    client_backend = ValkeyBackend(valkey_url)
    client_app = SwarmQ(client_broker, client_backend, queues=[queue])

    async with client_app:
        await client_app.broker.declare_queue(queue)
        async with _running_worker(rabbitmq_url, valkey_url, queue):
            # First half — publish normally.
            first_half: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = [(task_name, (i,), {}) for i in range(10)]
            ids_a = await client_app.schedule_many(first_half)

            # Trigger a reconnect on the client-side broker.
            connection = client_broker._connection
            assert isinstance(connection, aio_pika.abc.AbstractRobustConnection)
            await connection.reconnect()
            # Publish the second half DURING the recovery window; the
            # U1.1 retry wrapper must make this succeed.
            second_half: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = [
                (task_name, (i + 100,), {}) for i in range(10)
            ]
            ids_b = await client_app.schedule_many(second_half)

            assert len(ids_a) == 10
            assert len(ids_b) == 10
            all_ids = ids_a + ids_b

            # Every result must resolve — if any publish was lost we
            # hit the timeout here.
            results = await client_app.get_results(all_ids, timeout=12)
            expected = list(range(10)) + [i + 100 for i in range(10)]
            assert results == expected
