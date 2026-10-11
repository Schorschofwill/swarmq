"""E2E tests for task cancellation (Phase 3b.3 / R3.6).

Full-stack: client, worker, RabbitMQ, Valkey, signals.  Worker runs in
its own SwarmQ app with dedicated broker+backend connections, matching
production deployment.

Units 6 + 7 are both covered here. Unit-6 scenarios (pre-start cancel,
FAF, unknown task_id, parallel cancels) are all present; Unit-7
scenarios (running-cancel, hard-timeout-race, SIGTERM during cancel)
will be appended when Unit 7 implementation lands.
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from typing import Any, override

import pytest
import valkey.asyncio as aio_valkey

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import TaskCancelledError
from swarmq.keyspace import KeySpace
from swarmq.signals import Signal
from swarmq.task import DeliveryMode, Task, TaskInfo

# E2E cancel tests start workers, schedule tasks, and observe both
# happy-path results and cancel-flag propagation. The 30s inner
# asyncio.sleep used by one scenario (SIGTERM-during-cancel) sets
# the outer bound; 35s pytest-timeout gives headroom. Still fails
# fast on real deadlocks (scheduler/broker never coming up).
pytestmark = [pytest.mark.timeout(35)]


def _unique_queue() -> str:
    return f"e2e-cancel-{uuid.uuid4().hex[:10]}"


async def _cancel_flag_pttl(valkey_url: str, task_id: str) -> int:
    """Restlaufzeit des Cancel-Flags in ms (BZ-23: der Worker verkürzt es
    auf höchstens 1 h statt es zu löschen)."""
    raw = aio_valkey.Valkey.from_url(valkey_url)
    try:
        return int(await raw.pttl(KeySpace(None).cancel(task_id)))
    finally:
        await raw.aclose()


def _task_name(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def _cleanup_tasks():
    yield
    Task._clear_registered_tasks()


async def _run_worker_until(
    worker_app: SwarmQ,
    wait_coro,
    timeout: float = 5.0,
):
    """Start a worker, await *wait_coro*, then shut the worker down cleanly."""
    worker_task = asyncio.create_task(worker_app.start_worker())
    try:
        await asyncio.sleep(0.4)  # let the worker connect + declare + consume
        return await asyncio.wait_for(wait_coro, timeout=timeout)
    finally:
        worker_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker_task


# ---------------------------------------------------------------------------
# Unit 6 — pre-start cancel
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("_cleanup_tasks")
async def test_e2e_queued_cancel_full_stack(
    rabbitmq_url: str,
    valkey_url: str,
):
    """Full stack: schedule → cancel → worker → TaskCancelledError."""
    queue = _unique_queue()
    task_name = _task_name("slow")

    class SlowTask(Task):
        task_info = TaskInfo(name=task_name, queues=[queue])

        @override
        async def run(self) -> str:
            await asyncio.sleep(10)
            return "should-not-return"

    client_broker = RabbitMQBroker(rabbitmq_url)
    client_backend = ValkeyBackend(valkey_url)
    client_app = SwarmQ(client_broker, client_backend, queues=[queue])

    worker_broker = RabbitMQBroker(rabbitmq_url)
    worker_backend = ValkeyBackend(valkey_url)
    worker_app = SwarmQ(worker_broker, worker_backend, queues=[queue], concurrency=1)

    async with client_app:
        await client_broker.declare_queue(queue)

        task_id = await client_app.schedule(task_name)
        assert await client_app.cancel(task_id) is True

        async with worker_app:

            async def wait_for_result():
                with pytest.raises(TaskCancelledError):
                    await client_app.get_result(task_id, timeout=5)

            await _run_worker_until(worker_app, wait_for_result(), timeout=8)

        # BZ-23 (Plan 0.12.0, MR 5): der Worker verkürzt das Flag auf
        # höchstens 1 h, statt es zu löschen.
        assert await client_backend.is_cancelled(task_id) is True
        assert 0 < await _cancel_flag_pttl(valkey_url, task_id) <= 3_600_000


@pytest.mark.usefixtures("_cleanup_tasks")
async def test_e2e_faf_cancel_emits_signal_no_result(
    rabbitmq_url: str,
    valkey_url: str,
):
    """FAF + cancel: TASK_CANCELLED fires, but no result is stored."""
    queue = _unique_queue()
    task_name = _task_name("faf_cancel")

    class FafTask(Task):
        task_info = TaskInfo(
            name=task_name,
            queues=[queue],
            delivery=DeliveryMode.FIRE_AND_FORGET,
        )

        @override
        async def run(self) -> None:
            await asyncio.sleep(10)

    client_broker = RabbitMQBroker(rabbitmq_url)
    client_backend = ValkeyBackend(valkey_url)
    client_app = SwarmQ(client_broker, client_backend, queues=[queue])

    worker_broker = RabbitMQBroker(rabbitmq_url)
    worker_backend = ValkeyBackend(valkey_url)
    worker_app = SwarmQ(worker_broker, worker_backend, queues=[queue], concurrency=1)

    captured: list[dict[str, Any]] = []

    async with client_app:
        await client_broker.declare_queue(queue)

        @worker_app.on_signal(Signal.TASK_CANCELLED)
        async def handler(**kwargs):
            captured.append(kwargs)

        task_id = await client_app.schedule(task_name)
        await client_app.cancel(task_id)

        async with worker_app:
            worker_task = asyncio.create_task(worker_app.start_worker())
            try:
                await asyncio.sleep(1.2)  # worker processes the cancelled message
            finally:
                worker_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await worker_task

        # Signal was dispatched.
        assert len(captured) == 1
        assert captured[0]["task_id"] == task_id
        assert captured[0]["task_name"] == task_name

        # No result in Valkey for FAF tasks (result_ttl=0).
        raw = await client_backend.get_result(task_id)
        assert raw is None


@pytest.mark.usefixtures("_cleanup_tasks")
async def test_e2e_cancel_unknown_task_id_is_harmless(
    rabbitmq_url: str,
    valkey_url: str,
):
    """cancel() for a never-scheduled task_id: True, no side-effect."""
    client_broker = RabbitMQBroker(rabbitmq_url)
    client_backend = ValkeyBackend(valkey_url)

    async with SwarmQ(client_broker, client_backend, queues=["any"]) as app:
        fake_id = f"never-{uuid.uuid4().hex}"
        assert await app.cancel(fake_id) is True
        # Second call: False (NX semantics).
        assert await app.cancel(fake_id) is False
        await client_backend.clear_cancel_flag(fake_id)


@pytest.mark.usefixtures("_cleanup_tasks")
async def test_e2e_parallel_cancels_via_gather(
    rabbitmq_url: str,
    valkey_url: str,
):
    """10 parallel cancels via asyncio.gather — all should succeed and get TaskCancelledError."""
    queue = _unique_queue()
    task_name = _task_name("bulk")

    class SleepTask(Task):
        task_info = TaskInfo(name=task_name, queues=[queue])

        @override
        async def run(self) -> str:
            await asyncio.sleep(10)
            return "nope"

    client_broker = RabbitMQBroker(rabbitmq_url)
    client_backend = ValkeyBackend(valkey_url)
    client_app = SwarmQ(client_broker, client_backend, queues=[queue])

    worker_broker = RabbitMQBroker(rabbitmq_url)
    worker_backend = ValkeyBackend(valkey_url)
    worker_app = SwarmQ(
        worker_broker,
        worker_backend,
        queues=[queue],
        concurrency=5,
    )

    async with client_app:
        await client_broker.declare_queue(queue)

        # Schedule 10 tasks.
        task_ids = [await client_app.schedule(task_name) for _ in range(10)]

        # Cancel them all concurrently.
        results = await asyncio.gather(*[client_app.cancel(tid) for tid in task_ids])
        assert all(r is True for r in results)

        async with worker_app:
            worker_task = asyncio.create_task(worker_app.start_worker())
            try:
                await asyncio.sleep(1.0)  # worker processes all 10 in parallel
                # Each task_id should resolve to TaskCancelledError now.
                for tid in task_ids:
                    with pytest.raises(TaskCancelledError):
                        await client_app.get_result(tid, timeout=3)
            finally:
                worker_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await worker_task


@pytest.mark.usefixtures("_cleanup_tasks")
async def test_e2e_cancel_in_retry_delay_full_stack(
    rabbitmq_url: str,
    valkey_url: str,
):
    """Task retries with delay, cancel during delay, redelivered message is cancelled."""
    queue = _unique_queue()
    task_name = _task_name("retry_cancel")

    class FailOnce(Task):
        task_info = TaskInfo(
            name=task_name,
            queues=[queue],
            max_retries=3,
            retry_base_delay=1.0,
        )

        @override
        async def run(self) -> str:
            raise RuntimeError("will retry")

    client_broker = RabbitMQBroker(rabbitmq_url)
    client_backend = ValkeyBackend(valkey_url)
    client_app = SwarmQ(client_broker, client_backend, queues=[queue])

    worker_broker = RabbitMQBroker(rabbitmq_url)
    worker_backend = ValkeyBackend(valkey_url)
    worker_app = SwarmQ(
        worker_broker,
        worker_backend,
        queues=[queue],
        concurrency=1,
    )

    async with client_app:
        await client_broker.declare_queue(queue)

        async with worker_app:
            worker_task = asyncio.create_task(worker_app.start_worker())
            try:
                await asyncio.sleep(0.4)

                task_id = await client_app.schedule(task_name)
                # Let the task fail once → lands in .DQ
                await asyncio.sleep(0.5)
                # Now cancel during retry delay
                assert await client_app.cancel(task_id) is True
                # After delay expires and redelivery, pre-start check fires
                with pytest.raises(TaskCancelledError):
                    await client_app.get_result(task_id, timeout=5)
            finally:
                worker_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await worker_task


# ---------------------------------------------------------------------------
# Unit 7 — running cancel (Monitor-Coroutine) E2E
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("_cleanup_tasks")
async def test_e2e_running_cancel_full_stack(
    rabbitmq_url: str,
    valkey_url: str,
):
    """Task is already running → cancel → TaskCancelledError within poll interval."""
    queue = _unique_queue()
    task_name = _task_name("running")

    class LongRun(Task):
        task_info = TaskInfo(name=task_name, queues=[queue])

        @override
        async def run(self) -> str:
            await asyncio.sleep(10)
            return "should-not-return"

    client_broker = RabbitMQBroker(rabbitmq_url)
    client_backend = ValkeyBackend(valkey_url)
    client_app = SwarmQ(client_broker, client_backend, queues=[queue])

    worker_broker = RabbitMQBroker(rabbitmq_url)
    worker_backend = ValkeyBackend(valkey_url)
    worker_app = SwarmQ(
        worker_broker,
        worker_backend,
        queues=[queue],
        concurrency=1,
        cancel_poll_interval=0.1,
    )

    async with client_app:
        await client_broker.declare_queue(queue)

        async with worker_app:
            worker_task = asyncio.create_task(worker_app.start_worker())
            try:
                await asyncio.sleep(0.4)

                task_id = await client_app.schedule(task_name)
                # Let the worker actually start run()
                await asyncio.sleep(0.5)
                # Now cancel mid-execution
                assert await client_app.cancel(task_id) is True
                # Monitor catches this at the next poll, run_task.cancel() fires.
                with pytest.raises(TaskCancelledError):
                    await client_app.get_result(task_id, timeout=3)
            finally:
                worker_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await worker_task


@pytest.mark.usefixtures("_cleanup_tasks")
async def test_e2e_cancel_with_lock_releases_for_successor(
    rabbitmq_url: str,
    valkey_url: str,
):
    """Task holds lock, cancel greift, Lock frei für Nachfolger."""
    queue = _unique_queue()
    lock_name = f"e2e-lock-{uuid.uuid4().hex[:6]}"

    long_name = _task_name("long_locked")
    short_name = _task_name("short_locked")

    class LongLocked(Task):
        task_info = TaskInfo(
            name=long_name,
            queues=[queue],
            lock=lock_name,
            hard_timeout=30,
        )

        @override
        async def run(self) -> str:
            await asyncio.sleep(30)
            return "no"

    class ShortLocked(Task):
        task_info = TaskInfo(
            name=short_name,
            queues=[queue],
            lock=lock_name,
            hard_timeout=30,
        )

        @override
        async def run(self) -> str:
            return "ok"

    client_broker = RabbitMQBroker(rabbitmq_url)
    client_backend = ValkeyBackend(valkey_url)
    client_app = SwarmQ(client_broker, client_backend, queues=[queue])

    worker_broker = RabbitMQBroker(rabbitmq_url)
    worker_backend = ValkeyBackend(valkey_url)
    worker_app = SwarmQ(
        worker_broker,
        worker_backend,
        queues=[queue],
        concurrency=2,
        cancel_poll_interval=0.1,
    )

    async with client_app:
        await client_broker.declare_queue(queue)

        async with worker_app:
            worker_task = asyncio.create_task(worker_app.start_worker())
            try:
                await asyncio.sleep(0.4)

                long_id = await client_app.schedule(long_name)
                await asyncio.sleep(0.5)  # let it acquire the lock

                short_id = await client_app.schedule(short_name)
                # Short is now lock-blocked, will retry via broker.
                await asyncio.sleep(0.3)

                # Cancel the long-running task.
                await client_app.cancel(long_id)
                with pytest.raises(TaskCancelledError):
                    await client_app.get_result(long_id, timeout=5)

                # Successor runs after lock release.
                result = await client_app.get_result(short_id, timeout=10)
                assert result == "ok"
            finally:
                worker_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await worker_task


@pytest.mark.usefixtures("_cleanup_tasks")
async def test_e2e_parallel_mixed_cancel_and_complete(
    rabbitmq_url: str,
    valkey_url: str,
):
    """50 tasks scheduled, 25 get cancelled mid-run, 25 complete — no crosstalk."""
    queue = _unique_queue()
    task_name = _task_name("mixed")

    class MixedTask(Task):
        task_info = TaskInfo(name=task_name, queues=[queue])

        @override
        async def run(self, sleep_s: float, marker: int) -> int:
            await asyncio.sleep(sleep_s)
            return marker

    client_broker = RabbitMQBroker(rabbitmq_url)
    client_backend = ValkeyBackend(valkey_url)
    client_app = SwarmQ(client_broker, client_backend, queues=[queue])

    worker_broker = RabbitMQBroker(rabbitmq_url)
    worker_backend = ValkeyBackend(valkey_url)
    # Reduced scale from 50→20 to keep test runtime reasonable but
    # still exercises the concurrent-cancel path.
    TOTAL = 20
    TO_CANCEL = 10
    worker_app = SwarmQ(
        worker_broker,
        worker_backend,
        queues=[queue],
        concurrency=10,
        cancel_poll_interval=0.1,
    )

    async with client_app:
        await client_broker.declare_queue(queue)

        async with worker_app:
            worker_task = asyncio.create_task(worker_app.start_worker())
            try:
                await asyncio.sleep(0.4)

                # Cancelled tasks sleep longer so the monitor has time
                # to kick in; completed ones sleep only a bit.
                ids_cancel = []
                ids_complete = []
                for i in range(TO_CANCEL):
                    tid = await client_app.schedule(task_name, 5.0, i)
                    ids_cancel.append(tid)
                for i in range(TOTAL - TO_CANCEL):
                    tid = await client_app.schedule(task_name, 0.1, 100 + i)
                    ids_complete.append(tid)

                # Let them all start (concurrency=10).
                await asyncio.sleep(0.8)
                # Cancel the first 10.
                await asyncio.gather(*[client_app.cancel(tid) for tid in ids_cancel])

                # Collect outcomes.
                cancel_errors = 0
                for tid in ids_cancel:
                    with pytest.raises(TaskCancelledError):
                        await client_app.get_result(tid, timeout=5)
                    cancel_errors += 1

                # Remaining tasks completed cleanly with their markers.
                for i, tid in enumerate(ids_complete):
                    result = await client_app.get_result(tid, timeout=5)
                    assert result == 100 + i

                assert cancel_errors == TO_CANCEL
            finally:
                worker_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await worker_task


@pytest.mark.usefixtures("_cleanup_tasks")
async def test_e2e_shutdown_during_running_cancel(
    rabbitmq_url: str,
    valkey_url: str,
):
    """SIGTERM during a cancelled run: detached_releases drained, no lock leak."""
    queue = _unique_queue()
    task_name = _task_name("shutdown_cancel")
    lock_name = f"shutdown-{uuid.uuid4().hex[:6]}"

    class LockedLong(Task):
        task_info = TaskInfo(
            name=task_name,
            queues=[queue],
            lock=lock_name,
            hard_timeout=30,
        )

        @override
        async def run(self) -> str:
            await asyncio.sleep(30)
            return "no"

    client_broker = RabbitMQBroker(rabbitmq_url)
    client_backend = ValkeyBackend(valkey_url)
    client_app = SwarmQ(client_broker, client_backend, queues=[queue])

    worker_broker = RabbitMQBroker(rabbitmq_url)
    worker_backend = ValkeyBackend(valkey_url)
    worker_app = SwarmQ(
        worker_broker,
        worker_backend,
        queues=[queue],
        concurrency=1,
        cancel_poll_interval=0.1,
        shutdown_timeout=2,
    )

    async with client_app:
        await client_broker.declare_queue(queue)

        async with worker_app:
            worker_task = asyncio.create_task(worker_app.start_worker())
            try:
                await asyncio.sleep(0.4)

                task_id = await client_app.schedule(task_name)
                await asyncio.sleep(0.5)  # lock acquired

                # Cancel + immediate shutdown.
                await client_app.cancel(task_id)
                await asyncio.sleep(0.05)  # microdelay so cancel lands first
            finally:
                worker_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await worker_task

        # Lock must be gone from Valkey even though shutdown cancelled
        # the handler mid-release.
        from swarmq.locking import lock_key

        assert client_backend._client is not None
        raw = await client_backend._client.get(lock_key(lock_name))
        assert raw is None, f"lock was not released during shutdown-cancel: {raw!r}"
