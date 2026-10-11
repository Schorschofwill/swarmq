"""Integration tests for worker pre-start cancel path (Phase 3b.3 / Unit 6).

Uses real RabbitMQ + Valkey via ``tests/integration/conftest.py``.
Worker runs in a background asyncio Task, exactly as in production.
"""

from __future__ import annotations

import asyncio
import uuid

import pytest
import valkey.asyncio as aio_valkey

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import TaskCancelledError
from swarmq.keyspace import KeySpace
from swarmq.signals import Signal
from swarmq.task import Task, TaskInfo

# Cancel integration tests run a worker and observe cancel propagation
# in real time. 15s covers the slowest paths (hard-timeout watchdogs,
# lock-wait sequences) with margin for load-induced latency.
pytestmark = [pytest.mark.integration, pytest.mark.timeout(15)]


def _unique_queue() -> str:
    return f"cancel-test-{uuid.uuid4().hex[:8]}"


async def _cancel_flag_pttl(valkey_url: str, task_id: str) -> int:
    """Restlaufzeit des Cancel-Flags in ms (BZ-23: der Worker verkürzt es
    auf höchstens 1 h statt es zu löschen)."""
    raw = aio_valkey.Valkey.from_url(valkey_url)
    try:
        return int(await raw.pttl(KeySpace(None).cancel(task_id)))
    finally:
        await raw.aclose()


@pytest.fixture
def _cleanup_tasks():
    """Clear the Task._registered_tasks list after each test."""
    yield
    Task._clear_registered_tasks()


async def _start_worker(app: SwarmQ) -> asyncio.Task:
    """Start the worker as a background Task; returns the Task."""
    worker_task = asyncio.create_task(app.start_worker())
    # Give the worker a moment to connect + declare + consume.
    await asyncio.sleep(0.2)
    return worker_task


async def _stop_worker(app: SwarmQ, worker_task: asyncio.Task) -> None:
    """Cleanly stop the worker by firing the shutdown event."""
    # Use the internal API to trigger graceful shutdown without SIGTERM.
    for attr in ("_worker",):
        if hasattr(app, attr):
            break
    getattr(app, attr, None)
    # There's no single app-level stop; drain via cancel+await is safe
    # because start_worker's shutdown path handles CancelledError.
    worker_task.cancel()
    try:
        await worker_task
    except asyncio.CancelledError:
        pass


class TestPreStartCancelIntegration:
    async def test_queued_task_cancel_before_worker_consumes(
        self,
        rabbitmq_url,
        valkey_url,
        _cleanup_tasks,
    ):
        """Schedule → cancel() → worker consumes → TaskCancelledError.

        Queue is declared first so the scheduled message actually lands
        in RabbitMQ before the cancel flag is set. The worker is then
        started and immediately sees the flag on dequeue.
        """
        queue = _unique_queue()

        class SlowTask(Task):
            task_info = TaskInfo(name="slow_q", queues=[queue])

            async def run(self) -> str:
                await asyncio.sleep(5)
                return "should-not-return"

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)

        async with SwarmQ(broker, backend, queues=[queue], concurrency=1) as app:
            # Declare the queue so the scheduled message isn't dropped
            # before the worker's own declare_queue runs.
            await app.broker.declare_queue(queue)

            task_id = await app.schedule("slow_q")
            assert await app.cancel(task_id) is True

            worker_task = await _start_worker(app)
            try:
                # Result should be written quickly — no 5s wait.
                with pytest.raises(TaskCancelledError):
                    await app.get_result(task_id, timeout=3)
            finally:
                await _stop_worker(app, worker_task)

            # BZ-23 (Plan 0.12.0, MR 5): der Worker verkürzt das Flag auf
            # höchstens 1 h, statt es zu löschen.
            assert await backend.is_cancelled(task_id) is True
            assert 0 < await _cancel_flag_pttl(valkey_url, task_id) <= 3_600_000

    async def test_double_cancel_returns_false_second_time(
        self,
        rabbitmq_url,
        valkey_url,
        _cleanup_tasks,
    ):
        """Two back-to-back cancel() calls: first True, second False."""
        queue = _unique_queue()

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)

        async with SwarmQ(broker, backend, queues=[queue], concurrency=1) as app:
            task_id = str(uuid.uuid7())
            first = await app.cancel(task_id)
            second = await app.cancel(task_id)
            assert first is True
            assert second is False
            # Clean up the flag.
            await backend.clear_cancel_flag(task_id)

    async def test_cancel_after_completion_has_no_effect(
        self,
        rabbitmq_url,
        valkey_url,
        _cleanup_tasks,
    ):
        """Task runs to completion → cancel() sets a flag with no effect.

        The real invariant we verify: cancelling an already-completed
        task must not retroactively mutate its stored result.  Earlier
        revisions of this test used `app.run()`, which discards the
        task_id — so the cancel was against a hardcoded placeholder
        and the invariant was never actually checked.
        """
        queue = _unique_queue()

        class FastTask(Task):
            task_info = TaskInfo(name="fast", queues=[queue])

            async def run(self) -> str:
                return "completed"

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)

        async with SwarmQ(broker, backend, queues=[queue], concurrency=1) as app:
            await app.broker.declare_queue(queue)
            worker_task = await _start_worker(app)
            try:
                # Schedule + wait for real completion; keep the task_id.
                task_id = await app.schedule("fast")
                result = await app.get_result(task_id, timeout=3)
                assert result == "completed"

                # Cancel the already-completed task.  Flag is newly set
                # (True) because there was no prior cancel for this id.
                assert await app.cancel(task_id) is True

                # Re-reading the result returns the real completion
                # value — cancel did NOT mutate the stored result.
                assert await app.get_result(task_id, timeout=1) == "completed"
            finally:
                # Clean up the orphan flag so it doesn't linger in the
                # shared Valkey across test runs.
                await backend.clear_cancel_flag(task_id)
                await _stop_worker(app, worker_task)

    async def test_cancel_unknown_task_id_harmless(
        self,
        rabbitmq_url,
        valkey_url,
        _cleanup_tasks,
    ):
        """cancel() for a task_id that was never scheduled: harmless."""
        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)

        async with SwarmQ(broker, backend, queues=["any"]) as app:
            fake_id = f"never-scheduled-{uuid.uuid4().hex}"
            assert await app.cancel(fake_id) is True
            # Flag exists but will TTL out; manually clean up.
            await backend.clear_cancel_flag(fake_id)

    async def test_signal_task_cancelled_dispatched_on_pre_start(
        self,
        rabbitmq_url,
        valkey_url,
        _cleanup_tasks,
    ):
        """TASK_CANCELLED signal fires when a queued task is cancelled."""
        queue = _unique_queue()

        class TraceTask(Task):
            task_info = TaskInfo(name="trace", queues=[queue])

            async def run(self) -> int:
                await asyncio.sleep(5)
                return 1

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)

        captured: list[dict] = []

        async with SwarmQ(broker, backend, queues=[queue], concurrency=1) as app:
            await app.broker.declare_queue(queue)

            @app.on_signal(Signal.TASK_CANCELLED)
            async def handler(**kwargs):
                captured.append(kwargs)

            task_id = await app.schedule("trace")
            await app.cancel(task_id)

            worker_task = await _start_worker(app)
            try:
                with pytest.raises(TaskCancelledError):
                    await app.get_result(task_id, timeout=3)
            finally:
                await _stop_worker(app, worker_task)

            assert len(captured) == 1
            assert captured[0]["task_id"] == task_id
            assert captured[0]["task_name"] == "trace"

    async def test_cancel_in_retry_delay(
        self,
        rabbitmq_url,
        valkey_url,
        _cleanup_tasks,
    ):
        """Task fails, lands in DQ with delay, cancel during delay → TaskCancelledError.

        Validates the Plan Open Question: cancel flag survives the delay
        window (default TTL 24h >> typical retry delay).
        """
        queue = _unique_queue()

        class FailOnceTask(Task):
            task_info = TaskInfo(
                name="fail_once",
                queues=[queue],
                max_retries=3,
                retry_base_delay=1.0,  # 1s delay before retry
            )

            async def run(self) -> str:
                raise RuntimeError("first-run-fail")

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)

        async with SwarmQ(broker, backend, queues=[queue], concurrency=1) as app:
            worker_task = await _start_worker(app)
            try:
                task_id = await app.schedule("fail_once")
                # Let the task fail + land in the delay queue.
                await asyncio.sleep(0.4)
                # Now cancel during the retry delay window.
                assert await app.cancel(task_id) is True
                # After delay expires, pre-start check catches the flag.
                with pytest.raises(TaskCancelledError):
                    await app.get_result(task_id, timeout=5)
            finally:
                await _stop_worker(app, worker_task)


# ---------------------------------------------------------------------------
# Unit 7 — running-cancel integration (Monitor-Coroutine)
# ---------------------------------------------------------------------------


class TestRunningCancelIntegration:
    async def test_running_task_cancel_via_monitor(
        self,
        rabbitmq_url,
        valkey_url,
        _cleanup_tasks,
    ):
        """asyncio.sleep(10) task cancelled 500ms in → TaskCancelledError in <2s."""
        queue = _unique_queue()

        class LongTask(Task):
            task_info = TaskInfo(name="long_run", queues=[queue])

            async def run(self) -> str:
                await asyncio.sleep(10)
                return "should-not-return"

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)

        # Short poll interval for fast test.
        async with SwarmQ(
            broker,
            backend,
            queues=[queue],
            concurrency=1,
            cancel_poll_interval=0.1,
        ) as app:
            await app.broker.declare_queue(queue)

            worker_task = await _start_worker(app)
            try:
                task_id = await app.schedule("long_run")
                # Let the worker dequeue and start run().
                await asyncio.sleep(0.5)
                assert await app.cancel(task_id) is True
                # Monitor should catch the flag at next poll (≤100ms).
                with pytest.raises(TaskCancelledError):
                    await app.get_result(task_id, timeout=3)
                # BZ-23 (Plan 0.12.0, MR 5): Flag auf höchstens 1 h verkürzt.
                assert await backend.is_cancelled(task_id) is True
                assert 0 < await _cancel_flag_pttl(valkey_url, task_id) <= 3_600_000
            finally:
                await _stop_worker(app, worker_task)

    async def test_hard_timeout_still_works_without_cancel(
        self,
        rabbitmq_url,
        valkey_url,
        _cleanup_tasks,
    ):
        """Regression: task exceeds hard_timeout, no cancel → task fails with timeout."""
        queue = _unique_queue()

        class TimeoutTask(Task):
            task_info = TaskInfo(
                name="timeout_task",
                queues=[queue],
                hard_timeout=1,
            )

            async def run(self) -> str:
                await asyncio.sleep(10)
                return "never"

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)

        async with SwarmQ(
            broker,
            backend,
            queues=[queue],
            concurrency=1,
            cancel_poll_interval=0.5,
        ) as app:
            await app.broker.declare_queue(queue)

            worker_task = await _start_worker(app)
            try:
                task_id = await app.schedule("timeout_task")
                # Expect a TaskError (NOT TaskCancelledError).
                from swarmq.exceptions import TaskError

                with pytest.raises(TaskError) as excinfo:
                    await app.get_result(task_id, timeout=5)
                # Cancel-specific exception must NOT be raised.
                assert not isinstance(excinfo.value, TaskCancelledError)
                assert "hard timeout" in str(excinfo.value).lower()
            finally:
                await _stop_worker(app, worker_task)

    async def test_cancel_with_lock_releases_lock_for_successor(
        self,
        rabbitmq_url,
        valkey_url,
        _cleanup_tasks,
    ):
        """Task holds lock, cancel greift, lock is released, next task can acquire."""
        queue = _unique_queue()
        lock_name = f"lock-test-{uuid.uuid4().hex[:6]}"

        order: list[str] = []

        class LockedLongTask(Task):
            task_info = TaskInfo(
                name="locked_long",
                queues=[queue],
                lock=lock_name,
                hard_timeout=30,
            )

            async def run(self) -> str:
                order.append("long_started")
                await asyncio.sleep(10)
                return "no"

        class LockedShortTask(Task):
            task_info = TaskInfo(
                name="locked_short",
                queues=[queue],
                lock=lock_name,
                hard_timeout=30,
            )

            async def run(self) -> str:
                order.append("short_ran")
                return "ok"

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)

        async with SwarmQ(
            broker,
            backend,
            queues=[queue],
            concurrency=2,
            cancel_poll_interval=0.1,
        ) as app:
            await app.broker.declare_queue(queue)

            worker_task = await _start_worker(app)
            try:
                long_id = await app.schedule("locked_long")
                await asyncio.sleep(0.4)  # let it acquire the lock
                # Schedule the successor — it must be gated by the lock.
                short_id = await app.schedule("locked_short")
                await asyncio.sleep(0.2)  # give it a chance to try-acquire
                # Now cancel the long task.
                await app.cancel(long_id)
                # Long task result is Cancelled.
                with pytest.raises(TaskCancelledError):
                    await app.get_result(long_id, timeout=5)
                # Short task eventually runs because the lock was released.
                result = await app.get_result(short_id, timeout=10)
                assert result == "ok"
                assert "short_ran" in order
            finally:
                await _stop_worker(app, worker_task)
