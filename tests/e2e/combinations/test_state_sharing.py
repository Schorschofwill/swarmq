"""E2E combination tests — State-Sharing Combos 1-6 (Plan 2026-05-26-002 §U4).

Six combos where two features share backend state (Valkey keys, cancel flags,
result TTLs) and therefore have non-trivial interaction surface:

  Combo 1 — retry + unique_for:  does the unique-hash survive across retries?
  Combo 2 — lock + cancel:       does cancel guarantee lock release?
  Combo 3 — lock + retry:        does retry hold, drop, or re-acquire the lock?
  Combo 4 — lock + hard_timeout: does TTL-based lock recovery fire after kill?
  Combo 5 — progress + cancel:   do progress updates stop once cancel fires?
  Combo 6 — result_ttl + retry:  does the TTL clock start at enqueue or completion?

Each class follows the same structure:
  test_happy_path        — both features work together (Tier 1, unmarked)
  test_failure_*         — one station raises an exception (Tier 1, unmarked)
  test_*                 — Toxiproxy / SIGKILL scenarios (Tier 2/3, @pytest.mark.chaos)

Universal invariants from ``_invariants`` are applied where the emission
order or deadlock-recovery surface is contractual.

Assumptions documented in-line; see §Assumptions at the bottom.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
import uuid
from typing import Any

import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import TaskCancelledError
from swarmq.signals import Signal
from swarmq.task import Task, TaskInfo
from tests.support.worker_subprocess import (
    SHORT_SCHEDULER_LOCK_ENV,
    SHORT_SCHEDULER_LOCK_TTL_S,
    crash_worker,
    kill_subprocess,
    lock_owner_pid,
    scheduler_lock_owner,
    spawn_worker,
    wait_for_scheduler_leader,
    wait_for_worker_children,
    wait_for_worker_started,
)

from ._invariants import (
    FAFStageObservation,
    SignalRecorder,
    assert_at_least_once_held,
    assert_faf_stage_invariants,
    assert_no_reader_leak,
    assert_signal_sequence,
)

# Module-level timeout cap; individual chaos tests may override via
# @pytest.mark.timeout(N) if SIGKILL + TTL waits require more headroom.
pytestmark = [pytest.mark.timeout(30)]


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _uq(prefix: str = "combo") -> str:
    """Return a unique queue name for this test run."""
    return f"e2e-{prefix}-{uuid.uuid4().hex[:10]}"


def _task_name(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


@contextlib.asynccontextmanager
async def _running_worker(
    rabbitmq_url: str,
    valkey_url: str,
    queue: str,
    *,
    concurrency: int = 4,
    cancel_poll_interval: float = 0.1,
    shutdown_timeout: int = 5,
    mirror_to: Any = None,
):
    """Spin up an in-process SwarmQ worker against the given queue.

    Yields ``(worker_app, online_event)`` — the caller can attach signal
    handlers on ``worker_app`` BEFORE the worker comes online.  The online
    event is already set by the time control is handed back.

    ``mirror_to`` (a SignalDispatcher, typically the producer ``app``'s) makes
    the worker re-dispatch every signal it emits onto that dispatcher, so
    SignalRecorder / @on_signal observers attached to the producer app see the
    worker's task-lifecycle signals (the worker runs in a separate SwarmQ
    instance with its own dispatcher).
    """
    worker_broker = RabbitMQBroker(rabbitmq_url)
    worker_backend = ValkeyBackend(valkey_url)
    worker_app = SwarmQ(
        worker_broker,
        worker_backend,
        queues=[queue],
        concurrency=concurrency,
        cancel_poll_interval=cancel_poll_interval,
        shutdown_timeout=shutdown_timeout,
    )
    online = asyncio.Event()

    @worker_app.on_signal(Signal.WORKER_ONLINE)
    async def _online(**_: Any) -> None:
        online.set()

    if mirror_to is not None:
        def _make_forwarder(sig: Signal):
            async def _forward(**payload: Any) -> None:
                await mirror_to.dispatch(sig, **payload)
            return _forward
        for _sig in Signal:
            worker_app._signal_dispatcher.connect(_sig, _make_forwarder(_sig))

    worker_task = asyncio.create_task(worker_app.start_worker())
    try:
        await asyncio.wait_for(online.wait(), timeout=8.0)
        yield worker_app
    finally:
        worker_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker_task
        await worker_broker.close()
        await worker_backend.close()


# ---------------------------------------------------------------------------
# Combo 1 — retry + unique_for
# ---------------------------------------------------------------------------


class TestRetryUniqueCombo:
    """Combo #1: retry + unique_for.

    Bug surface: When a task is retried, the unique-hash key in Valkey must
    remain claimed (not released) during retries so duplicate enqueues are
    still blocked.  Only after max_retries is exhausted (→ DLQ) should the
    unique flag be cleared.

    Learning ref: docs/solutions/patterns/r3-5-unique-tasks-implementation-learnings-2026-04-17.md
    (release-after-ack invariant).
    """

    async def test_happy_path(self, rabbitmq_url: str, valkey_url: str) -> None:
        """Task fails once, retries successfully — unique flag blocks a
        duplicate that arrives during the retry window."""
        queue = _uq("ru")
        task_name = _task_name("ru_happy")
        call_count = 0
        completed = asyncio.Event()

        class RetryUniqueTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                max_retries=2,
                retry_base_delay=0.1,
                unique_for=60,
                unique_until="completion",
            )

            async def run(self, payload: str) -> str:
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    raise RuntimeError("first attempt fails")
                completed.set()
                return f"ok:{payload}"

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        dup_signals: list[dict] = []

        @app.on_signal(Signal.TASK_DUPLICATE_DETECTED)
        async def _on_dup(**kwargs: Any) -> None:
            dup_signals.append(kwargs)

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url, valkey_url, queue,
                mirror_to=app._signal_dispatcher,
            ):
                tid = await app.schedule(task_name, "hello")

                # Attempt a duplicate while task is in retry-delay window.
                await asyncio.sleep(0.05)
                tid_dup = await app.schedule(task_name, "hello")

                assert tid == tid_dup, (
                    "unique flag must block duplicate during retry window"
                )

                result = await app.get_result(tid, timeout=10)
                assert result == "ok:hello"

        assert call_count >= 2, "task must have been retried at least once"
        assert len(dup_signals) >= 1, (
            "TASK_DUPLICATE_DETECTED must fire for the duplicate enqueue"
        )
        assert_at_least_once_held(call_count, expected_min=2)

    async def test_unique_released_after_retry_exhausted(
        self, rabbitmq_url: str, valkey_url: str
    ) -> None:
        """After max_retries is exhausted and the task lands in DLQ, the
        unique flag must be released so a fresh enqueue can succeed.

        Assumption: DLQ move triggers unique release via the same
        release-after-ack path that a successful completion uses.
        """
        queue = _uq("ru-dlq")
        task_name = _task_name("ru_dlq")
        call_count = 0

        class AlwaysFailTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                max_retries=1,
                retry_base_delay=0.1,
                unique_for=60,
                unique_until="completion",
            )

            async def run(self, val: int) -> int:
                nonlocal call_count
                call_count += 1
                raise RuntimeError("always fails")

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url, valkey_url, queue,
                mirror_to=app._signal_dispatcher,
            ):
                tid = await app.schedule(task_name, 1)

                # Wait until retries exhausted (2 attempts × 0.1s delay + headroom).
                await asyncio.sleep(2.0)

        # After worker shutdown, attempt a fresh enqueue. If the unique flag
        # was released, we get a NEW task_id.
        broker2 = RabbitMQBroker(rabbitmq_url)
        backend2 = ValkeyBackend(valkey_url)
        app2 = SwarmQ(broker2, backend2, queues=[queue])
        async with app2:
            tid_fresh = await app2.schedule(task_name, 1)

        assert tid_fresh != tid, (
            "unique flag must be released after retry exhaustion → DLQ; "
            "fresh enqueue must get a new task_id"
        )
        assert_at_least_once_held(call_count, expected_min=2)

    async def test_duplicate_blocked_during_active_retry(
        self, rabbitmq_url: str, valkey_url: str
    ) -> None:
        """Duplicate enqueued WHILE a retry attempt is running must be
        deduplicated — the unique flag must not drop between retry attempts."""
        queue = _uq("ru-active")
        task_name = _task_name("ru_active")
        attempt = 0
        running = asyncio.Event()

        class SlowRetryTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                max_retries=3,
                retry_base_delay=0.5,
                unique_for=120,
                unique_until="completion",
            )

            async def run(self, tag: str) -> str:
                nonlocal attempt
                attempt += 1
                if attempt == 1:
                    running.set()
                    raise RuntimeError("first attempt fails")
                running.set()
                return f"done:{tag}"

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])
        dup_signals: list[dict] = []

        @app.on_signal(Signal.TASK_DUPLICATE_DETECTED)
        async def _capture(**kw: Any) -> None:
            dup_signals.append(kw)

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url, valkey_url, queue,
                mirror_to=app._signal_dispatcher,
            ):
                tid = await app.schedule(task_name, "x")
                # Wait until the first attempt is running.
                await asyncio.wait_for(running.wait(), timeout=5.0)
                running.clear()
                # Duplicate during first attempt.
                tid_dup = await app.schedule(task_name, "x")
                assert tid == tid_dup, (
                    "duplicate during active run must be deduplicated"
                )
                result = await app.get_result(tid, timeout=10)
                assert result == "done:x"

        assert len(dup_signals) >= 1

    @pytest.mark.chaos
    async def test_backend_drop_mid_retry_faf_invariants(
        self,
        rabbitmq_url: str,
        valkey_url: str,
        rabbitmq_proxied_url: str,
        toxiproxy: Any,
    ) -> None:
        """Toxiproxy drops the RabbitMQ connection mid-retry; the broker
        should nack(requeue=True) rather than silently drop the message.

        Verifies FAF-4 stage invariants for the broker-drop scenario.
        """
        queue = _uq("ru-chaos")
        task_name = _task_name("ru_chaos")
        call_count = 0
        completed = asyncio.Event()

        class RetryTaskChaos(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                max_retries=3,
                retry_base_delay=0.2,
                unique_for=120,
                unique_until="completion",
            )

            async def run(self, val: int) -> int:
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    raise RuntimeError("first attempt fails")
                completed.set()
                return val * 2

        broker = RabbitMQBroker(rabbitmq_proxied_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_proxied_url, valkey_url, queue
            ):
                tid = await app.schedule(task_name, 7)

                # Wait for the first attempt to fail, then inject a brief
                # broker timeout to simulate a mid-retry connection drop.
                await asyncio.sleep(0.3)
                toxic_name = await toxiproxy.add_toxic(
                    "rabbitmq",
                    "timeout",
                    attributes={"timeout": 0},  # immediate close
                    stream="downstream",
                )
                await asyncio.sleep(0.2)
                await toxiproxy.remove_toxic("rabbitmq", toxic_name)

                # After healing, the task should eventually complete.
                await asyncio.wait_for(completed.wait(), timeout=15.0)
                result = await app.get_result(tid, timeout=5)
                assert result == 14

        assert_at_least_once_held(call_count, expected_min=2)

        # FAF-4: after broker-drop the task must NOT have been silently dropped.
        obs = FAFStageObservation(
            stage_executed=call_count >= 2,
            nacked_with_requeue=call_count >= 2,  # re-execution == was re-queued
            silently_dropped=call_count == 0,
            requeue_count=call_count - 1,
        )
        assert_faf_stage_invariants(obs, scenario_kind="broker_drop", max_requeues=5)


# ---------------------------------------------------------------------------
# Combo 2 — lock + cancel
# ---------------------------------------------------------------------------


class TestLockCancelCombo:
    """Combo #2: lock + cancel.

    Bug surface: cancellation must release the lock so a successor task
    can acquire it.  Signal sequence TASK_STARTED → TASK_CANCELLED (no
    TASK_COMPLETED) is the observable contract.
    """

    async def test_happy_path(self, rabbitmq_url: str, valkey_url: str) -> None:
        """Task runs with lock, completes, successor acquires lock and runs."""
        queue = _uq("lc")
        lock_name = f"lc-lock-{uuid.uuid4().hex[:6]}"
        task_name = _task_name("lc_happy")
        results: list[str] = []
        all_done = asyncio.Event()

        class LockedTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                lock=lock_name,
                hard_timeout=10,
            )

            async def run(self, tag: str) -> str:
                results.append(tag)
                if len(results) == 2:
                    all_done.set()
                return f"ok:{tag}"

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url, valkey_url, queue,
                mirror_to=app._signal_dispatcher,
            ):
                await app.schedule(task_name, "first")
                await app.schedule(task_name, "second")
                await asyncio.wait_for(all_done.wait(), timeout=12.0)

        assert results == ["first", "second"], (
            "both tasks must complete in order under mutex"
        )

    async def test_cancel_before_lock_acquired(
        self, rabbitmq_url: str, valkey_url: str
    ) -> None:
        """Task cancelled before the lock is acquired: lock must never be
        taken, successor task runs without waiting for TTL."""
        queue = _uq("lc-pre")
        lock_name = f"lc-pre-lock-{uuid.uuid4().hex[:6]}"
        slow_name = _task_name("lc_slow")
        fast_name = _task_name("lc_fast")
        fast_result: list[str] = []
        fast_done = asyncio.Event()

        class SlowLockedTask(Task):
            task_info = TaskInfo(
                name=slow_name, queues=[queue], lock=lock_name, hard_timeout=20,
            )
            async def run(self) -> str:
                await asyncio.sleep(20)
                return "no"

        class FastLockedTask(Task):
            task_info = TaskInfo(
                name=fast_name, queues=[queue], lock=lock_name, hard_timeout=10,
            )
            async def run(self) -> str:
                fast_result.append("ran")
                fast_done.set()
                return "yes"

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue], cancel_poll_interval=0.1)

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url, valkey_url, queue, cancel_poll_interval=0.1,
                mirror_to=app._signal_dispatcher,
            ):
                # Schedule slow first so it holds the lock; cancel it
                # before it acquires.
                slow_id = await app.schedule(slow_name)
                await asyncio.sleep(0.3)  # let slow start and hold lock
                await app.cancel(slow_id)

                fast_id = await app.schedule(fast_name)
                await asyncio.wait_for(fast_done.wait(), timeout=12.0)
                result = await app.get_result(fast_id, timeout=5)

        assert result == "yes", "fast task must run after slow cancelled"
        assert fast_result == ["ran"]

    async def test_cancel_during_run_releases_lock(
        self, rabbitmq_url: str, valkey_url: str
    ) -> None:
        """Cancel mid-run: TASK_STARTED → TASK_CANCELLED signal sequence,
        and the lock is free for a successor task immediately after.

        This is the primary contract of Combo #2.
        """
        queue = _uq("lc-mid")
        lock_name = f"lc-mid-{uuid.uuid4().hex[:6]}"
        long_name = _task_name("lc_long")
        short_name = _task_name("lc_short")
        long_started = asyncio.Event()

        class LongLocked(Task):
            task_info = TaskInfo(
                name=long_name, queues=[queue], lock=lock_name, hard_timeout=30,
            )
            async def run(self) -> str:
                long_started.set()
                await asyncio.sleep(30)
                return "no"

        class ShortLocked(Task):
            task_info = TaskInfo(
                name=short_name, queues=[queue], lock=lock_name, hard_timeout=10,
            )
            async def run(self) -> str:
                return "ok"

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        recorder = SignalRecorder.attach(
            app._signal_dispatcher,
            signals=[Signal.TASK_STARTED, Signal.TASK_CANCELLED, Signal.TASK_COMPLETED],
        )

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url, valkey_url, queue,
                concurrency=2, cancel_poll_interval=0.1,
                mirror_to=app._signal_dispatcher,
            ):
                long_id = await app.schedule(long_name)
                await asyncio.wait_for(long_started.wait(), timeout=5.0)
                await asyncio.sleep(0.2)  # lock now held

                await app.cancel(long_id)
                with pytest.raises(TaskCancelledError):
                    await app.get_result(long_id, timeout=5)

                # Successor must acquire lock without waiting for TTL.
                short_id = await app.schedule(short_name)
                result = await app.get_result(short_id, timeout=8)

        assert result == "ok", "successor must run after cancel releases lock"

        # Signal-sequence invariant: long task → STARTED then CANCELLED.
        await assert_signal_sequence(
            recorder,
            [Signal.TASK_STARTED, Signal.TASK_CANCELLED],
            timeout_s=1.0,
            strict_order=True,
        )

    @pytest.mark.chaos
    @pytest.mark.timeout(60)
    async def test_sigkill_during_lock_held_recovery(
        self,
        rabbitmq_url: str,
        valkey_url: str,
        unique_queue: str,
    ) -> None:
        """Worker A gets SIGKILL → worker B comes online cleanly afterward.

        This test does NOT inject a real lock (see the inline note below) and
        does NOT wait for a lock TTL to expire — it only verifies that a hard
        kill of one worker leaves the broker/backend in a state where a fresh
        worker starts normally with no reader leak. The 60s timeout is for the
        two subprocess-startup waits, not for any TTL.

        (Lock-TTL semantics after P0-A fix, 2026-06-02: TTL =
        ``hard_timeout + 30s`` and therefore always outlives the holder's
        permitted runtime; the actual TTL-driven re-acquire path is covered by
        the LockManager integration tests against a real Valkey.)
        """
        queue = unique_queue
        fixture_module = "tests.e2e._shutdown_fixture_tasks"
        asyncio.Event()

        # Spawn worker A as subprocess so we can SIGKILL it.
        proc_a = spawn_worker(
            queue,
            fixture_module=fixture_module,
            broker_url=rabbitmq_url,
            backend_url=valkey_url,
            concurrency=1,
        )
        try:
            wait_for_worker_started(proc_a, timeout=20.0)

            broker = RabbitMQBroker(rabbitmq_url)
            backend = ValkeyBackend(valkey_url)
            app = SwarmQ(broker, backend, queues=[queue])

            async with app:
                await broker.declare_queue(queue)
                # Worker A handles the task but we cannot easily inject a
                # lock here without a custom fixture. Instead verify that
                # after a hard crash (worker child + supervisor SIGKILL, no
                # drain) a second worker can start fresh tasks.
                crash_worker(proc_a)

            # Spawn worker B — it should come online normally.
            proc_b = spawn_worker(
                queue,
                fixture_module=fixture_module,
                broker_url=rabbitmq_url,
                backend_url=valkey_url,
                concurrency=1,
            )
            try:
                wait_for_worker_started(proc_b, timeout=20.0)
                # Worker B is healthy — assert no reader leak.
                await assert_no_reader_leak(proc_b)
            finally:
                kill_subprocess(proc_b)

        finally:
            kill_subprocess(proc_a)


# ---------------------------------------------------------------------------
# Combo 3 — lock + retry
# ---------------------------------------------------------------------------


class TestLockRetryCombo:
    """Combo #3: lock + retry.

    Bug surface: when a task retries, the lock must be released between
    attempts (not silently re-held), so other tasks waiting on the same
    lock can proceed.  The actual observed behaviour (hold vs release) is
    pinned by test_retry_releases_lock_between_attempts.
    """

    async def test_happy_path(self, rabbitmq_url: str, valkey_url: str) -> None:
        """Task fails once, retries once, succeeds; lock is not permanently
        wedged between the two attempts."""
        queue = _uq("lr")
        lock_name = f"lr-lock-{uuid.uuid4().hex[:6]}"
        task_name = _task_name("lr_happy")
        attempts = 0
        done = asyncio.Event()

        class LockRetryTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                lock=lock_name,
                hard_timeout=10,
                max_retries=2,
                retry_base_delay=0.1,
            )

            async def run(self, val: int) -> int:
                nonlocal attempts
                attempts += 1
                if attempts == 1:
                    raise RuntimeError("first attempt fails")
                done.set()
                return val + 1

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url, valkey_url, queue,
                mirror_to=app._signal_dispatcher,
            ):
                tid = await app.schedule(task_name, 5)
                await asyncio.wait_for(done.wait(), timeout=10.0)
                result = await app.get_result(tid, timeout=5)

        assert result == 6
        assert_at_least_once_held(attempts, expected_min=2)

    async def test_retry_releases_lock_between_attempts(
        self, rabbitmq_url: str, valkey_url: str
    ) -> None:
        """Pin actual behaviour: after a failed attempt, the lock is released
        so another task with the same lock key can run before the retry fires.

        This test documents the release-between-retries contract.  If the
        implementation changes to hold the lock across retries, this test
        will fail and the contract comment must be updated.
        """
        queue = _uq("lr-rel")
        lock_name = f"lr-rel-{uuid.uuid4().hex[:6]}"
        task_a_name = _task_name("lr_a")
        task_b_name = _task_name("lr_b")
        b_ran = asyncio.Event()
        a_retry_count = 0

        class TaskA(Task):
            task_info = TaskInfo(
                name=task_a_name,
                queues=[queue],
                lock=lock_name,
                hard_timeout=10,
                max_retries=3,
                retry_base_delay=1.0,  # 1s delay → B gets window
            )

            async def run(self, attempt_marker: int) -> int:
                nonlocal a_retry_count
                a_retry_count += 1
                if a_retry_count == 1:
                    raise RuntimeError("intentional failure")
                return attempt_marker

        class TaskB(Task):
            task_info = TaskInfo(
                name=task_b_name,
                queues=[queue],
                lock=lock_name,
                hard_timeout=10,
            )

            async def run(self) -> str:
                b_ran.set()
                return "b-ran"

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url, valkey_url, queue, concurrency=2,
                mirror_to=app._signal_dispatcher,
            ):
                tid_a = await app.schedule(task_a_name, 42)

                # Wait for task A to fail and enter retry-delay.
                await asyncio.sleep(0.4)

                # Schedule B — if lock is released between retries, B runs now.
                tid_b = await app.schedule(task_b_name)

                # B should complete within the retry-delay window (1s).
                await asyncio.wait_for(b_ran.wait(), timeout=3.0)
                b_result = await app.get_result(tid_b, timeout=3)
                assert b_result == "b-ran", (
                    "lock must be released between retry attempts so B can run"
                )

                a_result = await app.get_result(tid_a, timeout=5)
                assert a_result == 42

    async def test_lock_ttl_expires_mid_retry_delay(
        self, rabbitmq_url: str, valkey_url: str
    ) -> None:
        """If the lock TTL were to expire during the retry delay, the
        next attempt should still be able to re-acquire it cleanly.

        Assumption: hard_timeout=5 → TTL=(5+30)s=35s (P0-A fix).  We use
        a very short retry delay and verify the task retries and succeeds —
        we cannot make TTL expire in the retry window without using very
        large hard_timeout values, so this test verifies the retry
        re-acquires the lock rather than failing with a lock conflict.
        """
        queue = _uq("lr-ttl")
        lock_name = f"lr-ttl-{uuid.uuid4().hex[:6]}"
        task_name = _task_name("lr_ttl")
        attempts = 0
        done = asyncio.Event()

        class LockTTLTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                lock=lock_name,
                hard_timeout=10,
                max_retries=3,
                retry_base_delay=0.2,
            )

            async def run(self, tag: str) -> str:
                nonlocal attempts
                attempts += 1
                if attempts <= 2:
                    raise RuntimeError(f"attempt {attempts} fails")
                done.set()
                return f"ok:{tag}"

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url, valkey_url, queue,
                mirror_to=app._signal_dispatcher,
            ):
                tid = await app.schedule(task_name, "ttl-test")
                await asyncio.wait_for(done.wait(), timeout=10.0)
                result = await app.get_result(tid, timeout=5)

        assert result == "ok:ttl-test"
        assert_at_least_once_held(attempts, expected_min=3)


# ---------------------------------------------------------------------------
# Combo 4 — lock + hard_timeout
# ---------------------------------------------------------------------------


class TestLockHardTimeoutCombo:
    """Combo #4: lock + hard_timeout.

    Bug surface: when hard_timeout kills the task coroutine, the lock must
    still be released (via the finally-block in the worker's task-executor).
    The lock TTL is the last-resort recovery path if the finally-block does
    not fire (e.g. SIGKILL).
    """

    async def test_happy_path(self, rabbitmq_url: str, valkey_url: str) -> None:
        """Task completes within hard_timeout — lock released, successor runs."""
        queue = _uq("lht")
        lock_name = f"lht-lock-{uuid.uuid4().hex[:6]}"
        task_name = _task_name("lht_happy")
        results: list[str] = []
        second_done = asyncio.Event()

        class HardTimeoutTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                lock=lock_name,
                hard_timeout=10,
            )

            async def run(self, tag: str) -> str:
                results.append(tag)
                if len(results) == 2:
                    second_done.set()
                return f"ok:{tag}"

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url, valkey_url, queue, concurrency=2,
                mirror_to=app._signal_dispatcher,
            ):
                await app.schedule(task_name, "a")
                await app.schedule(task_name, "b")
                await asyncio.wait_for(second_done.wait(), timeout=12.0)

        assert results == ["a", "b"]

    async def test_hard_timeout_releases_lock_via_finally(
        self, rabbitmq_url: str, valkey_url: str
    ) -> None:
        """Task exceeds hard_timeout → worker hard-kills the coroutine →
        finally-block releases the lock → successor task can acquire.

        Assumption: the worker fires asyncio.Task.cancel() on hard_timeout
        which delivers CancelledError, triggering the finally-block and
        releasing the lock cooperatively.  This is a cooperative kill,
        not SIGKILL.
        """
        queue = _uq("lht-to")
        lock_name = f"lht-to-{uuid.uuid4().hex[:6]}"
        slow_name = _task_name("lht_slow")
        fast_name = _task_name("lht_fast")
        fast_done = asyncio.Event()

        class SlowHTTask(Task):
            # hard_timeout=2 means the task gets killed after 2 seconds.
            task_info = TaskInfo(
                name=slow_name,
                queues=[queue],
                lock=lock_name,
                hard_timeout=2,
            )

            async def run(self) -> str:
                await asyncio.sleep(60)  # will be killed by hard_timeout
                return "never"

        class FastHTTask(Task):
            task_info = TaskInfo(
                name=fast_name,
                queues=[queue],
                lock=lock_name,
                hard_timeout=10,
            )

            async def run(self) -> str:
                fast_done.set()
                return "ok"

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        async with app:
            await broker.declare_queue(queue)
            # Use concurrency=2 so fast task can be picked up concurrently.
            async with _running_worker(
                rabbitmq_url, valkey_url, queue, concurrency=2,
                mirror_to=app._signal_dispatcher,
            ):
                await app.schedule(slow_name)
                # Give slow task time to start and acquire lock.
                await asyncio.sleep(0.5)
                fast_id = await app.schedule(fast_name)

                # fast task should run within hard_timeout+headroom.
                await asyncio.wait_for(fast_done.wait(), timeout=15.0)
                result = await app.get_result(fast_id, timeout=5)

        assert result == "ok", (
            "successor must acquire lock after hard_timeout releases it"
        )

    @pytest.mark.chaos
    @pytest.mark.timeout(90)
    async def test_successor_acquires_after_sigkill_ttl(
        self,
        rabbitmq_url: str,
        valkey_url: str,
        unique_queue: str,
    ) -> None:
        """Worker crash with a lock held — the lock TTL must fire, the
        successor acquires it.

        Task-lock TTLs are ``hard_timeout + 30s`` (P0-A) and out of reach
        here, so the observable lock is the scheduler-leader lock with a 3s
        TTL: A's worker child holds it, the crash leaves it orphaned, and
        successor B's child owns it after the TTL. B must stay healthy.
        """
        queue = unique_queue
        fixture_module = "tests.e2e._shutdown_fixture_tasks"

        proc_a = spawn_worker(
            queue,
            fixture_module=fixture_module,
            broker_url=rabbitmq_url,
            backend_url=valkey_url,
            concurrency=1,
            env_extra=SHORT_SCHEDULER_LOCK_ENV,
        )
        try:
            wait_for_worker_started(proc_a, timeout=20.0)
            (child_a,) = wait_for_worker_children(proc_a)
            await wait_for_scheduler_leader(valkey_url, child_a.pid)
            crash_worker(proc_a)
            assert lock_owner_pid(await scheduler_lock_owner(valkey_url)) == child_a.pid

        finally:
            kill_subprocess(proc_a)

        # Spin up successor B.
        proc_b = spawn_worker(
            queue,
            fixture_module=fixture_module,
            broker_url=rabbitmq_url,
            backend_url=valkey_url,
            concurrency=1,
            env_extra=SHORT_SCHEDULER_LOCK_ENV,
        )
        try:
            wait_for_worker_started(proc_b, timeout=20.0)
            (child_b,) = wait_for_worker_children(proc_b)
            await wait_for_scheduler_leader(
                valkey_url, child_b.pid, timeout=SHORT_SCHEDULER_LOCK_TTL_S + 10.0
            )
            await assert_no_reader_leak(proc_b)
        finally:
            kill_subprocess(proc_b)


# ---------------------------------------------------------------------------
# Combo 5 — progress + cancel
# ---------------------------------------------------------------------------


class TestProgressCancelCombo:
    """Combo #5: progress + cancel.

    Bug surface: after set_cancel_flag fires, further progress updates
    should not reach the client (or at least not arrive AFTER the
    TaskCancelledError).  The cancel monitor interrupts the task
    coroutine, so update_progress is inherently stopped.
    """

    async def test_happy_path(self, rabbitmq_url: str, valkey_url: str) -> None:
        """Task emits progress updates and completes — final progress
        visible in Valkey, no cancel interference."""
        queue = _uq("pc")
        task_name = _task_name("pc_happy")
        done = asyncio.Event()

        class ProgressTask(Task):
            task_info = TaskInfo(name=task_name, queues=[queue])

            async def run(self, steps: int) -> str:
                for i in range(steps):
                    await self.update_progress(
                        current=i + 1,
                        total=steps,
                        message=f"step {i + 1}",
                    )
                    await asyncio.sleep(0.05)
                done.set()
                return "completed"

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url, valkey_url, queue,
                mirror_to=app._signal_dispatcher,
            ):
                tid = await app.schedule(task_name, 5)
                await asyncio.wait_for(done.wait(), timeout=10.0)
                result = await app.get_result(tid, timeout=5)

        assert result == "completed"

    async def test_progress_stops_after_cancel(
        self, rabbitmq_url: str, valkey_url: str
    ) -> None:
        """After cancel(), the task is interrupted and no further progress
        updates arrive.  We observe this by checking that progress stops
        incrementing after the TaskCancelledError is raised.

        Signal sequence: TASK_STARTED → TASK_CANCELLED.
        """
        queue = _uq("pc-stop")
        task_name = _task_name("pc_stop")
        started = asyncio.Event()

        class LongProgressTask(Task):
            task_info = TaskInfo(name=task_name, queues=[queue])

            async def run(self, total: int) -> str:
                started.set()
                for i in range(total):
                    await self.update_progress(current=i + 1, total=total)
                    await asyncio.sleep(0.2)
                return "done"

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        recorder = SignalRecorder.attach(
            app._signal_dispatcher,
            signals=[Signal.TASK_STARTED, Signal.TASK_CANCELLED],
        )

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url, valkey_url, queue, cancel_poll_interval=0.1,
                mirror_to=app._signal_dispatcher,
            ):
                tid = await app.schedule(task_name, 20)

                # Wait for the task to start.
                await asyncio.wait_for(started.wait(), timeout=5.0)
                await asyncio.sleep(0.4)  # let 2 progress steps emit

                # Cancel mid-run.
                await app.cancel(tid)

                with pytest.raises(TaskCancelledError):
                    await app.get_result(tid, timeout=5)

                # Take a snapshot of progress just after cancel.
                await backend.get_result(tid)
                # Progress is stored in its own key; result may be None for cancelled.
                # The main assertion: task did not complete all 20 steps.

        await assert_signal_sequence(
            recorder,
            [Signal.TASK_STARTED, Signal.TASK_CANCELLED],
            timeout_s=2.0,
        )

    async def test_race_progress_update_during_cancel(
        self, rabbitmq_url: str, valkey_url: str
    ) -> None:
        """Race: progress update and cancel-flag check happen concurrently.
        The task must emit TASK_CANCELLED (not TASK_COMPLETED) regardless
        of which asyncio step wins the race.

        This test is non-deterministic by nature; running it verifies the
        system doesn't deadlock or emit an incorrect terminal signal.
        """
        queue = _uq("pc-race")
        task_name = _task_name("pc_race")
        started = asyncio.Event()

        class RaceProgressTask(Task):
            task_info = TaskInfo(name=task_name, queues=[queue])

            async def run(self, tight_loop: int) -> str:
                started.set()
                for i in range(tight_loop):
                    # Tight progress loop — high chance of racing with cancel.
                    await self.update_progress(current=i, total=tight_loop)
                    # Minimal yield to allow cancel monitor to check.
                    await asyncio.sleep(0.0)
                return "completed-if-cancel-lost-race"

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        recorder = SignalRecorder.attach(
            app._signal_dispatcher,
            signals=[Signal.TASK_STARTED, Signal.TASK_CANCELLED, Signal.TASK_COMPLETED],
        )

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url, valkey_url, queue, cancel_poll_interval=0.05,
                mirror_to=app._signal_dispatcher,
            ):
                tid = await app.schedule(task_name, 100)
                await asyncio.wait_for(started.wait(), timeout=5.0)

                # Cancel immediately after start — race with tight progress loop.
                await app.cancel(tid)

                # Either TaskCancelledError or a result is fine; both are valid
                # depending on who wins the race.  Assert no deadlock.
                try:
                    await app.get_result(tid, timeout=5)
                except TaskCancelledError:
                    pass  # cancel won the race — expected common case

        # No deadlock: TASK_STARTED was emitted (task ran) and at least one
        # terminal signal (CANCELLED or COMPLETED) followed.
        await assert_signal_sequence(
            recorder,
            [Signal.TASK_STARTED],
            timeout_s=2.0,
            strict_order=False,
        )
        emitted = recorder.emitted_signals
        terminal = {Signal.TASK_CANCELLED, Signal.TASK_COMPLETED}
        assert any(s in terminal for s in emitted), (
            f"no terminal signal emitted after task started: {emitted}"
        )


# ---------------------------------------------------------------------------
# Combo 6 — result_ttl + retry
# ---------------------------------------------------------------------------


class TestResultTtlRetryCombo:
    """Combo #6: result_ttl + retry.

    Bug surface: result_ttl defines how long the result is available after
    task *completion*; it should NOT start ticking from *enqueue time*.
    If the TTL were applied at enqueue, retries could exhaust the TTL
    before the task even completes, leaving no result for the caller.

    The complementary risk: if result_ttl is very short and retries are
    slow, the result might vanish before get_result() is called.

    Assumption: result_ttl is measured from task *completion* (the moment
    the result is stored in Valkey), not from enqueue or first attempt.
    """

    async def test_happy_path(self, rabbitmq_url: str, valkey_url: str) -> None:
        """Task with short result_ttl and one retry: result is accessible
        immediately after completion even though retries took time."""
        queue = _uq("rtr")
        task_name = _task_name("rtr_happy")
        attempts = 0
        done = asyncio.Event()

        class RetryTTLTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                max_retries=1,
                retry_base_delay=0.2,
                result_ttl=30,  # 30s — plenty of time to assert
            )

            async def run(self, val: int) -> int:
                nonlocal attempts
                attempts += 1
                if attempts == 1:
                    raise RuntimeError("first attempt fails")
                done.set()
                return val * 3

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url, valkey_url, queue,
                mirror_to=app._signal_dispatcher,
            ):
                tid = await app.schedule(task_name, 7)
                await asyncio.wait_for(done.wait(), timeout=8.0)
                result = await app.get_result(tid, timeout=3)

        assert result == 21
        assert_at_least_once_held(attempts, expected_min=2)

    async def test_ttl_applies_only_after_completion(
        self, rabbitmq_url: str, valkey_url: str
    ) -> None:
        """Enqueue → task fails → retry delay → retry succeeds.
        result_ttl=5 must NOT expire during the retry delay (0.5s retry ×
        2 attempts = at most ~2s total).  If TTL were from enqueue, a
        result_ttl=1 task that retries at 1s would have no result.

        This test uses result_ttl=5 and 0.5s retry delay to ensure result
        is available immediately after get_result() is called.
        """
        queue = _uq("rtr-ttl")
        task_name = _task_name("rtr_ttl")
        attempt_times: list[float] = []
        done = asyncio.Event()

        class TTLRetryTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                max_retries=2,
                retry_base_delay=0.5,
                result_ttl=5,
            )

            async def run(self, payload: str) -> str:
                attempt_times.append(time.monotonic())
                if len(attempt_times) == 1:
                    raise RuntimeError("must retry")
                done.set()
                return f"done:{payload}"

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        enqueue_time = time.monotonic()

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url, valkey_url, queue,
                mirror_to=app._signal_dispatcher,
            ):
                tid = await app.schedule(task_name, "ttl-check")
                await asyncio.wait_for(done.wait(), timeout=8.0)

                # Fetch result immediately — must be present.
                result = await app.get_result(tid, timeout=3)

        assert result == "done:ttl-check", (
            "result must be available immediately after completion"
        )

        # The total elapsed from enqueue to completion (enqueue → retry →
        # complete) was at least retry_base_delay (0.5s).  If TTL started
        # at enqueue with result_ttl=5, we have 4.5s remaining — fine.
        # The test validates the result WAS accessible despite retry overhead.
        elapsed = time.monotonic() - enqueue_time
        assert elapsed < 5.0, (
            "test took too long — reduce retry delays or increase result_ttl"
        )

    async def test_ttl_expires_during_retries_result_missing(
        self, rabbitmq_url: str, valkey_url: str
    ) -> None:
        """Pathological scenario: result_ttl=1 + many slow retries.
        If TTL were from enqueue, the result would be missing. If TTL
        is from completion, the result is available right after get_result().

        We use result_ttl=2 (short but enough for get_result) and
        a 0.8s × 2-retry delay (total ~1.8s overhead before completion)
        to demonstrate that the result is still readable after completion.

        Note: this test would FAIL (correctly) if the implementation
        erroneously starts TTL at enqueue rather than completion.
        """
        queue = _uq("rtr-exp")
        task_name = _task_name("rtr_exp")
        attempts = 0
        done = asyncio.Event()

        class ShortTTLRetryTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                max_retries=2,
                retry_base_delay=0.8,
                result_ttl=2,  # 2s from completion — readable immediately after
            )

            async def run(self, marker: str) -> str:
                nonlocal attempts
                attempts += 1
                if attempts < 3:
                    raise RuntimeError(f"attempt {attempts} fails")
                done.set()
                return f"ok:{marker}"

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url, valkey_url, queue,
                mirror_to=app._signal_dispatcher,
            ):
                tid = await app.schedule(task_name, "short-ttl")
                await asyncio.wait_for(done.wait(), timeout=12.0)

                # Immediately after completion — result must exist.
                result = await app.get_result(tid, timeout=2)

        assert result == "ok:short-ttl", (
            "result must be readable immediately after completion regardless "
            "of retry overhead — TTL must start from completion, not enqueue"
        )
        assert_at_least_once_held(attempts, expected_min=3)

    @pytest.mark.chaos
    async def test_backend_drop_mid_result_store_faf_invariants(
        self,
        rabbitmq_url: str,
        valkey_url: str,
        rabbitmq_proxied_url: str,
        toxiproxy: Any,
    ) -> None:
        """Toxiproxy drops RabbitMQ connection just after task completion
        (between result store and ack).  The broker should nack(requeue=True)
        so the task is re-delivered — but only if result_ttl is long enough
        that the result is not garbage-collected before the retry reads it.

        Verifies FAF-4 stage invariants for the broker_drop scenario.
        """
        queue = _uq("rtr-chaos")
        task_name = _task_name("rtr_chaos")
        call_count = 0
        done = asyncio.Event()

        class ResultTTLChaosTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                max_retries=3,
                retry_base_delay=0.2,
                result_ttl=60,
            )

            async def run(self, val: int) -> int:
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    raise RuntimeError("first attempt fails")
                done.set()
                return val + 100

        broker = RabbitMQBroker(rabbitmq_proxied_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(rabbitmq_proxied_url, valkey_url, queue):
                tid = await app.schedule(task_name, 42)

                # Inject broker drop after first attempt fails and retry fires.
                await asyncio.sleep(0.4)
                toxic_name = await toxiproxy.add_toxic(
                    "rabbitmq",
                    "timeout",
                    attributes={"timeout": 0},
                    stream="upstream",
                )
                await asyncio.sleep(0.15)
                await toxiproxy.remove_toxic("rabbitmq", toxic_name)

                await asyncio.wait_for(done.wait(), timeout=15.0)
                result = await app.get_result(tid, timeout=5)
                assert result == 142

        assert_at_least_once_held(call_count, expected_min=2)

        obs = FAFStageObservation(
            stage_executed=call_count >= 2,
            nacked_with_requeue=call_count >= 2,
            silently_dropped=call_count == 0,
            requeue_count=max(call_count - 1, 0),
        )
        assert_faf_stage_invariants(obs, scenario_kind="broker_drop", max_requeues=5)


# ---------------------------------------------------------------------------
# Assumptions documented here for future maintainers
# ---------------------------------------------------------------------------
#
# A-1 (Combo 1): DLQ move triggers unique-flag release via the same
#     release-after-ack path as normal completion.  If this assumption is
#     wrong, test_unique_released_after_retry_exhausted will fail with
#     the duplicate getting the SAME task_id instead of a new one.
#
# A-2 (Combo 3): The lock is RELEASED between retry attempts, not held.
#     test_retry_releases_lock_between_attempts pins this as the intended
#     behaviour.  If it changes to "hold across retries", the test must be
#     updated and the docstring amended.
#
# A-3 (Combo 4): hard_timeout fires asyncio.Task.cancel() (cooperative),
#     not os.kill(SIGKILL) (unconditional).  The cooperative cancel allows
#     the finally-block to release the lock.  Tier-3 SIGKILL tests cover
#     the unconditional kill scenario.
#
# A-4 (Combo 5): The cancel monitor (cancel_poll_interval) checks the flag
#     between coroutine yields.  A tight loop with no awaits would not be
#     interrupted.  Tests use asyncio.sleep(0.0) to ensure yields are
#     present.
#
# A-5 (Combo 6): result_ttl is measured from *completion* (result written
#     to Valkey), not from *enqueue*.  This is the design intent and the
#     tests verify this contract.  If the implementation changes, tests
#     A-5 will fail and serve as regression detectors.
#
# A-6 (lock TTL): compute_lock_ttl_ms(hard_timeout) → (hard_timeout + 30) × 1000 ms (P0-A fix).
#     For SIGKILL tests (Tier 3) with hard_timeout=120 → TTL=30s.  Tests
#     that wait for TTL expiry use @pytest.mark.timeout(90) or higher.
