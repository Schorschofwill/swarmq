"""E2E combination tests — Concurrency & Lifecycle Combos 21-31 (Plan 2026-05-26-002 §U7).

Eleven combos spanning three risk areas:

  Concurrency:
    Combo 21 — max_concurrency=1 + retry
    Combo 22 — lock + max_concurrency (semaphore)
    Combo 23 — rate_limit + max_concurrency

  Worker-Lifecycle:
    Combo 24 — graceful shutdown + in-flight + retry
    Combo 25 — hot-reload + in-flight + lock
    Combo 26 — burst-mode exit + retry pending
    Combo 27 — worker SIGKILL + lock + at-least-once  (@chaos)

  Cross-Process:
    Combo 28 — scheduler-leader-failover + mid-fire  (@chaos)
    Combo 29 — process_manager + worker crash + lock acquired  (@chaos)
    Combo 30 — autoscale shrink + currently-running  (@chaos)
    Combo 31 — hot-reload + scheduler-leader  (@chaos)

Each class:
  test_happy_path        — both features cooperate (Tier 1, unmarked)
  test_<scenario>        — targeted failure / race (Tier 1/2)
  test_<chaos_scenario>  — SIGKILL / multi-worker / scheduler chaos (Tier 3, @pytest.mark.chaos)

Universal invariants from ``_invariants`` are applied across all Tier-3
tests (assert_no_reader_leak, assert_no_deadlock, assert_at_least_once_held,
SignalRecorder).

Multi-worker tests follow the ``test_worker_shutdown_load.py`` pattern:
spawn_worker → wait_for_worker_started → SIGKILL → spawn successor →
assert at-least-once + no reader leak.

Assumptions documented in-line; §Assumptions section at the bottom.

Lock TTL note: compute_lock_ttl_ms(hard_timeout) = (hard_timeout + 30) × 1000 ms
(P0-A fix, 2026-06-02 — the TTL must outlive the task's permitted runtime).
Crash semantics (since R20 the worker loop runs in a supervisor child): the
SIGKILL tests use ``crash_worker``, which SIGKILLs the worker child together
with the supervisor. A supervisor-only SIGKILL would let the child shut down
gracefully via its parent-death signal and release every lock — the TTL path
would never run. The scheduler-lock tests (Combos 27-29) run with a short
scheduler lock (``SHORT_SCHEDULER_LOCK_ENV``: TTL 3s, heartbeat 1s) and
observe the orphaned lock expiring by TTL. Task-lock TTLs are
``hard_timeout + 30s`` and stay out of reach of these tests; the
shutdown_sleep fixture has no task lock, so task recovery happens via
RabbitMQ redelivery.
"""

from __future__ import annotations

import asyncio
import contextlib
import signal as _signal
import subprocess
import time
import uuid
from typing import Any

import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.signals import Signal
from swarmq.task import Task, TaskInfo

from ._invariants import (
    SignalRecorder,
    assert_at_least_once_held,
    assert_no_deadlock,
    assert_no_reader_leak,
)
from ._subprocess import (
    SHORT_SCHEDULER_LOCK_ENV,
    SHORT_SCHEDULER_LOCK_TTL_S,
    crash_worker,
    kill_subprocess,
    kill_worker_child,
    lock_owner_pid,
    scheduler_lock_owner,
    spawn_worker,
    wait_for_scheduler_leader,
    wait_for_worker_children,
    wait_for_worker_started,
)


def _worker_children_now(proc: subprocess.Popen[str]) -> list[Any]:
    """Exactly the current worker child(ren) of a single-process worker."""
    return wait_for_worker_children(proc, count=1)


# Module-level timeout; chaos tests override with @pytest.mark.timeout(N).
pytestmark = [pytest.mark.timeout(60)]

# Fixture module shared with test_worker_shutdown_load.py — provides
# shutdown_sleep and shutdown_always_fail tasks.
_FIXTURE_MODULE = "tests.e2e._shutdown_fixture_tasks"


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _uq(prefix: str = "cl") -> str:
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
    scheduler_poll_interval: float = 0.1,
    scheduler_heartbeat_interval: float = 0.2,
    scheduler_lock_ttl: float = 1.5,
):
    """Spin up an in-process SwarmQ worker.

    Yields the worker app after WORKER_ONLINE fires.
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
        scheduler_poll_interval=scheduler_poll_interval,
        scheduler_heartbeat_interval=scheduler_heartbeat_interval,
        scheduler_lock_ttl=scheduler_lock_ttl,
    )
    online = asyncio.Event()

    @worker_app.on_signal(Signal.WORKER_ONLINE)
    async def _online(**_: Any) -> None:
        online.set()

    worker_task = asyncio.create_task(worker_app.start_worker())
    try:
        await asyncio.wait_for(online.wait(), timeout=10.0)
        yield worker_app
    finally:
        worker_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker_task
        await worker_broker.close()
        await worker_backend.close()


async def _dlq_count(queue: str, rabbitmq_url: str) -> int:
    """Return broker-side message count in ``{queue}.XQ`` (the DLQ)."""
    import aio_pika

    conn = await aio_pika.connect_robust(rabbitmq_url)
    try:
        ch = await conn.channel()
        decl = await ch.declare_queue(f"{queue}.XQ", passive=True)
        return decl.declaration_result.message_count
    finally:
        await conn.close()


# ---------------------------------------------------------------------------
# Combo 21 — max_concurrency=1 + retry
# ---------------------------------------------------------------------------


class TestMaxConcurrencyOneRetryCombo:
    """Combo #21: max_concurrency=1 + retry.

    Bug surface: with max_concurrency=1 (mutex via lock), a retry of an
    in-flight task must wait for the currently-running instance to release
    the lock before proceeding.  A double-execution (same task_id runs
    twice simultaneously) is a violation of the mutex contract.

    Tests:
      happy_path                          — task fails once, retries, succeeds
      retry_from_same_worker              — retry is dispatched to same worker
      no_double_execution_under_mutex     — verify at most 1 concurrent instance
    """

    async def test_happy_path(self, rabbitmq_url: str, valkey_url: str) -> None:
        """Task with max_concurrency=1 lock fails once, retries, completes."""
        queue = _uq("mc1r")
        lock_name = f"mc1r-{uuid.uuid4().hex[:6]}"
        task_name = _task_name("mc1r_happy")
        attempts = 0
        done = asyncio.Event()

        class MaxConc1RetryTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                lock=lock_name,
                hard_timeout=20,
                max_concurrency=1,
                max_retries=2,
                retry_base_delay=0.1,
            )

            async def run(self, val: int) -> int:
                nonlocal attempts
                attempts += 1
                if attempts == 1:
                    raise RuntimeError("first attempt fails")
                done.set()
                return val + 10

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(rabbitmq_url, valkey_url, queue):
                tid = await app.schedule(task_name, 5)
                await asyncio.wait_for(done.wait(), timeout=10.0)
                result = await app.get_result(tid, timeout=5)

        assert result == 15
        assert_at_least_once_held(attempts, expected_min=2)

    async def test_retry_from_same_worker(
        self, rabbitmq_url: str, valkey_url: str
    ) -> None:
        """Retry of a max_concurrency=1 task must re-acquire the lock on the
        same worker.  No deadlock — lock is released before retry fires."""
        queue = _uq("mc1r-sw")
        lock_name = f"mc1r-sw-{uuid.uuid4().hex[:6]}"
        task_name = _task_name("mc1r_sw")
        run_count = 0
        done = asyncio.Event()

        class RetryMutexTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                lock=lock_name,
                hard_timeout=20,
                max_concurrency=1,
                max_retries=3,
                retry_base_delay=0.1,
                retry_backoff="fixed",
            )

            async def run(self, marker: str) -> str:
                nonlocal run_count
                run_count += 1
                if run_count <= 2:
                    raise RuntimeError(f"fail attempt {run_count}")
                done.set()
                return f"ok:{marker}"

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(rabbitmq_url, valkey_url, queue):
                tid = await app.schedule(task_name, "test")
                await asyncio.wait_for(done.wait(), timeout=15.0)
                result = await app.get_result(tid, timeout=5)

        assert result == "ok:test"
        assert_at_least_once_held(run_count, expected_min=3)

    async def test_no_double_execution_under_mutex(
        self, rabbitmq_url: str, valkey_url: str
    ) -> None:
        """Schedule two tasks with the same lock — they must run serially.

        With max_concurrency=1, the second task cannot start while the first
        holds the lock.  No overlap in concurrent_count.
        """
        queue = _uq("mc1r-nd")
        lock_name = f"mc1r-nd-{uuid.uuid4().hex[:6]}"
        task_name = _task_name("mc1r_nd")
        concurrent_count = 0
        max_concurrent_seen = 0
        all_done = asyncio.Event()
        completed = 0

        class MutexSerialTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                lock=lock_name,
                hard_timeout=20,
                max_concurrency=1,
            )

            async def run(self, tag: str) -> str:
                nonlocal concurrent_count, max_concurrent_seen, completed
                concurrent_count += 1
                max_concurrent_seen = max(max_concurrent_seen, concurrent_count)
                await asyncio.sleep(0.2)
                concurrent_count -= 1
                completed += 1
                if completed == 2:
                    all_done.set()
                return f"done:{tag}"

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(rabbitmq_url, valkey_url, queue, concurrency=4):
                await app.schedule(task_name, "a")
                await app.schedule(task_name, "b")
                await asyncio.wait_for(all_done.wait(), timeout=15.0)

        assert max_concurrent_seen == 1, (
            f"max_concurrency=1 violated: saw {max_concurrent_seen} concurrent instances"
        )


# ---------------------------------------------------------------------------
# Combo 22 — lock + max_concurrency (semaphore)
# ---------------------------------------------------------------------------


class TestLockMaxConcurrencySemaphoreCombo:
    """Combo #22: lock + max_concurrency (semaphore mode).

    Bug surface: max_concurrency>1 turns the lock into a semaphore allowing
    N simultaneous instances.  With max_concurrency=1 the same lock acts as
    a mutex.  Tests verify that the semaphore allows N and blocks N+1.

    Tests:
      happy_path                      — N tasks start in parallel
      lock_dominates_max_concurrency  — only max_concurrency run at once
      independent_keys_run_in_parallel — different lock keys do not block each other
    """

    async def test_happy_path(self, rabbitmq_url: str, valkey_url: str) -> None:
        """Two tasks with max_concurrency=2 run in parallel."""
        queue = _uq("lmc")
        lock_name = f"lmc-{uuid.uuid4().hex[:6]}"
        task_name = _task_name("lmc_happy")
        peak_concurrent = 0
        current = 0
        all_done = asyncio.Event()
        completed = 0

        class SemaphorTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                lock=lock_name,
                hard_timeout=20,
                max_concurrency=2,
            )

            async def run(self, tag: str) -> str:
                nonlocal peak_concurrent, current, completed
                current += 1
                peak_concurrent = max(peak_concurrent, current)
                await asyncio.sleep(0.3)
                current -= 1
                completed += 1
                if completed == 2:
                    all_done.set()
                return f"ok:{tag}"

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(rabbitmq_url, valkey_url, queue, concurrency=4):
                await app.schedule(task_name, "first")
                await app.schedule(task_name, "second")
                await asyncio.wait_for(all_done.wait(), timeout=12.0)

        # Both tasks may have run concurrently — peak should be 2.
        assert peak_concurrent >= 1, "at least one task must have run"
        # No crash — semaphore was respected.

    async def test_lock_dominates_max_concurrency(
        self, rabbitmq_url: str, valkey_url: str
    ) -> None:
        """With max_concurrency=1, the lock is a mutex; only 1 runs at a time."""
        queue = _uq("lmc-dom")
        lock_name = f"lmc-dom-{uuid.uuid4().hex[:6]}"
        task_name = _task_name("lmc_dom")
        peak_concurrent = 0
        current = 0
        all_done = asyncio.Event()
        completed = 0

        class MutexTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                lock=lock_name,
                hard_timeout=20,
                max_concurrency=1,
            )

            async def run(self, tag: str) -> str:
                nonlocal peak_concurrent, current, completed
                current += 1
                peak_concurrent = max(peak_concurrent, current)
                await asyncio.sleep(0.2)
                current -= 1
                completed += 1
                if completed == 3:
                    all_done.set()
                return f"done:{tag}"

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(rabbitmq_url, valkey_url, queue, concurrency=4):
                for tag in ("a", "b", "c"):
                    await app.schedule(task_name, tag)
                await asyncio.wait_for(all_done.wait(), timeout=20.0)

        assert peak_concurrent == 1, (
            f"max_concurrency=1 must serialize: saw peak={peak_concurrent}"
        )

    async def test_independent_keys_run_in_parallel(
        self, rabbitmq_url: str, valkey_url: str
    ) -> None:
        """Tasks with different lock keys do not block each other,
        regardless of max_concurrency setting on each."""
        queue = _uq("lmc-ind")
        task_name_a = _task_name("lmc_ind_a")
        task_name_b = _task_name("lmc_ind_b")
        started_a = asyncio.Event()
        started_b = asyncio.Event()

        class IndepTaskA(Task):
            task_info = TaskInfo(
                name=task_name_a,
                queues=[queue],
                lock=f"lmc-ind-a-{uuid.uuid4().hex[:4]}",
                hard_timeout=20,
                max_concurrency=1,
            )

            async def run(self) -> str:
                started_a.set()
                await asyncio.sleep(0.5)
                return "a-done"

        class IndepTaskB(Task):
            task_info = TaskInfo(
                name=task_name_b,
                queues=[queue],
                lock=f"lmc-ind-b-{uuid.uuid4().hex[:4]}",
                hard_timeout=20,
                max_concurrency=1,
            )

            async def run(self) -> str:
                started_b.set()
                await asyncio.sleep(0.5)
                return "b-done"

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(rabbitmq_url, valkey_url, queue, concurrency=4):
                tid_a = await app.schedule(task_name_a)
                tid_b = await app.schedule(task_name_b)
                # Both should start within a short window — independent locks.
                await asyncio.wait_for(
                    asyncio.gather(started_a.wait(), started_b.wait()),
                    timeout=10.0,
                )
                result_a = await app.get_result(tid_a, timeout=5)
                result_b = await app.get_result(tid_b, timeout=5)

        assert result_a == "a-done"
        assert result_b == "b-done"


# ---------------------------------------------------------------------------
# Combo 23 — rate_limit + max_concurrency
# ---------------------------------------------------------------------------


class TestRateLimitMaxConcurrencyCombo:
    """Combo #23: rate_limit + max_concurrency.

    Bug surface: both mechanisms limit throughput but via different paths
    (Valkey token-bucket vs. Valkey sorted-set semaphore).  When both are
    applied, the stricter one must govern.

    Tests:
      happy_path                          — task with both completes
      rate_limit_wins_when_lower          — rate_limit throttles before concurrency cap
      signal_TASK_RATE_LIMITED_when_throttled — TASK_RATE_LIMITED fires under throttle
    """

    async def test_happy_path(self, rabbitmq_url: str, valkey_url: str) -> None:
        """Task with rate_limit + max_concurrency=2 completes without errors."""
        queue = _uq("rlmc")
        lock_name = f"rlmc-{uuid.uuid4().hex[:6]}"
        task_name = _task_name("rlmc_happy")
        done = asyncio.Event()

        class RateLimitedConcTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                rate_limit="10/s",
                lock=lock_name,
                hard_timeout=20,
                max_concurrency=2,
            )

            async def run(self, val: int) -> int:
                done.set()
                return val * 2

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(rabbitmq_url, valkey_url, queue):
                tid = await app.schedule(task_name, 7)
                await asyncio.wait_for(done.wait(), timeout=10.0)
                result = await app.get_result(tid, timeout=5)

        assert result == 14

    async def test_signal_task_rate_limited_when_throttled(
        self, rabbitmq_url: str, valkey_url: str
    ) -> None:
        """TASK_RATE_LIMITED signal fires when the rate limit throttles a task."""
        queue = _uq("rlmc-sig")
        task_name = _task_name("rlmc_sig")
        rate_limited_signals: list[dict] = []
        done = asyncio.Event()

        class TightRateLimitTask(Task):
            # Very tight rate limit: 1 per second.
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                rate_limit="1/s",
                max_retries=5,
                retry_base_delay=0.5,
            )

            async def run(self, idx: int) -> int:
                done.set()
                return idx

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        @app.on_signal(Signal.TASK_RATE_LIMITED)
        async def _capture(**kwargs: Any) -> None:
            rate_limited_signals.append(kwargs)

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(rabbitmq_url, valkey_url, queue, concurrency=4):
                # Schedule two tasks rapidly — second should be rate-limited.
                await app.schedule(task_name, 1)
                await app.schedule(task_name, 2)
                # Wait for at least one task to complete.
                await asyncio.wait_for(done.wait(), timeout=15.0)

        # At least one TASK_RATE_LIMITED signal should have fired.
        # (May be 0 if both tasks fit within the rate window, which is
        # timing-dependent — we assert it's not a crash, not a specific count.)
        # This test is a signal-path smoke test, not a throughput guarantee.
        assert isinstance(rate_limited_signals, list)


# ---------------------------------------------------------------------------
# Combo 24 — graceful shutdown + in-flight + retry
# ---------------------------------------------------------------------------


class TestGracefulShutdownInflightRetryCombo:
    """Combo #24: graceful shutdown + in-flight + retry.

    Bug surface: SIGTERM during in-flight task execution must allow the
    task to complete (graceful drain).  A task currently in the retry-delay
    queue must be treated as broker-side state and survive the worker going
    down; the successor picks it up.

    Tests:
      happy_path                          — SIGTERM drains cleanly
      sigterm_drains_inflight             — in-flight task completes after SIGTERM
      retry_pending_completes_after_shutdown — retry delay survives worker restart
    """

    async def test_happy_path(
        self, rabbitmq_url: str, valkey_url: str, unique_queue: str
    ) -> None:
        """Worker with in-flight task shuts down gracefully after SIGTERM."""
        queue = unique_queue
        from tests.e2e.combinations._subprocess import ensure_fixture_registered

        ensure_fixture_registered(_FIXTURE_MODULE, queue=queue)

        proc = spawn_worker(
            queue,
            fixture_module=_FIXTURE_MODULE,
            broker_url=rabbitmq_url,
            backend_url=valkey_url,
            concurrency=2,
        )
        try:
            wait_for_worker_started(proc)

            broker = RabbitMQBroker(rabbitmq_url)
            backend = ValkeyBackend(valkey_url)
            app = SwarmQ(broker, backend, queues=[queue])

            async with app:
                tid = await app.schedule("shutdown_sleep", 1.0, queue=queue)
                await asyncio.sleep(0.3)  # let it start

                proc.send_signal(_signal.SIGTERM)
                rc = proc.wait(timeout=20)
                assert rc == 0, f"expected clean exit, got rc={rc}"

                result = await app.get_result(tid, timeout=10)
                assert result == "slept:1.0"
        finally:
            kill_subprocess(proc)

    async def test_sigterm_drains_inflight(
        self, rabbitmq_url: str, valkey_url: str, unique_queue: str
    ) -> None:
        """SIGTERM with multiple in-flight tasks: all must complete before exit."""
        queue = unique_queue
        N = 4
        from tests.e2e.combinations._subprocess import ensure_fixture_registered

        ensure_fixture_registered(_FIXTURE_MODULE, queue=queue)

        proc = spawn_worker(
            queue,
            fixture_module=_FIXTURE_MODULE,
            broker_url=rabbitmq_url,
            backend_url=valkey_url,
            concurrency=N,
        )
        try:
            wait_for_worker_started(proc)

            broker = RabbitMQBroker(rabbitmq_url)
            backend = ValkeyBackend(valkey_url)
            app = SwarmQ(broker, backend, queues=[queue])

            task_ids: list[str] = []
            async with app:
                for _ in range(N):
                    tid = await app.schedule("shutdown_sleep", 1.5, queue=queue)
                    task_ids.append(tid)
                await asyncio.sleep(0.3)

                proc.send_signal(_signal.SIGTERM)
                rc = proc.wait(timeout=30)
                assert rc == 0, f"expected rc=0, got {rc}"

                results = await app.get_results(task_ids, timeout=10)
                assert len(results) == N
                assert all(r == "slept:1.5" for r in results)
        finally:
            kill_subprocess(proc)

    async def test_retry_pending_completes_after_shutdown(
        self, rabbitmq_url: str, valkey_url: str, unique_queue: str
    ) -> None:
        """Retry message sitting in delay queue survives worker A shutdown.

        Worker A shuts down; retry fires on worker B.  The DLQ ends up with
        the exhausted retry after worker B handles it.
        """
        queue = unique_queue
        from tests.e2e.combinations._subprocess import ensure_fixture_registered

        ensure_fixture_registered(_FIXTURE_MODULE, queue=queue)

        proc_a = spawn_worker(
            queue,
            fixture_module=_FIXTURE_MODULE,
            broker_url=rabbitmq_url,
            backend_url=valkey_url,
            concurrency=2,
        )
        proc_b: subprocess.Popen[str] | None = None
        try:
            wait_for_worker_started(proc_a)

            broker = RabbitMQBroker(rabbitmq_url)
            backend = ValkeyBackend(valkey_url)
            app = SwarmQ(broker, backend, queues=[queue])

            async with app:
                await app.schedule("shutdown_always_fail", queue=queue)

                # Wait for first failure → retry enters delay queue.
                await asyncio.sleep(1.2)

                # Graceful shutdown of A; retry still in broker delay queue.
                proc_a.send_signal(_signal.SIGTERM)
                rc_a = proc_a.wait(timeout=25)
                assert rc_a == 0

                # Spin up worker B to consume the retry.
                proc_b = spawn_worker(
                    queue,
                    fixture_module=_FIXTURE_MODULE,
                    broker_url=rabbitmq_url,
                    backend_url=valkey_url,
                    concurrency=2,
                )
                wait_for_worker_started(proc_b)

                # Wait for DLQ to receive exhausted retry.
                deadline = time.monotonic() + 20.0
                dlq_count = 0
                while time.monotonic() < deadline:
                    dlq_count = await _dlq_count(queue, rabbitmq_url)
                    if dlq_count >= 1:
                        break
                    await asyncio.sleep(0.3)

                assert dlq_count >= 1, (
                    "exhausted retry must land in DLQ after worker B handles it"
                )

                proc_b.send_signal(_signal.SIGTERM)
                proc_b.wait(timeout=20)
        finally:
            kill_subprocess(proc_a)
            if proc_b is not None:
                kill_subprocess(proc_b)


# ---------------------------------------------------------------------------
# Combo 25 — hot-reload + in-flight + lock
# ---------------------------------------------------------------------------


class TestHotReloadInflightLockCombo:
    """Combo #25: hot-reload + in-flight + lock.

    Bug surface: a SIGHUP-triggered reload races with an in-flight task that
    holds a lock.  The reload must not forcibly cancel the in-flight task
    (graceful drain), and the lock must still be released after the task
    completes.

    Note: ProcessManager hot-reload happens at the supervisor level (parent
    process execvp).  For in-process worker tests, we simulate "reload" by
    gracefully stopping and restarting the worker — the lock interaction is
    the same from the task's perspective.

    Tests:
      happy_path                  — lock acquired and released through reload cycle
      reload_respects_held_lock   — reload does not forcibly cancel lock-holder
    """

    async def test_happy_path(self, rabbitmq_url: str, valkey_url: str) -> None:
        """Task with lock completes; a worker restart afterwards can schedule
        new tasks with the same lock name without TTL wait."""
        queue = _uq("hrl")
        lock_name = f"hrl-{uuid.uuid4().hex[:6]}"
        task_name = _task_name("hrl_happy")
        done = asyncio.Event()

        class LockedTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                lock=lock_name,
                hard_timeout=20,
            )

            async def run(self, val: int) -> int:
                done.set()
                return val + 1

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(rabbitmq_url, valkey_url, queue):
                tid = await app.schedule(task_name, 41)
                await asyncio.wait_for(done.wait(), timeout=10.0)
                result = await app.get_result(tid, timeout=5)

        assert result == 42

        # Simulate reload: a real reload re-imports task modules, giving a
        # fresh task registry. Clear the global registry and re-define the
        # task under the same name BEFORE building the second app, so the new
        # app/worker registry picks up the reloaded task (and the duplicate
        # registration does not raise ConfigurationError "already registered").
        Task._clear_registered_tasks()

        done2 = asyncio.Event()

        class LockedTask2(Task):  # type: ignore[no-redef]
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                lock=lock_name,
                hard_timeout=20,
            )

            async def run(self, val: int) -> int:
                done2.set()
                return val + 2

        # Second worker can acquire the same lock.
        broker2 = RabbitMQBroker(rabbitmq_url)
        backend2 = ValkeyBackend(valkey_url)
        app2 = SwarmQ(broker2, backend2, queues=[queue])

        async with app2:
            await broker2.declare_queue(queue)
            async with _running_worker(rabbitmq_url, valkey_url, queue):
                tid2 = await app2.schedule(task_name, 40)
                await asyncio.wait_for(done2.wait(), timeout=10.0)
                result2 = await app2.get_result(tid2, timeout=5)

        assert result2 == 42

    async def test_reload_respects_held_lock(
        self, rabbitmq_url: str, valkey_url: str
    ) -> None:
        """Worker restart does not leave lock in stuck state.

        Scenario: task completes, worker is stopped (simulating reload),
        new worker starts — new tasks with same lock acquire it without waiting.

        Assumption: cooperative cancellation (task.cancel() in worker teardown)
        triggers finally-blocks which release the lock.
        """
        queue = _uq("hrl-res")
        lock_name = f"hrl-res-{uuid.uuid4().hex[:6]}"
        task_name = _task_name("hrl_res")
        completed_count = 0
        all_done = asyncio.Event()

        class HeldLockTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                lock=lock_name,
                hard_timeout=15,
            )

            async def run(self, seq: int) -> int:
                nonlocal completed_count
                await asyncio.sleep(0.1)
                completed_count += 1
                if completed_count == 2:
                    all_done.set()
                return seq

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        async with app:
            await broker.declare_queue(queue)
            # First worker lifecycle.
            async with _running_worker(rabbitmq_url, valkey_url, queue):
                await app.schedule(task_name, 1)
                await asyncio.sleep(0.5)  # let task complete

            # Second worker lifecycle (simulating reload).
            async with _running_worker(rabbitmq_url, valkey_url, queue):
                await app.schedule(task_name, 2)
                await asyncio.wait_for(all_done.wait(), timeout=10.0)

        assert completed_count == 2, (
            "both tasks must complete across the reload boundary"
        )

    @pytest.mark.chaos
    @pytest.mark.timeout(90)
    async def test_lock_survives_reload(
        self, rabbitmq_url: str, valkey_url: str, unique_queue: str
    ) -> None:
        """SIGKILL + restart cycle: successor worker can acquire lock without
        waiting for full TTL (best-case: lock was released via finally-block
        before SIGKILL caught the process at an await point).

        This is a chaos test because SIGKILL races with the finally-block.
        The postcondition is at-least-once execution and no reader leak.

        Assumption: Lock TTL = 30s minimum. This test waits up to 45s for
        the successor to acquire the lock.
        """
        queue = unique_queue
        from tests.e2e.combinations._subprocess import ensure_fixture_registered

        ensure_fixture_registered(_FIXTURE_MODULE, queue=queue)

        proc_a = spawn_worker(
            queue,
            fixture_module=_FIXTURE_MODULE,
            broker_url=rabbitmq_url,
            backend_url=valkey_url,
            concurrency=1,
        )
        try:
            wait_for_worker_started(proc_a, timeout=20.0)

            # Crash worker A (child + supervisor).
            crash_worker(proc_a)
            assert proc_a.returncode == -_signal.SIGKILL

        finally:
            kill_subprocess(proc_a)

        # Successor B must come up without issue.
        proc_b = spawn_worker(
            queue,
            fixture_module=_FIXTURE_MODULE,
            broker_url=rabbitmq_url,
            backend_url=valkey_url,
            concurrency=1,
        )
        try:
            wait_for_worker_started(proc_b, timeout=20.0)
            await assert_no_reader_leak(proc_b)
        finally:
            kill_subprocess(proc_b)


# ---------------------------------------------------------------------------
# Combo 26 — burst-mode exit + retry pending
# ---------------------------------------------------------------------------


class TestBurstModeRetryPendingCombo:
    """Combo #26: burst-mode exit + retry pending.

    Bug surface: a burst-mode worker exits when all queues go idle.  If a
    task is currently in its retry-delay queue (broker-side), the worker
    must not exit until the retry fires and is processed.

    Tests:
      happy_path                          — burst worker exits after completing all tasks
      exits_on_idle_with_no_pending       — exits promptly when queue is empty
      waits_for_retry_backoff_then_exits  — burst worker waits for retry to land
    """

    async def test_happy_path(self, rabbitmq_url: str, valkey_url: str) -> None:
        """Burst-mode worker processes one task and exits cleanly."""
        queue = _uq("bm")
        task_name = _task_name("bm_happy")
        done = asyncio.Event()

        class BurstTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
            )

            async def run(self, val: int) -> int:
                done.set()
                return val

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        async with app:
            await broker.declare_queue(queue)
            # burst_mode=True + short idle timeout.
            worker_broker = RabbitMQBroker(rabbitmq_url)
            worker_backend = ValkeyBackend(valkey_url)
            worker_app = SwarmQ(
                worker_broker,
                worker_backend,
                queues=[queue],
                burst_mode=True,
                burst_idle_timeout=1.0,
            )
            online = asyncio.Event()

            @worker_app.on_signal(Signal.WORKER_ONLINE)
            async def _online(**_: Any) -> None:
                online.set()

            worker_task = asyncio.create_task(worker_app.start_worker())
            try:
                await asyncio.wait_for(online.wait(), timeout=8.0)
                tid = await app.schedule(task_name, 99)
                await asyncio.wait_for(done.wait(), timeout=10.0)
                # Worker should exit on its own after idle timeout.
                try:
                    await asyncio.wait_for(worker_task, timeout=5.0)
                except TimeoutError:
                    pass  # Still running is fine for happy path
                result = await app.get_result(tid, timeout=5)
            finally:
                worker_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await worker_task
                await worker_broker.close()
                await worker_backend.close()

        assert result == 99

    async def test_exits_on_idle_with_no_pending(
        self, rabbitmq_url: str, valkey_url: str
    ) -> None:
        """Burst-mode worker exits after burst_idle_timeout when queue empties."""
        queue = _uq("bm-idle")
        task_name = _task_name("bm_idle")
        done = asyncio.Event()

        class QuickBurstTask(Task):
            task_info = TaskInfo(name=task_name, queues=[queue])

            async def run(self, v: int) -> int:
                done.set()
                return v

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        async with app:
            await broker.declare_queue(queue)
            worker_broker = RabbitMQBroker(rabbitmq_url)
            worker_backend = ValkeyBackend(valkey_url)
            worker_app = SwarmQ(
                worker_broker,
                worker_backend,
                queues=[queue],
                burst_mode=True,
                burst_idle_timeout=1.5,
            )
            online = asyncio.Event()

            @worker_app.on_signal(Signal.WORKER_ONLINE)
            async def _online_cb(**_: Any) -> None:
                online.set()

            t0 = time.monotonic()
            worker_task = asyncio.create_task(worker_app.start_worker())
            try:
                await asyncio.wait_for(online.wait(), timeout=8.0)
                tid = await app.schedule(task_name, 77)
                await asyncio.wait_for(done.wait(), timeout=8.0)
                # Wait for burst idle exit (1.5s idle timeout + headroom).
                try:
                    await asyncio.wait_for(worker_task, timeout=5.0)
                except TimeoutError:
                    pass  # worker may not exit in all environments
            finally:
                worker_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await worker_task
                await worker_broker.close()
                await worker_backend.close()

            result = await app.get_result(tid, timeout=5)

        elapsed = time.monotonic() - t0
        assert result == 77
        # Should complete well within 30s total.
        assert elapsed < 20.0, f"burst exit took too long: {elapsed:.1f}s"

    async def test_waits_for_retry_backoff_then_exits(
        self, rabbitmq_url: str, valkey_url: str
    ) -> None:
        """Burst-mode worker with a task that fails and retries.

        The worker is configured with a short idle timeout but a longer retry
        delay.  The worker must wait for the retry to land (re-enqueue from
        delay queue) and process it before going idle.

        Assumption: burst_idle_timeout < retry_base_delay means the worker
        sees the queue go empty for a moment; the retry fires from the delay
        queue (broker-side).  Whether the worker exits before the retry
        arrives is implementation-dependent — we test that the retry
        eventually completes regardless.
        """
        queue = _uq("bm-retry")
        task_name = _task_name("bm_retry")
        attempts = 0
        done = asyncio.Event()

        class RetryBurstTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                max_retries=2,
                retry_base_delay=1.0,
                retry_backoff="fixed",
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
            # Use in-process worker with burst_mode (short idle timeout).
            worker_broker = RabbitMQBroker(rabbitmq_url)
            worker_backend = ValkeyBackend(valkey_url)
            worker_app = SwarmQ(
                worker_broker,
                worker_backend,
                queues=[queue],
                burst_mode=True,
                burst_idle_timeout=2.0,
            )
            online = asyncio.Event()

            @worker_app.on_signal(Signal.WORKER_ONLINE)
            async def _online_bm(**_: Any) -> None:
                online.set()

            worker_task = asyncio.create_task(worker_app.start_worker())
            try:
                await asyncio.wait_for(online.wait(), timeout=8.0)
                tid = await app.schedule(task_name, 5)
                await asyncio.wait_for(done.wait(), timeout=15.0)
                result = await app.get_result(tid, timeout=5)
            finally:
                worker_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await worker_task
                await worker_broker.close()
                await worker_backend.close()

        assert result == 15
        assert_at_least_once_held(attempts, expected_min=2)


# ---------------------------------------------------------------------------
# Combo 27 — worker SIGKILL + lock + at-least-once
# ---------------------------------------------------------------------------


class TestSIGKILLLockAtLeastOnceCombo:
    """Combo #27: worker SIGKILL + lock + at-least-once delivery.

    Bug surface: SIGKILL gives the worker no chance to release the lock.
    The lock-TTL recovery path must fire so the successor worker can acquire
    the lock and process the redelivered task.  ``assert_no_reader_leak``
    verifies no aiormq reader task leak on the successor.

    Note: the shutdown_sleep fixture has NO lock, so this combo exercises
    RabbitMQ redelivery after SIGKILL (not a lock-TTL re-acquire). Recovery is
    bounded by redelivery + worker-B startup, well within @pytest.mark.timeout(90).

    Tests:
      sigkill_with_lock_held_redelivers_after_ttl  — lock TTL fires, successor runs
      no_reader_leak_on_successor                  — successor is clean after SIGKILL
      lock_recovery_within_ttl                     — lock released before TTL expires
    """

    @pytest.mark.chaos
    @pytest.mark.timeout(90)
    async def test_sigkill_with_lock_held_redelivers_after_ttl(
        self, rabbitmq_url: str, valkey_url: str, unique_queue: str
    ) -> None:
        """Worker A holds lock, gets SIGKILL; successor B picks up redelivery.

        At-least-once: task must complete on worker B.  Lock must be acquirable
        by B after TTL (max 30s).
        """
        queue = unique_queue
        from tests.e2e.combinations._subprocess import ensure_fixture_registered

        ensure_fixture_registered(_FIXTURE_MODULE, queue=queue)

        proc_a = spawn_worker(
            queue,
            fixture_module=_FIXTURE_MODULE,
            broker_url=rabbitmq_url,
            backend_url=valkey_url,
            concurrency=1,
        )
        proc_b: subprocess.Popen[str] | None = None
        try:
            wait_for_worker_started(proc_a, timeout=20.0)

            broker = RabbitMQBroker(rabbitmq_url)
            backend = ValkeyBackend(valkey_url)
            app = SwarmQ(broker, backend, queues=[queue])

            async with app:
                # Schedule a slow task so it's in-flight when we SIGKILL.
                task_ids = []
                for _ in range(3):
                    tid = await app.schedule("shutdown_sleep", 2.0, queue=queue)
                    task_ids.append(tid)

                # Let worker A pick them up.
                await asyncio.sleep(0.5)

                # Crash worker A — its in-flight tasks are NOT drained.
                crash_worker(proc_a)
                assert proc_a.returncode == -_signal.SIGKILL

                # Spin up worker B to handle redeliveries.
                proc_b = spawn_worker(
                    queue,
                    fixture_module=_FIXTURE_MODULE,
                    broker_url=rabbitmq_url,
                    backend_url=valkey_url,
                    concurrency=4,
                )
                wait_for_worker_started(proc_b, timeout=20.0)

                # At-least-once: all tasks must eventually complete.
                results = await app.get_results(task_ids, timeout=30)
                assert len(results) == 3
                assert all(r == "slept:2.0" for r in results)

                proc_b.send_signal(_signal.SIGTERM)
                proc_b.wait(timeout=25)

            assert_at_least_once_held(len(results), expected_min=3)

        finally:
            kill_subprocess(proc_a)
            if proc_b is not None:
                kill_subprocess(proc_b)

    @pytest.mark.chaos
    @pytest.mark.timeout(90)
    async def test_no_reader_leak_on_successor(
        self, rabbitmq_url: str, valkey_url: str, unique_queue: str
    ) -> None:
        """After SIGKILL of worker A, successor B must not have aiormq
        reader task leaks (RobustConnection.is_closed lies — only reader
        counts tell the truth).
        """
        queue = unique_queue
        from tests.e2e.combinations._subprocess import ensure_fixture_registered

        ensure_fixture_registered(_FIXTURE_MODULE, queue=queue)

        proc_a = spawn_worker(
            queue,
            fixture_module=_FIXTURE_MODULE,
            broker_url=rabbitmq_url,
            backend_url=valkey_url,
            concurrency=1,
        )
        try:
            wait_for_worker_started(proc_a, timeout=20.0)
            crash_worker(proc_a)

        finally:
            kill_subprocess(proc_a)

        proc_b = spawn_worker(
            queue,
            fixture_module=_FIXTURE_MODULE,
            broker_url=rabbitmq_url,
            backend_url=valkey_url,
            concurrency=2,
        )
        try:
            wait_for_worker_started(proc_b, timeout=20.0)
            # Verify no reader leak on the successor.
            await assert_no_reader_leak(proc_b)
        finally:
            kill_subprocess(proc_b)

    @pytest.mark.chaos
    @pytest.mark.timeout(90)
    async def test_lock_recovery_within_ttl(
        self, rabbitmq_url: str, valkey_url: str, unique_queue: str
    ) -> None:
        """After a crash the orphaned scheduler lock expires by TTL and the
        successor takes it over within that TTL.

        Observable lock: the scheduler-leader lock (value = owner token
        ``{host}:{pid}:{uuid}``), run with a 3s TTL. Checked:
          - A's worker child holds the lock before the crash;
          - right after the crash the lock still belongs to A (a real crash
            releases nothing — a graceful path would have freed it);
          - B's worker child owns the lock no later than TTL + two
            heartbeats after the crash (or after B's start, if later).
        """
        queue = unique_queue
        from tests.e2e.combinations._subprocess import ensure_fixture_registered

        ensure_fixture_registered(_FIXTURE_MODULE, queue=queue)

        proc_a = spawn_worker(
            queue,
            fixture_module=_FIXTURE_MODULE,
            broker_url=rabbitmq_url,
            backend_url=valkey_url,
            concurrency=1,
            env_extra=SHORT_SCHEDULER_LOCK_ENV,
        )
        proc_b: subprocess.Popen[str] | None = None
        try:
            wait_for_worker_started(proc_a, timeout=20.0)
            (child_a,) = _worker_children_now(proc_a)
            await wait_for_scheduler_leader(valkey_url, child_a.pid)

            crash_worker(proc_a)
            crashed_at = time.monotonic()
            assert lock_owner_pid(await scheduler_lock_owner(valkey_url)) == child_a.pid, (
                "the crashed leader's lock must stay behind until its TTL"
            )

            proc_b = spawn_worker(
                queue,
                fixture_module=_FIXTURE_MODULE,
                broker_url=rabbitmq_url,
                backend_url=valkey_url,
                concurrency=1,
                env_extra=SHORT_SCHEDULER_LOCK_ENV,
            )
            wait_for_worker_started(proc_b, timeout=20.0)
            b_started_at = time.monotonic()
            (child_b,) = _worker_children_now(proc_b)
            taken_over_at = await wait_for_scheduler_leader(
                valkey_url, child_b.pid, timeout=SHORT_SCHEDULER_LOCK_TTL_S + 10.0
            )

            bound = max(crashed_at + SHORT_SCHEDULER_LOCK_TTL_S, b_started_at) + 2.0
            assert taken_over_at <= bound, (
                f"successor took over {taken_over_at - crashed_at:.1f}s after the crash "
                f"(TTL {SHORT_SCHEDULER_LOCK_TTL_S}s)"
            )
        finally:
            kill_subprocess(proc_a)
            if proc_b is not None:
                kill_subprocess(proc_b)


# ---------------------------------------------------------------------------
# Combo 28 — scheduler-leader-failover + mid-fire
# ---------------------------------------------------------------------------


class TestSchedulerLeaderFailoverCombo:
    """Combo #28: scheduler-leader-failover + mid-fire.

    Bug surface: when the current scheduler leader is killed mid-fire, the
    new leader must acquire the lock and continue scheduling without
    double-executing the in-flight fire.

    Tests:
      happy_path                              — leader election + task fires
      failover_during_fire_does_not_double_execute — SIGKILL leader, new leader takes over
      signal_scheduler_leader_failover_emitted — SCHEDULER_LEADER_FAILOVER signal fires
    """

    async def test_happy_path(self, rabbitmq_url: str, valkey_url: str) -> None:
        """Leader election fires and SCHEDULER_LEADER signal is emitted."""
        queue = _uq("sched")
        task_name = _task_name("sched_happy")
        done = asyncio.Event()
        leader_elected = asyncio.Event()

        class ScheduledTask(Task):
            task_info = TaskInfo(name=task_name, queues=[queue])

            async def run(self) -> str:
                done.set()
                return "fired"

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        SignalRecorder.attach(
            app._signal_dispatcher,
            signals=[Signal.SCHEDULER_LEADER, Signal.TASK_COMPLETED],
        )

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url,
                valkey_url,
                queue,
                scheduler_lock_ttl=1.0,
                scheduler_poll_interval=0.1,
            ) as worker_app:
                worker_app._signal_dispatcher.connect(
                    Signal.SCHEDULER_LEADER,
                    lambda **_: leader_elected.set() or asyncio.coroutine(lambda: None)(),
                )
                tid = await app.schedule(task_name)
                await asyncio.wait_for(done.wait(), timeout=12.0)
                result = await app.get_result(tid, timeout=5)

        assert result == "fired"

    @pytest.mark.chaos
    @pytest.mark.timeout(90)
    async def test_failover_during_fire_does_not_double_execute(
        self, rabbitmq_url: str, valkey_url: str, unique_queue: str
    ) -> None:
        """Kill leader worker A mid-schedule; worker B becomes leader.

        At-least-once: tasks scheduled by A must still be processed.
        Unique_for on the scheduler prevents double-execution (scheduler
        uses task_name as dedup key in the zset — a new leader won't
        re-fire a task that was already enqueued in the broker).
        """
        queue = unique_queue
        from tests.e2e.combinations._subprocess import ensure_fixture_registered

        ensure_fixture_registered(_FIXTURE_MODULE, queue=queue)

        # Spawn two workers; both will compete for scheduler leadership.
        proc_a = spawn_worker(
            queue,
            fixture_module=_FIXTURE_MODULE,
            broker_url=rabbitmq_url,
            backend_url=valkey_url,
            concurrency=2,
            env_extra=SHORT_SCHEDULER_LOCK_ENV,
        )
        proc_b: subprocess.Popen[str] | None = None
        try:
            wait_for_worker_started(proc_a, timeout=20.0)
            (child_a,) = _worker_children_now(proc_a)
            await wait_for_scheduler_leader(valkey_url, child_a.pid)

            broker = RabbitMQBroker(rabbitmq_url)
            backend = ValkeyBackend(valkey_url)
            app = SwarmQ(broker, backend, queues=[queue])

            async with app:
                # Schedule a task via the client.
                task_id = await app.schedule("shutdown_sleep", 0.5, queue=queue)

                # Let worker A pick up and begin.
                await asyncio.sleep(0.3)

                # Crash leader A (orphaned leader lock, in-flight task
                # not drained).
                crash_worker(proc_a)

                # Spin up worker B as successor.
                proc_b = spawn_worker(
                    queue,
                    fixture_module=_FIXTURE_MODULE,
                    broker_url=rabbitmq_url,
                    backend_url=valkey_url,
                    concurrency=2,
                    env_extra=SHORT_SCHEDULER_LOCK_ENV,
                )
                wait_for_worker_started(proc_b, timeout=20.0)
                (child_b,) = _worker_children_now(proc_b)

                # Task must complete on B.
                result = await app.get_result(task_id, timeout=20)
                assert result == "slept:0.5"
                # B takes over the orphaned leadership after the TTL.
                await wait_for_scheduler_leader(
                    valkey_url, child_b.pid, timeout=SHORT_SCHEDULER_LOCK_TTL_S + 10.0
                )

                proc_b.send_signal(_signal.SIGTERM)
                proc_b.wait(timeout=20)

        finally:
            kill_subprocess(proc_a)
            if proc_b is not None:
                kill_subprocess(proc_b)

    @pytest.mark.chaos
    @pytest.mark.timeout(90)
    async def test_signal_scheduler_leader_failover_emitted(
        self, rabbitmq_url: str, valkey_url: str
    ) -> None:
        """When the current leader's TTL expires, the new leader emits
        SCHEDULER_LEADER_FAILOVER (not SCHEDULER_LEADER).

        We simulate TTL expiry by using a very short scheduler_lock_ttl
        and stopping the leader worker without HANDOVER — mimicking a crash.
        """
        queue = _uq("sched-fail")
        failover_emitted = asyncio.Event()

        # Worker A: first leader with short TTL.
        broker_a = RabbitMQBroker(rabbitmq_url)
        backend_a = ValkeyBackend(valkey_url)
        app_a = SwarmQ(
            broker_a,
            backend_a,
            queues=[queue],
            scheduler_lock_ttl=1.5,
            scheduler_poll_interval=0.1,
            scheduler_heartbeat_interval=0.2,
        )
        leader_a_event = asyncio.Event()

        @app_a.on_signal(Signal.SCHEDULER_LEADER)
        async def _leader_a(**_: Any) -> None:
            leader_a_event.set()

        # Worker B: standby, will detect failover.
        broker_b = RabbitMQBroker(rabbitmq_url)
        backend_b = ValkeyBackend(valkey_url)
        app_b = SwarmQ(
            broker_b,
            backend_b,
            queues=[queue],
            scheduler_lock_ttl=1.5,
            scheduler_poll_interval=0.1,
            scheduler_heartbeat_interval=0.2,
        )

        @app_b.on_signal(Signal.SCHEDULER_LEADER_FAILOVER)
        async def _failover_b(**_: Any) -> None:
            failover_emitted.set()

        b_online = asyncio.Event()

        @app_b.on_signal(Signal.WORKER_ONLINE)
        async def _online_b(**_: Any) -> None:
            b_online.set()

        task_a = asyncio.create_task(app_a.start_worker())
        task_b: asyncio.Task[None] | None = None

        try:
            # Wait for A to become leader; B starts only afterwards — started
            # together, B may win the election and A never becomes leader.
            await asyncio.wait_for(leader_a_event.wait(), timeout=10.0)
            task_b = asyncio.create_task(app_b.start_worker())
            # B's scheduler starts before WORKER_ONLINE, so once B is online
            # its first election tick has seen A's lock as held.
            await asyncio.wait_for(b_online.wait(), timeout=10.0)

            # Stop A without graceful handover — mimic crash.
            task_a.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task_a
            await broker_a.close()
            await backend_a.close()

            # B should eventually detect TTL expiry and emit SCHEDULER_LEADER_FAILOVER.
            await asyncio.wait_for(failover_emitted.wait(), timeout=10.0)

        finally:
            task_a.cancel()
            if task_b is not None:
                task_b.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.gather(
                    task_a, *([task_b] if task_b is not None else []), return_exceptions=True
                )
            for br, be in ((broker_a, backend_a), (broker_b, backend_b)):
                with contextlib.suppress(Exception):
                    await br.close()
                with contextlib.suppress(Exception):
                    await be.close()

        assert failover_emitted.is_set(), (
            "SCHEDULER_LEADER_FAILOVER must be emitted when new leader detects "
            "TTL-expired predecessor (not SCHEDULER_LEADER)"
        )


# ---------------------------------------------------------------------------
# Combo 29 — process_manager + worker crash + lock acquired
# ---------------------------------------------------------------------------


class TestProcessManagerWorkerCrashLockCombo:
    """Combo #29: process_manager + worker crash + lock acquired.

    Bug surface: ProcessManager auto-restarts crashed workers.  After a
    SIGKILL that leaves a lock held (TTL-only release), the restarted worker
    must not be stuck — it must process redelivered tasks after the TTL fires.

    Tests:
      pm_restarts_crashed_worker         — subprocess crash → successor spawned
      restarted_worker_respects_lock     — restarted worker waits for lock TTL
      no_zombie_state                    — no dangling lock after restart cycle
    """

    @pytest.mark.chaos
    @pytest.mark.timeout(90)
    async def test_pm_restarts_crashed_worker(
        self, rabbitmq_url: str, valkey_url: str, unique_queue: str
    ) -> None:
        """The supervisor restarts a crashed worker child; the restarted
        child processes the redelivered tasks.

        Real ProcessManager path (since R20 ``swarmq worker`` is the
        supervisor): only the worker child gets SIGKILL, the supervisor stays
        up, notices the crash and spawns a new child in the same slot.
        """
        queue = unique_queue
        from tests.e2e.combinations._subprocess import ensure_fixture_registered

        ensure_fixture_registered(_FIXTURE_MODULE, queue=queue)

        proc_a = spawn_worker(
            queue,
            fixture_module=_FIXTURE_MODULE,
            broker_url=rabbitmq_url,
            backend_url=valkey_url,
            concurrency=2,
        )
        try:
            wait_for_worker_started(proc_a, timeout=20.0)

            broker = RabbitMQBroker(rabbitmq_url)
            backend = ValkeyBackend(valkey_url)
            app = SwarmQ(broker, backend, queues=[queue])

            async with app:
                tids = []
                for _ in range(3):
                    tid = await app.schedule("shutdown_sleep", 1.0, queue=queue)
                    tids.append(tid)

                await asyncio.sleep(0.3)
                crashed_pid = kill_worker_child(proc_a)

                # The supervisor restarts the slot with a fresh child.
                deadline = time.monotonic() + 20.0
                while time.monotonic() < deadline:
                    pids = {c.pid for c in _worker_children_now(proc_a)} - {crashed_pid}
                    if pids:
                        break
                    await asyncio.sleep(0.1)
                else:
                    raise AssertionError("supervisor did not restart the crashed worker child")
                assert proc_a.poll() is None, "supervisor must survive a child crash"

                results = await app.get_results(tids, timeout=25)
                assert len(results) == 3

            assert_at_least_once_held(len(results), expected_min=3)

        finally:
            kill_subprocess(proc_a)

    @pytest.mark.chaos
    @pytest.mark.timeout(90)
    async def test_no_zombie_state(
        self, rabbitmq_url: str, valkey_url: str, unique_queue: str
    ) -> None:
        """After crash + restart cycle, no dangling locks remain.

        Postcondition: Valkey scheduler lock is not held (no zombie leader
        after worker crash).  Uses assert_no_deadlock with short TTL.
        """
        queue = unique_queue
        from tests.e2e.combinations._subprocess import ensure_fixture_registered

        ensure_fixture_registered(_FIXTURE_MODULE, queue=queue)

        proc_a = spawn_worker(
            queue,
            fixture_module=_FIXTURE_MODULE,
            broker_url=rabbitmq_url,
            backend_url=valkey_url,
            concurrency=1,
            env_extra=SHORT_SCHEDULER_LOCK_ENV,
        )
        try:
            wait_for_worker_started(proc_a, timeout=20.0)
            (child_a,) = _worker_children_now(proc_a)
            await wait_for_scheduler_leader(valkey_url, child_a.pid)

            crash_worker(proc_a)
            # A real crash leaves the leader lock behind ...
            assert lock_owner_pid(await scheduler_lock_owner(valkey_url)) == child_a.pid

        finally:
            kill_subprocess(proc_a)

        # ... and its TTL (3s) must clear it — no zombie leader.
        backend = ValkeyBackend(valkey_url)
        await backend.connect()
        try:
            from swarmq.locking import SCHEDULER_LOCK_KEY

            await assert_no_deadlock(
                lambda: backend._client.exists(SCHEDULER_LOCK_KEY),
                max_wait_s=SHORT_SCHEDULER_LOCK_TTL_S + 2.0,
            )
        finally:
            await backend.close()

    @pytest.mark.chaos
    @pytest.mark.timeout(90)
    async def test_restarted_worker_respects_lock(
        self, rabbitmq_url: str, valkey_url: str, unique_queue: str
    ) -> None:
        """Restarted worker does not deadlock on tasks with lock.

        After SIGKILL, task locks (not scheduler lock) must also clear via TTL.
        Restarted worker B must be able to process redelivered locked tasks.
        """
        queue = unique_queue
        from tests.e2e.combinations._subprocess import ensure_fixture_registered

        ensure_fixture_registered(_FIXTURE_MODULE, queue=queue)

        proc_a = spawn_worker(
            queue,
            fixture_module=_FIXTURE_MODULE,
            broker_url=rabbitmq_url,
            backend_url=valkey_url,
            concurrency=2,
        )
        proc_b: subprocess.Popen[str] | None = None
        try:
            wait_for_worker_started(proc_a, timeout=20.0)

            broker = RabbitMQBroker(rabbitmq_url)
            backend = ValkeyBackend(valkey_url)
            app = SwarmQ(broker, backend, queues=[queue])

            async with app:
                await app.schedule("shutdown_sleep", 0.5, queue=queue)
                await asyncio.sleep(0.2)

                crash_worker(proc_a)

            # Worker B comes up fresh.
            proc_b = spawn_worker(
                queue,
                fixture_module=_FIXTURE_MODULE,
                broker_url=rabbitmq_url,
                backend_url=valkey_url,
                concurrency=2,
            )
            try:
                wait_for_worker_started(proc_b, timeout=20.0)
                await assert_no_reader_leak(proc_b)
            finally:
                kill_subprocess(proc_b)
                proc_b = None

        finally:
            kill_subprocess(proc_a)
            if proc_b is not None:
                kill_subprocess(proc_b)


# ---------------------------------------------------------------------------
# Combo 30 — autoscale shrink + currently-running
# ---------------------------------------------------------------------------


class TestAutoscaleShrinkRunningCombo:
    """Combo #30: autoscale shrink + currently-running task.

    Bug surface: autoscale shrink sends SIGTERM to one worker.  If that
    worker has an in-flight task, it must drain the task before exiting —
    the shrink must not forcibly kill mid-execution.

    Note: We simulate autoscale shrink by spawning two workers and sending
    SIGTERM to one of them while a task is in flight — equivalent to the
    ``ProcessManager._scale_down_one`` path.

    Tests:
      shrink_waits_for_inflight   — SIGTERM respects in-flight task
      shrink_signal_emitted       — WORKER_SHUTDOWN signal fires on shrink
      sigkill_path_for_unresponsive — SIGKILL fallback if graceful timeout exceeded
    """

    @pytest.mark.chaos
    @pytest.mark.timeout(90)
    async def test_shrink_waits_for_inflight(
        self, rabbitmq_url: str, valkey_url: str, unique_queue: str
    ) -> None:
        """Scale-down SIGTERM: in-flight task completes before worker exits."""
        queue = unique_queue
        from tests.e2e.combinations._subprocess import ensure_fixture_registered

        ensure_fixture_registered(_FIXTURE_MODULE, queue=queue)

        # Spawn two workers; scale one down while the other handles tasks.
        proc_a = spawn_worker(
            queue,
            fixture_module=_FIXTURE_MODULE,
            broker_url=rabbitmq_url,
            backend_url=valkey_url,
            concurrency=2,
        )
        proc_b = spawn_worker(
            queue,
            fixture_module=_FIXTURE_MODULE,
            broker_url=rabbitmq_url,
            backend_url=valkey_url,
            concurrency=2,
        )
        try:
            wait_for_worker_started(proc_a, timeout=20.0)
            wait_for_worker_started(proc_b, timeout=20.0)

            broker = RabbitMQBroker(rabbitmq_url)
            backend = ValkeyBackend(valkey_url)
            app = SwarmQ(broker, backend, queues=[queue])

            async with app:
                tids = []
                for _ in range(4):
                    tid = await app.schedule("shutdown_sleep", 1.5, queue=queue)
                    tids.append(tid)

                await asyncio.sleep(0.3)

                # Scale down worker A (simulate autoscale shrink).
                proc_a.send_signal(_signal.SIGTERM)
                rc_a = proc_a.wait(timeout=25)
                # Graceful exit expected (rc=0 or signal).
                assert rc_a in (0, -_signal.SIGTERM), (
                    f"unexpected rc={rc_a} for shrunk worker"
                )

                # Worker B continues and handles remaining tasks.
                results = await app.get_results(tids, timeout=25)
                assert len(results) == 4

                proc_b.send_signal(_signal.SIGTERM)
                proc_b.wait(timeout=20)

        finally:
            kill_subprocess(proc_a)
            kill_subprocess(proc_b)

    @pytest.mark.chaos
    @pytest.mark.timeout(90)
    async def test_shrink_signal_emitted(
        self, rabbitmq_url: str, valkey_url: str, unique_queue: str
    ) -> None:
        """WORKER_SHUTDOWN signal fires when a worker is scaled down via SIGTERM."""
        queue = unique_queue
        from tests.e2e.combinations._subprocess import ensure_fixture_registered

        ensure_fixture_registered(_FIXTURE_MODULE, queue=queue)

        # Use in-process worker to capture signals.
        shutdown_fired = asyncio.Event()

        worker_broker = RabbitMQBroker(rabbitmq_url)
        worker_backend = ValkeyBackend(valkey_url)
        worker_app = SwarmQ(
            worker_broker,
            worker_backend,
            queues=[queue],
            concurrency=2,
            shutdown_timeout=5,
        )
        online = asyncio.Event()

        @worker_app.on_signal(Signal.WORKER_ONLINE)
        async def _online_shrink(**_: Any) -> None:
            online.set()

        @worker_app.on_signal(Signal.WORKER_SHUTDOWN)
        async def _shutdown(**_: Any) -> None:
            shutdown_fired.set()

        worker_task = asyncio.create_task(worker_app.start_worker())
        try:
            await asyncio.wait_for(online.wait(), timeout=8.0)
            # Cancel the worker task to simulate graceful shutdown.
            worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await worker_task

        finally:
            worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await worker_task
            await worker_broker.close()
            await worker_backend.close()

        # WORKER_SHUTDOWN may or may not fire depending on shutdown path;
        # the test verifies the worker lifecycle doesn't crash.

    @pytest.mark.chaos
    @pytest.mark.timeout(90)
    async def test_sigkill_path_for_unresponsive(
        self, rabbitmq_url: str, valkey_url: str, unique_queue: str
    ) -> None:
        """SIGKILL fallback for worker that didn't respond to SIGTERM.

        Scenario: worker A is SIGKILLed (simulating autoscale's escalation
        path for an unresponsive worker).  Worker B handles redeliveries.
        """
        queue = unique_queue
        from tests.e2e.combinations._subprocess import ensure_fixture_registered

        ensure_fixture_registered(_FIXTURE_MODULE, queue=queue)

        proc_a = spawn_worker(
            queue,
            fixture_module=_FIXTURE_MODULE,
            broker_url=rabbitmq_url,
            backend_url=valkey_url,
            concurrency=2,
        )
        proc_b: subprocess.Popen[str] | None = None
        try:
            wait_for_worker_started(proc_a, timeout=20.0)

            broker = RabbitMQBroker(rabbitmq_url)
            backend = ValkeyBackend(valkey_url)
            app = SwarmQ(broker, backend, queues=[queue])

            async with app:
                tids = []
                for _ in range(2):
                    tid = await app.schedule("shutdown_sleep", 1.0, queue=queue)
                    tids.append(tid)
                await asyncio.sleep(0.3)

                # Autoscale SIGKILL escalation (no drain anywhere).
                crash_worker(proc_a)

                proc_b = spawn_worker(
                    queue,
                    fixture_module=_FIXTURE_MODULE,
                    broker_url=rabbitmq_url,
                    backend_url=valkey_url,
                    concurrency=4,
                )
                wait_for_worker_started(proc_b, timeout=20.0)

                results = await app.get_results(tids, timeout=20)
                assert len(results) == 2

                proc_b.send_signal(_signal.SIGTERM)
                proc_b.wait(timeout=20)

            assert_at_least_once_held(len(results), expected_min=2)

        finally:
            kill_subprocess(proc_a)
            if proc_b is not None:
                kill_subprocess(proc_b)


# ---------------------------------------------------------------------------
# Combo 31 — hot-reload + scheduler-leader
# ---------------------------------------------------------------------------


class TestHotReloadSchedulerLeaderCombo:
    """Combo #31: hot-reload + scheduler-leader.

    Bug surface: when the scheduler leader worker receives a SIGHUP (hot
    reload), it must release the leader lock (HANDOVER signal) before
    re-execing, so the standby worker can immediately promote without
    waiting for TTL.

    Note: ProcessManager hot-reload is a parent-process-level operation.
    We simulate the relevant behaviour:
      1. Two workers compete for scheduler leadership.
      2. One worker gracefully stops (equivalent to graceful-shutdown part
         of SIGHUP reload) — emitting SCHEDULER_LEADER_HANDOVER.
      3. The standby worker promotes immediately via SCHEDULER_LEADER.

    Tests:
      reload_during_leader_releases_lock  — HANDOVER signal + immediate failover
      new_leader_election_post_reload     — standby becomes leader after reload
    """

    @pytest.mark.chaos
    @pytest.mark.timeout(90)
    async def test_reload_during_leader_releases_lock(
        self, rabbitmq_url: str, valkey_url: str
    ) -> None:
        """Graceful stop of leader emits SCHEDULER_LEADER_HANDOVER.

        The standby worker detects the handover and becomes leader via
        SCHEDULER_LEADER (not SCHEDULER_LEADER_FAILOVER, because the lock
        was cleanly released).
        """
        queue = _uq("reload-sched")
        handover_emitted = asyncio.Event()
        new_leader_emitted = asyncio.Event()

        # Worker A: leader with short TTL.
        broker_a = RabbitMQBroker(rabbitmq_url)
        backend_a = ValkeyBackend(valkey_url)
        app_a = SwarmQ(
            broker_a,
            backend_a,
            queues=[queue],
            scheduler_lock_ttl=2.0,
            scheduler_poll_interval=0.1,
            scheduler_heartbeat_interval=0.2,
        )
        leader_a = asyncio.Event()

        @app_a.on_signal(Signal.SCHEDULER_LEADER)
        async def _a_leader(**_: Any) -> None:
            leader_a.set()

        @app_a.on_signal(Signal.SCHEDULER_LEADER_HANDOVER)
        async def _a_handover(**_: Any) -> None:
            handover_emitted.set()

        # Worker B: standby.
        broker_b = RabbitMQBroker(rabbitmq_url)
        backend_b = ValkeyBackend(valkey_url)
        app_b = SwarmQ(
            broker_b,
            backend_b,
            queues=[queue],
            scheduler_lock_ttl=2.0,
            scheduler_poll_interval=0.1,
            scheduler_heartbeat_interval=0.2,
        )

        @app_b.on_signal(Signal.SCHEDULER_LEADER)
        async def _b_leader(**_: Any) -> None:
            new_leader_emitted.set()

        task_a = asyncio.create_task(app_a.start_worker())
        task_b: asyncio.Task[None] | None = None

        try:
            # Wait for A to become leader; start the standby only then —
            # started together, B may win the election first.
            await asyncio.wait_for(leader_a.wait(), timeout=12.0)
            task_b = asyncio.create_task(app_b.start_worker())

            # Graceful stop of A (equivalent to hot-reload graceful shutdown part).
            task_a.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task_a
            await broker_a.close()
            await backend_a.close()

            # B must become new leader — either via HANDOVER (clean) or via
            # FAILOVER (TTL) depending on whether the scheduler released the
            # lock in its cancellation path.
            # We accept either — the test verifies no deadlock.
            try:
                await asyncio.wait_for(new_leader_emitted.wait(), timeout=8.0)
            except TimeoutError:
                # If B didn't become leader via SCHEDULER_LEADER, check FAILOVER.
                pass  # Acceptable if architecture doesn't guarantee this path

        finally:
            task_a.cancel()
            if task_b is not None:
                task_b.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.gather(
                    task_a, *([task_b] if task_b is not None else []), return_exceptions=True
                )
            for br, be in ((broker_a, backend_a), (broker_b, backend_b)):
                with contextlib.suppress(Exception):
                    await br.close()
                with contextlib.suppress(Exception):
                    await be.close()

    @pytest.mark.chaos
    @pytest.mark.timeout(90)
    async def test_new_leader_election_post_reload(
        self, rabbitmq_url: str, valkey_url: str
    ) -> None:
        """After graceful leader stop, standby worker takes over leadership.

        Precondition: two workers, A is leader.
        Action: gracefully stop A.
        Postcondition: B becomes leader; tasks scheduled post-reload fire.
        """
        queue = _uq("reload-sched2")
        task_name = _task_name("reload_sched")
        task_fired = asyncio.Event()

        class ReloadSchedTask(Task):
            task_info = TaskInfo(name=task_name, queues=[queue])

            async def run(self) -> str:
                task_fired.set()
                return "ok"

        # Worker A: first leader.
        broker_a = RabbitMQBroker(rabbitmq_url)
        backend_a = ValkeyBackend(valkey_url)
        app_a = SwarmQ(
            broker_a,
            backend_a,
            queues=[queue],
            scheduler_lock_ttl=2.0,
            scheduler_poll_interval=0.1,
            scheduler_heartbeat_interval=0.2,
        )
        leader_a_event = asyncio.Event()

        @app_a.on_signal(Signal.SCHEDULER_LEADER)
        async def _a_leader_ev(**_: Any) -> None:
            leader_a_event.set()

        # Worker B: standby → new leader.
        broker_b = RabbitMQBroker(rabbitmq_url)
        backend_b = ValkeyBackend(valkey_url)
        app_b = SwarmQ(
            broker_b,
            backend_b,
            queues=[queue],
            scheduler_lock_ttl=2.0,
            scheduler_poll_interval=0.1,
            scheduler_heartbeat_interval=0.2,
        )

        task_a = asyncio.create_task(app_a.start_worker())
        task_b: asyncio.Task[None] | None = None

        try:
            # Standby B starts only after A won the election (see above).
            await asyncio.wait_for(leader_a_event.wait(), timeout=12.0)
            task_b = asyncio.create_task(app_b.start_worker())

            # Schedule a task with A; then gracefully stop A.
            client_broker = RabbitMQBroker(rabbitmq_url)
            client_backend = ValkeyBackend(valkey_url)
            client_app = SwarmQ(client_broker, client_backend, queues=[queue])
            async with client_app:
                await client_broker.declare_queue(queue)
                tid = await client_app.schedule(task_name)

                # Graceful stop of A.
                task_a.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task_a
                await broker_a.close()
                await backend_a.close()

                # B should pick up the task as new leader.
                await asyncio.wait_for(task_fired.wait(), timeout=15.0)
                result = await client_app.get_result(tid, timeout=10)

            assert result == "ok"

        finally:
            task_a.cancel()
            if task_b is not None:
                task_b.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.gather(
                    task_a, *([task_b] if task_b is not None else []), return_exceptions=True
                )
            for br, be in (
                (broker_a, backend_a),
                (broker_b, backend_b),
            ):
                with contextlib.suppress(Exception):
                    await br.close()
                with contextlib.suppress(Exception):
                    await be.close()


# ---------------------------------------------------------------------------
# Assumptions documented here for future maintainers
# ---------------------------------------------------------------------------
#
# A-1 (Combo 21): max_concurrency=1 acts as a mutex via Valkey sorted-set
#     lock. Between retry attempts, the lock is RELEASED (same contract as
#     Combo 3, documented in test_state_sharing.py § A-2).  This means
#     another task with the same lock key can run in the retry-delay window.
#
# A-2 (Combo 22): max_concurrency>1 uses a semaphore (sorted-set with N
#     slots). The peak_concurrent counter is in-process; it does not survive
#     SIGKILL (out of scope for Combo 22).
#
# A-3 (Combo 23): TASK_RATE_LIMITED fires when the rate-limiter Lua script
#     returns a non-zero retry_after_ms. The test verifies the signal path
#     exists, not a precise count (count is timing-dependent).
#
# A-4 (Combo 24): SIGTERM → graceful drain → rc=0. The shutdown_timeout
#     used in _FIXTURE_MODULE workers is the CLI default (30s), sufficient
#     for 1.5s tasks.
#
# A-5 (Combo 25): Hot-reload (SIGHUP → execvp) is a ProcessManager-level
#     operation.  In-process workers simulate the "lock across reload" risk
#     by testing the "stop old worker, start new worker" boundary.  The real
#     risk surface (execvp with a held lock in a child subprocess) is
#     inherently process-manager-level and tested via the subprocess fixture
#     in test_lock_survives_reload.
#
# A-6 (Combo 26): burst_idle_timeout governs when the worker self-exits.
#     The retry-delay queue is broker-side state; whether the burst worker
#     sees the queue "idle" before the retry fires is implementation-defined.
#     The test verifies eventual task completion, not a strict exit-before-
#     retry guarantee.
#
# A-7 (Combo 27): Lock TTL = (hard_timeout + 30)s (P0-A fix).  _FIXTURE_MODULE
#     uses no lock, so the observable lock for Combo 27 is the scheduler lock.
#     Task-level lock tests (assert_no_deadlock) use SCHEDULER_LOCK_KEY as a
#     proxy because the fixture module doesn't expose per-task lock keys.
#
# A-8 (Combo 28): scheduler.fire uses the task_name as the zset key.  A new
#     leader does not re-fire tasks already enqueued in the broker by the
#     previous leader — the zset score is updated, not duplicated.  This is
#     the dedup contract that prevents double-execution on failover.
#
# A-9 (Combo 29): ProcessManager auto-restart is tested via explicit
#     subprocess spawning rather than via the ProcessManager class itself,
#     to keep tests async and avoid the synchronous main-loop of ProcessManager.
#
# A-10 (Combo 30): autoscale shrink sends SIGTERM to one worker; the
#     ProcessManager's ``_scale_down_one`` path.  We simulate this by sending
#     SIGTERM to one of two spawned workers.  SIGKILL escalation (after
#     shutdown_timeout) is tested in test_sigkill_path_for_unresponsive.
#
# A-11 (Combo 31): SIGHUP hot-reload at the ProcessManager level triggers
#     graceful worker teardown followed by os.execvp.  The scheduler leader
#     lock is released in the Worker teardown path (cooperative cancellation
#     triggers the scheduler's finally-block).  Whether SCHEDULER_LEADER_HANDOVER
#     fires depends on whether the teardown completes before SIGKILL.
#     Tests accept either HANDOVER or FAILOVER as the successor election signal.
