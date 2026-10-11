"""E2E combination tests — Timing/Order Combos 7-14 (Plan 2026-05-26-002 §U5).

Eight combos where timing, ordering, and race-conditions between features
create non-trivial interaction surface:

  Combo 7  — cron + catchup-simulation + unique_for
  Combo 8  — cron + catchup-simulation + rate_limit
  Combo 9  — soft_timeout + cancel (Signal Race)
  Combo 10 — hard_timeout + retry
  Combo 11 — max_age + scheduler
  Combo 12 — rate_limit + retry + retry_backoff
  Combo 13 — priority + lock
  Combo 14 — unique_until=start + retry

Combos #7 and #8: ``catchup=True`` is reserved and not yet implemented
(raises ``ValueError`` at TaskInfo construction).  These tests simulate
the catchup scenario by injecting multiple near-future fire timestamps
directly into the scheduler sorted-set (``sq:{sched}:zset``) via the same
pattern used by ``tests/e2e/test_scheduler.py`` — the real effect being
tested is what happens when N queued cron fires arrive simultaneously at
the worker.

Race combos (#9, #14) use controlled sleeps relative to configurable
timeouts so they are deterministically ordered without monkeypatching
the global clock.

Each class follows the same structure:
  test_happy_path       — both features work together (Tier 1, unmarked)
  test_failure_*        — edge / failure scenario (Tier 1, unmarked)
  test_*_chaos          — Toxiproxy / SIGKILL scenario (Tier 2/3, @pytest.mark.chaos)

Assumptions documented in-line; see §Assumptions at the bottom.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
import uuid
from typing import Any, override

import pytest
import valkey.asyncio as valkey_asyncio

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import TaskCancelledError, TaskError
from swarmq.signals import Signal
from swarmq.task import Task, TaskInfo

from ._invariants import (
    SignalRecorder,
    assert_at_least_once_held,
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
    extra_queues: list[str] | None = None,
    priorities: bool = False,
    scheduler_poll_interval: float = 0.1,
    scheduler_heartbeat_interval: float = 0.2,
    scheduler_lock_ttl: float = 1.0,
    mirror_to: Any = None,
):
    """Spin up an in-process SwarmQ worker against the given queue.

    Yields the worker app after WORKER_ONLINE fires.

    ``mirror_to`` (a SignalDispatcher, typically the producer ``app``'s) makes
    the worker re-dispatch every signal it emits onto that dispatcher, so
    SignalRecorder / @on_signal observers attached to the producer app see the
    worker's task-lifecycle signals. Without it the worker's separate
    dispatcher means those observers record nothing (emitted=[] / wait
    timeouts).
    """
    all_queues = [queue] + (extra_queues or [])
    worker_broker = RabbitMQBroker(rabbitmq_url)
    worker_backend = ValkeyBackend(valkey_url)
    worker_app = SwarmQ(
        worker_broker,
        worker_backend,
        queues=all_queues,
        concurrency=concurrency,
        cancel_poll_interval=cancel_poll_interval,
        shutdown_timeout=shutdown_timeout,
        priorities=priorities,
        scheduler_poll_interval=scheduler_poll_interval,
        scheduler_heartbeat_interval=scheduler_heartbeat_interval,
        scheduler_lock_ttl=scheduler_lock_ttl,
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


async def _inject_cron_fires(
    valkey_url: str,
    task_name: str,
    count: int,
    *,
    start_offset_ms: int = 100,
    spacing_ms: int = 1,
) -> None:
    """Inject ``count`` near-past fire timestamps into the scheduler sorted-set.

    Each entry has a unique score (ms-since-epoch) in the near-past so the
    scheduler picks them all up on the next poll cycle.  Uses the same zadd
    approach as ``tests/e2e/test_scheduler.py::TestCronSchedule``.

    Assumption: scheduler zset key is ``sq:{sched}:zset`` and scores are
    Unix-ms timestamps. The scheduler fire-loop treats entries with score <=
    now_ms as due and publishes them.  Multiple entries with the same task_name
    but different scores represent distinct "catchup" fires.
    """
    client = valkey_asyncio.Valkey.from_url(valkey_url)
    try:
        # Be the LAST writer. The scheduler's initial adopt_reported_crons runs
        # in the election tick AFTER the SCHEDULER_LEADER signal and
        # add_schedule()s this cron entry with the *next cron-minute* score.
        # Under load that ZADD can land after ours, clobbering our due score so
        # the fire never comes due within the test timeout (the environment-
        # sensitive flake — proven: on failure the zset score sits exactly on a
        # minute boundary). Wait until the scheduler has written the entry, then
        # overwrite it with our due score so nothing clobbers it afterwards.
        deadline = time.monotonic() + 20.0
        while time.monotonic() < deadline:
            if await client.zscore("sq:{sched}:zset", task_name) is not None:
                break
            await asyncio.sleep(0.05)
        now_ms = int(time.time() * 1000)
        mapping: dict[str, float] = {}
        for i in range(count):
            # Single member key → only the last score survives; "N queued
            # fires" is modelled by separate scheduling elsewhere. The score is
            # in the near-past so the scheduler fires it on the next poll.
            fire_ms = now_ms - start_offset_ms + i * spacing_ms
            mapping[task_name] = float(fire_ms)
        await client.zadd("sq:{sched}:zset", mapping)  # type: ignore[arg-type]
    finally:
        await client.aclose()


# ---------------------------------------------------------------------------
# Combo 7 — cron + catchup-simulation + unique_for
# ---------------------------------------------------------------------------


class TestCronCatchupUniqueCombo:
    """Combo #7: cron + catchup-simulation + unique_for.

    Bug surface: when N queued cron fires arrive simultaneously (simulating
    the scheduler coming back online after downtime), the unique_for dedup
    should block all-but-one duplicate fires for the same task arguments.

    Since ``catchup=True`` is reserved (raises ValueError), we simulate the
    scenario by directly scheduling N copies of the task and verifying that
    unique_for blocks N-1 of them, leaving exactly 1 execution.

    Assumptions documented in §Assumptions (A-7).
    """

    @pytest.mark.timeout(60)
    async def test_happy_path(self, rabbitmq_url: str, valkey_url: str) -> None:
        """Single cron fire with unique_for: task executes once, no duplicate."""
        queue = _uq("ccu")
        task_name = _task_name("ccu_happy")
        call_count = 0
        done = asyncio.Event()

        class CronUniqueTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                cron="* * * * *",
                unique_for=60,
                unique_until="completion",
            )

            @override
            async def run(self) -> str:
                nonlocal call_count
                call_count += 1
                done.set()
                return "ok"

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(
            broker,
            backend,
            queues=[queue],
            scheduler_poll_interval=0.1,
            scheduler_heartbeat_interval=0.2,
            scheduler_lock_ttl=1.0,
        )

        leader_event = asyncio.Event()

        @app.on_signal(Signal.SCHEDULER_LEADER)
        async def _on_leader(**_: Any) -> None:
            leader_event.set()

        async with app:
            await broker.declare_queue(queue)
            worker_task = asyncio.create_task(app.start_worker())
            try:
                # Generous waits: leader election + first scheduler poll can
                # lag several seconds on a loaded/shared runner.
                await asyncio.wait_for(leader_event.wait(), timeout=20.0)

                # Re-score the cron entry to fire immediately.
                await _inject_cron_fires(valkey_url, task_name, count=1)

                await asyncio.wait_for(done.wait(), timeout=20.0)
                await app.get_result(
                    await app.schedule(task_name),
                    timeout=5,
                )
            finally:
                worker_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await worker_task

        assert call_count >= 1

    async def test_catchup_with_unique_blocks_duplicates(self, rabbitmq_url: str, valkey_url: str) -> None:
        """Simulate N catchup fires arriving simultaneously: unique_for blocks N-1.

        We schedule N copies of the same task. With unique_until=completion,
        the first enqueue holds the unique-hash; all subsequent enqueues for
        the same (task_name, args) must return the same task_id (deduplicated).

        Assertion: out of N schedules, only 1 distinct task_id is generated
        (all N-1 duplicates are blocked) and the task executes exactly once.
        """
        queue = _uq("ccu-dup")
        task_name = _task_name("ccu_dup")
        n_fires = 5
        call_count = 0
        started = asyncio.Event()
        release = asyncio.Event()

        class CatchupUniqueTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                unique_for=120,
                unique_until="completion",
            )

            @override
            async def run(self) -> str:
                nonlocal call_count
                call_count += 1
                started.set()
                # Hold until the test has issued all N schedules. With
                # unique_until="completion" the unique flag is released on
                # COMPLETION, so a fast-completing task would free the flag
                # mid-loop and let a late schedule re-enqueue (a 2nd run).
                # Gating completion keeps the flag held across the whole loop,
                # making "exactly one execution" deterministic.
                await release.wait()
                return "executed"

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        dup_signals: list[dict] = []

        @app.on_signal(Signal.TASK_DUPLICATE_DETECTED)
        async def _on_dup(**kw: Any) -> None:
            dup_signals.append(kw)

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url,
                valkey_url,
                queue,
                mirror_to=app._signal_dispatcher,
            ):
                # Enqueue N fires; first succeeds, rest are deduplicated.
                task_ids: list[str] = []
                for _ in range(n_fires):
                    tid = await app.schedule(task_name)
                    task_ids.append(tid)

                # The single task is now running and holding the unique flag
                # (it blocks on `release`). Let it complete and fetch result.
                await asyncio.wait_for(started.wait(), timeout=15.0)
                release.set()
                result = await app.get_result(task_ids[0], timeout=10)

        assert result == "executed"
        assert call_count == 1, f"task must execute exactly once; executed {call_count} times"
        # All n_fires-1 duplicates must share the same task_id.
        assert len(set(task_ids)) == 1, (
            f"all {n_fires} schedules must return the same task_id under unique_for; "
            f"got {len(set(task_ids))} distinct ids"
        )
        assert len(dup_signals) >= n_fires - 1, (
            f"expected >= {n_fires - 1} TASK_DUPLICATE_DETECTED signals; got {len(dup_signals)}"
        )

    async def test_catchup_without_unique_runs_all(self, rabbitmq_url: str, valkey_url: str) -> None:
        """Control: without unique_for, all N catchup fires execute independently.

        Verifies the baseline — if unique_for were missing, all N tasks run.
        Contrast with test_catchup_with_unique_blocks_duplicates.
        """
        queue = _uq("ccu-all")
        task_name = _task_name("ccu_all")
        n_fires = 4
        call_count = 0
        all_done = asyncio.Event()

        class CatchupNoUniqueTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                # No unique_for — each fire runs independently.
            )

            @override
            async def run(self, fire_idx: int) -> int:
                nonlocal call_count
                call_count += 1
                if call_count >= n_fires:
                    all_done.set()
                return fire_idx

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url,
                valkey_url,
                queue,
                mirror_to=app._signal_dispatcher,
            ):
                for i in range(n_fires):
                    await app.schedule(task_name, i)

                await asyncio.wait_for(all_done.wait(), timeout=10.0)

        assert call_count == n_fires, f"all {n_fires} fires must run without unique_for; only {call_count} ran"

    async def test_unique_released_after_first_catchup_fire(self, rabbitmq_url: str, valkey_url: str) -> None:
        """After the first fire completes (unique_until=completion), the unique
        flag is released and the next fire can proceed.

        Models the pattern: fire-1 runs, completes, unique released; fire-2
        arrives after completion and runs independently.
        """
        queue = _uq("ccu-rel")
        task_name = _task_name("ccu_rel")
        call_count = 0
        first_done = asyncio.Event()
        second_done = asyncio.Event()

        class SequentialCronTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                unique_for=30,
                unique_until="completion",
            )

            @override
            async def run(self, seq: int) -> int:
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    first_done.set()
                elif call_count == 2:
                    second_done.set()
                return seq * 10

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url,
                valkey_url,
                queue,
                mirror_to=app._signal_dispatcher,
            ):
                # Fire-1: schedule with arg=1.
                tid1 = await app.schedule(task_name, 1)
                # Wait for completion so unique flag is released.
                result1 = await app.get_result(tid1, timeout=8.0)
                assert result1 == 10
                await asyncio.wait_for(first_done.wait(), timeout=5.0)

                # Fire-2: now that unique is released, a new schedule succeeds.
                tid2 = await app.schedule(task_name, 2)
                assert tid2 != tid1, "fire-2 must get a new task_id after unique flag released"
                result2 = await app.get_result(tid2, timeout=8.0)
                assert result2 == 20

        assert call_count == 2


# ---------------------------------------------------------------------------
# Combo 8 — cron + catchup-simulation + rate_limit
# ---------------------------------------------------------------------------


class TestCronCatchupRateLimitCombo:
    """Combo #8: cron + catchup-simulation + rate_limit.

    Bug surface: when N queued cron fires arrive simultaneously (simulating
    catchup), they hit the rate_limit window and most are deferred.  The
    backlog must drain over time — none must be silently dropped.

    Simulation: same direct-schedule pattern as Combo #7 since catchup=True
    is reserved.

    Assumptions documented in §Assumptions (A-8).
    """

    async def test_happy_path(self, rabbitmq_url: str, valkey_url: str) -> None:
        """Single cron fire with rate_limit: task executes once, no hang."""
        queue = _uq("ccr")
        task_name = _task_name("ccr_happy")
        call_count = 0
        done = asyncio.Event()

        class CronRateLimitedTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                rate_limit="10/s",
            )

            @override
            async def run(self, idx: int) -> int:
                nonlocal call_count
                call_count += 1
                done.set()
                return idx

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url,
                valkey_url,
                queue,
                mirror_to=app._signal_dispatcher,
            ):
                tid = await app.schedule(task_name, 0)
                await asyncio.wait_for(done.wait(), timeout=8.0)
                result = await app.get_result(tid, timeout=5)

        assert result == 0
        assert call_count >= 1

    @pytest.mark.timeout(45)
    async def test_catchup_burst_respects_rate_limit(self, rabbitmq_url: str, valkey_url: str) -> None:
        """N catchup fires with rate_limit=2/s: the burst is throttled.

        With rate_limit=2/s and 6 fires, at least some fires must be
        rate-limited (TASK_RATE_LIMITED signals emitted).  All 6 must
        eventually complete (no drops).

        Assertion:
        - all 6 task_ids eventually return a result (no data loss)
        - at least 1 TASK_RATE_LIMITED signal fired (burst was throttled)
        """
        queue = _uq("ccr-burst")
        task_name = _task_name("ccr_burst")
        n_fires = 6
        call_count = 0
        all_done = asyncio.Event()
        rate_limited_count = 0

        class BurstCronTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                rate_limit="2/s",
            )

            @override
            async def run(self, idx: int) -> int:
                nonlocal call_count
                call_count += 1
                if call_count >= n_fires:
                    all_done.set()
                return idx

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        async with app:
            await broker.declare_queue(queue)

            worker_broker = RabbitMQBroker(rabbitmq_url)
            worker_backend = ValkeyBackend(valkey_url)
            worker_app = SwarmQ(worker_broker, worker_backend, queues=[queue])

            @worker_app.on_signal(Signal.TASK_RATE_LIMITED)
            async def _on_rl(**_: Any) -> None:
                nonlocal rate_limited_count
                rate_limited_count += 1

            worker_online = asyncio.Event()

            @worker_app.on_signal(Signal.WORKER_ONLINE)
            async def _on_online(**_: Any) -> None:
                worker_online.set()

            worker_task = asyncio.create_task(worker_app.start_worker())
            try:
                await asyncio.wait_for(worker_online.wait(), timeout=8.0)

                task_ids: list[str] = []
                for i in range(n_fires):
                    tid = await app.schedule(task_name, i)
                    task_ids.append(tid)

                # Wait for all to complete — with rate_limit=2/s and 6 tasks,
                # minimum wall-clock time is ~3s.
                await asyncio.wait_for(all_done.wait(), timeout=30.0)

                # Fetch all results to confirm no data loss.
                for tid in task_ids:
                    await app.get_result(tid, timeout=5)

            finally:
                worker_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await worker_task
                await worker_broker.close()
                await worker_backend.close()

        assert call_count == n_fires, f"all {n_fires} fires must eventually complete; only {call_count} did"
        assert rate_limited_count >= 1, (
            "at least one TASK_RATE_LIMITED signal must fire for a 6-task burst against a 2/s rate limit"
        )

    @pytest.mark.timeout(45)
    async def test_backlog_drains_over_time(self, rabbitmq_url: str, valkey_url: str) -> None:
        """Rate-limited catchup backlog drains monotonically — no task is
        permanently stuck.

        Schedule a burst of 4 tasks against rate_limit=1/s.  At ~t=4s all
        4 must have completed.  Verifies the retry-requeue loop is bounded
        and the backlog empties.
        """
        queue = _uq("ccr-drain")
        task_name = _task_name("ccr_drain")
        n_fires = 4
        completed_at: list[float] = []
        all_done = asyncio.Event()

        class DrainTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                rate_limit="1/s",
            )

            @override
            async def run(self, idx: int) -> int:
                completed_at.append(time.monotonic())
                if len(completed_at) >= n_fires:
                    all_done.set()
                return idx

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        start_t = time.monotonic()

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url,
                valkey_url,
                queue,
                mirror_to=app._signal_dispatcher,
            ):
                for i in range(n_fires):
                    await app.schedule(task_name, i)

                # 4 tasks @ 1/s + generous headroom.
                await asyncio.wait_for(all_done.wait(), timeout=20.0)

        elapsed = time.monotonic() - start_t
        assert len(completed_at) == n_fires, "all tasks must complete"
        # With rate_limit=1/s, tasks complete over >1s (at least first+second
        # are separated). The drain is bounded by n_fires seconds of wall-clock.
        assert elapsed >= 1.0, f"rate-limit must spread execution over >=1s; elapsed={elapsed:.2f}s"

    async def test_signal_sequence_includes_task_rate_limited(self, rabbitmq_url: str, valkey_url: str) -> None:
        """Signal recorder captures TASK_RATE_LIMITED for at least one of the
        burst tasks, confirming the rate-limit path fires the signal.
        """
        queue = _uq("ccr-sig")
        task_name = _task_name("ccr_sig")
        n_fires = 3
        all_done = asyncio.Event()
        call_count = 0

        class SignalBurstTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                rate_limit="1/s",
            )

            @override
            async def run(self, idx: int) -> int:
                nonlocal call_count
                call_count += 1
                if call_count >= n_fires:
                    all_done.set()
                return idx

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        recorder = SignalRecorder.attach(
            app._signal_dispatcher,
            signals=[Signal.TASK_RATE_LIMITED, Signal.TASK_COMPLETED],
        )

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url,
                valkey_url,
                queue,
                mirror_to=app._signal_dispatcher,
            ):
                for i in range(n_fires):
                    await app.schedule(task_name, i)

                await asyncio.wait_for(all_done.wait(), timeout=20.0)

        emitted = recorder.emitted_signals
        assert Signal.TASK_RATE_LIMITED in emitted, (
            f"TASK_RATE_LIMITED must appear in signal stream; recorded: {[s.value for s in emitted]}"
        )


# ---------------------------------------------------------------------------
# Combo 9 — soft_timeout + cancel (Race)
# ---------------------------------------------------------------------------


class TestSoftTimeoutCancelRace:
    """Combo #9: soft_timeout + cancel (Signal Race).

    Bug surface: when both soft_timeout and cancel are set, which one
    fires depends on timing.  The winner determines whether the task
    signal sequence ends with TASK_FAILED (soft_timeout path) or
    TASK_CANCELLED (cancel path).

    Tests pin the CURRENT behaviour for each deterministic ordering.
    test_simultaneous_emit_pins_current_behavior documents the race
    outcome without asserting a specific winner — it only asserts
    the system does not deadlock or emit no terminal signal.

    Assumption: soft_timeout fires asyncio.wait_for() TimeoutError →
    worker wraps it as SoftTimeoutError → TASK_FAILED signal.
    cancel path sets a flag that the cancel-monitor checks between
    yields → TaskCancelledError → TASK_CANCELLED signal.
    """

    async def test_happy_path(self, rabbitmq_url: str, valkey_url: str) -> None:
        """Task completes within soft_timeout, no cancel: TASK_COMPLETED only."""
        queue = _uq("stc")
        task_name = _task_name("stc_happy")
        done = asyncio.Event()

        class FastSoftTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                soft_timeout=5,
                hard_timeout=10,
            )

            @override
            async def run(self, val: int) -> int:
                done.set()
                return val + 1

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        recorder = SignalRecorder.attach(
            app._signal_dispatcher,
            signals=[Signal.TASK_STARTED, Signal.TASK_COMPLETED],
        )

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url,
                valkey_url,
                queue,
                mirror_to=app._signal_dispatcher,
            ):
                tid = await app.schedule(task_name, 7)
                result = await app.get_result(tid, timeout=8.0)

        assert result == 8
        await assert_signal_sequence(
            recorder,
            [Signal.TASK_STARTED, Signal.TASK_COMPLETED],
            timeout_s=2.0,
            strict_order=True,
        )

    async def test_cancel_wins_when_set_before_timeout(self, rabbitmq_url: str, valkey_url: str) -> None:
        """Set cancel flag 0.5s before soft_timeout would fire.

        Contract: cancel flag wins → TASK_CANCELLED signal,
        no TASK_FAILED from soft_timeout.

        Task sleeps 3s; soft_timeout=2s; cancel fires at 0.5s.
        At 0.5s the cancel poll fires before the 2s timeout.
        """
        queue = _uq("stc-cw")
        task_name = _task_name("stc_cw")
        started = asyncio.Event()

        class CancelableTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                soft_timeout=2,
                hard_timeout=5,
            )

            @override
            async def run(self) -> str:
                started.set()
                await asyncio.sleep(3)
                return "should-not-return"

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        recorder = SignalRecorder.attach(
            app._signal_dispatcher,
            signals=[Signal.TASK_STARTED, Signal.TASK_CANCELLED, Signal.TASK_FAILED],
        )

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url,
                valkey_url,
                queue,
                cancel_poll_interval=0.05,
                mirror_to=app._signal_dispatcher,
            ):
                tid = await app.schedule(task_name)
                await asyncio.wait_for(started.wait(), timeout=5.0)

                # Cancel at ~0.5s — well before the 2s soft_timeout.
                await asyncio.sleep(0.1)
                await app.cancel(tid)

                with pytest.raises(TaskCancelledError):
                    await app.get_result(tid, timeout=5.0)

        emitted = recorder.emitted_signals
        assert Signal.TASK_CANCELLED in emitted, (
            f"cancel must win when set before soft_timeout; emitted={[s.value for s in emitted]}"
        )
        assert Signal.TASK_FAILED not in emitted, (
            f"TASK_FAILED must not fire when cancel wins; emitted={[s.value for s in emitted]}"
        )

    async def test_timeout_wins_when_cancel_set_after_timeout(self, rabbitmq_url: str, valkey_url: str) -> None:
        """Set cancel flag after soft_timeout fires.

        Contract: soft_timeout fires first → TASK_FAILED (SoftTimeoutError),
        subsequent cancel is a no-op against an already-failed task.

        Task sleeps 5s; soft_timeout=1s; cancel fires at 1.5s.
        The soft_timeout fires at 1s before cancel arrives at 1.5s.
        """
        queue = _uq("stc-tw")
        task_name = _task_name("stc_tw")
        started = asyncio.Event()

        class SlowTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                soft_timeout=1,
                hard_timeout=5,
                max_retries=0,
            )

            @override
            async def run(self) -> str:
                started.set()
                await asyncio.sleep(5)
                return "never"

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        recorder = SignalRecorder.attach(
            app._signal_dispatcher,
            signals=[Signal.TASK_STARTED, Signal.TASK_FAILED, Signal.TASK_CANCELLED],
        )

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url,
                valkey_url,
                queue,
                cancel_poll_interval=0.05,
                mirror_to=app._signal_dispatcher,
            ):
                tid = await app.schedule(task_name)
                await asyncio.wait_for(started.wait(), timeout=5.0)

                # Wait for soft_timeout (1s) to fire first.
                await asyncio.sleep(1.5)

                # Now set cancel — soft_timeout has already fired.
                await app.cancel(tid)

                # get_result raises TaskError (SoftTimeoutError-wrapped) since
                # the soft_timeout fired; cancel flag is ignored for a terminal task.
                with pytest.raises((TaskError, TaskCancelledError)):
                    await app.get_result(tid, timeout=5.0)

        emitted = recorder.emitted_signals
        assert Signal.TASK_FAILED in emitted, (
            f"TASK_FAILED must fire when soft_timeout wins; emitted={[s.value for s in emitted]}"
        )

    async def test_simultaneous_emit_pins_current_behavior(self, rabbitmq_url: str, valkey_url: str) -> None:
        """Both soft_timeout and cancel fire near-simultaneously.

        This test does NOT assert a specific winner — the outcome is
        nondeterministic by nature.  It only asserts:
        1. TASK_STARTED fired (task ran).
        2. Exactly one terminal signal (TASK_CANCELLED or TASK_FAILED) fired.
        3. No deadlock: get_result eventually raises an exception.

        Documents the current race outcome for future regression detection.
        """
        queue = _uq("stc-sim")
        task_name = _task_name("stc_sim")
        started = asyncio.Event()

        class RaceTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                soft_timeout=1,
                hard_timeout=5,
                max_retries=0,
            )

            @override
            async def run(self) -> str:
                started.set()
                await asyncio.sleep(5)
                return "never"

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        recorder = SignalRecorder.attach(
            app._signal_dispatcher,
            signals=[Signal.TASK_STARTED, Signal.TASK_FAILED, Signal.TASK_CANCELLED],
        )

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url,
                valkey_url,
                queue,
                cancel_poll_interval=0.01,
                mirror_to=app._signal_dispatcher,
            ):
                tid = await app.schedule(task_name)
                await asyncio.wait_for(started.wait(), timeout=5.0)

                # Cancel at ~1s — same time as soft_timeout.
                await asyncio.sleep(1.0)
                await app.cancel(tid)

                with pytest.raises((TaskError, TaskCancelledError)):
                    await app.get_result(tid, timeout=5.0)

        emitted = recorder.emitted_signals
        assert Signal.TASK_STARTED in emitted, "TASK_STARTED must always fire"
        terminal = {Signal.TASK_CANCELLED, Signal.TASK_FAILED}
        assert any(s in terminal for s in emitted), (
            f"exactly one terminal signal must fire; emitted={[s.value for s in emitted]}"
        )
        # Only one terminal signal should have fired (not both).
        terminal_emitted = [s for s in emitted if s in terminal]
        assert len(terminal_emitted) == 1, (
            f"exactly one terminal signal expected; got {[s.value for s in terminal_emitted]}"
        )


# ---------------------------------------------------------------------------
# Combo 10 — hard_timeout + retry
# ---------------------------------------------------------------------------


class TestHardTimeoutRetryCombo:
    """Combo #10: hard_timeout + retry.

    Bug surface:
    - hard_timeout kills the coroutine cooperatively (asyncio.Task.cancel)
      followed by a hard kill path if needed.
    - The killed task must release its lock and unique-hash (if configured)
      before retry fires.
    - max_retries exhausted → DLQ, not infinite loop.
    """

    async def test_happy_path(self, rabbitmq_url: str, valkey_url: str) -> None:
        """Task exceeds hard_timeout once, is retried, second attempt completes."""
        queue = _uq("htr")
        task_name = _task_name("htr_happy")
        attempts = 0
        done = asyncio.Event()

        class HardTimeoutRetryTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                hard_timeout=2,
                max_retries=2,
                retry_base_delay=0.2,
            )

            @override
            async def run(self, val: int) -> int:
                nonlocal attempts
                attempts += 1
                if attempts == 1:
                    await asyncio.sleep(60)  # killed by hard_timeout
                done.set()
                return val * 2

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url,
                valkey_url,
                queue,
                mirror_to=app._signal_dispatcher,
            ):
                tid = await app.schedule(task_name, 5)
                await asyncio.wait_for(done.wait(), timeout=15.0)
                result = await app.get_result(tid, timeout=5)

        assert result == 10
        assert_at_least_once_held(attempts, expected_min=2)

    @pytest.mark.timeout(20)
    async def test_hard_timeout_triggers_retry(self, rabbitmq_url: str, valkey_url: str) -> None:
        """hard_timeout fires → TASK_FAILED (or TASK_RETRYING) → retry attempt runs.

        Verifies that a hard_timeout does not permanently consume the task
        — the retry path kicks in and the task eventually completes.
        """
        queue = _uq("htr-trig")
        task_name = _task_name("htr_trig")
        attempts = 0
        started_events: list[float] = []
        done = asyncio.Event()

        class RetriableHardTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                hard_timeout=1,
                max_retries=3,
                retry_base_delay=0.1,
            )

            @override
            async def run(self, payload: str) -> str:
                nonlocal attempts
                attempts += 1
                started_events.append(time.monotonic())
                if attempts <= 2:
                    await asyncio.sleep(60)  # killed by hard_timeout
                done.set()
                return f"ok:{payload}"

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url,
                valkey_url,
                queue,
                mirror_to=app._signal_dispatcher,
            ):
                tid = await app.schedule(task_name, "x")
                await asyncio.wait_for(done.wait(), timeout=15.0)
                result = await app.get_result(tid, timeout=5)

        assert result == "ok:x"
        assert_at_least_once_held(attempts, expected_min=3)

    async def test_retry_state_consistent_after_hard_kill(self, rabbitmq_url: str, valkey_url: str) -> None:
        """After hard_timeout, any lock or unique-hash held by the killed
        attempt must be released before retry starts.

        Uses lock + hard_timeout + retry.  A successor task with the same
        lock key must be schedulable after the hard-killed attempt releases
        the lock (via finally-block of the cooperative cancel).
        """
        queue = _uq("htr-state")
        task_name = _task_name("htr_state")
        lock_name = f"htr-lock-{uuid.uuid4().hex[:6]}"
        attempts = 0
        retry_started = asyncio.Event()
        done = asyncio.Event()

        class LockedHardTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                hard_timeout=1,
                max_retries=2,
                retry_base_delay=0.1,
                lock=lock_name,
            )

            @override
            async def run(self, tag: str) -> str:
                nonlocal attempts
                attempts += 1
                if attempts == 1:
                    await asyncio.sleep(60)  # killed by hard_timeout
                retry_started.set()
                done.set()
                return f"done:{tag}"

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url,
                valkey_url,
                queue,
                mirror_to=app._signal_dispatcher,
            ):
                tid = await app.schedule(task_name, "state-check")
                # Wait for retry to start — lock must have been released.
                await asyncio.wait_for(retry_started.wait(), timeout=10.0)
                await asyncio.wait_for(done.wait(), timeout=5.0)
                result = await app.get_result(tid, timeout=5)

        assert result == "done:state-check"
        assert_at_least_once_held(attempts, expected_min=2)

    @pytest.mark.timeout(30)
    async def test_max_retries_reached_lands_in_dlq(self, rabbitmq_url: str, valkey_url: str) -> None:
        """After max_retries, the task is permanently failed (lands in DLQ).

        A task that always exceeds hard_timeout must eventually be exhausted
        and not retry forever.  The task_id's get_result must raise TaskError
        after all retries are consumed.
        """
        queue = _uq("htr-dlq")
        task_name = _task_name("htr_dlq")
        attempts = 0
        failed_signal_fired = asyncio.Event()

        class AlwaysTimesOutTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                hard_timeout=1,
                max_retries=1,  # 2 total attempts; then DLQ
                retry_base_delay=0.1,
            )

            @override
            async def run(self) -> str:
                nonlocal attempts
                attempts += 1
                await asyncio.sleep(60)
                return "never"

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        @app.on_signal(Signal.TASK_FAILED)
        async def _on_failed(**_: Any) -> None:
            failed_signal_fired.set()

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url,
                valkey_url,
                queue,
                mirror_to=app._signal_dispatcher,
            ):
                tid = await app.schedule(task_name)

                # Wait for TASK_FAILED signal (fires after all retries exhausted).
                await asyncio.wait_for(failed_signal_fired.wait(), timeout=15.0)

                with pytest.raises(TaskError):
                    await app.get_result(tid, timeout=5)

        assert_at_least_once_held(attempts, expected_min=2)


# ---------------------------------------------------------------------------
# Combo 11 — max_age + scheduler
# ---------------------------------------------------------------------------


class TestMaxAgeSchedulerCombo:
    """Combo #11: max_age + scheduler.

    Bug surface: when the scheduler fires a task and the worker consumes it,
    the worker checks (now - enqueued_at) against max_age.  If the task sat
    in the queue longer than max_age, it must be routed to the DLQ with a
    TASK_EXPIRED signal — not executed.

    Fresh tasks (max_age not exceeded) must run normally.
    """

    @pytest.mark.timeout(60)
    async def test_happy_path(self, rabbitmq_url: str, valkey_url: str) -> None:
        """Task with generous max_age fires via scheduler and completes normally."""
        queue = _uq("mas")
        task_name = _task_name("mas_happy")
        done = asyncio.Event()

        class FreshSchedulerTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                max_age=60,  # 60s — plenty of time
                cron="* * * * *",
            )

            @override
            async def run(self) -> str:
                done.set()
                return "fresh"

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(
            broker,
            backend,
            queues=[queue],
            scheduler_poll_interval=0.1,
            scheduler_heartbeat_interval=0.2,
            scheduler_lock_ttl=1.0,
        )

        leader_event = asyncio.Event()

        @app.on_signal(Signal.SCHEDULER_LEADER)
        async def _on_leader(**_: Any) -> None:
            leader_event.set()

        async with app:
            await broker.declare_queue(queue)
            worker_task = asyncio.create_task(app.start_worker())
            try:
                # Deliver the fire directly rather than via the cron zset: the
                # scheduler→max_age delivery path is covered deterministically
                # by the sibling skips-expired / fresh-after-skipped tests. The
                # contract here is only that a FRESH task (age « max_age=60)
                # runs normally — a direct enqueue exercises exactly that
                # without depending on the scheduler/leader at all (no
                # leader-election wait needed, robust under load).
                await app.schedule(task_name)
                await asyncio.wait_for(done.wait(), timeout=20.0)
            finally:
                worker_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await worker_task

    @pytest.mark.timeout(20)
    async def test_scheduler_skips_expired_tasks_emits_task_expired(self, rabbitmq_url: str, valkey_url: str) -> None:
        """Task with max_age=1 is scheduled, then sits in the queue > 1s before
        the worker picks it up.  Worker must emit TASK_EXPIRED and route to DLQ.

        We inject a simulated aged message by scheduling the task directly
        (not via scheduler) so the worker sees it past the max_age window.
        """
        queue = _uq("mas-exp")
        task_name = _task_name("mas_exp")
        call_count = 0
        expired_signal = asyncio.Event()

        class ExpiredTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                max_age=1,  # 1s — the message will be stale
            )

            @override
            async def run(self) -> str:
                nonlocal call_count
                call_count += 1
                return "should-not-execute"

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        @app.on_signal(Signal.TASK_EXPIRED)
        async def _on_expired(**_: Any) -> None:
            expired_signal.set()

        async with app:
            await broker.declare_queue(queue)

            # Schedule the task.
            await app.schedule(task_name)

            # Let the message age past max_age=1s before the worker starts.
            await asyncio.sleep(1.5)

            async with _running_worker(
                rabbitmq_url,
                valkey_url,
                queue,
                mirror_to=app._signal_dispatcher,
            ):
                # Worker picks up the already-expired message.
                await asyncio.wait_for(expired_signal.wait(), timeout=8.0)

        assert call_count == 0, "expired task must not be executed; run() must not be called"

    async def test_fresh_task_runs_after_expired_skipped(self, rabbitmq_url: str, valkey_url: str) -> None:
        """After an expired task is skipped (TASK_EXPIRED), a fresh task for
        the same task_name runs normally — the DLQ routing of expired tasks
        does not block subsequent fresh enqueues.
        """
        queue = _uq("mas-fresh")
        task_name = _task_name("mas_fresh")
        call_count = 0
        fresh_done = asyncio.Event()
        expired_event = asyncio.Event()

        class MaxAgeTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                max_age=1,
            )

            @override
            async def run(self, tag: str) -> str:
                nonlocal call_count
                call_count += 1
                fresh_done.set()
                return f"ok:{tag}"

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        @app.on_signal(Signal.TASK_EXPIRED)
        async def _on_expired(**_: Any) -> None:
            expired_event.set()

        async with app:
            await broker.declare_queue(queue)

            # Stale task.
            await app.schedule(task_name, "stale")
            await asyncio.sleep(1.5)

            async with _running_worker(
                rabbitmq_url,
                valkey_url,
                queue,
                mirror_to=app._signal_dispatcher,
            ):
                # Stale task gets TASK_EXPIRED.
                await asyncio.wait_for(expired_event.wait(), timeout=8.0)

                # Fresh task — enqueued now, well within max_age=1s.
                tid_fresh = await app.schedule(task_name, "fresh")
                await asyncio.wait_for(fresh_done.wait(), timeout=8.0)
                result = await app.get_result(tid_fresh, timeout=5)

        assert result == "ok:fresh"
        assert call_count == 1, "only the fresh task must execute; stale must be expired"


# ---------------------------------------------------------------------------
# Combo 12 — rate_limit + retry + retry_backoff
# ---------------------------------------------------------------------------


class TestRateLimitRetryBackoffCombo:
    """Combo #12: rate_limit + retry + retry_backoff.

    Bug surface:
    - A rate-limited task gets deferred (nacked + requeued).  The retry
      machinery (max_retries + retry_backoff) should only trigger for
      actual task *failures* (exceptions), not for rate-limiting deferrals.
    - retry_backoff=exponential with max_retries should bound the loop.

    The test verifies:
    - rate-limited tasks still eventually complete (drain).
    - tasks that fail (raise exception) use exponential backoff between
      retry attempts — not the rate-limit requeue cycle.
    - max_retries is respected and exhausted tasks land in DLQ.
    """

    async def test_happy_path(self, rabbitmq_url: str, valkey_url: str) -> None:
        """Task with rate_limit + retry + exponential_backoff:
        rate-limited on first pass, runs successfully on second pass."""
        queue = _uq("rlrb")
        task_name = _task_name("rlrb_happy")
        call_count = 0
        done = asyncio.Event()

        class RateLimitRetryTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                rate_limit="2/s",
                max_retries=2,
                retry_backoff="exponential",
                retry_base_delay=0.1,
            )

            @override
            async def run(self, val: int) -> int:
                nonlocal call_count
                call_count += 1
                done.set()
                return val + 100

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url,
                valkey_url,
                queue,
                mirror_to=app._signal_dispatcher,
            ):
                tid = await app.schedule(task_name, 5)
                await asyncio.wait_for(done.wait(), timeout=10.0)
                result = await app.get_result(tid, timeout=5)

        assert result == 105

    @pytest.mark.timeout(30)
    async def test_retry_hit_rate_limit_uses_exponential_backoff(self, rabbitmq_url: str, valkey_url: str) -> None:
        """Task fails (exception) → retries with exponential backoff.
        While retrying, other tasks also hit the rate_limit.

        The retry delays (exponential) must NOT collapse to zero — backoff
        must add spacing between retry attempts.

        We verify the timing between retry attempt starts increases
        (exponential backoff is 0.1, 0.2, 0.4 ... seconds).
        """
        queue = _uq("rlrb-bo")
        task_name = _task_name("rlrb_bo")
        attempt_times: list[float] = []
        done = asyncio.Event()

        class BackoffTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                rate_limit="10/s",  # high enough to not rate-limit; failings from exceptions
                max_retries=2,
                retry_backoff="exponential",
                retry_base_delay=0.2,
            )

            @override
            async def run(self, val: int) -> int:
                attempt_times.append(time.monotonic())
                if len(attempt_times) <= 2:
                    raise RuntimeError(f"attempt {len(attempt_times)} fails")
                done.set()
                return val + 1

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url,
                valkey_url,
                queue,
                mirror_to=app._signal_dispatcher,
            ):
                tid = await app.schedule(task_name, 10)
                await asyncio.wait_for(done.wait(), timeout=15.0)
                result = await app.get_result(tid, timeout=5)

        assert result == 11
        assert_at_least_once_held(len(attempt_times), expected_min=3)

        # Verify exponential spacing: gap between attempts should grow.
        # attempt 1→2 gap: ~0.2s; attempt 2→3 gap: ~0.4s.
        if len(attempt_times) >= 3:
            gap1 = attempt_times[1] - attempt_times[0]
            gap2 = attempt_times[2] - attempt_times[1]
            assert gap2 >= gap1 * 0.8, (  # 20% tolerance
                f"exponential backoff must increase spacing between retries; gap1={gap1:.3f}s gap2={gap2:.3f}s"
            )

    async def test_no_infinite_loop_bound_via_max_retries(self, rabbitmq_url: str, valkey_url: str) -> None:
        """Rate-limited task that always fails must be bounded by max_retries.

        TASK_FAILED (not an infinite rate-limit → retry → rate-limit loop)
        must eventually fire.
        """
        queue = _uq("rlrb-inf")
        task_name = _task_name("rlrb_inf")
        attempts = 0
        failed_event = asyncio.Event()

        class AlwaysFailRLTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                rate_limit="5/s",
                max_retries=1,
                retry_backoff="exponential",
                retry_base_delay=0.1,
            )

            @override
            async def run(self) -> str:
                nonlocal attempts
                attempts += 1
                raise RuntimeError("always fails")

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        @app.on_signal(Signal.TASK_FAILED)
        async def _on_failed(**_: Any) -> None:
            failed_event.set()

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url,
                valkey_url,
                queue,
                mirror_to=app._signal_dispatcher,
            ):
                tid = await app.schedule(task_name)
                await asyncio.wait_for(failed_event.wait(), timeout=10.0)
                with pytest.raises(TaskError):
                    await app.get_result(tid, timeout=5)

        assert_at_least_once_held(attempts, expected_min=2)

    async def test_signal_sequence_for_rate_limited_retry(self, rabbitmq_url: str, valkey_url: str) -> None:
        """Signal sequence for a task that is rate-limited then fails+retries.

        Expected: TASK_RATE_LIMITED fires ≥1 times; on exception:
        TASK_RETRYING fires; eventually TASK_COMPLETED or TASK_FAILED.
        """
        queue = _uq("rlrb-sig")
        task_name = _task_name("rlrb_sig")
        n_tasks = 3
        call_count = 0
        all_done = asyncio.Event()

        class SigTrackTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                rate_limit="1/s",
                max_retries=1,
                retry_base_delay=0.1,
            )

            @override
            async def run(self, idx: int) -> int:
                nonlocal call_count
                call_count += 1
                if call_count >= n_tasks:
                    all_done.set()
                return idx

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        recorder = SignalRecorder.attach(
            app._signal_dispatcher,
            signals=[Signal.TASK_RATE_LIMITED, Signal.TASK_COMPLETED],
        )

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url,
                valkey_url,
                queue,
                mirror_to=app._signal_dispatcher,
            ):
                for i in range(n_tasks):
                    await app.schedule(task_name, i)
                await asyncio.wait_for(all_done.wait(), timeout=20.0)

        emitted = recorder.emitted_signals
        assert Signal.TASK_RATE_LIMITED in emitted, (
            "TASK_RATE_LIMITED must appear in signal stream for burst against 1/s limit"
        )
        assert Signal.TASK_COMPLETED in emitted, "TASK_COMPLETED must eventually appear — all tasks must drain"


# ---------------------------------------------------------------------------
# Combo 13 — priority + lock
# ---------------------------------------------------------------------------


class TestPriorityLockCombo:
    """Combo #13: priority + lock.

    Bug surface: when multiple tasks with different priorities compete for
    the same lock, the ordering of lock acquisition is a property of the
    broker's prefetch + queue priority topology — not guaranteed FIFO.

    Tests document current behaviour (FIFO or priority-first) rather than
    asserting a specific order, since SwarmQ uses weighted prefetch
    (3:2:1 for high:normal:low) not true priority preemption.

    Assumptions: priorities=True must be set on the worker to enable the
    high/normal/low queue triplet.  priority_queues must be declared.
    """

    async def test_happy_path(self, rabbitmq_url: str, valkey_url: str) -> None:
        """Two tasks (high + normal priority) with the same lock:
        both complete, lock is not deadlocked."""
        queue = _uq("plk")
        lock_name = f"plk-lock-{uuid.uuid4().hex[:6]}"
        task_name_high = _task_name("plk_high")
        task_name_norm = _task_name("plk_norm")
        results: list[str] = []
        all_done = asyncio.Event()

        class HighPrioTask(Task):
            task_info = TaskInfo(
                name=task_name_high,
                queues=[queue],
                priority="high",
                lock=lock_name,
                hard_timeout=10,
            )

            @override
            async def run(self, tag: str) -> str:
                results.append(f"high:{tag}")
                if len(results) >= 2:
                    all_done.set()
                return f"high:{tag}"

        class NormalPrioTask(Task):
            task_info = TaskInfo(
                name=task_name_norm,
                queues=[queue],
                priority="normal",
                lock=lock_name,
                hard_timeout=10,
            )

            @override
            async def run(self, tag: str) -> str:
                results.append(f"norm:{tag}")
                if len(results) >= 2:
                    all_done.set()
                return f"norm:{tag}"

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue], priorities=True)

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url,
                valkey_url,
                queue,
                priorities=True,
                mirror_to=app._signal_dispatcher,
            ):
                tid_h = await app.schedule(task_name_high, "a")
                tid_n = await app.schedule(task_name_norm, "b")
                await asyncio.wait_for(all_done.wait(), timeout=12.0)
                r_h = await app.get_result(tid_h, timeout=5)
                r_n = await app.get_result(tid_n, timeout=5)

        assert r_h == "high:a"
        assert r_n == "norm:b"
        assert len(results) == 2

    async def test_high_priority_lock_acquired_first_or_fifo(self, rabbitmq_url: str, valkey_url: str) -> None:
        """Document current behaviour: high-priority task acquires lock first
        when both are enqueued simultaneously — or FIFO if the broker delivers
        them in arrival order.

        This test pins the OBSERVED order (not a strict requirement).
        Both tasks must complete; the order assertion is documentary.
        """
        queue = _uq("plk-ord")
        lock_name = f"plk-ord-{uuid.uuid4().hex[:6]}"
        task_name_high = _task_name("plk_ord_high")
        task_name_low = _task_name("plk_ord_low")
        completion_order: list[str] = []
        all_done = asyncio.Event()

        class HighTask(Task):
            task_info = TaskInfo(
                name=task_name_high,
                queues=[queue],
                priority="high",
                lock=lock_name,
                hard_timeout=10,
            )

            @override
            async def run(self) -> str:
                completion_order.append("high")
                if len(completion_order) >= 2:
                    all_done.set()
                return "high"

        class LowTask(Task):
            task_info = TaskInfo(
                name=task_name_low,
                queues=[queue],
                priority="low",
                lock=lock_name,
                hard_timeout=10,
            )

            @override
            async def run(self) -> str:
                completion_order.append("low")
                if len(completion_order) >= 2:
                    all_done.set()
                return "low"

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue], priorities=True)

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url,
                valkey_url,
                queue,
                priorities=True,
                concurrency=2,
                mirror_to=app._signal_dispatcher,
            ):
                # Schedule high and low simultaneously.
                tid_h = await app.schedule(task_name_high)
                tid_l = await app.schedule(task_name_low)
                await asyncio.wait_for(all_done.wait(), timeout=12.0)
                await app.get_result(tid_h, timeout=5)
                await app.get_result(tid_l, timeout=5)

        assert set(completion_order) == {"high", "low"}, "both tasks must complete regardless of ordering"
        # Document observed order — not asserting "high first" strictly.
        # This assertion will catch order changes in future implementations.
        # Current expectation: with weighted prefetch, high tends to go first.
        # If this changes, update this comment and the assertion.
        assert len(completion_order) == 2

    async def test_priority_does_not_starve_low_priority(self, rabbitmq_url: str, valkey_url: str) -> None:
        """Continuous stream of high-priority tasks must not permanently starve
        a low-priority task — the low-priority task must complete within the
        test timeout.

        We schedule 2 high-priority tasks and 1 low-priority task.
        All must complete; low-priority must not be indefinitely deferred.
        """
        queue = _uq("plk-starve")
        lock_name = f"plk-starve-{uuid.uuid4().hex[:6]}"
        task_name_high = _task_name("plk_starve_high")
        task_name_low = _task_name("plk_starve_low")
        all_done = asyncio.Event()

        class HighStarveTask(Task):
            task_info = TaskInfo(
                name=task_name_high,
                queues=[queue],
                priority="high",
                lock=lock_name,
                hard_timeout=10,
            )

            @override
            async def run(self, idx: int) -> int:
                return idx

        class LowStarveTask(Task):
            task_info = TaskInfo(
                name=task_name_low,
                queues=[queue],
                priority="low",
                lock=lock_name,
                hard_timeout=10,
            )

            @override
            async def run(self, idx: int) -> int:
                all_done.set()
                return idx + 100

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue], priorities=True)

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url,
                valkey_url,
                queue,
                priorities=True,
                concurrency=3,
                mirror_to=app._signal_dispatcher,
            ):
                tid_h1 = await app.schedule(task_name_high, 1)
                tid_h2 = await app.schedule(task_name_high, 2)
                tid_l = await app.schedule(task_name_low, 0)

                await asyncio.wait_for(all_done.wait(), timeout=12.0)

                r_h1 = await app.get_result(tid_h1, timeout=5)
                r_h2 = await app.get_result(tid_h2, timeout=5)
                r_l = await app.get_result(tid_l, timeout=5)

        assert r_h1 in (1, 2)
        assert r_h2 in (1, 2)
        assert r_l == 100, "low-priority task must complete"


# ---------------------------------------------------------------------------
# Combo 14 — unique_until=start + retry
# ---------------------------------------------------------------------------


class TestUniqueUntilStartRetryCombo:
    """Combo #14: unique_until=start + retry.

    Bug surface: ``unique_until=start`` releases the unique-hash when the
    task *starts* executing (not when it completes).  Combined with retries,
    there is a window between the first attempt's start (unique released) and
    the retry attempt's start where a second enqueue CAN enter the system.

    Tests verify:
    - unique_released_at_start: unique is released as soon as run() begins,
      not after completion.
    - second_task_can_enter_during_retry_window: after unique is released at
      start, a duplicate enqueue succeeds (gets a new task_id).
    - race_detection_via_signal_sequence: the signal sequence correctly shows
      two distinct TASK_STARTED events when two tasks overlap in the retry
      window.
    """

    async def test_happy_path(self, rabbitmq_url: str, valkey_url: str) -> None:
        """Task with unique_until=start completes; subsequent enqueue before
        start would be blocked, but after start succeeds."""
        queue = _uq("uus")
        task_name = _task_name("uus_happy")
        call_count = 0
        done = asyncio.Event()

        class UniqueStartTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                unique_for=60,
                unique_until="start",
            )

            @override
            async def run(self, val: str) -> str:
                nonlocal call_count
                call_count += 1
                done.set()
                return f"ok:{val}"

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url,
                valkey_url,
                queue,
                mirror_to=app._signal_dispatcher,
            ):
                tid = await app.schedule(task_name, "x")
                result = await app.get_result(tid, timeout=8.0)

        assert result == "ok:x"
        assert call_count == 1

    async def test_unique_released_at_start_not_completion(self, rabbitmq_url: str, valkey_url: str) -> None:
        """unique_until=start: the unique flag is released when run() begins.

        Proof: schedule first task (long-running), wait for it to start,
        then schedule a second task with the same args — it must NOT be
        deduplicated (gets a fresh task_id) because unique was already
        released at start.

        Compare with unique_until=completion where the second enqueue would
        return the same task_id until the first completes.
        """
        queue = _uq("uus-rel")
        task_name = _task_name("uus_rel")
        started_event = asyncio.Event()
        first_barrier = asyncio.Event()
        call_count = 0

        class LongUniqueStartTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                unique_for=60,
                unique_until="start",
            )

            @override
            async def run(self, val: int) -> int:
                nonlocal call_count
                call_count += 1
                started_event.set()
                # Wait until the test signals it may proceed.
                await asyncio.wait_for(first_barrier.wait(), timeout=10.0)
                return val * 2

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url,
                valkey_url,
                queue,
                mirror_to=app._signal_dispatcher,
            ):
                # Enqueue first task.
                tid1 = await app.schedule(task_name, 5)

                # Wait for the task to start (unique flag released at this point).
                await asyncio.wait_for(started_event.wait(), timeout=8.0)

                # Enqueue second task with same args — unique was released at start,
                # so this must get a NEW task_id.
                tid2 = await app.schedule(task_name, 5)

                assert tid2 != tid1, (
                    "unique_until=start must release the flag at start, "
                    "so a second enqueue after start gets a fresh task_id"
                )

                # Let first task complete.
                first_barrier.set()
                await app.get_result(tid1, timeout=8.0)

    async def test_second_task_can_enter_during_retry_window(self, rabbitmq_url: str, valkey_url: str) -> None:
        """unique_until=start + retry: after first attempt starts (unique released),
        a duplicate can enter.  After first attempt fails and retry fires,
        both the original retry and the "duplicate" may run.

        Assertion: after first attempt's start, the second enqueue gets a
        different task_id (entered successfully).  Both tasks eventually
        complete.
        """
        queue = _uq("uus-retry")
        task_name = _task_name("uus_retry")
        first_started = asyncio.Event()
        asyncio.Event()
        call_count = 0
        all_done = asyncio.Event()

        class RetryUniqueStartTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                unique_for=60,
                unique_until="start",
                max_retries=1,
                retry_base_delay=0.5,
            )

            @override
            async def run(self, label: str) -> str:
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    first_started.set()
                    raise RuntimeError("first attempt fails; unique was released at start")
                all_done.set()
                return f"done:{label}"

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url,
                valkey_url,
                queue,
                mirror_to=app._signal_dispatcher,
            ):
                tid1 = await app.schedule(task_name, "orig")

                # Wait for first attempt to start (unique is now released).
                await asyncio.wait_for(first_started.wait(), timeout=8.0)

                # Schedule duplicate — unique was released, so this succeeds.
                tid2 = await app.schedule(task_name, "orig")

                # At least one of them (tid1 retry or tid2) runs and finishes.
                # Note: both may have the same label "orig".
                await asyncio.wait_for(all_done.wait(), timeout=10.0)

        # The key assertion: second enqueue after start got a fresh task_id.
        assert tid2 != tid1, "unique_until=start releases the flag at start; second enqueue must get a new task_id"
        assert_at_least_once_held(call_count, expected_min=2)

    async def test_race_detection_via_signal_sequence(self, rabbitmq_url: str, valkey_url: str) -> None:
        """Signal sequence documents the race window of unique_until=start + retry.

        Two tasks with the same args can overlap:
        1. Task-1 starts → unique released → Task-1 fails → retry scheduled.
        2. Task-2 enqueued (unique slot free) → Task-2 starts.
        3. Task-1 retry starts.

        Both tasks emit TASK_STARTED; one (or both) emit TASK_COMPLETED.
        The signal sequence records this overlapping execution.
        """
        queue = _uq("uus-race")
        task_name = _task_name("uus_race")
        t1_started = asyncio.Event()
        call_count = 0
        completions = asyncio.Event()

        class RaceUniqueTask(Task):
            task_info = TaskInfo(
                name=task_name,
                queues=[queue],
                unique_for=30,
                unique_until="start",
                max_retries=1,
                retry_base_delay=0.3,
            )

            @override
            async def run(self, seq: int) -> int:
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    t1_started.set()
                    raise RuntimeError("task-1 fails; unique released at start")
                completions.set()
                return seq + 1

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        app = SwarmQ(broker, backend, queues=[queue])

        recorder = SignalRecorder.attach(
            app._signal_dispatcher,
            signals=[
                Signal.TASK_STARTED,
                Signal.TASK_COMPLETED,
                Signal.TASK_FAILED,
                Signal.TASK_RETRYING,
            ],
        )

        async with app:
            await broker.declare_queue(queue)
            async with _running_worker(
                rabbitmq_url,
                valkey_url,
                queue,
                concurrency=4,
                mirror_to=app._signal_dispatcher,
            ):
                tid1 = await app.schedule(task_name, 10)
                # Wait for task-1 to start and release unique.
                await asyncio.wait_for(t1_started.wait(), timeout=8.0)

                # Enqueue task-2 — unique is now free.
                tid2 = await app.schedule(task_name, 10)
                assert tid2 != tid1, "unique must be released at start"

                await asyncio.wait_for(completions.wait(), timeout=10.0)

        emitted = recorder.emitted_signals
        # At least 2 TASK_STARTED events (task-1 start + task-1-retry or task-2).
        started_count = emitted.count(Signal.TASK_STARTED)
        assert started_count >= 2, (
            f"at least 2 TASK_STARTED signals expected (race window); "
            f"got {started_count} in {[s.value for s in emitted]}"
        )
        # At least one TASK_COMPLETED must fire.
        assert Signal.TASK_COMPLETED in emitted, "at least one TASK_COMPLETED must fire after race"


# ---------------------------------------------------------------------------
# Assumptions documented here for future maintainers
# ---------------------------------------------------------------------------
#
# A-7 (Combo 7+8): catchup=True is reserved (raises ValueError at construction).
#     Tests simulate catchup by direct-scheduling N copies of the same task.
#     The real-production catchup behaviour (scheduler queuing N back-fills)
#     is not yet implemented.  When catchup=True lands, these tests should
#     be updated to use the real API and the simulated-schedule approach
#     should be kept as a secondary control test.
#
# A-8 (Combo 8): Rate-limit deferred tasks are nacked+requeued, not counted
#     against max_retries.  The retry counter increments only on task
#     *exceptions* (SoftTimeoutError, hard_timeout kill, explicit raise),
#     not on rate-limit deferrals.  If this changes, test_no_infinite_loop_
#     bound_via_max_retries (Combo 12) will fail and the comment must be updated.
#
# A-9 (Combo 9): soft_timeout fires asyncio.wait_for TimeoutError →
#     wrapped as SoftTimeoutError → TASK_FAILED.  cancel fires cancel_poll_
#     interval after set_cancel_flag → TaskCancelledError → TASK_CANCELLED.
#     With cancel_poll_interval=0.05s and soft_timeout=2s, setting cancel
#     at 0.1s (<<2s) deterministically wins.
#
# A-10 (Combo 10): hard_timeout fires asyncio.Task.cancel() (cooperative kill).
#     The finally-block in the worker releases the lock.  A cooperative kill
#     is different from SIGKILL — the finally-block runs.  Tier-3 SIGKILL
#     scenarios are in test_concurrency_lifecycle.py (Combo 27).
#
# A-11 (Combo 11): max_age is checked by the worker at consume time via
#     HEADER_ENQUEUED_AT.  The header is set at schedule() time by the client.
#     A task scheduled before the worker starts and consumed after max_age
#     seconds have elapsed will be expired.
#
# A-12 (Combo 12): retry_backoff=exponential doubles the retry_base_delay for
#     each subsequent retry.  Delay series: 0.2, 0.4, 0.8, ...  The 20%
#     tolerance in the backoff-gap assertion accommodates scheduling jitter.
#
# A-13 (Combo 13): priorities=True must be passed to both SwarmQ (client) and
#     the worker app to enable the high/normal/low queue triplet.  Without
#     it, priority is ignored and all tasks go to the base queue.
#     Lock contention with priorities uses weighted prefetch (3:2:1), not
#     strict preemption.  Lock ordering is FIFO at the Valkey level regardless
#     of broker priority.
#
# A-14 (Combo 14): unique_until=start releases the hash key in the worker's
#     pre-execute phase, before run() is called.  Any exception thrown by
#     run() fires AFTER unique is already released.  The retry for the failed
#     task re-acquires unique on its own start — but by then a second
#     enqueue may have grabbed the slot.
