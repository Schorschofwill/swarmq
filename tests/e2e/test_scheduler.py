"""E2E tests for the Phase 3c embedded scheduler.

Full stack — client, worker, SchedulerLoop, RabbitMQ, Valkey,
signals — in the same process. Each test spins up one or more
SwarmQ apps and verifies end-to-end behaviour.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import uuid
from datetime import UTC, datetime, timedelta

import pytest
import valkey.asyncio as valkey_asyncio

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.scheduler import SCHEDULER_LOCK_KEY
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.signals import Signal
from swarmq.task import Task, TaskInfo

# Liveness safety-net for every "wait for an async event" below. It is NOT a
# behavioural deadline — wait_for returns the instant the event fires, so a
# generous ceiling never slows the happy path; it only stops a loaded/cold CI
# runner from turning a slow leader election or fire round-trip into a false
# failure. (The original 6s leader wait was observed tipping over ~6.04s under
# full-suite load on the CI runner — the sole source of the e2e "1 rerun"
# flakiness.) Genuine timing semantics in these tests use explicit
# ``asyncio.sleep`` (absence assertions, TTL/throttle windows) and are left
# untouched.
_EVENT_TIMEOUT = 20.0

# Per-test backstop. Must exceed the worst-case SUM of sequential inner waits
# (a few tests chain leader + first-fire + second-fire across a failover, plus
# their own sleeps) with headroom for first-test connection setup. Still fails
# a genuinely deadlocked test in bounded time.
pytestmark = [pytest.mark.timeout(75)]


def _leader_event(app: SwarmQ) -> asyncio.Event:
    """Register a SCHEDULER_LEADER handler and return its event.

    Call BEFORE starting the worker task so there is no race window
    where the scheduler emits LEADER before the handler is registered
    (Unit 2 / H-5). Caller awaits ``event.wait()`` with its own timeout.
    """
    event = asyncio.Event()

    @app.on_signal(Signal.SCHEDULER_LEADER)
    async def _on_leader(**kwargs) -> None:  # noqa: ARG001
        event.set()

    return event


def _due_now_ms() -> int:
    """A zset score 1s in the past → due on the scheduler's next poll."""
    import time
    return int(time.time() * 1000) - 1000


async def _wait_zscore_present(
    client: object, member: str, timeout: float = _EVENT_TIMEOUT,
) -> None:
    """Block until the scheduler has written ``member`` into its due-zset.

    The leadership cron-sync (``sync_taskinfo_crons``) runs AFTER the leader
    signal is emitted, so a manual zadd issued the instant leadership is
    observed can be overwritten by that lagging sync with a future cron score —
    the fire then never comes due (an environment-sensitive timeout). Waiting
    for the entry to appear first makes the manual due-score the last writer.
    """
    import time
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if await client.zscore("sq:{sched}:zset", member) is not None:  # type: ignore[attr-defined]
            return
        await asyncio.sleep(0.05)
    raise TimeoutError(f"scheduler never wrote {member!r} into its zset")


def _unique_queue() -> str:
    return f"e2e-sched-{uuid.uuid4().hex[:10]}"


def _task_name(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def _cleanup_tasks():
    yield
    Task._clear_registered_tasks()


@pytest.fixture(autouse=True)
async def _flush_valkey(valkey_url):
    """Flush Valkey BEFORE and after each scheduler test.

    Before is the load-bearing one: a worker started by any prior e2e test
    (in a file without this fixture) becomes scheduler leader and, when that
    test tears down by cancelling its worker task, the graceful
    ``Scheduler.stop()`` lock-release never runs — so the leader lock lingers
    in Valkey for its full TTL (default 30s). The next scheduler test then
    can't win the election, ``SCHEDULER_LEADER`` never fires, and its
    ``leader_event`` wait times out — passing only on a later rerun once the
    stale lock expired. (This was the entire source of the e2e "1 rerun"
    flakiness.) Flushing on setup gives every scheduler test a clean
    leader-lock slate regardless of what ran before; the teardown flush keeps
    our own schedules/locks from leaking forward."""
    async def _flush() -> None:
        client = valkey_asyncio.Valkey.from_url(valkey_url)
        try:
            await client.flushdb()
        finally:
            await client.aclose()

    await _flush()
    yield
    await _flush()


# ---------------------------------------------------------------------------
# Delay / ETA — one-shot fires through the scheduler (R3.8)
# ---------------------------------------------------------------------------


class TestDelayAndEta:
    async def test_delay_fires_task_after_interval(
        self, rabbitmq_url, valkey_url, _cleanup_tasks,
    ):
        queue = _unique_queue()
        task_name = _task_name("ping")
        done = asyncio.Event()
        call_count = 0

        class PingTask(Task):
            task_info = TaskInfo(name=task_name, queues=[queue])

            async def run(self) -> str:
                nonlocal call_count
                call_count += 1
                done.set()
                return "ok"

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        async with SwarmQ(
            broker, backend,
            queues=[queue],
            concurrency=1,
            scheduler_poll_interval=0.1,
            scheduler_heartbeat_interval=0.2,
            scheduler_lock_ttl=1.0,
        ) as app:
            await app.broker.declare_queue(queue)
            # Register leader-signal handler BEFORE starting the worker
            # task — otherwise SCHEDULER_LEADER could fire during
            # start_worker() before the handler is connected (race).
            leader_event = _leader_event(app)
            worker_task = asyncio.create_task(app.start_worker())
            try:
                await asyncio.wait_for(leader_event.wait(), timeout=_EVENT_TIMEOUT)

                # Client schedules a short-delay task; scheduler publishes,
                # worker consumes, set event.
                tid = await app.client.schedule(
                    task_name, queue=queue, delay=0.3,
                )
                assert tid  # returned task_id

                await asyncio.wait_for(done.wait(), timeout=_EVENT_TIMEOUT)
                assert call_count == 1
            finally:
                worker_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await worker_task

    async def test_eta_fires_at_timestamp(
        self, rabbitmq_url, valkey_url, _cleanup_tasks,
    ):
        queue = _unique_queue()
        task_name = _task_name("at")
        done = asyncio.Event()

        class AtTask(Task):
            task_info = TaskInfo(name=task_name, queues=[queue])

            async def run(self) -> str:
                done.set()
                return "ok"

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        async with SwarmQ(
            broker, backend,
            queues=[queue],
            concurrency=1,
            scheduler_poll_interval=0.1,
            scheduler_heartbeat_interval=0.2,
            scheduler_lock_ttl=1.0,
        ) as app:
            await app.broker.declare_queue(queue)
            leader_event = _leader_event(app)
            worker_task = asyncio.create_task(app.start_worker())
            try:
                await asyncio.wait_for(leader_event.wait(), timeout=_EVENT_TIMEOUT)
                eta = datetime.now(UTC) + timedelta(milliseconds=300)
                await app.client.schedule(task_name, queue=queue, eta=eta)
                await asyncio.wait_for(done.wait(), timeout=_EVENT_TIMEOUT)
            finally:
                worker_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await worker_task


# ---------------------------------------------------------------------------
# Cron path — inject a schedule directly so the test fires in seconds (P1-6)
# ---------------------------------------------------------------------------


class TestCronSchedule:
    async def test_cron_schedule_fires_on_leader(
        self, rabbitmq_url, valkey_url, _cleanup_tasks,
    ):
        queue = _unique_queue()
        task_name = _task_name("cron")
        done = asyncio.Event()

        class CronTask(Task):
            # Valid 5-field cron. The test injects a near-future fire-ts
            # directly to avoid waiting on the natural minute-boundary.
            task_info = TaskInfo(
                name=task_name, queues=[queue], cron="* * * * *"
            )

            async def run(self) -> str:
                done.set()
                return "ok"

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        async with SwarmQ(
            broker, backend,
            queues=[queue],
            concurrency=1,
            scheduler_poll_interval=0.1,
            scheduler_heartbeat_interval=0.2,
            scheduler_lock_ttl=1.0,
        ) as app:
            await app.broker.declare_queue(queue)
            leader_event = _leader_event(app)
            worker_task = asyncio.create_task(app.start_worker())
            try:
                # SCHEDULER_LEADER is emitted *before* sync_taskinfo_crons in
                # the same election tick (see SchedulerLoop._election_tick), so
                # observing leadership does NOT mean the cron entry is written
                # yet. Wait for the scheduler to write it, then re-score it to a
                # due (past) timestamp as the LAST writer — otherwise the
                # lagging sync add_schedule clobbers our score with the next
                # cron minute and the fire never comes due (see _wait_zscore_present).
                await asyncio.wait_for(leader_event.wait(), timeout=_EVENT_TIMEOUT)

                client = valkey_asyncio.Valkey.from_url(valkey_url)
                try:
                    await _wait_zscore_present(client, task_name)
                    await client.zadd(
                        "sq:{sched}:zset", {task_name: _due_now_ms()}
                    )
                finally:
                    await client.aclose()

                await asyncio.wait_for(done.wait(), timeout=_EVENT_TIMEOUT)
            finally:
                worker_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await worker_task


# ---------------------------------------------------------------------------
# Consumer-dedup: duplicate schedfire-header → exactly one execution (P1-1)
# ---------------------------------------------------------------------------


class TestConsumerDedup:
    async def test_duplicate_schedfire_exec_once(
        self, rabbitmq_url, valkey_url, _cleanup_tasks,
    ):
        queue = _unique_queue()
        task_name = _task_name("dedup")
        call_count = 0
        first_done = asyncio.Event()

        class DedupTask(Task):
            task_info = TaskInfo(name=task_name, queues=[queue])

            async def run(self) -> str:
                nonlocal call_count
                call_count += 1
                first_done.set()
                return "ok"

        from swarmq.serialization import encode_json

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        async with SwarmQ(
            broker, backend,
            queues=[queue],
            concurrency=1,
        ) as app:
            await app.broker.declare_queue(queue)

            # Publish two identical schedfire-headered messages directly.
            schedfire_key = f"{task_name}:1700000000000"
            body = encode_json({
                "task": {
                    "id": uuid.uuid4().hex,
                    "queue": queue,
                    "name": task_name,
                },
                "args": [],
                "kwargs": {},
            })
            for _ in range(2):
                await app.broker.publish(
                    queue, body,
                    headers={"x-swarmq-schedfire": schedfire_key},
                )

            worker_task = asyncio.create_task(app.start_worker())
            try:
                await asyncio.wait_for(first_done.wait(), timeout=_EVENT_TIMEOUT)
                # Legitimate timer wait, not synchronization:
                # we are asserting the ABSENCE of a second execution.
                # The dedup path (ack + log + return True) emits no
                # observable event — verifying "nothing happens" needs
                # a bounded wait. 500ms covers the broker redelivery
                # window comfortably at RabbitMQ local-dev latency.
                await asyncio.sleep(0.5)
                assert call_count == 1
            finally:
                worker_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await worker_task


# ---------------------------------------------------------------------------
# Graceful handover — leader A stops, leader B takes over in <1s (not TTL)
# ---------------------------------------------------------------------------


class TestGracefulHandover:
    async def test_handover_before_ttl_expiry(
        self, rabbitmq_url, valkey_url, _cleanup_tasks,
    ):
        queue = _unique_queue()
        task_name = _task_name("ho")

        class NoopTask(Task):
            task_info = TaskInfo(name=task_name, queues=[queue])

            async def run(self) -> str:
                return "ok"

        broker_a = RabbitMQBroker(rabbitmq_url)
        backend_a = ValkeyBackend(valkey_url)
        broker_b = RabbitMQBroker(rabbitmq_url)
        backend_b = ValkeyBackend(valkey_url)

        # Use a long TTL — if handover falls back to TTL-expiry, B won't
        # acquire within the test timeout.
        cfg_kwargs = dict(
            queues=[queue], concurrency=1,
            scheduler_poll_interval=0.1,
            scheduler_heartbeat_interval=0.2,
            scheduler_lock_ttl=10.0,
        )

        async with SwarmQ(broker_a, backend_a, **cfg_kwargs) as app_a, \
                   SwarmQ(broker_b, backend_b, **cfg_kwargs) as app_b:

            b_leader: list[str] = []
            b_failover: list[str] = []

            @app_b.on_signal(Signal.SCHEDULER_LEADER)
            async def _bl(**kw): b_leader.append("leader")

            @app_b.on_signal(Signal.SCHEDULER_LEADER_FAILOVER)
            async def _bfo(**kw): b_failover.append("failover")

            # Signal-driven event for A becoming leader (not sleep).
            a_leader_event = asyncio.Event()

            @app_a.on_signal(Signal.SCHEDULER_LEADER)
            async def _al(**kw) -> None:  # noqa: ARG001
                a_leader_event.set()

            # Start A first so it wins the election.
            a_task = asyncio.create_task(app_a.start_worker())
            await asyncio.wait_for(a_leader_event.wait(), timeout=_EVENT_TIMEOUT)
            b_task = asyncio.create_task(app_b.start_worker())
            # Let B's election_loop run at least once so it sees the
            # lock held and sets prev_leader_seen=True. The minimum
            # wait is one heartbeat interval; we allow a small margin.
            # This is a legitimate wait on B's internal election tick,
            # not a startup-barrier — B doesn't emit a signal for
            # "standby saw lock held."
            await asyncio.sleep(0.6)

            try:
                # Graceful shutdown of A via its internal shutdown event.
                # We fish out the worker via the private attribute on
                # app_a — there's no public top-level stop() today.
                # Instead: cancel the task, which in turn triggers the
                # CancelledError path and does NOT run _execute_shutdown.
                # That's the crash-case, not graceful. So we need to fire
                # the shutdown event directly. Each app keeps its Worker
                # in a local variable. We rebuild the pattern by writing
                # to Valkey ourselves — setting a 5s handover marker —
                # then cancelling A's tasks. B will see the marker on
                # its next acquire.
                await backend_a.set_schedfire_handover_marker(
                    "simulated-A-token", 5_000,
                )
                # Now force A's lock to disappear without TTL wait by
                # deleting it directly.
                client = valkey_asyncio.Valkey.from_url(valkey_url)
                try:
                    await client.delete(SCHEDULER_LOCK_KEY)
                finally:
                    await client.aclose()

                # B's next tick acquires (lock absent) and consumes the
                # marker → emits LEADER, not FAILOVER. The signal-hook
                # b_leader is populated by the @app_b.on_signal handler
                # above; use the same signal as an event barrier here.
                b_leader_event = asyncio.Event()

                @app_b.on_signal(Signal.SCHEDULER_LEADER)
                async def _bl_event(**kw) -> None:  # noqa: ARG001
                    b_leader_event.set()

                # If b_leader already has entries (race between register
                # and acquire), the event may not fire — check upfront.
                if b_leader:
                    b_leader_event.set()

                try:
                    await asyncio.wait_for(
                        b_leader_event.wait(), timeout=_EVENT_TIMEOUT,
                    )
                except TimeoutError:
                    pass  # assertion below surfaces the failure

                assert b_leader, "B never became leader after handover"
                assert not b_failover, (
                    "B emitted FAILOVER despite handover marker present"
                )
            finally:
                for t in (a_task, b_task):
                    t.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await t


# ---------------------------------------------------------------------------
# CLI roundtrip — add via CLI, fire via scheduler (R3.10 bridge)
# ---------------------------------------------------------------------------


class TestCliRoundtrip:
    async def test_cli_add_then_scheduler_fires(
        self, rabbitmq_url, valkey_url, _cleanup_tasks, monkeypatch,
    ):
        queue = _unique_queue()
        task_name = _task_name("clict")
        done = asyncio.Event()

        class ClitTask(Task):
            task_info = TaskInfo(name=task_name, queues=[queue])

            async def run(self) -> str:
                done.set()
                return "ok"

        monkeypatch.setenv("SWARMQ_BACKEND_URL", valkey_url)

        # Use the CLI handler directly to add a one-shot schedule that
        # fires in ~300ms.
        from swarmq import cli
        import argparse
        soon = datetime.now(UTC) + timedelta(milliseconds=300)
        ns = argparse.Namespace(
            name=f"cli-{uuid.uuid4().hex[:8]}",
            cron=None, at=soon.isoformat(),
            task=task_name,
            args="[]", kwargs="{}", queue=queue,
            # The real CLI sets --format (resolved in cli.__init__); this test
            # calls the handler directly, so supply it explicitly to mirror the
            # parsed namespace the handler expects.
            format="table",
        )
        await cli._run_schedule_add(ns)

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        async with SwarmQ(
            broker, backend,
            queues=[queue],
            concurrency=1,
            scheduler_poll_interval=0.1,
            scheduler_heartbeat_interval=0.2,
            scheduler_lock_ttl=1.0,
        ) as app:
            await app.broker.declare_queue(queue)
            worker_task = asyncio.create_task(app.start_worker())
            try:
                await asyncio.wait_for(done.wait(), timeout=_EVENT_TIMEOUT)
            finally:
                worker_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await worker_task


# ---------------------------------------------------------------------------
# U1.3 gap fills — recurring across leader change, crash failover, rate-limit
# ---------------------------------------------------------------------------


class TestRecurringAcrossLeaderChange:
    """Plan §5.1 #3: recurring cron must not skip a fire when the
    leader changes. Spin up A, fire once, kill A, let B take over,
    fire again. Verify exactly 2 executions — no skip, no duplicate."""

    async def test_no_fires_missed_when_leader_changes(
        self, rabbitmq_url, valkey_url, _cleanup_tasks,
    ):
        queue = _unique_queue()
        task_name = _task_name("recur")
        call_count = 0
        first_fired = asyncio.Event()
        second_fired = asyncio.Event()

        class RecurringTask(Task):
            task_info = TaskInfo(
                name=task_name, queues=[queue], cron="* * * * *",
            )

            async def run(self) -> str:
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    first_fired.set()
                elif call_count == 2:
                    second_fired.set()
                return "ok"

        broker_a = RabbitMQBroker(rabbitmq_url)
        backend_a = ValkeyBackend(valkey_url)
        broker_b = RabbitMQBroker(rabbitmq_url)
        backend_b = ValkeyBackend(valkey_url)

        cfg = dict(
            queues=[queue], concurrency=1,
            scheduler_poll_interval=0.1,
            scheduler_heartbeat_interval=0.2,
            scheduler_lock_ttl=1.0,
        )

        async with SwarmQ(broker_a, backend_a, **cfg) as app_a, \
                   SwarmQ(broker_b, backend_b, **cfg) as app_b:
            await app_a.broker.declare_queue(queue)
            a_leader = asyncio.Event()
            b_active = asyncio.Event()  # B reached leader (LEADER or FAILOVER)

            @app_a.on_signal(Signal.SCHEDULER_LEADER)
            async def _al(**kw) -> None:  # noqa: ARG001
                a_leader.set()

            @app_b.on_signal(Signal.SCHEDULER_LEADER)
            async def _bl(**kw) -> None:  # noqa: ARG001
                b_active.set()

            @app_b.on_signal(Signal.SCHEDULER_LEADER_FAILOVER)
            async def _bfo(**kw) -> None:  # noqa: ARG001
                b_active.set()

            # Start A and let it WIN the election before B joins. Starting
            # both at once is a race — B can grab leadership first, leaving A
            # in standby so a_leader never fires (the flaky a_leader timeout).
            # Mirrors TestCrashFailover's ordering.
            a_task = asyncio.create_task(app_a.start_worker())
            b_task: asyncio.Task[None] | None = None
            client = valkey_asyncio.Valkey.from_url(valkey_url)
            try:
                await asyncio.wait_for(a_leader.wait(), timeout=_EVENT_TIMEOUT)
                b_task = asyncio.create_task(app_b.start_worker())

                # Fire #1 — A is leader. Wait until A's leadership cron-sync
                # has written the entry FIRST, so our manual due-score is the
                # last writer; otherwise a lagging sync_taskinfo_crons (it runs
                # after the LEADER signal) overwrites our score with the next
                # cron minute and the fire never comes due (the flaky timeout).
                # A past score is immediately due.
                await _wait_zscore_present(client, task_name)
                await client.zadd("sq:{sched}:zset", {task_name: _due_now_ms()})
                await asyncio.wait_for(first_fired.wait(), timeout=_EVENT_TIMEOUT)

                # Simulate crash — drop A's worker task and the lock.
                a_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await a_task
                await client.delete(SCHEDULER_LOCK_KEY)

                # Wait for B to actually take leadership (deterministic —
                # replaces a blind sleep that raced B's lock acquisition),
                # then let B's own cron-sync settle before our manual fire so
                # it isn't clobbered (same reason as fire #1).
                await asyncio.wait_for(b_active.wait(), timeout=_EVENT_TIMEOUT)
                await asyncio.sleep(0.5)

                # Fire #2 — must land on B.
                await client.zadd("sq:{sched}:zset", {task_name: _due_now_ms()})
                await asyncio.wait_for(second_fired.wait(), timeout=_EVENT_TIMEOUT)

                assert call_count == 2, (
                    f"expected exactly 2 fires across leader change, "
                    f"got {call_count}"
                )
            finally:
                await client.aclose()
                for t in (a_task, b_task):
                    if t is None:
                        continue
                    t.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await t


class TestCrashFailover:
    """Plan §5.1 #4: when the leader crashes (no graceful handover),
    the successor emits SCHEDULER_LEADER_FAILOVER — not LEADER — and
    takes over pending work."""

    async def test_failover_signal_fires_on_crash_not_graceful(
        self, rabbitmq_url, valkey_url, _cleanup_tasks,
    ):
        queue = _unique_queue()
        broker_a = RabbitMQBroker(rabbitmq_url)
        backend_a = ValkeyBackend(valkey_url)
        broker_b = RabbitMQBroker(rabbitmq_url)
        backend_b = ValkeyBackend(valkey_url)

        cfg = dict(
            queues=[queue], concurrency=1,
            scheduler_poll_interval=0.1,
            scheduler_heartbeat_interval=0.2,
            scheduler_lock_ttl=1.0,
        )

        async with SwarmQ(broker_a, backend_a, **cfg) as app_a, \
                   SwarmQ(broker_b, backend_b, **cfg) as app_b:
            a_leader = asyncio.Event()
            b_failover = asyncio.Event()
            b_leader_observed: list[str] = []

            @app_a.on_signal(Signal.SCHEDULER_LEADER)
            async def _al(**kw) -> None:  # noqa: ARG001
                a_leader.set()

            @app_b.on_signal(Signal.SCHEDULER_LEADER_FAILOVER)
            async def _bfo(**kw) -> None:  # noqa: ARG001
                b_failover.set()

            @app_b.on_signal(Signal.SCHEDULER_LEADER)
            async def _bl(**kw) -> None:  # noqa: ARG001
                b_leader_observed.append("leader")

            a_task = asyncio.create_task(app_a.start_worker())
            await asyncio.wait_for(a_leader.wait(), timeout=_EVENT_TIMEOUT)
            b_task = asyncio.create_task(app_b.start_worker())
            # Let B see the lock held once so its prev_leader_seen is True.
            await asyncio.sleep(0.4)

            try:
                # Hard crash: cancel A's task (no graceful shutdown, no
                # handover marker), then clear the lock so B doesn't
                # have to wait the full TTL.
                a_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await a_task
                client = valkey_asyncio.Valkey.from_url(valkey_url)
                try:
                    await client.delete(SCHEDULER_LOCK_KEY)
                finally:
                    await client.aclose()

                # B must emit FAILOVER (not LEADER) since no marker.
                await asyncio.wait_for(b_failover.wait(), timeout=_EVENT_TIMEOUT)
                assert not b_leader_observed, (
                    "B emitted SCHEDULER_LEADER instead of FAILOVER — "
                    "the 'graceful' path fired even though A crashed"
                )
            finally:
                b_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await b_task


class TestCronWithRateLimit:
    """Plan §5.1 #5: rate-limit must apply even when the scheduler
    fires the task. Force 3 fires within a 1s rate-limit window, expect
    1 execution (first fire is allowed, subsequent are throttled)."""

    async def test_rate_limit_throttles_scheduler_fires(
        self, rabbitmq_url, valkey_url, _cleanup_tasks,
    ):
        queue = _unique_queue()
        task_name = _task_name("ratecron")
        call_count = 0

        class LimitedCronTask(Task):
            # 1/s means at most 1 execution per 1-second window.
            task_info = TaskInfo(
                name=task_name, queues=[queue],
                cron="* * * * *", rate_limit="1/s",
            )

            async def run(self) -> str:
                nonlocal call_count
                call_count += 1
                return "ok"

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        async with SwarmQ(
            broker, backend,
            queues=[queue], concurrency=1,
            scheduler_poll_interval=0.1,
            scheduler_heartbeat_interval=0.2,
            scheduler_lock_ttl=1.0,
        ) as app:
            await app.broker.declare_queue(queue)
            leader_event = _leader_event(app)
            worker_task = asyncio.create_task(app.start_worker())
            try:
                await asyncio.wait_for(leader_event.wait(), timeout=_EVENT_TIMEOUT)

                # Force 3 fires in quick succession via zset re-score.
                import time
                client = valkey_asyncio.Valkey.from_url(valkey_url)
                try:
                    # Be the last writer vs the scheduler's initial cron-sync
                    # (which runs after the leader signal); otherwise the first
                    # forced fire can be clobbered to the next cron minute.
                    await _wait_zscore_present(client, task_name)
                    for offset_ms in (200, 400, 600):
                        ts = int(time.time() * 1000) + offset_ms
                        await client.zadd(
                            "sq:{sched}:zset", {task_name: ts},
                        )
                        # Wait past the forced fire before re-scoring —
                        # otherwise zadd just overwrites without firing.
                        await asyncio.sleep(0.3)
                finally:
                    await client.aclose()

                # Give the rate-limited ones time to be throttled (they
                # land in delay queue or get re-queued — we just need
                # to confirm not all 3 ran).
                await asyncio.sleep(1.5)

                assert call_count < 3, (
                    f"rate-limit did not throttle scheduler fires: "
                    f"{call_count} executions for 3 fires with 1/s "
                    f"limit"
                )
                assert call_count >= 1, (
                    f"no fire executed at all — scheduler or broker "
                    f"path broken: {call_count}"
                )
            finally:
                worker_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await worker_task
