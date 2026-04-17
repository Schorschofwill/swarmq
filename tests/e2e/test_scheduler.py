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

# E2E tests spin up real brokers + scheduler + worker, wait for
# leader election, publish a schedule, and wait for it to fire.
# 20s covers: leader election (6s inner wait), fire round-trip
# (5s inner wait), plus broker/Valkey connection latency that can
# spike on the first E2E test after a long integration run. Still
# fails fast on genuine deadlocks (inner waits never resolve → 11s
# sum; 20s gives connection setup 9s of headroom).
pytestmark = [pytest.mark.timeout(20)]


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
    """Flush Valkey keys between E2E tests so schedules from a prior
    test don't leak into the next scheduler loop."""
    yield
    client = valkey_asyncio.Valkey.from_url(valkey_url)
    try:
        await client.flushdb()
    finally:
        await client.aclose()


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
                await asyncio.wait_for(leader_event.wait(), timeout=6)

                # Client schedules a short-delay task; scheduler publishes,
                # worker consumes, set event.
                tid = await app.client.schedule(
                    task_name, queue=queue, delay=0.3,
                )
                assert tid  # returned task_id

                await asyncio.wait_for(done.wait(), timeout=5)
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
                await asyncio.wait_for(leader_event.wait(), timeout=6)
                eta = datetime.now(UTC) + timedelta(milliseconds=300)
                await app.client.schedule(task_name, queue=queue, eta=eta)
                await asyncio.wait_for(done.wait(), timeout=5)
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
                # Wait for leader election (SCHEDULER_LEADER fires *after*
                # sync_taskinfo_crons runs — see SchedulerLoop._election_tick).
                await asyncio.wait_for(leader_event.wait(), timeout=6)

                # The sync loaded the cron with its natural-next fire (in
                # up to 60s). Re-score it to "now + 300ms" so the poll
                # picks it up quickly.
                client = valkey_asyncio.Valkey.from_url(valkey_url)
                try:
                    import time
                    soon_ms = int(time.time() * 1000) + 300
                    await client.zadd(
                        "sq:sched:zset", {task_name: soon_ms}
                    )
                finally:
                    await client.aclose()

                await asyncio.wait_for(done.wait(), timeout=5)
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
                await asyncio.wait_for(first_done.wait(), timeout=5)
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
            await asyncio.wait_for(a_leader_event.wait(), timeout=3)
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
                    await asyncio.wait_for(b_leader_event.wait(), timeout=3)
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
                await asyncio.wait_for(done.wait(), timeout=5)
            finally:
                worker_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await worker_task
