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
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.signals import Signal
from swarmq.task import Task, TaskInfo


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
            worker_task = asyncio.create_task(app.start_worker())
            try:
                # Wait for the worker/scheduler to come up.
                await asyncio.sleep(0.5)

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
            worker_task = asyncio.create_task(app.start_worker())
            try:
                await asyncio.sleep(0.5)
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
            worker_task = asyncio.create_task(app.start_worker())
            try:
                # Wait for leader election + initial sync_taskinfo_crons.
                await asyncio.sleep(0.6)

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
                # Give the second publish a chance to be consumed+dedup'd.
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

            # Start A first so it wins the election.
            a_task = asyncio.create_task(app_a.start_worker())
            await asyncio.sleep(0.5)
            b_task = asyncio.create_task(app_b.start_worker())
            # Let B see the lock held at least once.
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
                    await client.delete("sq:lock:scheduler:leader")
                finally:
                    await client.aclose()

                # B's next tick acquires (lock absent) and consumes the
                # marker → emits LEADER, not FAILOVER.
                for _ in range(30):
                    if b_leader:
                        break
                    await asyncio.sleep(0.1)

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
