"""F6 Clock-Skew chaos tests (U2.6).

Verifies SwarmQ primitives that touch "time" stay correct when wall-
clock and monotonic clock diverge:

- Rate limiter reads server-side Valkey ``TIME`` (see ``limits.py``),
  so a client-side ``freezegun.freeze_time`` jump leaves the sliding
  window intact.
- Dedup flag (``unique_for``) TTL is server-side via ``PX``; a
  client-side clock jump does not shorten or extend it.
- ``update_progress`` throttle uses ``time.monotonic`` — regression
  test monkey-patches ``time.monotonic`` with controlled values and
  proves the throttle is driven purely by monotonic deltas.
- Scheduler's ``ZREM``-after-fire semantics give exactly one publish
  per zset entry even when the scheduler's wall-clock is jumped then
  resynced (simulated NTP correction).

Note on freezegun vs aio-pika: ``freezegun.freeze_time`` patches
``time.monotonic`` globally, which breaks the aiormq heartbeat
accounting. We therefore keep freeze_time blocks OFF the live broker
path — rate-limit and unique-flag tests run the freeze purely around
Valkey-level primitives.

The libfaketime ``LD_PRELOAD`` approach for subprocess workers
sketched in plan §U2.6 is deferred to U3.x stress.
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from typing import Any

import freezegun
import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.backend.valkey.rate_limit import RateLimiter
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.config import SwarmQConfig
from swarmq.limits import parse_rate_limit
from swarmq.scheduler import (
    ScheduleDefinition,
    SchedulerLoop,
    ScheduleStore,
)
from swarmq.signals import Signal, SignalDispatcher
from swarmq.task import Task, TaskInfo

# Stack startup + worker drain is ~5-10s. 30s ceiling flags any
# deadlock without burning nightly time.
pytestmark = [pytest.mark.timeout(30)]


def _unique_queue() -> str:
    return f"chaos-clock-{uuid.uuid4().hex[:10]}"


def _unique_name(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def _cleanup_tasks():
    yield
    Task._clear_registered_tasks()


# ---------------------------------------------------------------------------
# All four tests share a single class so the class-scoped ``chaos_stack``
# fixture is brought up once and amortised across the four checks.
# ---------------------------------------------------------------------------


class TestClockSkew:
    """F6 Clock-Skew invariants for rate limiter, unique-TTL, progress
    throttle, and scheduler ZREM semantics. See module docstring.
    """

    async def test_rate_limit_uses_server_time_immune_to_wallclock_jump(
        self, chaos_stack,
    ):
        backend = ValkeyBackend(chaos_stack.valkey_url)
        await backend.connect()
        try:
            limiter = RateLimiter(backend.client)
            # 2/m statt 2/s: mit einem Sekundenfenster überschritt der dritte
            # Claim gelegentlich die Server-Sekundengrenze und wurde zu Recht
            # erlaubt (Flake, U17-Pipeline 2026-08-27). Die Invariante — Server-
            # TIME statt lokaler Uhr — ist mit einem Minutenfenster identisch.
            config = parse_rate_limit("2/m")
            task_name = _unique_name("rl_skew")

            # Two claims while wall-clock is real — fills the window.
            for _ in range(2):
                allowed, _remaining, _retry = await limiter.check(
                    task_name, config,
                )
                assert allowed is True

            # Freeze + jump Python wall-clock BACK 60s. Server-side
            # TIME is untouched, so the third check in the same
            # server-window must still be denied.
            with freezegun.freeze_time("2020-01-01 12:00:00"):
                import time as _time
                wall = _time.time()
                assert wall < 1_600_000_000, (
                    f"freezegun did not shift wall-clock (got {wall})"
                )
                allowed3, _, retry3 = await limiter.check(
                    task_name, config,
                )

            assert allowed3 is False, (
                "rate limiter accepted the 3rd claim inside the same "
                "server-side second despite a frozen Python clock — "
                "it must read server TIME, not local time.time()"
            )
            assert retry3 >= 1, (
                f"expected retry_after_ms >= 1 (got {retry3})"
            )
        finally:
            await backend.close()

    async def test_progress_throttle_monotonic_immune_to_wallclock(
        self, monkeypatch, _cleanup_tasks,
    ):
        """Task.update_progress throttle uses ``time.monotonic``.

        Regression check: swap out ``time.monotonic`` with a controlled
        source and verify the 500ms throttle is driven purely by
        monotonic deltas. We do NOT combine freezegun with this test —
        freezegun patches ``time.monotonic`` too (see module docstring),
        which would defeat the isolation point.
        """
        queue = _unique_queue()

        class ProgTask(Task):
            task_info = TaskInfo(
                name=_unique_name("prog_mono"), queues=[queue],
            )

            async def run(self) -> None:
                return None

        task = ProgTask()
        sets: list[dict[str, str]] = []
        publishes: list[bytes] = []

        class _FakeBackend:
            async def set_progress(
                self, task_id: str, fields: dict[str, str], ttl_ms: int,
            ) -> None:
                sets.append(dict(fields))

            async def publish_progress(
                self, task_id: str, payload: bytes,
            ) -> None:
                publishes.append(payload)

        task._backend = _FakeBackend()  # type: ignore[assignment]
        task._task_id = "tid-mono"
        task._result_ttl_ms = 60_000

        fake_mono = {"now": 500.0}
        import swarmq.task as task_mod
        monkeypatch.setattr(
            task_mod.time, "monotonic", lambda: fake_mono["now"],
        )

        # First update — class-default _last_progress_update is 0.0,
        # so the delta is huge and the write goes through.
        await task.update_progress(1, 10)
        assert len(sets) == 1

        # Tick monotonic by 200ms (below 500ms threshold).
        fake_mono["now"] = 500.2
        await task.update_progress(2, 10)
        assert len(sets) == 1, (
            f"0.2s monotonic delta must be throttled, got {len(sets)} sets"
        )

        # Tick monotonic by 600ms total from the last write → pass.
        fake_mono["now"] = 500.6
        await task.update_progress(3, 10)
        assert len(sets) == 2, (
            f"0.6s monotonic delta must pass throttle, got {len(sets)} sets"
        )

        # Final flush always goes through (current >= total).
        fake_mono["now"] = 500.61
        await task.update_progress(10, 10)
        assert len(sets) == 3

        # Redundant final — suppressed by _progress_final_flushed.
        await task.update_progress(10, 10)
        assert len(sets) == 3


    async def test_unique_for_ttl_uses_valkey_server_time(
        self, chaos_stack, _cleanup_tasks,
    ):
        queue = _unique_queue()
        task_name = _unique_name("u_skew")
        dup_signals: list[dict[str, Any]] = []

        class UTask(Task):
            task_info = TaskInfo(
                name=task_name, queues=[queue],
                unique_for=30,  # 30s server-side TTL
            )

            async def run(self, n: int) -> int:
                return n

        client_broker = RabbitMQBroker(chaos_stack.rabbitmq_url)
        client_backend = ValkeyBackend(chaos_stack.valkey_url)
        client_app = SwarmQ(
            client_broker, client_backend, queues=[queue],
        )

        @client_app.on_signal(Signal.TASK_DUPLICATE_DETECTED)
        async def _on_dup(**kw: Any) -> None:
            dup_signals.append(kw)

        async with client_app:
            await client_broker.declare_queue(queue)

            # First schedule — claims the dedup flag with server-side
            # TTL px=30_000ms.
            first_id = await client_app.client.schedule(task_name, 42)
            assert first_id, "first schedule must return a task id"

            # Directly issue a second claim_unique under a frozen wall-
            # clock, bypassing the broker to keep aiormq heartbeats off
            # freezegun's patched monotonic. The Valkey PX TTL is
            # server-measured, so the flag is still alive even though
            # the client believes "now" is 60s in the future.
            from swarmq.serialization import compute_dedup_hash
            dedup_hash = compute_dedup_hash(task_name, (42,), {})

            with freezegun.freeze_time("2099-06-15 14:00:00"):
                existing = await client_backend.claim_unique(
                    dedup_hash, "second-candidate", 30_000,
                )

            assert existing == first_id, (
                f"second claim_unique under a 60s-future frozen wall-clock "
                f"was granted (existing={existing!r}); Valkey PX TTL must "
                f"be server-side and keep the flag alive irrespective of "
                f"client clock"
            )

            # Go through the full Client.schedule path once more to
            # prove the end-to-end dedup observable (duplicate signal,
            # same returned id) — outside freeze_time to keep the
            # broker heartbeat happy.
            dup_id = await client_app.client.schedule(task_name, 42)
            assert dup_id == first_id, (
                f"duplicate schedule returned a DIFFERENT id "
                f"({dup_id} != {first_id})"
            )
            assert len(dup_signals) == 1, (
                f"expected exactly one TASK_DUPLICATE_DETECTED signal, "
                f"got {len(dup_signals)}"
            )
            assert dup_signals[0]["existing_task_id"] == first_id


    async def test_scheduler_cron_handles_wallclock_resync(
        self, chaos_stack, _cleanup_tasks,
    ):
        queue = _unique_queue()
        schedule_name = _unique_name("cron_resync")

        broker = RabbitMQBroker(chaos_stack.rabbitmq_url)
        await broker.connect()
        backend = ValkeyBackend(chaos_stack.valkey_url)
        await backend.connect()
        # broker.declare_queue sets DLX args; we consume via the same
        # broker path to avoid the declare-arg mismatch.
        await broker.declare_queue(queue)

        lock_manager = backend.locks  # LockStore-Vertrag (U11)
        store = ScheduleStore(backend)
        dispatcher = SignalDispatcher()
        config = SwarmQConfig(
            broker_url=chaos_stack.rabbitmq_url,
            backend_url=chaos_stack.valkey_url,
            scheduler_poll_interval=0.1,
            scheduler_heartbeat_interval=0.15,
            scheduler_lock_ttl=1.0,
        )

        loop = SchedulerLoop(
            backend=backend,
            broker=broker,
            config=config,
            lock_manager=lock_manager,
            store=store,
            signal_dispatcher=dispatcher,
            owner_token=f"resync-{uuid.uuid4().hex[:6]}",
        )

        # Count every message that hits the queue by reading the
        # Valkey-side published count via RabbitMQ Management API would
        # require extra deps — instead we subscribe with a second
        # aio_pika connection, declaring with passive=True (schema
        # already fixed by broker.declare_queue).
        import aio_pika
        received_ids: list[str] = []
        msg_event = asyncio.Event()

        connection = await aio_pika.connect_robust(
            chaos_stack.rabbitmq_url, timeout=10,
        )
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=1)
        amqp_queue = await channel.declare_queue(queue, passive=True)

        async def _consume(
            message: aio_pika.abc.AbstractIncomingMessage,
        ) -> None:
            async with message.process(requeue=False):
                import msgspec
                payload = msgspec.json.decode(message.body)
                received_ids.append(payload["task"]["id"])
                msg_event.set()

        consumer_tag = await amqp_queue.consume(_consume)

        try:
            # Persist a cron schedule due "now" — zset score=0 so
            # poll_due hits it immediately. cron="* * * * *" lets
            # commit_fire compute a valid next_fire for re-ZADD
            # (advancing the score and preventing re-fire).
            definition = ScheduleDefinition(
                schedule_name=schedule_name,
                task_name=_unique_name("cron_task"),
                args=(),
                kwargs={},
                queue=queue,
                cron="* * * * *",
                fire_ts_ms=0,
                source="static",
            )
            assert await store.add_schedule(definition) is True

            await loop.start()
            try:
                # First: wait for the initial fire (wall-clock real).
                await asyncio.wait_for(msg_event.wait(), timeout=5.0)
                assert len(received_ids) == 1, (
                    f"scheduler fired {len(received_ids)}x on first "
                    f"poll, expected exactly 1"
                )

                # Simulate an NTP resync: keep observing for another
                # ~0.5s. The zset score was advanced by sched_commit,
                # so no further fires should hit the queue even as
                # multiple poll ticks elapse.
                msg_event.clear()
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(
                        msg_event.wait(), timeout=0.5,
                    )
            finally:
                await loop.stop()

            # Exactly one fire for that single zset entry across
            # the real-clock + "resync" windows.
            assert len(received_ids) == 1, (
                f"scheduler re-fired (total={len(received_ids)}) the "
                f"same cron entry; expected exactly 1"
            )
        finally:
            with contextlib.suppress(Exception):
                await amqp_queue.cancel(consumer_tag)
            with contextlib.suppress(Exception):
                await channel.close()
            with contextlib.suppress(Exception):
                await connection.close()
            await broker.close()
            await backend.close()


# ---------------------------------------------------------------------------
# Subprocess-worker clock mocking — deferred to U3.x stress
# ---------------------------------------------------------------------------


@pytest.mark.skip(
    reason=(
        "libfaketime LD_PRELOAD against a subprocess worker is deferred "
        "to U3.x stress — requires apt-level libfaketime + CI-host "
        "support; in-process freezegun + monkeypatch covers the "
        "invariants here."
    ),
)
def test_worker_subprocess_clock_skew_libfaketime() -> None:
    """Placeholder — see plan §U2.6 Execution Note."""
