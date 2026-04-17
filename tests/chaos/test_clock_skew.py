"""F6 Clock-Skew chaos tests (U2.6).

Verifies SwarmQ primitives that touch "time" stay correct when wall-
clock and monotonic clock diverge:

- Rate limiter reads server-side Valkey ``TIME`` (not Python wall-
  clock) so a ``freezegun.freeze_time`` jump on the client side leaves
  the sliding window intact.
- ``update_progress`` throttle uses ``time.monotonic`` — freezegun
  does NOT patch monotonic by default, so frozen wall-clock leaves
  the throttle alive. We additionally monkey-patch monotonic with
  controlled values for a direct regression test.
- Dedup flag (``unique_for``) TTL is stored server-side via
  ``PX`` in Valkey; a client-side clock jump does not shorten it.
- Scheduler's zset/ZREM semantics fire each entry exactly once
  regardless of a simulated NTP resync on the scheduler's side.

The chaos harness is stock RabbitMQ + Valkey — no toxic injection is
required here (time-mocking is in-process). The libfaketime
``LD_PRELOAD`` approach for subprocess workers sketched in the plan
is deferred to U3.x stress.
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
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.config import SwarmQConfig
from swarmq.locking import LockManager
from swarmq.scheduler import (
    ScheduleDefinition,
    ScheduleStore,
    SchedulerLoop,
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


@contextlib.asynccontextmanager
async def _running_worker(rabbitmq_url: str, valkey_url: str, queue: str):
    """Spawn a worker and block until WORKER_ONLINE.

    Mirrors the pattern from ``tests/e2e/test_unique_tasks.py`` so the
    clock-skew tests converge on the same "wait-on-signal, no sleeps"
    synchronisation contract (AGENTS.md).
    """
    broker = RabbitMQBroker(rabbitmq_url)
    backend = ValkeyBackend(valkey_url)
    app = SwarmQ(broker, backend, queues=[queue])

    online = asyncio.Event()

    @app.on_signal(Signal.WORKER_ONLINE)
    async def _on_online(**_: Any) -> None:
        online.set()

    worker_task = asyncio.create_task(app.start_worker())
    try:
        await asyncio.wait_for(online.wait(), timeout=10.0)
        yield app
    finally:
        worker_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker_task
        await broker.close()
        await backend.close()


# ---------------------------------------------------------------------------
# Test 1 — Rate-limit window is immune to wall-clock jumps
# ---------------------------------------------------------------------------


class TestRateLimitClockSkew:
    """Rate limiter reads Valkey ``TIME`` server-side (see limits.py:109).
    A client-side ``freeze_time`` + jump-backward therefore leaves the
    sliding-window counter untouched — the third schedule inside the
    same server-side second still trips the limit.
    """

    async def test_rate_limit_uses_server_time_immune_to_wallclock_jump(
        self, chaos_stack, _cleanup_tasks,
    ):
        queue = _unique_queue()
        task_name = _unique_name("rl_skew")
        executed: list[int] = []
        rate_limited = 0
        completed = asyncio.Event()

        class RLTask(Task):
            task_info = TaskInfo(
                name=task_name, queues=[queue], rate_limit="2/s",
            )

            async def run(self, idx: int) -> int:
                executed.append(idx)
                # All 3 messages eventually run — rate-limiter requeues,
                # it does not drop. Second arrival of idx=2 after the
                # window shift is the terminal condition.
                if len(executed) >= 3:
                    completed.set()
                return idx

        client_broker = RabbitMQBroker(chaos_stack.rabbitmq_url)
        client_backend = ValkeyBackend(chaos_stack.valkey_url)
        client_app = SwarmQ(
            client_broker, client_backend, queues=[queue],
        )

        async with client_app:
            await client_broker.declare_queue(queue)

            async with _running_worker(
                chaos_stack.rabbitmq_url, chaos_stack.valkey_url, queue,
            ) as worker_app:

                @worker_app.on_signal(Signal.TASK_RATE_LIMITED)
                async def _on_rl(**_: Any) -> None:
                    nonlocal rate_limited
                    rate_limited += 1

                # Baseline: schedule two messages while both clocks are real.
                await client_app.client.schedule(task_name, 0)
                await client_app.client.schedule(task_name, 1)

                # Freeze Python wall-clock and jump BACKWARD 60s. The
                # rate-limiter calls valkey.TIME server-side, so this
                # jump is invisible to the sliding window.
                with freezegun.freeze_time("2020-01-01 12:00:00") as frozen:
                    # Sanity: Python wall-clock was indeed jumped.
                    import time as _time
                    wall = _time.time()
                    assert wall < 1_600_000_000, (
                        f"freezegun did not shift wall-clock (got {wall})"
                    )
                    # Ship a third message — must be rate-limited at
                    # least once since the server-side window still
                    # remembers the two fresh claims.
                    await client_app.client.schedule(task_name, 2)
                    frozen.tick(delta=0.01)  # silence unused warning

                # Allow the rate-limiter to requeue + eventually fire.
                # 2/s window → third message delayed by ~1s.
                await asyncio.wait_for(completed.wait(), timeout=5.0)

        assert len(executed) == 3, (
            f"expected all 3 tasks to eventually run (got {executed})"
        )
        assert rate_limited >= 1, (
            "expected at least one TASK_RATE_LIMITED signal — the "
            "server-side window should have throttled the 3rd message "
            "despite the frozen Python wall-clock"
        )


# ---------------------------------------------------------------------------
# Test 2 — Unique-for TTL is server-side (PX); client clock jump is irrelevant
# ---------------------------------------------------------------------------


class TestUniqueForServerTTL:
    """``setnx_unique`` issues a Valkey ``SET ... PX`` — TTL is measured
    by the Valkey server clock. A worker/client whose wall-clock is
    frozen or jumped still sees the dedup flag alive and dedupes the
    duplicate schedule.
    """

    async def test_unique_for_ttl_uses_valkey_server_time(
        self, chaos_stack, _cleanup_tasks,
    ):
        queue = _unique_queue()
        task_name = _unique_name("u_skew")
        executions: list[int] = []
        dup_signals: list[dict[str, Any]] = []

        class UTask(Task):
            task_info = TaskInfo(
                name=task_name, queues=[queue],
                unique_for=30,  # 30s server-side TTL
            )

            async def run(self, n: int) -> int:
                executions.append(n)
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

            # First schedule — claims the dedup flag (TTL px=30s).
            first_id = await client_app.client.schedule(task_name, 42)
            assert first_id, "first schedule must return a task id"

            # Freeze worker's wall-clock 60s into the future. Server-
            # side the TTL still has ~30s left, so the duplicate must
            # be deduped (returning the same task_id).
            future = "2099-06-15 14:00:00"
            with freezegun.freeze_time(future):
                dup_id = await client_app.client.schedule(task_name, 42)

            assert dup_id == first_id, (
                f"duplicate schedule returned a DIFFERENT id "
                f"({dup_id} != {first_id}) — the client clock jump "
                f"should NOT have invalidated the server-side TTL flag"
            )
            assert len(dup_signals) == 1, (
                f"expected exactly one TASK_DUPLICATE_DETECTED signal, "
                f"got {len(dup_signals)}"
            )
            assert dup_signals[0]["existing_task_id"] == first_id


# ---------------------------------------------------------------------------
# Test 3 — update_progress throttle uses monotonic clock (direct unit test)
# ---------------------------------------------------------------------------


class _FakeBackend:
    """Minimal stand-in for ValkeyBackend used by ``Task.update_progress``.

    Records set_progress + publish_progress calls; no network. The
    signature matches the two methods the throttle touches.
    """

    def __init__(self) -> None:
        self.sets: list[tuple[str, dict[str, str]]] = []
        self.publishes: list[tuple[str, bytes]] = []

    async def set_progress(
        self, task_id: str, fields: dict[str, str], ttl_ms: int,
    ) -> None:
        self.sets.append((task_id, dict(fields)))

    async def publish_progress(self, task_id: str, payload: bytes) -> None:
        self.publishes.append((task_id, payload))


class TestProgressThrottleMonotonic:
    """Direct Task() instantiation — no worker, no broker. Monkey-patch
    ``time.monotonic`` in ``swarmq.task`` and verify the 500ms throttle
    behaves purely against monotonic (ignores ``time.time``).
    """

    async def test_progress_throttle_uses_monotonic_clock(
        self, monkeypatch, _cleanup_tasks,
    ):
        # Register a task class so Task.__init_subclass__ validates cleanly.
        q = _unique_queue()

        class ProgTask(Task):
            task_info = TaskInfo(name=_unique_name("prog"), queues=[q])

            async def run(self) -> None:
                return None

        task = ProgTask()
        fake_backend = _FakeBackend()
        task._backend = fake_backend  # type: ignore[assignment]
        task._task_id = "tid-1"
        task._result_ttl_ms = 60_000

        # Controlled monotonic clock: start at t=100.0, tick forward in
        # tests as needed. We patch the module-global `time` object so
        # both update_progress() reads (loop_time, elapsed) hit the fake.
        fake_mono = {"now": 100.0}

        def _mono() -> float:
            return fake_mono["now"]

        import swarmq.task as task_mod
        monkeypatch.setattr(task_mod.time, "monotonic", _mono)

        # Freeze Python wall-clock too — to prove the throttle does NOT
        # consult it. If a regression accidentally swapped monotonic for
        # time(), the throttle would still fire because a frozen wall-
        # clock shows 0 delta and the throttle would block every update.
        # With the correct monotonic-based throttle we can still advance
        # updates by ticking fake_mono.
        with freezegun.freeze_time("2030-01-01 00:00:00"):
            # First update — should go through (instance default is 0.0).
            await task.update_progress(1, 10)
            assert len(fake_backend.sets) == 1, (
                "first update must publish regardless of throttle"
            )

            # 0.2s monotonic delta — below 500ms threshold → throttled.
            fake_mono["now"] = 100.2
            await task.update_progress(2, 10)
            assert len(fake_backend.sets) == 1, (
                "update at +0.2s must be throttled (<500ms)"
            )

            # 0.6s monotonic delta from last write → allowed.
            fake_mono["now"] = 100.6
            await task.update_progress(3, 10)
            assert len(fake_backend.sets) == 2, (
                f"update at +0.6s must go through "
                f"(got {len(fake_backend.sets)} sets)"
            )

            # Final flush — current >= total bypasses throttle unless
            # already flushed.
            fake_mono["now"] = 100.61  # barely any delta
            await task.update_progress(10, 10)
            assert len(fake_backend.sets) == 3, (
                "final update (current==total) must always flush"
            )

            # Redundant final — guarded by _progress_final_flushed.
            await task.update_progress(10, 10)
            assert len(fake_backend.sets) == 3, (
                "redundant final update must be suppressed"
            )


# ---------------------------------------------------------------------------
# Test 4 — Scheduler: exactly one fire per zset entry across a "NTP resync"
# ---------------------------------------------------------------------------


class TestSchedulerNtpResync:
    """A cron schedule whose zset score we manipulate to ``fire NOW``:
    even with a wall-clock freeze around the fire + a subsequent
    unfreeze (simulating an NTP correction), the scheduler publishes
    exactly one message per zset entry (ZREM is unconditional via
    sched_commit.lua).
    """

    async def test_scheduler_cron_handles_wallclock_resync(
        self, chaos_stack, _cleanup_tasks,
    ):
        queue = _unique_queue()
        schedule_name = _unique_name("cron_resync")

        broker = RabbitMQBroker(chaos_stack.rabbitmq_url)
        await broker.connect()
        backend = ValkeyBackend(chaos_stack.valkey_url)
        await backend.connect()
        await broker.declare_queue(queue)

        lock_manager = LockManager(backend._client)
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

        # Observe published messages on the target queue directly —
        # no worker — so we can assert "exactly one fire".
        received_ids: list[str] = []
        msg_event = asyncio.Event()

        import aio_pika
        connection = await aio_pika.connect_robust(
            chaos_stack.rabbitmq_url, timeout=10,
        )
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=1)
        amqp_queue = await channel.declare_queue(queue, durable=True)

        async def _consume(message: aio_pika.abc.AbstractIncomingMessage) -> None:
            async with message.process(requeue=False):
                import msgspec
                payload = msgspec.json.decode(message.body)
                received_ids.append(payload["task"]["id"])
                msg_event.set()

        consumer_tag = await amqp_queue.consume(_consume)

        try:
            # Persist a cron schedule that is "due now" — zset score
            # manipulated via a direct add_schedule with fire_ts_ms=0.
            definition = ScheduleDefinition(
                schedule_name=schedule_name,
                task_name="does_not_matter",
                args=(),
                kwargs={},
                queue=queue,
                cron="* * * * *",  # every minute — scheduler advances it
                fire_ts_ms=0,  # -> ZADD score=0 so poll_due hits immediately
                source="static",
            )
            assert await store.add_schedule(definition) is True

            # Run the scheduler loop across a freeze/unfreeze window.
            await loop.start()
            try:
                # First: wait for the initial fire (wall-clock is real).
                await asyncio.wait_for(msg_event.wait(), timeout=5.0)
                first_count = len(received_ids)
                assert first_count == 1, (
                    f"scheduler fired {first_count}x on first due-poll, "
                    f"expected exactly 1"
                )

                # Simulate an NTP resync: freeze wall-clock 30s into
                # the future then unfreeze. sched_commit.lua has
                # already advanced the zset score via the live cron
                # iterator, so even two more poll-ticks should NOT
                # re-fire the same entry (the old score is gone).
                msg_event.clear()
                with freezegun.freeze_time("2099-12-31 23:59:00"):
                    await asyncio.sleep(0.4)  # ~4 poll ticks at 0.1s
                # Unfreeze (resync to real wall-clock) and give the loop
                # a few more ticks to re-poll.
                await asyncio.sleep(0.4)
            finally:
                await loop.stop()

            # Exactly one fire for that single zset entry.
            assert len(received_ids) == 1, (
                f"scheduler re-fired (total={len(received_ids)}) the "
                f"same cron entry across a wall-clock resync; "
                f"expected exactly 1"
            )
        finally:
            await amqp_queue.cancel(consumer_tag)
            await channel.close()
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
        "support; in-process freezegun covers the invariants here."
    ),
)
def test_worker_subprocess_clock_skew_libfaketime() -> None:
    """Placeholder — see plan §U2.6 Execution Note."""
