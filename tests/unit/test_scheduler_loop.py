"""Unit tests for swarmq.scheduler.SchedulerLoop.

Mock-based coverage for the election, heartbeat, poll, and
sync_taskinfo_crons paths. Integration with 2 real workers lives in
tests/integration/test_scheduler_leader_election.py.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.config import SwarmQConfig
from swarmq.exceptions import BackendConnectionError
from swarmq.scheduler import (
    SCHEDULER_LOCK_NAME,
    ScheduleDefinition,
    SchedulerLoop,
)
from swarmq.signals import Signal, SignalDispatcher
from swarmq.task import Task, TaskInfo


@pytest.fixture
def config():
    return SwarmQConfig(
        broker_url="amqp://localhost",
        backend_url="valkey://localhost",
        # Short intervals so tests finish fast
        scheduler_poll_interval=0.05,
        scheduler_heartbeat_interval=0.05,
        scheduler_lock_ttl=1.0,
    )


@pytest.fixture
def backend():
    be = MagicMock()
    be.hget_schedule_def = AsyncMock(return_value=None)
    be.hset_schedule_def = AsyncMock()
    be.hdel_schedule_def = AsyncMock()
    be.hgetall_schedule_defs = AsyncMock(return_value={})
    be.zadd_schedule = AsyncMock()
    be.zrem_schedule = AsyncMock()
    be.set_schedfire_handover_marker = AsyncMock()
    be.get_and_del_schedfire_handover_marker = AsyncMock(return_value=False)
    return be


@pytest.fixture
def broker():
    b = MagicMock()
    b.publish = AsyncMock()
    return b


@pytest.fixture
def lock_manager():
    lm = MagicMock()
    lm.acquire = AsyncMock(return_value=True)
    lm.renew = AsyncMock(return_value=True)
    lm.release = AsyncMock(return_value=True)
    return lm


@pytest.fixture
def store():
    s = MagicMock()
    s.poll_due = AsyncMock(return_value=[])
    s.commit_fire = AsyncMock(return_value=True)
    s.add_schedule = AsyncMock(return_value=True)
    s.update_static_schedule = AsyncMock()
    return s


@pytest.fixture
def signal_dispatcher():
    return SignalDispatcher()


@pytest.fixture
def _cleanup_tasks():
    yield
    Task._clear_registered_tasks()


def _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher,
               owner_token="test-owner"):
    return SchedulerLoop(
        backend=backend,
        broker=broker,
        config=config,
        lock_manager=lock_manager,
        store=store,
        signal_dispatcher=signal_dispatcher,
        owner_token=owner_token,
        worker_id="w-1",
    )


def _def(name="cron_a", *, cron="*/5 * * * *", fire_ts_ms=1_700_000_000_000,
         source="static", disabled=False) -> ScheduleDefinition:
    return ScheduleDefinition(
        schedule_name=name,
        task_name="ping",
        args=(),
        kwargs={},
        queue="default",
        cron=cron,
        fire_ts_ms=fire_ts_ms,
        source=source,
        disabled=disabled,
    )


# ---------------------------------------------------------------------------
# Election loop
# ---------------------------------------------------------------------------


class TestElectionLoop:
    async def test_standby_acquire_success_emits_leader(
        self, backend, broker, config, lock_manager, store, signal_dispatcher
    ):
        events: list[tuple[Signal, dict]] = []
        @signal_dispatcher.on_signal(Signal.SCHEDULER_LEADER)
        async def _on_leader(**kwargs):
            events.append((Signal.SCHEDULER_LEADER, kwargs))

        loop = _make_loop(
            backend, broker, config, lock_manager, store, signal_dispatcher
        )
        lock_manager.acquire.return_value = True
        backend.get_and_del_schedfire_handover_marker.return_value = False

        await loop._election_tick()

        assert loop.state == "leader"
        assert any(e[0] is Signal.SCHEDULER_LEADER for e in events)
        lock_manager.acquire.assert_awaited_once()

    async def test_standby_acquire_fails_no_state_change(
        self, backend, broker, config, lock_manager, store, signal_dispatcher
    ):
        loop = _make_loop(
            backend, broker, config, lock_manager, store, signal_dispatcher
        )
        lock_manager.acquire.return_value = False

        await loop._election_tick()

        assert loop.state == "standby"
        assert loop._prev_leader_seen is True

    async def test_failover_emits_when_prior_leader_seen_without_marker(
        self, backend, broker, config, lock_manager, store, signal_dispatcher
    ):
        """Standby sees the lock held first, then later acquires it
        after TTL-expiry → emit FAILOVER."""
        events: list[Signal] = []
        @signal_dispatcher.on_signal(Signal.SCHEDULER_LEADER_FAILOVER)
        async def _on_fo(**kwargs):
            events.append(Signal.SCHEDULER_LEADER_FAILOVER)

        loop = _make_loop(
            backend, broker, config, lock_manager, store, signal_dispatcher
        )
        # First tick: lock held by someone else
        lock_manager.acquire.return_value = False
        await loop._election_tick()
        assert loop._prev_leader_seen is True

        # Second tick: acquire wins, no handover marker → failover
        lock_manager.acquire.return_value = True
        backend.get_and_del_schedfire_handover_marker.return_value = False
        await loop._election_tick()
        assert events == [Signal.SCHEDULER_LEADER_FAILOVER]

    async def test_clean_handover_emits_leader_not_failover(
        self, backend, broker, config, lock_manager, store, signal_dispatcher
    ):
        """Standby first saw the lock held (prev_leader_seen=True), then
        acquire succeeds AND handover marker was consumed → LEADER
        (clean handover), not FAILOVER."""
        leader_events: list[Signal] = []
        @signal_dispatcher.on_signal(Signal.SCHEDULER_LEADER)
        async def _l(**kw):
            leader_events.append(Signal.SCHEDULER_LEADER)
        fo_events: list[Signal] = []
        @signal_dispatcher.on_signal(Signal.SCHEDULER_LEADER_FAILOVER)
        async def _fo(**kw):
            fo_events.append(Signal.SCHEDULER_LEADER_FAILOVER)

        loop = _make_loop(
            backend, broker, config, lock_manager, store, signal_dispatcher
        )
        lock_manager.acquire.return_value = False
        await loop._election_tick()
        assert loop._prev_leader_seen is True

        lock_manager.acquire.return_value = True
        backend.get_and_del_schedfire_handover_marker.return_value = True
        await loop._election_tick()

        assert leader_events == [Signal.SCHEDULER_LEADER]
        assert fo_events == []

    async def test_leader_renew_success_emits_heartbeat(
        self, backend, broker, config, lock_manager, store, signal_dispatcher
    ):
        hb: list[Signal] = []
        @signal_dispatcher.on_signal(Signal.SCHEDULER_HEARTBEAT)
        async def _h(**kw):
            hb.append(Signal.SCHEDULER_HEARTBEAT)

        loop = _make_loop(
            backend, broker, config, lock_manager, store, signal_dispatcher
        )
        loop._state = "leader"
        lock_manager.renew.return_value = True

        await loop._election_tick()

        assert hb == [Signal.SCHEDULER_HEARTBEAT]
        lock_manager.renew.assert_awaited_once()

    async def test_leader_renew_lost_transitions_to_standby(
        self, backend, broker, config, lock_manager, store, signal_dispatcher
    ):
        loop = _make_loop(
            backend, broker, config, lock_manager, store, signal_dispatcher
        )
        loop._state = "leader"
        lock_manager.renew.return_value = False

        await loop._election_tick()

        assert loop.state == "standby"
        assert loop._prev_leader_seen is True


# ---------------------------------------------------------------------------
# Poll loop
# ---------------------------------------------------------------------------


class TestPollLoop:
    async def test_standby_poll_is_noop(
        self, backend, broker, config, lock_manager, store, signal_dispatcher
    ):
        loop = _make_loop(
            backend, broker, config, lock_manager, store, signal_dispatcher
        )
        # state is "standby" by default — _poll_tick should not even
        # run inside _poll_loop. Cover via direct branch:
        # run the top-level _poll_loop briefly.
        task = asyncio.create_task(loop._poll_loop())
        await asyncio.sleep(0.12)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        store.poll_due.assert_not_awaited()

    async def test_leader_poll_fires_due_schedules(
        self, backend, broker, config, lock_manager, store, signal_dispatcher
    ):
        loop = _make_loop(
            backend, broker, config, lock_manager, store, signal_dispatcher
        )
        loop._state = "leader"
        d = _def(name="cron_a", cron="*/5 * * * *", fire_ts_ms=1_000)
        store.poll_due.return_value = [("cron_a", 1_000, d)]

        await loop._poll_tick()

        broker.publish.assert_awaited_once()
        call = broker.publish.await_args
        # First positional arg = queue, second = body, headers=kwarg
        assert call.args[0] == "default"
        headers = call.kwargs.get("headers") or {}
        assert "x-swarmq-schedfire" in headers
        assert headers["x-swarmq-schedfire"].startswith("cron_a:")

        store.commit_fire.assert_awaited_once()
        commit_args = store.commit_fire.await_args
        # Positional: schedule_name, next_fire_ts, updated_def, owner_token
        assert commit_args.args[0] == "cron_a"
        # next_fire_ts should be a positive future-ts
        assert commit_args.args[1] > 0
        # owner_token is passed through
        assert commit_args.args[3] == "test-owner"

    async def test_oneshot_commit_uses_neg_one_and_no_def(
        self, backend, broker, config, lock_manager, store, signal_dispatcher
    ):
        loop = _make_loop(
            backend, broker, config, lock_manager, store, signal_dispatcher
        )
        loop._state = "leader"
        d = _def(name="oneshot:abc", cron=None, fire_ts_ms=1_000,
                 source="cli")
        store.poll_due.return_value = [("oneshot:abc", 1_000, d)]

        await loop._poll_tick()

        commit_args = store.commit_fire.await_args
        assert commit_args.args[1] == -1  # one-shot sentinel
        assert commit_args.args[2] is None  # no updated_def

    async def test_disabled_schedule_is_not_fired(
        self, backend, broker, config, lock_manager, store, signal_dispatcher
    ):
        loop = _make_loop(
            backend, broker, config, lock_manager, store, signal_dispatcher
        )
        loop._state = "leader"
        d = _def(disabled=True)
        store.poll_due.return_value = [(d.schedule_name, 1_000, d)]

        await loop._poll_tick()

        broker.publish.assert_not_awaited()
        store.commit_fire.assert_not_awaited()

    async def test_publish_failure_skips_commit(
        self, backend, broker, config, lock_manager, store, signal_dispatcher
    ):
        """If broker.publish raises, we must NOT commit — re-poll will
        pick the schedule up again and the consumer-dedup handles the
        rare case where publish actually succeeded past a network blip."""
        loop = _make_loop(
            backend, broker, config, lock_manager, store, signal_dispatcher
        )
        loop._state = "leader"
        d = _def(cron="*/5 * * * *", fire_ts_ms=1_000)
        store.poll_due.return_value = [(d.schedule_name, 1_000, d)]
        broker.publish.side_effect = RuntimeError("conn refused")

        await loop._poll_tick()

        store.commit_fire.assert_not_awaited()

    async def test_commit_owner_mismatch_transitions_to_standby(
        self, backend, broker, config, lock_manager, store, signal_dispatcher
    ):
        """Lost lock mid-batch → commit returns False → transition to
        standby so the remaining items in the batch are skipped."""
        loop = _make_loop(
            backend, broker, config, lock_manager, store, signal_dispatcher
        )
        loop._state = "leader"
        d1 = _def(name="first", cron="*/5 * * * *", fire_ts_ms=1_000)
        d2 = _def(name="second", cron="*/5 * * * *", fire_ts_ms=2_000)
        store.poll_due.return_value = [
            ("first", 1_000, d1),
            ("second", 2_000, d2),
        ]
        # first commit returns 0 (owner-mismatch), second shouldn't be
        # reached because state transitioned to standby
        store.commit_fire.side_effect = [False, False]

        await loop._poll_tick()

        # Only one commit attempt before aborting
        assert store.commit_fire.await_count == 1
        assert loop.state == "standby"


# ---------------------------------------------------------------------------
# stop() / graceful handover
# ---------------------------------------------------------------------------


class TestStop:
    async def test_stop_as_leader_emits_handover_and_releases(
        self, backend, broker, config, lock_manager, store, signal_dispatcher
    ):
        events: list[Signal] = []
        @signal_dispatcher.on_signal(Signal.SCHEDULER_LEADER_HANDOVER)
        async def _h(**kw):
            events.append(Signal.SCHEDULER_LEADER_HANDOVER)

        loop = _make_loop(
            backend, broker, config, lock_manager, store, signal_dispatcher
        )
        await loop.start()
        loop._state = "leader"

        await loop.stop()

        assert events == [Signal.SCHEDULER_LEADER_HANDOVER]
        backend.set_schedfire_handover_marker.assert_awaited_once()
        lock_manager.release.assert_awaited_once()
        assert loop.state == "stopped"

    async def test_stop_as_standby_does_not_emit_handover(
        self, backend, broker, config, lock_manager, store, signal_dispatcher
    ):
        events: list[Signal] = []
        @signal_dispatcher.on_signal(Signal.SCHEDULER_LEADER_HANDOVER)
        async def _h(**kw):
            events.append(Signal.SCHEDULER_LEADER_HANDOVER)

        loop = _make_loop(
            backend, broker, config, lock_manager, store, signal_dispatcher
        )
        await loop.start()
        # state stays "standby"

        await loop.stop()

        assert events == []
        backend.set_schedfire_handover_marker.assert_not_awaited()
        lock_manager.release.assert_not_awaited()


# ---------------------------------------------------------------------------
# sync_taskinfo_crons (Fork 1)
# ---------------------------------------------------------------------------


class TestSyncTaskinfoCrons:
    async def test_sync_inserts_new_static_crons(
        self, backend, broker, config, lock_manager, store, signal_dispatcher,
        _cleanup_tasks,
    ):
        class Cron5m(Task):
            task_info = TaskInfo(name="cron_5m", queues=["default"],
                                 cron="*/5 * * * *")
            async def run(self):
                return None

        loop = _make_loop(
            backend, broker, config, lock_manager, store, signal_dispatcher
        )
        backend.hget_schedule_def.return_value = None  # fresh

        await loop.sync_taskinfo_crons()

        store.add_schedule.assert_awaited()

    async def test_sync_noop_for_same_static_cron(
        self, backend, broker, config, lock_manager, store, signal_dispatcher,
        _cleanup_tasks,
    ):
        class SameCron(Task):
            task_info = TaskInfo(name="same", queues=["default"],
                                 cron="*/5 * * * *")
            async def run(self):
                return None

        existing = ScheduleDefinition(
            schedule_name="same", task_name="same", args=(), kwargs={},
            queue="default", cron="*/5 * * * *",
            fire_ts_ms=1_700_000_000_000, source="static",
        )
        import msgspec
        backend.hget_schedule_def.return_value = msgspec.json.encode(existing)

        loop = _make_loop(
            backend, broker, config, lock_manager, store, signal_dispatcher
        )
        await loop.sync_taskinfo_crons()

        store.add_schedule.assert_not_awaited()
        store.update_static_schedule.assert_not_awaited()

    async def test_sync_updates_on_static_cron_change(
        self, backend, broker, config, lock_manager, store, signal_dispatcher,
        _cleanup_tasks,
    ):
        class ChangedCron(Task):
            task_info = TaskInfo(name="changed", queues=["default"],
                                 cron="*/10 * * * *")
            async def run(self):
                return None

        existing = ScheduleDefinition(
            schedule_name="changed", task_name="changed", args=(), kwargs={},
            queue="default", cron="*/5 * * * *",  # OLD
            fire_ts_ms=1_700_000_000_000, source="static",
        )
        import msgspec
        backend.hget_schedule_def.return_value = msgspec.json.encode(existing)

        loop = _make_loop(
            backend, broker, config, lock_manager, store, signal_dispatcher
        )
        await loop.sync_taskinfo_crons()

        store.update_static_schedule.assert_awaited_once()
        store.add_schedule.assert_not_awaited()

    async def test_sync_preserves_disabled_flag_on_static_cron_change(
        self, backend, broker, config, lock_manager, store, signal_dispatcher,
        _cleanup_tasks,
    ):
        """Review-finding correctness C-01: an operator-disabled static
        schedule must remain disabled after the cron string is modified
        in code. Without this preservation, the next redeploy silently
        reverses the operator's `schedule remove` intent."""
        class ChangedDisabledCron(Task):
            task_info = TaskInfo(name="disabled_static", queues=["default"],
                                 cron="*/10 * * * *")
            async def run(self):
                return None

        existing = ScheduleDefinition(
            schedule_name="disabled_static", task_name="disabled_static",
            args=(), kwargs={}, queue="default",
            cron="*/5 * * * *",          # OLD cron — differs from code
            fire_ts_ms=1_700_000_000_000,
            source="static",
            disabled=True,                # operator disabled this entry
        )
        import msgspec
        backend.hget_schedule_def.return_value = msgspec.json.encode(existing)

        loop = _make_loop(
            backend, broker, config, lock_manager, store, signal_dispatcher
        )
        await loop.sync_taskinfo_crons()

        store.update_static_schedule.assert_awaited_once()
        propagated = store.update_static_schedule.await_args.args[0]
        assert propagated.disabled is True
        assert propagated.cron == "*/10 * * * *"  # new cron applied

    async def test_sync_disabled_false_stays_false_on_cron_change(
        self, backend, broker, config, lock_manager, store, signal_dispatcher,
        _cleanup_tasks,
    ):
        """Inverse regression: the preservation path must not flip a
        disabled=False entry to True."""
        class ActiveCron(Task):
            task_info = TaskInfo(name="active_static", queues=["default"],
                                 cron="0 */4 * * *")
            async def run(self):
                return None

        existing = ScheduleDefinition(
            schedule_name="active_static", task_name="active_static",
            args=(), kwargs={}, queue="default",
            cron="0 3 * * *",
            fire_ts_ms=1_700_000_000_000,
            source="static",
            disabled=False,
        )
        import msgspec
        backend.hget_schedule_def.return_value = msgspec.json.encode(existing)

        loop = _make_loop(
            backend, broker, config, lock_manager, store, signal_dispatcher
        )
        await loop.sync_taskinfo_crons()

        propagated = store.update_static_schedule.await_args.args[0]
        assert propagated.disabled is False

    async def test_sync_preserves_cli_source_even_on_cron_diff(
        self, backend, broker, config, lock_manager, store, signal_dispatcher,
        _cleanup_tasks,
    ):
        """CD11: CLI ownership beats code — even when code has a
        different cron, we must not overwrite a CLI entry."""
        class CronTask(Task):
            task_info = TaskInfo(name="conflict", queues=["default"],
                                 cron="*/10 * * * *")
            async def run(self):
                return None

        existing = ScheduleDefinition(
            schedule_name="conflict", task_name="conflict", args=(),
            kwargs={}, queue="default", cron="*/5 * * * *",
            fire_ts_ms=1_700_000_000_000, source="cli",  # CLI entry
        )
        import msgspec
        backend.hget_schedule_def.return_value = msgspec.json.encode(existing)

        loop = _make_loop(
            backend, broker, config, lock_manager, store, signal_dispatcher
        )
        await loop.sync_taskinfo_crons()

        store.update_static_schedule.assert_not_awaited()
        store.add_schedule.assert_not_awaited()
