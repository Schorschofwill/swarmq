"""SchedulerLoop-Tests (Plan U11) — verbatim: Election (LockStore-Token-
API), Poll/Fire, Schedfire-Dedup (``claim_unique``), Stop/Abort."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from swarmq.config import SwarmQConfig
from swarmq.exceptions import BackendConnectionError
from swarmq.scheduler import (
    ScheduleDefinition,
)
from swarmq.signals import Signal
from swarmq.task import Task, TaskInfo
from tests.unit.scheduler._helpers import _LEADER_TOKEN, _cron_def, _def, _make_loop

# AGENTS.md §Async-Code testen rule 3: 5s per-test timeout. Narrower
# than the pyproject default (30s); fails fast on deadlock without
# burning CI time (Unit 3 / H-6).
pytestmark = [pytest.mark.timeout(5)]




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
        lock_manager.acquire.return_value = _LEADER_TOKEN
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
        lock_manager.acquire.return_value = None

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
        lock_manager.acquire.return_value = None
        await loop._election_tick()
        assert loop._prev_leader_seen is True

        # Second tick: acquire wins, no handover marker → failover
        lock_manager.acquire.return_value = _LEADER_TOKEN
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
        lock_manager.acquire.return_value = None
        await loop._election_tick()
        assert loop._prev_leader_seen is True

        lock_manager.acquire.return_value = _LEADER_TOKEN
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

    async def test_renew_exception_steps_down_after_threshold(
        self, backend, broker, config, lock_manager, store, signal_dispatcher
    ):
        """Härtung R2 P1-5: ein Renew-NETZWERKFEHLER ließ den Leader im
        Amt ('return', state bleibt leader) — bei einer Valkey-Störung
        länger als die Lock-TTL feuerten alter UND neuer Leader parallel,
        über die gesamte Störungsdauer. Nach konsekutiven Renew-Fehlern
        muss der Leader proaktiv heruntertreten, BEVOR die TTL sicher
        abgelaufen ist."""
        standby_events: list[dict] = []

        @signal_dispatcher.on_signal(Signal.SCHEDULER_STANDBY)
        async def _s(**kw):
            standby_events.append(kw)

        loop = _make_loop(
            backend, broker, config, lock_manager, store, signal_dispatcher
        )
        loop._state = "leader"
        lock_manager.renew.side_effect = BackendConnectionError("valkey down")

        limit = loop._renew_failure_limit
        assert limit >= 1
        for _ in range(limit - 1):
            await loop._election_tick()
            assert loop.state == "leader", (
                "must tolerate failures below the threshold"
            )

        await loop._election_tick()

        assert loop.state == "standby"
        assert len(standby_events) == 1

    async def test_renew_error_counter_resets_on_success(
        self, backend, broker, config, lock_manager, store, signal_dispatcher
    ):
        """Ein einzelner Renew-Fehler gefolgt von Erfolg setzt den
        Zähler zurück — kein schleichender Step-down über Stunden."""
        loop = _make_loop(
            backend, broker, config, lock_manager, store, signal_dispatcher
        )
        loop._state = "leader"
        limit = loop._renew_failure_limit

        # One failure short of the limit, then a success, then again
        # one failure short — never steps down.
        seq: list = []
        for _ in range(limit - 1):
            seq.append(BackendConnectionError("blip"))
        seq.append(True)
        for _ in range(limit - 1):
            seq.append(BackendConnectionError("blip"))
        lock_manager.renew.side_effect = seq

        for _ in range(len(seq)):
            await loop._election_tick()

        assert loop.state == "leader"

    async def test_renew_failure_limit_derived_from_ttl_and_heartbeat(
        self, backend, broker, lock_manager, store, signal_dispatcher
    ):
        """Limit = max(1, ttl//heartbeat - 1): Step-down passiert einen
        Heartbeat VOR dem sicheren TTL-Ablauf."""
        config = SwarmQConfig(
            broker_url="amqp://localhost",
            backend_url="valkey://localhost",
            scheduler_heartbeat_interval=10.0,
            scheduler_lock_ttl=30.0,
        )
        loop = _make_loop(
            backend, broker, config, lock_manager, store, signal_dispatcher
        )
        assert loop._renew_failure_limit == 2


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

    async def test_schedfire_key_uses_zset_score_not_definition_ts(
        self, backend, broker, config, lock_manager, store, signal_dispatcher
    ):
        """Unit 7 / M-11: the schedfire-dedup key must be derived from
        the authoritative zset score returned by poll_due, not from
        definition.fire_ts_ms which can diverge during update_static_schedule."""
        loop = _make_loop(
            backend, broker, config, lock_manager, store, signal_dispatcher
        )
        loop._state = "leader"
        # Simulate divergence: zset score says 1000, definition thinks 9999.
        # The dedup key must reflect 1000 (what consumers will see on
        # redelivery once the broker carries the header forward).
        d = _def(name="drift", cron="*/5 * * * *", fire_ts_ms=9_999)
        store.poll_due.return_value = [("drift", 1_000, d)]

        await loop._poll_tick()

        headers = broker.publish.await_args.kwargs.get("headers") or {}
        assert headers["x-swarmq-schedfire"] == "drift:1000"

    async def test_unregistered_task_fire_logs_warning(
        self, backend, broker, config, lock_manager, store, signal_dispatcher,
        _cleanup_tasks,
    ):
        """Härtung R2 P2: ein Schedule, dessen Task umbenannt/entfernt
        wurde, publiziert kommentarlos ins Leere. Warn-only (kein Skip):
        in Multi-Modul-Deployments kann der Task in der Registry eines
        ANDEREN Workers existieren — ein harter Skip wäre dort falsch.
        Die Warnung gibt dem Operator das Signal, das vorher fehlte."""
        from unittest.mock import patch

        loop = _make_loop(
            backend, broker, config, lock_manager, store, signal_dispatcher
        )
        loop._state = "leader"
        d = _def(name="ghost", cron="*/5 * * * *", fire_ts_ms=1_000)
        store.poll_due.return_value = [("ghost", 1_000, d)]

        with patch("swarmq.scheduler.loop.logger") as mock_logger:
            await loop._poll_tick()

        # Publish still happens (warn-only), but the operator sees it.
        broker.publish.assert_awaited_once()
        warned_events = [
            c.args[0] for c in mock_logger.warning.call_args_list
        ]
        assert "scheduler_task_not_in_local_registry" in warned_events

    async def test_fire_skipped_signal_on_missed_cron_fires(
        self, backend, broker, config, lock_manager, store, signal_dispatcher
    ):
        """Härtung R2 P2: Catch-up-skip ist korrekt (genau ein
        Nachhol-Fire), aber bisher unsichtbar — ein übersprungener
        täglicher Report fiel niemandem auf. Beim Catch-up muss
        SCHEDULER_FIRE_SKIPPED mit skipped_count emittiert werden."""
        skipped: list[dict] = []

        @signal_dispatcher.on_signal(Signal.SCHEDULER_FIRE_SKIPPED)
        async def _sk(**kw):
            skipped.append(kw)

        loop = _make_loop(
            backend, broker, config, lock_manager, store, signal_dispatcher
        )
        loop._state = "leader"
        # Every-minute cron whose due score lies ~10 minutes in the
        # past → ~9 fires were missed during the downtime.
        now_ms = int(datetime.now(UTC).timestamp() * 1000)
        stale_score = now_ms - 10 * 60 * 1000
        d = _def(name="minutely", cron="* * * * *", fire_ts_ms=stale_score)
        store.poll_due.return_value = [("minutely", stale_score, d)]

        await loop._poll_tick()

        assert len(skipped) == 1
        assert skipped[0]["schedule_name"] == "minutely"
        assert skipped[0]["skipped_count"] >= 8
        # Exactly ONE catch-up publish — no backlog storm.
        broker.publish.assert_awaited_once()

    async def test_no_fire_skipped_signal_on_punctual_fire(
        self, backend, broker, config, lock_manager, store, signal_dispatcher
    ):
        skipped: list[dict] = []

        @signal_dispatcher.on_signal(Signal.SCHEDULER_FIRE_SKIPPED)
        async def _sk(**kw):
            skipped.append(kw)

        loop = _make_loop(
            backend, broker, config, lock_manager, store, signal_dispatcher
        )
        loop._state = "leader"
        # Due score = the most recent occurrence of a YEARLY cron —
        # deterministic: the interval (score, now] contains no further
        # occurrence for up to a year. (A naive "now - 2s" score on a
        # minutely cron straddles the minute boundary whenever the test
        # runs within 2s after it → flaky skipped==1.)
        from croniter import croniter as _croniter

        yearly = "0 0 1 1 *"
        prev_dt = _croniter(yearly, datetime.now(UTC)).get_prev(datetime)
        score = int(prev_dt.timestamp() * 1000)
        d = _def(name="yearly", cron=yearly, fire_ts_ms=score)
        store.poll_due.return_value = [("yearly", score, d)]

        await loop._poll_tick()

        assert skipped == []

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


class TestSchedulerUniqueDedup:
    """Scheduler-side SETNX + commit-on-dup for R3.5 cron/delay fires."""

    async def test_static_cron_without_unique_for_does_not_call_setnx(
        self, backend, broker, config, lock_manager, store, signal_dispatcher,
        _cleanup_tasks,
    ):
        class PlainCron(Task):
            task_info = TaskInfo(
                name="plain_task", queues=["default"],
                cron="*/5 * * * *",
            )
            async def run(self): return None

        backend.claim_unique = AsyncMock()
        loop = _make_loop(
            backend, broker, config, lock_manager, store, signal_dispatcher
        )
        loop._state = "leader"
        d = _cron_def(name="plain_task", task_name="plain_task")
        store.poll_due.return_value = [("plain_task", 1_000, d)]

        await loop._poll_tick()

        backend.claim_unique.assert_not_awaited()
        broker.publish.assert_awaited_once()
        # Only schedfire header, no unique hash.
        headers = broker.publish.await_args.kwargs.get("headers") or {}
        assert "x-swarmq-unique-hash" not in headers

    async def test_static_cron_with_unique_for_sets_flag_and_header(
        self, backend, broker, config, lock_manager, store, signal_dispatcher,
        _cleanup_tasks,
    ):
        class UniqueCron(Task):
            task_info = TaskInfo(
                name="uniq_task", queues=["default"],
                cron="*/5 * * * *",
                unique_for=120, unique_until="completion",
            )
            async def run(self): return None

        backend.claim_unique = AsyncMock(return_value=None)  # NX granted
        loop = _make_loop(
            backend, broker, config, lock_manager, store, signal_dispatcher
        )
        loop._state = "leader"
        d = _cron_def(name="uniq_task", task_name="uniq_task")
        store.poll_due.return_value = [("uniq_task", 1_000, d)]

        await loop._poll_tick()

        backend.claim_unique.assert_awaited_once()
        ttl_ms = backend.claim_unique.await_args.args[2]
        assert ttl_ms == 120_000

        headers = broker.publish.await_args.kwargs["headers"]
        assert "x-swarmq-unique-hash" in headers
        assert headers["x-swarmq-unique-until"] == "completion"
        store.commit_fire.assert_awaited_once()

    async def test_static_cron_dup_skips_publish_but_commits(
        self, backend, broker, config, lock_manager, store, signal_dispatcher,
        _cleanup_tasks,
    ):
        """Dup path: no publish, but commit_fire MUST advance the zset
        otherwise the scheduler re-polls the same entry forever."""
        class UniqueCron(Task):
            task_info = TaskInfo(
                name="dup_task", queues=["default"],
                cron="*/5 * * * *", unique_for=60,
            )
            async def run(self): return None

        received: list[dict] = []
        @signal_dispatcher.on_signal(Signal.TASK_DUPLICATE_DETECTED)
        async def handler(**kwargs):
            received.append(kwargs)

        backend.claim_unique = AsyncMock(return_value="t-existing")
        loop = _make_loop(
            backend, broker, config, lock_manager, store, signal_dispatcher
        )
        loop._state = "leader"
        d = _cron_def(name="dup_task", task_name="dup_task")
        store.poll_due.return_value = [("dup_task", 1_000, d)]

        await loop._poll_tick()

        broker.publish.assert_not_awaited()
        store.commit_fire.assert_awaited_once()
        assert len(received) == 1
        assert received[0]["existing_task_id"] == "t-existing"
        assert received[0]["source"] == "scheduler.fire"
        assert received[0]["task_name"] == "dup_task"

    async def test_cli_unique_source_skips_scheduler_setnx(
        self, backend, broker, config, lock_manager, store, signal_dispatcher,
        _cleanup_tasks,
    ):
        """Producer already claimed the flag (Unit 7) — scheduler must not
        re-SETNX or the flag's TTL would reset. Header is propagated from
        the stored dedup_hash + unique_until (P1 #5 / ADV-007: unique_until
        persisted alongside dedup_hash so post-deploy TaskInfo changes
        cannot silently flip release semantics)."""
        backend.claim_unique = AsyncMock()
        loop = _make_loop(
            backend, broker, config, lock_manager, store, signal_dispatcher
        )
        loop._state = "leader"
        d = ScheduleDefinition(
            schedule_name="oneshot:xyz",
            task_name="deferred_uniq",
            args=(), kwargs={}, queue="default",
            cron=None, fire_ts_ms=1_000,
            source="cli_unique",
            dedup_hash="pre-hashed-by-client",
            unique_until="start",
        )
        store.poll_due.return_value = [("oneshot:xyz", 1_000, d)]

        await loop._poll_tick()

        backend.claim_unique.assert_not_awaited()
        broker.publish.assert_awaited_once()
        headers = broker.publish.await_args.kwargs["headers"]
        assert headers["x-swarmq-unique-hash"] == "pre-hashed-by-client"
        # unique_until read directly from the persisted definition.
        assert headers["x-swarmq-unique-until"] == "start"

    async def test_cli_unique_pre_r35_entry_defaults_until_completion(
        self, backend, broker, config, lock_manager, store, signal_dispatcher,
        _cleanup_tasks,
    ):
        """Legacy entries without unique_until (pre-P1-#5) decode with
        the struct default 'completion'. Ensures backwards-compatibility
        with schedules stored before the persisted field existed."""
        backend.claim_unique = AsyncMock()
        loop = _make_loop(
            backend, broker, config, lock_manager, store, signal_dispatcher
        )
        loop._state = "leader"
        d = ScheduleDefinition(
            schedule_name="oneshot:mystery",
            task_name="not_registered_here",
            args=(), kwargs={}, queue="default",
            cron=None, fire_ts_ms=1_000,
            source="cli_unique",
            dedup_hash="abc123",
            # unique_until omitted → struct default "completion"
        )
        store.poll_due.return_value = [("oneshot:mystery", 1_000, d)]

        await loop._poll_tick()

        headers = broker.publish.await_args.kwargs["headers"]
        assert headers["x-swarmq-unique-hash"] == "abc123"
        assert headers["x-swarmq-unique-until"] == "completion"

    async def test_setnx_backend_error_skips_publish_and_commit(
        self, backend, broker, config, lock_manager, store, signal_dispatcher,
        _cleanup_tasks,
    ):
        """Valkey hiccup at SETNX → do NOT publish, do NOT commit. Next
        poll tick retries."""
        class UniqueCron(Task):
            task_info = TaskInfo(
                name="flap_task", queues=["default"],
                cron="*/5 * * * *", unique_for=60,
            )
            async def run(self): return None

        backend.claim_unique = AsyncMock(
            side_effect=BackendConnectionError("valkey down")
        )
        loop = _make_loop(
            backend, broker, config, lock_manager, store, signal_dispatcher
        )
        loop._state = "leader"
        d = _cron_def(name="flap_task", task_name="flap_task")
        store.poll_due.return_value = [("flap_task", 1_000, d)]

        await loop._poll_tick()

        broker.publish.assert_not_awaited()
        store.commit_fire.assert_not_awaited()

    async def test_hash_stable_for_same_args_across_fires(
        self, backend, broker, config, lock_manager, store, signal_dispatcher,
        _cleanup_tasks,
    ):
        """Two fires of the same schedule with same args produce the
        same dedup_hash (Valkey sees repeat owner on second attempt)."""
        class UniqueCron(Task):
            task_info = TaskInfo(
                name="repeat_task", queues=["default"],
                cron="*/1 * * * *", unique_for=60,
            )
            async def run(self): return None

        captured: list[str] = []

        async def setnx_side_effect(dedup_hash, *_a, **_kw):
            captured.append(dedup_hash)
            return None

        backend.claim_unique = AsyncMock(side_effect=setnx_side_effect)
        loop = _make_loop(
            backend, broker, config, lock_manager, store, signal_dispatcher
        )
        loop._state = "leader"
        d = _cron_def(
            name="repeat_task", task_name="repeat_task",
            args=(1, 2), kwargs={"x": "y"},
        )
        store.poll_due.return_value = [("repeat_task", 1_000, d)]

        await loop._poll_tick()
        # Reset broker/store for second tick so commit_fire doesn't see
        # the first call in its counter (poll returns same item again).
        store.poll_due.return_value = [("repeat_task", 2_000, d)]
        await loop._poll_tick()

        assert len(captured) == 2
        assert captured[0] == captured[1]


class TestAbort:
    """abort() cancels the sub-coroutines WITHOUT the graceful
    handover/lock-release that stop() performs — for the cancel path
    (process crash / task cancellation) where a successor takes over via
    TTL-based failover. Mirrors the cleanup tail of stop() minus the
    leader-release block."""

    async def test_abort_cancels_tasks_without_graceful_release(
        self, backend, broker, config, lock_manager, store, signal_dispatcher
    ):
        loop = _make_loop(
            backend, broker, config, lock_manager, store, signal_dispatcher
        )
        lock_manager.acquire.return_value = _LEADER_TOKEN  # would-be leader
        await loop.start()
        assert loop._election_task is not None
        assert loop._poll_task is not None
        await asyncio.sleep(0.05)  # let a tick run so it is "leader"

        await loop.abort()

        assert loop._election_task is None
        assert loop._poll_task is None
        assert loop.state == "stopped"
        # Crash-style abort must NOT perform the graceful lock release.
        lock_manager.release.assert_not_awaited()

    async def test_abort_is_idempotent(
        self, backend, broker, config, lock_manager, store, signal_dispatcher
    ):
        loop = _make_loop(
            backend, broker, config, lock_manager, store, signal_dispatcher
        )
        await loop.start()
        await loop.abort()
        await loop.abort()  # second call must not raise
        assert loop._election_task is None
        assert loop._poll_task is None
