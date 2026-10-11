"""SchedulerLoop-Tests (Plan U11) — verbatim: Election (LockStore-Token-
API), Poll/Fire, Schedfire-Dedup (``claim_unique``), Stop/Abort."""

# Test list (Plan 2026-10-06-0831, U9, R17/R18) -- TestSchedulerLoopLogging:
# 1. Dedup-Treffer loggt ``dedup_detected`` (vorher ``swarmq.dedup.detected``).
# 2. Fehler-Events tragen ``error_type``/``error`` (je Aufrufstelle ein Test):
#    handover_marker_failed, lock_release_failed, election_tick_error,
#    renew_error, acquire_error, handover_marker_read_failed,
#    cron_adopt_failed (seit U12), poll_tick_error, poll_due_failed,
#    unique_setnx_failed, publish_failed, commit_failed,
#    duplicate_signal_dispatch_error, signal_dispatch_error.
# 3. ``scheduler_task_not_in_local_registry`` ueber ``swarmq_logs`` statt
#    gepatchtem Logger.
#
# Test list (0.9.0, Scheduler-Commit-Wettlauf):
# 4. Der Fire-Commit traegt den gefeuerten Index-Score (``fired_ts_ms``),
#    damit eine Aenderung zwischen Poll und Commit nicht ueberschrieben wird.
#
# Test list (Plan 2026-10-08-1541, U2): Termine in der Zone der Definition.
# 5. Der naechste Score nach dem Fire gilt in ``definition.timezone``.
# 6. Die Zaehlung uebersprungener Fires rechnet in derselben Zone.
#
# Test list (Plan 2026-10-08-1541, U3, R18, KTD12): Fire ohne Vergiftung.
# 7. Laesst sich die Folgezeit nicht berechnen, wird der Eintrag nicht
#    publiziert, ``scheduler_next_fire_failed`` geloggt, und die anderen
#    faelligen Crons im selben Batch feuern.
# 8. ... und der Eintrag wird aus dem Fire-Index genommen (Definition bleibt).
# 9. Eine Uebernahme zwischen Poll und Commit wird vom Commit nicht
#    zurueckgesetzt (In-Memory-Store, echter ScheduleStore).
#
# Test list (Plan 2026-10-08-1541, U13, KTD11): Leader-Version.
# 10. Nach dem Lock-Erwerb schreibt der Leader seine Version mit Lock-TTL.
# 11. ... ebenso nach jeder erfolgreichen Verlängerung.
# 12. Schlägt das Schreiben fehl, bleibt er Leader; Warnung mit Fehlerfeldern.
# 13. Covers AE9: ein Nicht-Leader, der das Lock ohne Versionsangabe sieht,
#     loggt ``scheduler_leader_outdated`` einmal pro Wechsel und sendet
#     ``SCHEDULER_LEADER_OUTDATED`` (``outdated=True``, Metrik).
# 14. Lock mit gleicher Version: keine Warnung, kein Signal.
# 15. Wird der Beobachter selbst Leader, nimmt er die Warnung zurück
#     (Signal mit ``outdated=False``).
# 16. Lesefehler bei der Beobachtung: Warnung, die Election läuft weiter.
# 17. Jeder Melde-Takt meldet die eigene Version (TTL = Lebendigkeitsfenster).
#
# Test list (Review-Befund #1): Uebernahme und Poll im Leader serialisiert.
# 18. Eine Uebernahme, die waehrend eines Fires (zwischen Poll und Commit)
#     faellig wird, wartet bis nach dem Commit; der Folge-Score kommt aus
#     Cron und Zone der uebernommenen Definition.
# 19. Laeuft die Uebernahme vor dem Poll, feuert der Poll die uebernommene
#     Definition und plant den Folge-Score nach deren Cron und Zone.
#
# Test list (Review-Befund #3): Parken einer nicht planbaren Definition.
# 20. Ein ``BackendConnectionError`` beim Parken bricht den Batch nicht ab;
#     ``scheduler_park_failed`` wird geloggt.
# 21. Ein Owner-Mismatch beim Parken wird geloggt.
#
# Test list (Plan 0.12.0, MR 4, BZ-28): kaputte Definition im Poll.
# 22. Eine nicht dekodierbare Definition blockiert den Poll nicht: der
#     Leader parkt sie (Fire-Index leer, Definition bleibt), die übrigen
#     fälligen Crons feuern.
#
# Test list (Plan 0.12.0, MR 4, BZ-07): Verlängerung direkt nach der Übernahme.
# 23. Liefert sie ``False`` (Lock während der Übernahme abgelaufen), bleibt
#     der Loop Standby: kein Leader-Signal, ``scheduler_lock_lost``.
# 24. Wirft sie ``BackendConnectionError``, wird er trotzdem Leader (Lock
#     gerade erworben) und loggt ``scheduler_renew_error``; die folgenden
#     Wahl-Takte entscheiden wie bei jeder Verlängerung.
# 25. Der Standby merkt sich den zuletzt gesehenen Leader und reicht ihn an
#     ``take_handover_marker`` (Handover eines 0.11-Leaders).

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from zoneinfo import ZoneInfo

import msgspec
import pytest

from swarmq.config import SwarmQConfig
from swarmq.contracts import LeaderInfo
from swarmq.exceptions import BackendConnectionError
from swarmq.locking import SCHEDULER_LOCK_NAME
from swarmq.scheduler import (
    ScheduleDefinition,
    ScheduleStore,
)
from swarmq.scheduler.reports import cron_fingerprint, live_window_ms
from swarmq.signals import Signal
from swarmq.task import Task, TaskInfo
from tests.support.inmemory import InMemoryBackend
from tests.support.inmemory.clock import FakeClock
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

        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
        lock_manager.acquire.return_value = _LEADER_TOKEN
        backend.schedules.take_handover_marker.return_value = False

        await loop._election_tick()

        assert loop.state == "leader"
        assert any(e[0] is Signal.SCHEDULER_LEADER for e in events)
        lock_manager.acquire.assert_awaited_once()

    async def test_standby_acquire_fails_no_state_change(
        self, backend, broker, config, lock_manager, store, signal_dispatcher
    ):
        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
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

        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
        # First tick: lock held by someone else
        lock_manager.acquire.return_value = None
        await loop._election_tick()
        assert loop._prev_leader_seen is True

        # Second tick: acquire wins, no handover marker → failover
        lock_manager.acquire.return_value = _LEADER_TOKEN
        backend.schedules.take_handover_marker.return_value = False
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

        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
        lock_manager.acquire.return_value = None
        await loop._election_tick()
        assert loop._prev_leader_seen is True

        lock_manager.acquire.return_value = _LEADER_TOKEN
        backend.schedules.take_handover_marker.return_value = True
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

        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
        loop._state = "leader"
        lock_manager.renew.return_value = True

        await loop._election_tick()

        assert hb == [Signal.SCHEDULER_HEARTBEAT]
        lock_manager.renew.assert_awaited_once()

    async def test_leader_renew_lost_transitions_to_standby(
        self, backend, broker, config, lock_manager, store, signal_dispatcher
    ):
        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
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

        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
        loop._state = "leader"
        lock_manager.renew.side_effect = BackendConnectionError("valkey down")

        limit = loop._renew_failure_limit
        assert limit >= 1
        for _ in range(limit - 1):
            await loop._election_tick()
            assert loop.state == "leader", "must tolerate failures below the threshold"

        await loop._election_tick()

        assert loop.state == "standby"
        assert len(standby_events) == 1

    async def test_renew_error_counter_resets_on_success(
        self, backend, broker, config, lock_manager, store, signal_dispatcher
    ):
        """Ein einzelner Renew-Fehler gefolgt von Erfolg setzt den
        Zähler zurück — kein schleichender Step-down über Stunden."""
        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
        loop._state = "leader"
        limit = loop._renew_failure_limit

        # One failure short of the limit, then a success, then again
        # one failure short — never steps down.
        seq: list = [BackendConnectionError("blip") for _ in range(limit - 1)]
        seq.append(True)
        seq.extend(BackendConnectionError("blip") for _ in range(limit - 1))
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
        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
        assert loop._renew_failure_limit == 2


class TestLeaderVersion:
    async def test_leader_writes_version_after_acquire(
        self, backend, broker, config, lock_manager, store, signal_dispatcher
    ):
        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)

        await loop._election_tick()

        backend.schedules.set_leader_version.assert_awaited_once_with(
            "test-owner",
            "0.10.0",
            ttl_ms=1_000,
        )

    async def test_leader_writes_version_after_each_renew(
        self, backend, broker, config, lock_manager, store, signal_dispatcher
    ):
        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
        loop._state = "leader"

        await loop._election_tick()
        await loop._election_tick()

        assert backend.schedules.set_leader_version.await_count == 2
        backend.schedules.set_leader_version.assert_awaited_with(
            "test-owner",
            "0.10.0",
            ttl_ms=1_000,
        )

    @pytest.mark.parametrize("state", ["standby", "leader"])
    async def test_version_write_failure_keeps_leadership(
        self,
        backend,
        broker,
        config,
        lock_manager,
        store,
        signal_dispatcher,
        swarmq_logs,
        state,
    ):
        backend.schedules.set_leader_version.side_effect = BackendConnectionError("valkey down")
        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
        loop._state = state

        await loop._election_tick()

        assert loop.state == "leader"
        [entry] = [e for e in swarmq_logs if e["event"] == "scheduler_leader_version_write_failed"]
        assert entry["log_level"] == "warning"
        assert entry["error_type"] == "BackendConnectionError"
        assert entry["error"] == "valkey down"

    async def test_outdated_leader_is_reported_once_per_change(
        self,
        backend,
        broker,
        config,
        lock_manager,
        store,
        signal_dispatcher,
        swarmq_logs,
    ):
        """Covers AE9: 0.9.0-Leader (kein Versions-Key)."""
        signals: list[dict] = []

        @signal_dispatcher.on_signal(Signal.SCHEDULER_LEADER_OUTDATED)
        async def _outdated(**kw):
            signals.append(kw)

        lock_manager.acquire.return_value = None
        backend.schedules.leader_info.return_value = LeaderInfo(owner="old:1:aa", version=None)
        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)

        for _ in range(3):
            await loop._election_tick()

        [entry] = [e for e in swarmq_logs if e["event"] == "scheduler_leader_outdated"]
        assert entry["log_level"] == "warning"
        assert (entry["leader"], entry["leader_version"], entry["version"]) == ("old:1:aa", None, "0.10.0")
        # KTD11 (Plan 2026-10-09-2314, MR 5): Hinweis nennt die Mindestversion,
        # nicht mehr nur die Zeitzonen.
        assert "0.12 or later" in entry["hint"]
        assert "timezone" not in entry["hint"]
        assert signals == [
            {"worker_id": "w-1", "leader": "old:1:aa", "leader_version": None, "outdated": True},
        ]

    async def test_leader_with_same_version_is_not_reported(
        self,
        backend,
        broker,
        config,
        lock_manager,
        store,
        signal_dispatcher,
        swarmq_logs,
    ):
        signals: list[dict] = []

        @signal_dispatcher.on_signal(Signal.SCHEDULER_LEADER_OUTDATED)
        async def _outdated(**kw):
            signals.append(kw)

        lock_manager.acquire.return_value = None
        backend.schedules.leader_info.return_value = LeaderInfo(owner="new:1:aa", version="0.12.0")
        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)

        for _ in range(3):
            await loop._election_tick()

        assert [e for e in swarmq_logs if e["event"] == "scheduler_leader_outdated"] == []
        assert signals == []
        assert backend.schedules.leader_info.await_count == 3

    async def test_becoming_leader_withdraws_the_warning(
        self,
        backend,
        broker,
        config,
        lock_manager,
        store,
        signal_dispatcher,
    ):
        signals: list[dict] = []

        @signal_dispatcher.on_signal(Signal.SCHEDULER_LEADER_OUTDATED)
        async def _outdated(**kw):
            signals.append(kw)

        lock_manager.acquire.return_value = None
        backend.schedules.leader_info.return_value = LeaderInfo(owner="old:1:aa", version=None)
        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher, version="0.12.0")
        await loop._election_tick()
        await loop._election_tick()
        lock_manager.acquire.return_value = _LEADER_TOKEN

        await loop._election_tick()

        assert loop.state == "leader"
        assert [kw["outdated"] for kw in signals] == [True, False]
        assert signals[-1] == {
            "worker_id": "w-1",
            "leader": "test-owner",
            "leader_version": "0.12.0",
            "outdated": False,
        }

    async def test_version_check_failure_is_logged(
        self,
        backend,
        broker,
        config,
        lock_manager,
        store,
        signal_dispatcher,
        swarmq_logs,
    ):
        lock_manager.acquire.return_value = None
        backend.schedules.leader_info.side_effect = BackendConnectionError("valkey down")
        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)

        await loop._election_tick()

        assert loop.state == "standby"
        assert loop._prev_leader_seen is True
        [entry] = [e for e in swarmq_logs if e["event"] == "scheduler_leader_version_check_failed"]
        assert entry["log_level"] == "warning"
        assert entry["error_type"] == "BackendConnectionError"
        assert entry["error"] == "valkey down"

    async def test_report_tick_reports_own_version(
        self,
        backend,
        broker,
        config,
        lock_manager,
        store,
        signal_dispatcher,
    ):
        """Auch ohne statische Crons; TTL = Lebendigkeitsfenster der Meldungen."""
        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)

        await loop._report_crons()

        backend.schedules.report_worker_version.assert_awaited_once_with(
            "w-1",
            "0.10.0",
            ttl_ms=live_window_ms(config.scheduler_seen_interval_s),
        )


class TestPollLoop:
    async def test_standby_poll_is_noop(self, backend, broker, config, lock_manager, store, signal_dispatcher):
        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
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
        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
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

    async def test_commit_carries_fired_zset_score(
        self, backend, broker, config, lock_manager, store, signal_dispatcher
    ):
        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
        loop._state = "leader"
        d = _def(name="cron_a", cron="*/5 * * * *", fire_ts_ms=9_999)
        store.poll_due.return_value = [("cron_a", 1_000, d)]

        await loop._poll_tick()

        assert store.commit_fire.await_args.kwargs["fired_ts_ms"] == 1_000

    async def test_schedfire_key_uses_zset_score_not_definition_ts(
        self, backend, broker, config, lock_manager, store, signal_dispatcher
    ):
        """Unit 7 / M-11: the schedfire-dedup key must be derived from
        the authoritative zset score returned by poll_due, not from
        definition.fire_ts_ms which can diverge during update_static_schedule."""
        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
        loop._state = "leader"
        # Simulate divergence: zset score says 1000, definition thinks 9999.
        # The dedup key must reflect 1000 (what consumers will see on
        # redelivery once the broker carries the header forward).
        d = _def(name="drift", cron="*/5 * * * *", fire_ts_ms=9_999)
        store.poll_due.return_value = [("drift", 1_000, d)]

        await loop._poll_tick()

        headers = broker.publish.await_args.kwargs.get("headers") or {}
        assert headers["x-swarmq-schedfire"] == "drift:1000"

    @pytest.mark.usefixtures("_cleanup_tasks")
    async def test_unregistered_task_fire_logs_warning(
        self,
        backend,
        broker,
        config,
        lock_manager,
        store,
        signal_dispatcher,
        swarmq_logs,
    ):
        """Härtung R2 P2: ein Schedule, dessen Task umbenannt/entfernt
        wurde, publiziert kommentarlos ins Leere. Warn-only (kein Skip):
        in Multi-Modul-Deployments kann der Task in der Registry eines
        ANDEREN Workers existieren — ein harter Skip wäre dort falsch.
        Die Warnung gibt dem Operator das Signal, das vorher fehlte."""
        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
        loop._state = "leader"
        d = _def(name="ghost", cron="*/5 * * * *", fire_ts_ms=1_000)
        store.poll_due.return_value = [("ghost", 1_000, d)]

        await loop._poll_tick()

        # Publish still happens (warn-only), but the operator sees it.
        broker.publish.assert_awaited_once()
        warned_events = [e["event"] for e in swarmq_logs if e["log_level"] == "warning"]
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

        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
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

        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
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
        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
        loop._state = "leader"
        d = _def(name="oneshot:abc", cron=None, fire_ts_ms=1_000, source="cli")
        store.poll_due.return_value = [("oneshot:abc", 1_000, d)]

        await loop._poll_tick()

        commit_args = store.commit_fire.await_args
        assert commit_args.args[1] == -1  # one-shot sentinel
        assert commit_args.args[2] is None  # no updated_def

    async def test_disabled_schedule_is_not_fired(
        self, backend, broker, config, lock_manager, store, signal_dispatcher
    ):
        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
        loop._state = "leader"
        d = _def(disabled=True)
        store.poll_due.return_value = [(d.schedule_name, 1_000, d)]

        await loop._poll_tick()

        broker.publish.assert_not_awaited()
        store.commit_fire.assert_not_awaited()

    async def test_publish_failure_skips_commit(self, backend, broker, config, lock_manager, store, signal_dispatcher):
        """If broker.publish raises, we must NOT commit — re-poll will
        pick the schedule up again and the consumer-dedup handles the
        rare case where publish actually succeeded past a network blip."""
        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
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
        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
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

        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
        await loop.start()
        loop._state = "leader"

        await loop.stop()

        assert events == [Signal.SCHEDULER_LEADER_HANDOVER]
        backend.schedules.set_handover_marker.assert_awaited_once()
        lock_manager.release.assert_awaited_once()
        assert loop.state == "stopped"

    async def test_stop_as_standby_does_not_emit_handover(
        self, backend, broker, config, lock_manager, store, signal_dispatcher
    ):
        events: list[Signal] = []

        @signal_dispatcher.on_signal(Signal.SCHEDULER_LEADER_HANDOVER)
        async def _h(**kw):
            events.append(Signal.SCHEDULER_LEADER_HANDOVER)

        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
        await loop.start()
        # state stays "standby"

        await loop.stop()

        assert events == []
        backend.schedules.set_handover_marker.assert_not_awaited()
        lock_manager.release.assert_not_awaited()


class TestSchedulerUniqueDedup:
    """Scheduler-side SETNX + commit-on-dup for R3.5 cron/delay fires."""

    @pytest.mark.usefixtures("_cleanup_tasks")
    async def test_static_cron_without_unique_for_does_not_call_setnx(
        self,
        backend,
        broker,
        config,
        lock_manager,
        store,
        signal_dispatcher,
    ):
        class PlainCron(Task):
            task_info = TaskInfo(
                name="plain_task",
                queues=["default"],
                cron="*/5 * * * *",
            )

            async def run(self):
                return None

        backend.claim_unique = AsyncMock()
        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
        loop._state = "leader"
        d = _cron_def(name="plain_task", task_name="plain_task")
        store.poll_due.return_value = [("plain_task", 1_000, d)]

        await loop._poll_tick()

        backend.claim_unique.assert_not_awaited()
        broker.publish.assert_awaited_once()
        # Only schedfire header, no unique hash.
        headers = broker.publish.await_args.kwargs.get("headers") or {}
        assert "x-swarmq-unique-hash" not in headers

    @pytest.mark.usefixtures("_cleanup_tasks")
    async def test_static_cron_with_unique_for_sets_flag_and_header(
        self,
        backend,
        broker,
        config,
        lock_manager,
        store,
        signal_dispatcher,
    ):
        class UniqueCron(Task):
            task_info = TaskInfo(
                name="uniq_task",
                queues=["default"],
                cron="*/5 * * * *",
                unique_for=120,
                unique_until="completion",
            )

            async def run(self):
                return None

        backend.claim_unique = AsyncMock(return_value=None)  # NX granted
        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
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

    @pytest.mark.usefixtures("_cleanup_tasks")
    async def test_static_cron_dup_skips_publish_but_commits(
        self,
        backend,
        broker,
        config,
        lock_manager,
        store,
        signal_dispatcher,
    ):
        """Dup path: no publish, but commit_fire MUST advance the zset
        otherwise the scheduler re-polls the same entry forever."""

        class UniqueCron(Task):
            task_info = TaskInfo(
                name="dup_task",
                queues=["default"],
                cron="*/5 * * * *",
                unique_for=60,
            )

            async def run(self):
                return None

        received: list[dict] = []

        @signal_dispatcher.on_signal(Signal.TASK_DUPLICATE_DETECTED)
        async def handler(**kwargs):
            received.append(kwargs)

        backend.claim_unique = AsyncMock(return_value="t-existing")
        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
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

    @pytest.mark.usefixtures("_cleanup_tasks")
    async def test_cli_unique_source_skips_scheduler_setnx(
        self,
        backend,
        broker,
        config,
        lock_manager,
        store,
        signal_dispatcher,
    ):
        """Producer already claimed the flag (Unit 7) — scheduler must not
        re-SETNX or the flag's TTL would reset. Header is propagated from
        the stored dedup_hash + unique_until (P1 #5 / ADV-007: unique_until
        persisted alongside dedup_hash so post-deploy TaskInfo changes
        cannot silently flip release semantics)."""
        backend.claim_unique = AsyncMock()
        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
        loop._state = "leader"
        d = ScheduleDefinition(
            schedule_name="oneshot:xyz",
            task_name="deferred_uniq",
            args=(),
            kwargs={},
            queue="default",
            cron=None,
            fire_ts_ms=1_000,
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

    @pytest.mark.usefixtures("_cleanup_tasks")
    async def test_cli_unique_pre_r35_entry_defaults_until_completion(
        self,
        backend,
        broker,
        config,
        lock_manager,
        store,
        signal_dispatcher,
    ):
        """Legacy entries without unique_until (pre-P1-#5) decode with
        the struct default 'completion'. Ensures backwards-compatibility
        with schedules stored before the persisted field existed."""
        backend.claim_unique = AsyncMock()
        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
        loop._state = "leader"
        d = ScheduleDefinition(
            schedule_name="oneshot:mystery",
            task_name="not_registered_here",
            args=(),
            kwargs={},
            queue="default",
            cron=None,
            fire_ts_ms=1_000,
            source="cli_unique",
            dedup_hash="abc123",
            # unique_until omitted → struct default "completion"
        )
        store.poll_due.return_value = [("oneshot:mystery", 1_000, d)]

        await loop._poll_tick()

        headers = broker.publish.await_args.kwargs["headers"]
        assert headers["x-swarmq-unique-hash"] == "abc123"
        assert headers["x-swarmq-unique-until"] == "completion"

    @pytest.mark.usefixtures("_cleanup_tasks")
    async def test_setnx_backend_error_skips_publish_and_commit(
        self,
        backend,
        broker,
        config,
        lock_manager,
        store,
        signal_dispatcher,
    ):
        """Valkey hiccup at SETNX → do NOT publish, do NOT commit. Next
        poll tick retries."""

        class UniqueCron(Task):
            task_info = TaskInfo(
                name="flap_task",
                queues=["default"],
                cron="*/5 * * * *",
                unique_for=60,
            )

            async def run(self):
                return None

        backend.claim_unique = AsyncMock(side_effect=BackendConnectionError("valkey down"))
        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
        loop._state = "leader"
        d = _cron_def(name="flap_task", task_name="flap_task")
        store.poll_due.return_value = [("flap_task", 1_000, d)]

        await loop._poll_tick()

        broker.publish.assert_not_awaited()
        store.commit_fire.assert_not_awaited()

    @pytest.mark.usefixtures("_cleanup_tasks")
    async def test_hash_stable_for_same_args_across_fires(
        self,
        backend,
        broker,
        config,
        lock_manager,
        store,
        signal_dispatcher,
    ):
        """Two fires of the same schedule with same args produce the
        same dedup_hash (Valkey sees repeat owner on second attempt)."""

        class UniqueCron(Task):
            task_info = TaskInfo(
                name="repeat_task",
                queues=["default"],
                cron="*/1 * * * *",
                unique_for=60,
            )

            async def run(self):
                return None

        captured: list[str] = []

        async def setnx_side_effect(dedup_hash, *_a, **_kw):
            captured.append(dedup_hash)

        backend.claim_unique = AsyncMock(side_effect=setnx_side_effect)
        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
        loop._state = "leader"
        d = _cron_def(
            name="repeat_task",
            task_name="repeat_task",
            args=(1, 2),
            kwargs={"x": "y"},
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
        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
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

    async def test_abort_is_idempotent(self, backend, broker, config, lock_manager, store, signal_dispatcher):
        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
        await loop.start()
        await loop.abort()
        await loop.abort()  # second call must not raise
        assert loop._election_task is None
        assert loop._poll_task is None


class TestSchedulerLoopLogging:
    @pytest.fixture
    def loop(self, backend, broker, config, lock_manager, store, signal_dispatcher):
        return _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)

    @staticmethod
    def _entry(logs, event):
        [entry] = [e for e in logs if e["logger"] == "swarmq.scheduler.loop" and e["event"] == event]
        return entry

    @staticmethod
    def _unique_cron_task():
        class UniqueCron(Task):
            task_info = TaskInfo(name="dup_task", queues=["default"], cron="*/5 * * * *", unique_for=60)

            async def run(self):
                return None

    @pytest.mark.usefixtures("_cleanup_tasks")
    async def test_dedup_hit_logs_dedup_detected(self, loop, backend, store, swarmq_logs):
        self._unique_cron_task()
        backend.claim_unique = AsyncMock(return_value="t-existing")
        loop._state = "leader"
        store.poll_due.return_value = [("dup_task", 1_000, _cron_def(name="dup_task", task_name="dup_task"))]

        await loop._poll_tick()

        entry = self._entry(swarmq_logs, "dedup_detected")
        assert entry["log_level"] == "info"
        assert entry["existing_task_id"] == "t-existing"
        assert entry["task_name"] == "dup_task"
        assert entry["source"] == "scheduler.fire"
        assert entry["dedup_hash"]

    async def test_handover_marker_failure_logs_error_fields(self, loop, backend, swarmq_logs):
        loop._state = "leader"
        backend.schedules.set_handover_marker.side_effect = BackendConnectionError("valkey down")
        await loop.stop()
        entry = self._entry(swarmq_logs, "scheduler_handover_marker_failed")
        assert entry["log_level"] == "warning"
        assert entry["error_type"] == "BackendConnectionError"
        assert entry["error"] == "valkey down"

    async def test_lock_release_failure_logs_error_fields(self, loop, lock_manager, swarmq_logs):
        loop._state = "leader"
        lock_manager.release.side_effect = BackendConnectionError("valkey down")
        await loop.stop()
        entry = self._entry(swarmq_logs, "scheduler_lock_release_failed")
        assert entry["log_level"] == "warning"
        assert entry["error_type"] == "BackendConnectionError"
        assert entry["error"] == "valkey down"

    async def test_election_tick_error_logs_error_fields(self, loop, swarmq_logs, monkeypatch):
        async def _boom():
            loop._stop_event.set()
            raise RuntimeError("tick broke")

        monkeypatch.setattr(loop, "_election_tick", _boom)
        await loop._election_loop()
        entry = self._entry(swarmq_logs, "scheduler_election_tick_error")
        assert entry["log_level"] == "warning"
        assert entry["error_type"] == "RuntimeError"
        assert entry["error"] == "tick broke"

    async def test_renew_error_logs_error_fields(self, loop, lock_manager, swarmq_logs):
        loop._state = "leader"
        lock_manager.renew.side_effect = BackendConnectionError("valkey down")
        await loop._election_tick()
        entry = self._entry(swarmq_logs, "scheduler_renew_error")
        assert entry["log_level"] == "warning"
        assert entry["error_type"] == "BackendConnectionError"
        assert entry["error"] == "valkey down"
        assert entry["consecutive_failures"] == 1

    async def test_acquire_error_logs_error_fields(self, loop, lock_manager, swarmq_logs):
        lock_manager.acquire.side_effect = BackendConnectionError("valkey down")
        await loop._election_tick()
        entry = self._entry(swarmq_logs, "scheduler_acquire_error")
        assert entry["log_level"] == "warning"
        assert entry["error_type"] == "BackendConnectionError"
        assert entry["error"] == "valkey down"

    async def test_handover_marker_read_failure_logs_error_fields(self, loop, backend, swarmq_logs):
        backend.schedules.take_handover_marker.side_effect = BackendConnectionError("valkey down")
        await loop._election_tick()
        entry = self._entry(swarmq_logs, "scheduler_handover_marker_read_failed")
        assert entry["log_level"] == "warning"
        assert entry["error_type"] == "BackendConnectionError"
        assert entry["error"] == "valkey down"

    async def test_cron_adopt_failure_logs_error_fields(self, loop, swarmq_logs, monkeypatch):
        """U12: die Übernahme der Cron-Meldungen läuft im eigenen Leader-Takt;
        ein Fehler wird geloggt, der Takt läuft weiter."""
        failed = asyncio.Event()

        async def _broken():
            failed.set()
            raise RuntimeError("adopt broke")

        monkeypatch.setattr(loop, "adopt_reported_crons", _broken)
        await loop.start()
        try:
            await asyncio.wait_for(failed.wait(), timeout=2)
            await loop._election_tick()  # Renew: der Loop lebt weiter.
        finally:
            await loop.stop()
        entry = self._entry(swarmq_logs, "scheduler_cron_adopt_failed")
        assert entry["log_level"] == "warning"
        assert entry["error_type"] == "RuntimeError"
        assert entry["error"] == "adopt broke"

    async def test_poll_tick_error_logs_error_fields(self, loop, swarmq_logs, monkeypatch):
        async def _boom():
            loop._stop_event.set()
            raise RuntimeError("poll broke")

        loop._state = "leader"
        monkeypatch.setattr(loop, "_poll_tick", _boom)
        await loop._poll_loop()
        entry = self._entry(swarmq_logs, "scheduler_poll_tick_error")
        assert entry["log_level"] == "warning"
        assert entry["error_type"] == "RuntimeError"
        assert entry["error"] == "poll broke"

    async def test_poll_due_failure_logs_error_fields(self, loop, store, swarmq_logs):
        store.poll_due.side_effect = BackendConnectionError("valkey down")
        await loop._poll_tick()
        entry = self._entry(swarmq_logs, "scheduler_poll_due_failed")
        assert entry["log_level"] == "warning"
        assert entry["error_type"] == "BackendConnectionError"
        assert entry["error"] == "valkey down"

    @pytest.mark.usefixtures("_cleanup_tasks")
    async def test_unique_setnx_failure_logs_error_fields(self, loop, backend, store, swarmq_logs):
        self._unique_cron_task()
        backend.claim_unique = AsyncMock(side_effect=BackendConnectionError("valkey down"))
        loop._state = "leader"
        store.poll_due.return_value = [("dup_task", 1_000, _cron_def(name="dup_task", task_name="dup_task"))]
        await loop._poll_tick()
        entry = self._entry(swarmq_logs, "scheduler_unique_setnx_failed")
        assert entry["log_level"] == "warning"
        assert entry["error_type"] == "BackendConnectionError"
        assert entry["error"] == "valkey down"
        assert entry["schedule_name"] == "dup_task"

    async def test_publish_failure_logs_error_fields(self, loop, broker, store, swarmq_logs):
        loop._state = "leader"
        d = _def(cron="*/5 * * * *", fire_ts_ms=1_000)
        store.poll_due.return_value = [(d.schedule_name, 1_000, d)]
        broker.publish.side_effect = RuntimeError("conn refused")
        await loop._poll_tick()
        entry = self._entry(swarmq_logs, "scheduler_publish_failed")
        assert entry["log_level"] == "warning"
        assert entry["error_type"] == "RuntimeError"
        assert entry["error"] == "conn refused"
        assert entry["schedule_name"] == d.schedule_name

    async def test_commit_failure_logs_error_fields(self, loop, store, swarmq_logs):
        loop._state = "leader"
        d = _def(cron="*/5 * * * *", fire_ts_ms=1_000)
        store.poll_due.return_value = [(d.schedule_name, 1_000, d)]
        store.commit_fire.side_effect = BackendConnectionError("valkey down")
        await loop._poll_tick()
        entry = self._entry(swarmq_logs, "scheduler_commit_failed")
        assert entry["log_level"] == "warning"
        assert entry["error_type"] == "BackendConnectionError"
        assert entry["error"] == "valkey down"
        assert entry["schedule_name"] == d.schedule_name
        assert entry["dup_detected"] is False

    async def test_duplicate_signal_dispatch_error_logs_error_fields(
        self, loop, signal_dispatcher, swarmq_logs, monkeypatch
    ):
        monkeypatch.setattr(signal_dispatcher, "dispatch", AsyncMock(side_effect=RuntimeError("dispatch broke")))
        await loop._emit_duplicate_detected(existing_task_id="t-1", task_name="dup_task", dedup_hash="h")
        entry = self._entry(swarmq_logs, "scheduler_duplicate_signal_dispatch_error")
        assert entry["log_level"] == "warning"
        assert entry["error_type"] == "RuntimeError"
        assert entry["error"] == "dispatch broke"
        assert entry["task_name"] == "dup_task"

    async def test_signal_dispatch_error_logs_error_fields(self, loop, signal_dispatcher, swarmq_logs, monkeypatch):
        monkeypatch.setattr(signal_dispatcher, "dispatch", AsyncMock(side_effect=RuntimeError("dispatch broke")))
        await loop._emit(Signal.SCHEDULER_HEARTBEAT)
        entry = self._entry(swarmq_logs, "scheduler_signal_dispatch_error")
        assert entry["log_level"] == "warning"
        assert entry["error_type"] == "RuntimeError"
        assert entry["error"] == "dispatch broke"
        assert entry["signal"] == Signal.SCHEDULER_HEARTBEAT.value


def _utc_ms(*args: int) -> int:
    return int(datetime(*args, tzinfo=UTC).timestamp() * 1000)


class TestSchedulerLoopTimezone:
    """Plan 2026-10-08-1541 (U2): Score und Skip-Zaehlung in der Zone der
    Definition. Die Uhr ist fest (``time.time`` im Loop-Modul gepatcht)."""

    @pytest.fixture
    def loop(self, backend, broker, config, lock_manager, store, signal_dispatcher, monkeypatch):
        # 2026-10-06 10:00 UTC = 19:00 in Tokio.
        fake_time = SimpleNamespace(time=lambda: _utc_ms(2026, 10, 6, 10, 0) / 1000)
        monkeypatch.setattr("swarmq.scheduler.loop.time", fake_time)
        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
        loop._state = "leader"
        return loop

    @staticmethod
    def _tokyo_def() -> ScheduleDefinition:
        # Faellig war 09:00 Tokio am 06.10. (= 00:00 UTC).
        return msgspec.structs.replace(
            _def(name="tokyo", cron="0 9 * * *", fire_ts_ms=_utc_ms(2026, 10, 6, 0, 0)),
            timezone="Asia/Tokyo",
        )

    async def test_next_score_uses_definition_timezone(self, loop, store):
        d = self._tokyo_def()
        store.poll_due.return_value = [("tokyo", d.fire_ts_ms, d)]

        await loop._poll_tick()

        commit_args = store.commit_fire.await_args
        assert commit_args.args[1] == _utc_ms(2026, 10, 7, 0, 0)
        assert commit_args.args[2].fire_ts_ms == _utc_ms(2026, 10, 7, 0, 0)

    async def test_skipped_fires_are_counted_in_definition_timezone(self, loop, store, signal_dispatcher):
        """Zwischen 00:00 und 10:00 UTC liegt kein Termin ``0 9`` in Tokio
        (09:00 UTC waere es in UTC gewesen) -- also nichts uebersprungen."""
        skipped: list[dict] = []

        @signal_dispatcher.on_signal(Signal.SCHEDULER_FIRE_SKIPPED)
        async def _sk(**kw):
            skipped.append(kw)

        d = self._tokyo_def()
        store.poll_due.return_value = [("tokyo", d.fire_ts_ms, d)]

        await loop._poll_tick()

        assert skipped == []


class TestNextFireFailure:
    """Plan 2026-10-08-1541 (U3, R18): eine Definition, deren naechster
    Termin sich im Leader nicht berechnen laesst, blockiert nichts."""

    @pytest.fixture
    def loop(self, backend, broker, config, lock_manager, store, signal_dispatcher):
        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
        loop._state = "leader"
        return loop

    @staticmethod
    def _batch():
        broken = msgspec.structs.replace(
            _def(name="broken", fire_ts_ms=1_000),
            timezone="Europe/Atlantis",
        )
        good = _def(name="good", fire_ts_ms=1_000)
        return [("broken", 1_000, broken), ("good", 1_000, good)]

    async def test_broken_entry_is_logged_and_rest_of_batch_fires(
        self,
        loop,
        broker,
        store,
        swarmq_logs,
    ):
        store.poll_due.return_value = self._batch()

        await loop._poll_tick()

        headers = broker.publish.await_args.kwargs["headers"]
        assert broker.publish.await_count == 1
        assert headers["x-swarmq-schedfire"] == "good:1000"
        [entry] = [e for e in swarmq_logs if e["event"] == "scheduler_next_fire_failed"]
        assert entry["schedule_name"] == "broken"
        assert entry["error_type"]

    async def test_broken_entry_leaves_the_fire_index(self, loop, store):
        """Ohne Folgezeit kein neuer Score: der Eintrag verlaesst den
        Fire-Index (``-1``), die Definition bleibt stehen."""
        batch = self._batch()
        store.poll_due.return_value = batch

        await loop._poll_tick()

        [broken_commit] = [c for c in store.commit_fire.await_args_list if c.args[0] == "broken"]
        assert broken_commit.args[1:4] == (-1, batch[0][2], "test-owner")
        assert broken_commit.kwargs["fired_ts_ms"] == 1_000

    async def test_park_connection_error_does_not_abort_batch(
        self,
        loop,
        broker,
        store,
        swarmq_logs,
    ):
        """Liste 20 (Review-Befund #3): R18-Isolation auch beim Parken."""
        store.poll_due.return_value = self._batch()

        async def _commit(name, *_args, **_kwargs):
            if name == "broken":
                raise BackendConnectionError("valkey down")
            return True

        store.commit_fire.side_effect = _commit

        await loop._poll_tick()

        assert broker.publish.await_args.kwargs["headers"]["x-swarmq-schedfire"] == "good:1000"
        [entry] = [e for e in swarmq_logs if e["event"] == "scheduler_park_failed"]
        assert entry["schedule_name"] == "broken"
        assert entry["error_type"]

    async def test_park_owner_mismatch_is_logged(self, loop, store, swarmq_logs):
        """Liste 21 (Review-Befund #3): das abgelehnte Parken bleibt sichtbar."""
        store.poll_due.return_value = self._batch()[:1]
        store.commit_fire.return_value = False

        await loop._poll_tick()

        [entry] = [e for e in swarmq_logs if e["event"] == "scheduler_park_owner_mismatch"]
        assert entry["schedule_name"] == "broken"


class TestTakeoverBetweenPollAndCommit:
    """Liste 9 (KTD12): echter ScheduleStore ueber dem In-Memory-Backend.
    Die Uebernahme laeuft waehrend des Publish, also zwischen Poll und
    Commit; der faellige Score blieb dabei stehen."""

    async def test_commit_does_not_reset_taken_over_definition(
        self,
        broker,
        config,
        lock_manager,
        signal_dispatcher,
    ):
        memory = InMemoryBackend(FakeClock())
        await memory.connect()
        await memory.locks.acquire(
            SCHEDULER_LOCK_NAME,
            owner="test-owner",
            ttl_ms=60_000,
            max_concurrency=1,
        )
        polled = _def(name="cron_a", fire_ts_ms=1_000)
        taken_over = msgspec.structs.replace(polled, timezone="Asia/Tokyo")
        await memory.schedules.add("cron_a", msgspec.json.encode(polled), 1_000)

        async def _takeover(*_args, **_kwargs):
            await memory.schedules.put("cron_a", msgspec.json.encode(taken_over), None)

        broker.publish.side_effect = _takeover
        loop = _make_loop(
            memory,
            broker,
            config,
            lock_manager,
            ScheduleStore(memory),
            signal_dispatcher,
        )
        loop._state = "leader"

        await loop._poll_tick()

        stored = msgspec.json.decode(await memory.schedules.get("cron_a"), type=ScheduleDefinition)
        assert stored == taken_over
        assert await memory.schedules.fire_ts("cron_a") > 1_000


class TestRenewRightAfterTakeover:
    """Liste 23-25 (BZ-07)."""

    async def test_renew_false_after_takeover_stays_standby(
        self, backend, broker, config, lock_manager, store, signal_dispatcher, swarmq_logs
    ):
        """Liste 23."""
        signals: list[Signal] = []

        @signal_dispatcher.on_signal(Signal.SCHEDULER_LEADER)
        async def _on_leader(**_kw):
            signals.append(Signal.SCHEDULER_LEADER)

        lock_manager.renew.return_value = False
        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)

        await loop._election_tick()

        assert loop.state == "standby"
        assert signals == []
        assert any(e["event"] == "scheduler_lock_lost" for e in swarmq_logs)

    async def test_renew_error_after_takeover_still_leads(
        self, backend, broker, config, lock_manager, store, signal_dispatcher, swarmq_logs
    ):
        """Liste 24."""
        lock_manager.renew.side_effect = BackendConnectionError("valkey down")
        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)

        await loop._election_tick()

        assert loop.state == "leader"
        [entry] = [e for e in swarmq_logs if e["event"] == "scheduler_renew_error"]
        assert entry["error_type"] == "BackendConnectionError"

    async def test_takeover_offers_last_seen_leader_for_its_handover_marker(
        self, backend, broker, config, lock_manager, store, signal_dispatcher
    ):
        """Liste 25."""
        backend.schedules.leader_info.return_value = LeaderInfo(owner="old-leader", version="0.11.0")
        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
        lock_manager.acquire.return_value = None
        await loop._election_tick()

        lock_manager.acquire.return_value = _LEADER_TOKEN
        await loop._election_tick()

        backend.schedules.take_handover_marker.assert_awaited_once_with(previous_owner="old-leader")


class TestUndecodableDefinitionInPoll:
    """Liste 22 (BZ-28): echter ScheduleStore ueber dem In-Memory-Backend."""

    async def test_leader_parks_undecodable_definition_and_fires_the_rest(
        self, broker, config, lock_manager, signal_dispatcher
    ):
        memory = InMemoryBackend(FakeClock())
        await memory.connect()
        await memory.locks.acquire(SCHEDULER_LOCK_NAME, owner="test-owner", ttl_ms=60_000, max_concurrency=1)
        await memory.schedules.add("broken", b"{not json", 1_000)
        await memory.schedules.add("cron_a", msgspec.json.encode(_def(name="cron_a", fire_ts_ms=2_000)), 2_000)
        loop = _make_loop(memory, broker, config, lock_manager, ScheduleStore(memory), signal_dispatcher)
        loop._state = "leader"

        await loop._poll_tick()

        fired = [call.kwargs["headers"]["x-swarmq-schedfire"] for call in broker.publish.await_args_list]
        assert fired == ["cron_a:2000"]
        assert await memory.schedules.fire_ts("broken") is None
        assert await memory.schedules.get("broken") == b"{not json"


class TestAdoptDuringFire:
    """Liste 18/19 (Review-Befund #1): echter ScheduleStore ueber dem
    In-Memory-Backend, Uebernahme ueber eine Cron-Meldung eines Workers."""

    _NEW_CRON = "7 3 * * *"  # Minute 7 trifft nie ein */5-Raster
    _NEW_ZONE = "Asia/Tokyo"

    async def _setup(self, broker, config, lock_manager, signal_dispatcher):
        memory = InMemoryBackend(FakeClock())
        await memory.connect()
        await memory.locks.acquire(
            SCHEDULER_LOCK_NAME,
            owner="test-owner",
            ttl_ms=60_000,
            max_concurrency=1,
        )
        polled = _def(name="cron_a", fire_ts_ms=1_000)
        reported = msgspec.structs.replace(
            polled,
            cron=self._NEW_CRON,
            timezone=self._NEW_ZONE,
        )
        await memory.schedules.add("cron_a", msgspec.json.encode(polled), 1_000)
        await memory.schedules.report_cron(
            "cron_a",
            cron_fingerprint(reported),
            msgspec.json.encode(reported),
            live_window_ms=live_window_ms(config.scheduler_seen_interval_s),
        )
        loop = _make_loop(
            memory,
            broker,
            config,
            lock_manager,
            ScheduleStore(memory),
            signal_dispatcher,
        )
        loop._state = "leader"
        adopted = asyncio.Event()
        adopt = loop.adopt_reported_crons

        async def _adopt_and_signal():
            await adopt()
            adopted.set()

        loop.adopt_reported_crons = _adopt_and_signal
        return memory, loop, adopted

    def _assert_scored_by_new_cron(self, score_ms):
        local = datetime.fromtimestamp(score_ms / 1000, ZoneInfo(self._NEW_ZONE))
        assert (local.hour, local.minute) == (3, 7)

    async def _stop(self, loop, *tasks):
        loop._stop_event.set()
        loop._adopt_now.set()
        await asyncio.gather(*tasks)

    async def test_adoption_waits_for_running_fire(
        self,
        broker,
        config,
        lock_manager,
        signal_dispatcher,
    ):
        memory, loop, adopted = await self._setup(
            broker,
            config,
            lock_manager,
            signal_dispatcher,
        )
        published, release = asyncio.Event(), asyncio.Event()

        async def _publish_and_trigger_adoption(*_args, **_kwargs):
            loop._adopt_now.set()
            published.set()
            await release.wait()

        broker.publish.side_effect = _publish_and_trigger_adoption
        adopt_task = asyncio.create_task(loop._adopt_loop())
        poll_task = asyncio.create_task(loop._poll_loop())
        await published.wait()
        release.set()
        await adopted.wait()
        await self._stop(loop, adopt_task, poll_task)

        self._assert_scored_by_new_cron(await memory.schedules.fire_ts("cron_a"))

    async def test_adoption_before_poll_plans_with_new_cron(
        self,
        broker,
        config,
        lock_manager,
        signal_dispatcher,
    ):
        memory, loop, adopted = await self._setup(
            broker,
            config,
            lock_manager,
            signal_dispatcher,
        )
        loop._adopt_now.set()
        adopt_task = asyncio.create_task(loop._adopt_loop())
        await adopted.wait()

        await loop._poll_tick()
        await self._stop(loop, adopt_task)

        headers = broker.publish.await_args.kwargs["headers"]
        assert headers["x-swarmq-schedfire"] == "cron_a:1000"
        self._assert_scored_by_new_cron(await memory.schedules.fire_ts("cron_a"))
