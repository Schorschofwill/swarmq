"""Zurueckhalten verwaister Crons (Plan 0.12.0, MR 11, BZ-11).

Der Leader feuert keinen statischen Cron, den kein lebender Worker mehr
meldet und den er selbst nicht registriert: Er verschiebt den Termin per
Commit auf ``now + scheduler_seen_interval_s``, ohne zu publizieren. Die
Entscheidung braucht die Meldungen der aktuellen Amtszeit (Stempel je
Amtszeit); direkt nach dem Erwerb wartet der Poll hoechstens 5 s auf das
erste Adopt, danach gilt fail-open.

Test-Liste:
- [x] verwaist (je gemeldet, nicht lebend, nicht eigener, static):
      kein Publish, Commit auf now + seen_interval
- [x] lebend gemeldet: feuert
- [x] eigener statischer Cron: feuert
- [x] nie gemeldet (nicht in ``known``): feuert
- [x] ``source="cli"``: feuert
- [x] ohne Meldungen dieser Amtszeit (Adopt noch nicht gelaufen, < 5 s nach
      dem Erwerb): der Poll feuert nichts und wartet
- [x] Adopt gelaufen, aber Meldungen nicht lesbar: fail-open, feuert
- [x] mehr als 5 s nach dem Erwerb ohne Adopt: fail-open, feuert, WARNING
      ``scheduler_unowned_check_skipped`` einmal je Amtszeit
- [x] Log ``scheduler_cron_held_unowned`` und Signal ``SCHEDULER_CRON_UNOWNED``
      nur beim Uebergang (zweiter Poll: still)
- [x] wieder gemeldet: ``scheduler_cron_unowned_released`` und Signal
      ``held=False``
- [x] ``known_cron_names`` fehlt (Fremd-Store, Q15): nichts wird zurueckgehalten
- [x] neue Amtszeit setzt die Uebergaenge zurueck
- [x] Gauge ``swarmq_scheduler_crons_held`` aus dem Signal, 0 beim Standby
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from swarmq.contracts import CronReport, CronReports
from swarmq.scheduler import ScheduleDefinition
from swarmq.signals import Signal
from tests.unit.scheduler._helpers import _def, _make_loop

pytestmark = [pytest.mark.timeout(5)]

_NOW_S = 1_700_000_100.0
_NOW_MS = int(_NOW_S * 1000)
_SCORE_MS = 1_700_000_000_000


@pytest.fixture
def clock(monkeypatch):
    monkeypatch.setattr("swarmq.scheduler.loop.time.time", lambda: _NOW_S)


@pytest.fixture
def known(backend):
    backend.schedules.cron_reports = AsyncMock(return_value=CronReports(now_ms=_NOW_MS, reports=()))
    backend.schedules.known_cron_names = AsyncMock(return_value=frozenset({"ghost"}))
    backend.schedules.prune_cron_reports = AsyncMock(return_value=0)
    return backend


@pytest.fixture
def leader(known, broker, config, lock_manager, store, signal_dispatcher, clock):
    loop = _make_loop(known, broker, config, lock_manager, store, signal_dispatcher)
    loop._state = "leader"
    return loop


def _ghost(**fields: object) -> ScheduleDefinition:
    return _def(name="ghost", fire_ts_ms=_SCORE_MS, **fields)


@pytest.mark.entscheidung("E-014")
async def test_unowned_cron_is_held_instead_of_fired(leader, store, broker, config) -> None:
    store.poll_due.return_value = [("ghost", _SCORE_MS, _ghost())]
    await leader.adopt_reported_crons()

    await leader._poll_tick()

    broker.publish.assert_not_awaited()
    store.commit_fire.assert_awaited_once()
    name, next_fire_ts_ms, updated, owner = store.commit_fire.await_args.args
    assert (name, owner) == ("ghost", "test-owner")
    assert next_fire_ts_ms == _NOW_MS + int(config.scheduler_seen_interval_s * 1000)
    assert updated.fire_ts_ms == next_fire_ts_ms
    assert store.commit_fire.await_args.kwargs["fired_ts_ms"] == _SCORE_MS


def _live_report(name: str = "ghost") -> CronReport:
    return CronReport(name, "fp", _NOW_MS - 1_000, _NOW_MS - 1_000, b"{}")


async def _poll(leader, store, definition: ScheduleDefinition, name: str = "ghost") -> None:
    store.poll_due.return_value = [(name, _SCORE_MS, definition)]
    await leader._poll_tick()


async def test_live_reported_cron_fires(leader, known, store, broker) -> None:
    known.schedules.cron_reports.return_value = CronReports(now_ms=_NOW_MS, reports=(_live_report(),))
    await leader.adopt_reported_crons()

    await _poll(leader, store, _ghost())

    broker.publish.assert_awaited_once()


async def test_own_static_cron_fires(known, broker, config, lock_manager, store, signal_dispatcher, clock) -> None:
    loop = _make_loop(known, broker, config, lock_manager, store, signal_dispatcher)
    loop._static_crons = (_ghost(),)
    loop._state = "leader"
    await loop.adopt_reported_crons()

    await _poll(loop, store, _ghost())

    broker.publish.assert_awaited_once()


@pytest.mark.entscheidung("E-014")
async def test_cron_never_reported_fires(leader, known, store, broker) -> None:
    known.schedules.known_cron_names.return_value = frozenset()
    await leader.adopt_reported_crons()

    await _poll(leader, store, _ghost())

    broker.publish.assert_awaited_once()


async def test_cli_schedule_of_the_same_name_fires(leader, store, broker) -> None:
    await leader.adopt_reported_crons()

    await _poll(leader, store, _ghost(source="cli"))

    broker.publish.assert_awaited_once()


async def test_poll_waits_for_the_first_adopt_of_the_term(leader, store, broker) -> None:
    leader._begin_term(_NOW_MS - 4_000)
    store.poll_due.return_value = [("ghost", _SCORE_MS, _ghost())]

    await leader._poll_tick()

    store.poll_due.assert_not_awaited()
    broker.publish.assert_not_awaited()


async def test_adopt_without_readable_reports_fails_open(leader, known, store, broker, swarmq_logs) -> None:
    from swarmq.exceptions import BackendConnectionError

    leader._begin_term(_NOW_MS - 1_000)
    known.schedules.cron_reports.side_effect = BackendConnectionError("valkey down")
    await leader.adopt_reported_crons()
    leader._note_adopt_ran()

    await _poll(leader, store, _ghost())

    broker.publish.assert_awaited_once()
    [skipped] = [e for e in swarmq_logs if e["event"] == "scheduler_unowned_check_skipped"]
    assert skipped["log_level"] == "warning"


@pytest.mark.entscheidung("E-014")
async def test_poll_fails_open_five_seconds_after_the_takeover(leader, store, broker, swarmq_logs) -> None:
    leader._begin_term(_NOW_MS - 5_000)

    await _poll(leader, store, _ghost())
    await _poll(leader, store, _ghost())

    assert broker.publish.await_count == 2
    assert len([e for e in swarmq_logs if e["event"] == "scheduler_unowned_check_skipped"]) == 1


def _recorded(signal_dispatcher, signal):
    seen: list[dict[str, object]] = []

    async def _handler(**kw: object) -> None:
        seen.append(kw)

    signal_dispatcher.connect(signal, _handler)
    return seen


@pytest.mark.entscheidung("E-014")
async def test_hold_logs_and_signals_only_on_the_transition(leader, store, signal_dispatcher, swarmq_logs) -> None:
    seen = _recorded(signal_dispatcher, Signal.SCHEDULER_CRON_UNOWNED)
    await leader.adopt_reported_crons()

    await _poll(leader, store, _ghost())
    await _poll(leader, store, _ghost())

    assert store.commit_fire.await_count == 2
    [held] = [e for e in swarmq_logs if e["event"] == "scheduler_cron_held_unowned"]
    assert held["log_level"] == "warning"
    assert held["schedule_name"] == "ghost"
    assert held["task_name"] == "ping"
    assert seen == [{"worker_id": "w-1", "schedule_name": "ghost", "task_name": "ping", "held": True, "held_count": 1}]


async def test_the_hold_is_committed_before_it_is_signalled(leader, store, signal_dispatcher) -> None:
    """Wer auf das Signal hin den Termin liest, sieht schon den verschobenen
    (bz11-e2e unter Coverage: der Commit kam nach dem Signal)."""
    committed_at_signal: list[int] = []

    async def _handler(**_kw: object) -> None:
        committed_at_signal.append(store.commit_fire.await_count)

    signal_dispatcher.connect(Signal.SCHEDULER_CRON_UNOWNED, _handler)
    await leader.adopt_reported_crons()

    await _poll(leader, store, _ghost())

    assert committed_at_signal == [1]


async def test_reported_again_releases_the_hold(leader, known, store, broker, signal_dispatcher, swarmq_logs) -> None:
    seen = _recorded(signal_dispatcher, Signal.SCHEDULER_CRON_UNOWNED)
    await leader.adopt_reported_crons()
    await _poll(leader, store, _ghost())
    known.schedules.cron_reports.return_value = CronReports(now_ms=_NOW_MS, reports=(_live_report(),))
    await leader.adopt_reported_crons()

    await _poll(leader, store, _ghost())

    broker.publish.assert_awaited_once()
    [released] = [e for e in swarmq_logs if e["event"] == "scheduler_cron_unowned_released"]
    assert released["log_level"] == "info"
    assert released["schedule_name"] == "ghost"
    assert [(e["held"], e["held_count"]) for e in seen] == [(True, 1), (False, 0)]


async def test_pruned_cron_releases_the_hold(leader, known, store, signal_dispatcher) -> None:
    seen = _recorded(signal_dispatcher, Signal.SCHEDULER_CRON_UNOWNED)
    await leader.adopt_reported_crons()
    await _poll(leader, store, _ghost())
    known.schedules.known_cron_names.return_value = frozenset()

    await leader.adopt_reported_crons()

    assert [(e["held"], e["held_count"]) for e in seen] == [(True, 1), (False, 0)]


async def test_store_without_known_names_holds_nothing(leader, known, store, broker, swarmq_logs) -> None:
    # Q15: ein Fremd-Store ohne ``known_cron_names`` (MagicMock: nicht awaitbar).
    del known.schedules.known_cron_names
    await leader.adopt_reported_crons()

    await _poll(leader, store, _ghost())

    broker.publish.assert_awaited_once()
    assert [e["event"] for e in swarmq_logs if e["event"] == "scheduler_known_crons_unavailable"]


async def test_new_term_forgets_the_transitions(leader, store, signal_dispatcher) -> None:
    seen = _recorded(signal_dispatcher, Signal.SCHEDULER_CRON_UNOWNED)
    await leader.adopt_reported_crons()
    await _poll(leader, store, _ghost())

    leader._begin_term(_NOW_MS)
    await leader.adopt_reported_crons()
    await _poll(leader, store, _ghost())

    assert [e["held"] for e in seen] == [True, True]
