"""Cron-Aufräumen über Lebenszeichen (Plan 2026-10-08-1541, U10, KTD8, R11/R13)."""

# Test list (U10):
# Auswahl (rein, ``plan_prune``):
# 1. Gemeldeter statischer Cron, jüngste Meldung älter als die Frist → stale.
# 2. Frische Meldung → bleibt.
# 3. CLI-Cron (``cli``, ``cli_unique``) → nie betroffen, auch ohne Meldung.
# 4. Nie gemeldeter statischer Cron → Kandidat (orphan), nicht stale.
# 5. Rolling Deploy: alter Abdruck frisch, neuer nicht (oder umgekehrt) →
#    die jüngste Meldung über alle Abdrücke zählt, Cron bleibt.
# Leader-Takt (``adopt_reported_crons`` mit In-Memory-Store):
# 6. Stale → entfernt samt Fire-Index-Eintrag, Log ``scheduler_cron_pruned``.
# 7. ``cron=`` entfernt, Task bleibt (Worker meldet nichts mehr) → nach Frist
#    entfernt.
# 8. Orphan → bleibt, Log ``scheduler_cron_prune_candidate`` (einmal je Name).
# 9. ``scheduler_prune=log`` → nur Log, ``off`` → nichts.
# 10. Entfernter deaktivierter Cron → beim nächsten Melden deaktiviert wieder
#     angelegt, ohne Fire-Index-Eintrag; danach ist die Markierung verbraucht.
# 11. Meldungen bleiben mindestens 2 x Frist erhalten (sonst würde eine lange
#     Frist nie greifen, weil die Meldung vorher gelöscht wäre).
# 12. Fehler beim Entfernen → geloggt, Takt läuft weiter.

from __future__ import annotations

import dataclasses

import msgspec
import pytest

from swarmq.contracts import CronReport, CronReports
from swarmq.exceptions import BackendConnectionError
from swarmq.scheduler import ScheduleDefinition, ScheduleStore
from swarmq.scheduler.prune import plan_prune
from swarmq.scheduler.reports import cron_fingerprint, static_definitions
from swarmq.task import Task, TaskInfo
from tests.support.inmemory import FakeClock, InMemoryBackend
from tests.unit.scheduler._helpers import _make_loop

pytestmark = [pytest.mark.timeout(5)]

_NOW_MS = 1_800_000_000_000
_DAY_MS = 86_400_000


def _static(name: str = "nightly", **fields) -> ScheduleDefinition:
    base = {
        "schedule_name": name, "task_name": name, "args": (), "kwargs": {},
        "queue": "default", "cron": "0 1 * * *", "fire_ts_ms": None,
        "source": "static",
    }
    base.update(fields)
    return ScheduleDefinition(**base)


def _report(name: str = "nightly", *, last_seen_ms: int, fingerprint: str = "fp1") -> CronReport:
    return CronReport(name, fingerprint, last_seen_ms - 1000, last_seen_ms, b"{}")


def _snapshot(*reports: CronReport) -> CronReports:
    return CronReports(now_ms=_NOW_MS, reports=reports)


class TestPlanPrune:
    def test_reported_cron_silent_longer_than_prune_after_is_stale(self):
        plan = plan_prune(
            [_static()], _snapshot(_report(last_seen_ms=_NOW_MS - _DAY_MS - 1)),
            prune_after_ms=_DAY_MS,
        )

        assert plan.stale == ("nightly",)

    def test_fresh_report_keeps_the_cron(self):
        plan = plan_prune(
            [_static()], _snapshot(_report(last_seen_ms=_NOW_MS - _DAY_MS)),
            prune_after_ms=_DAY_MS,
        )

        assert plan.stale == ()

    @pytest.mark.parametrize("source", ["cli", "cli_unique"])
    def test_cli_cron_is_never_pruned(self, source):
        plan = plan_prune(
            [_static(source=source)],
            _snapshot(_report(last_seen_ms=_NOW_MS - 2 * _DAY_MS)),
            prune_after_ms=_DAY_MS,
        )

        assert plan.stale == ()

    def test_never_reported_static_cron_is_an_orphan_not_stale(self):
        plan = plan_prune(
            [_static("legacy"), _static("cli_only", source="cli")],
            _snapshot(_report("other", last_seen_ms=_NOW_MS)),
            prune_after_ms=_DAY_MS,
        )

        assert (plan.stale, plan.orphans) == ((), ("legacy",))

    def test_rolling_deploy_youngest_report_over_all_fingerprints_counts(self):
        """Die alte Version meldet X weiter, die neue nicht (oder mit anderem
        Abdruck): X bleibt, solange irgendein Abdruck frisch ist."""
        plan = plan_prune(
            [_static()],
            _snapshot(
                _report(last_seen_ms=_NOW_MS - 3 * _DAY_MS, fingerprint="new"),
                _report(last_seen_ms=_NOW_MS - 60_000, fingerprint="old"),
            ),
            prune_after_ms=_DAY_MS,
        )

        assert plan.stale == ()


class TestLeaderPrunes:
    """Leader-Takt gegen einen In-Memory-Store mit fester Backend-Uhr; die
    Frist ist der Standard (24 h)."""

    @pytest.fixture
    def clock(self):
        return FakeClock(_NOW_MS)

    @pytest.fixture
    async def memory(self, clock):
        be = InMemoryBackend(clock)
        await be.connect()
        return be

    @pytest.fixture
    def leader(self, memory, broker, config, lock_manager, signal_dispatcher):
        return _make_loop(
            memory, broker, config, lock_manager, ScheduleStore(memory),
            signal_dispatcher,
        )

    @staticmethod
    async def _seed(memory, definition: ScheduleDefinition, *, fire_ts_ms=_NOW_MS + _DAY_MS):
        stored = msgspec.structs.replace(definition, fire_ts_ms=fire_ts_ms)
        await memory.schedules.add(
            definition.schedule_name, msgspec.json.encode(stored),
            None if definition.disabled else fire_ts_ms,
        )

    @staticmethod
    async def _report(memory, definition: ScheduleDefinition):
        await memory.schedules.report_cron(
            definition.schedule_name, cron_fingerprint(definition),
            msgspec.json.encode(definition), live_window_ms=180_000,
        )

    async def test_silent_reported_cron_is_removed_with_its_fire_entry(
        self, memory, leader, clock, swarmq_logs,
    ):
        await self._seed(memory, _static())
        await self._report(memory, _static())
        await clock.advance(_DAY_MS / 1000 + 1)

        await leader.adopt_reported_crons()

        assert await memory.schedules.get("nightly") is None
        assert await memory.schedules.fire_ts("nightly") is None
        [entry] = [e for e in swarmq_logs if e["event"] == "scheduler_cron_pruned"]
        assert entry["schedule_name"] == "nightly" and entry["log_level"] == "info"

    async def test_cron_removed_from_task_is_pruned_after_the_period(
        self, memory, leader, clock, _cleanup_tasks,
    ):
        """``cron=`` entfernt, Task bleibt: der Worker meldet den Namen nicht
        mehr; nach der Frist verschwindet der Cron."""
        await self._seed(memory, _static())
        await self._report(memory, _static())

        class Nightly(Task):
            task_info = TaskInfo(name="nightly", queues=["default"])

            async def run(self):
                return None

        assert static_definitions([Nightly], None) == []
        await clock.advance(_DAY_MS / 1000 - 1)
        await leader.adopt_reported_crons()
        assert await memory.schedules.get("nightly") is not None

        await clock.advance(2)
        await leader.adopt_reported_crons()
        assert await memory.schedules.get("nightly") is None

    async def test_never_reported_cron_stays_and_is_logged_once_as_candidate(
        self, memory, leader, clock, swarmq_logs,
    ):
        await self._seed(memory, _static("legacy"))
        await self._seed(memory, _static("by_cli", source="cli"))

        await leader.adopt_reported_crons()
        await clock.advance(30 * _DAY_MS / 1000)
        await leader.adopt_reported_crons()

        assert await memory.schedules.get("legacy") is not None
        assert await memory.schedules.fire_ts("legacy") is not None
        entries = [e for e in swarmq_logs if e["event"] == "scheduler_cron_prune_candidate"]
        assert [e["schedule_name"] for e in entries] == ["legacy"]
        assert "schedule prune --orphans" in entries[0]["hint"]

    @staticmethod
    def _loop_with(memory, broker, config, lock_manager, signal_dispatcher, mode):
        config = dataclasses.replace(config, scheduler_prune=mode)
        return _make_loop(
            memory, broker, config, lock_manager, ScheduleStore(memory),
            signal_dispatcher,
        )

    async def test_log_mode_only_logs(
        self, memory, broker, config, lock_manager, signal_dispatcher, clock, swarmq_logs,
    ):
        leader = self._loop_with(memory, broker, config, lock_manager, signal_dispatcher, "log")
        await self._seed(memory, _static())
        await self._report(memory, _static())
        await clock.advance(_DAY_MS / 1000 + 1)

        await leader.adopt_reported_crons()
        await leader.adopt_reported_crons()

        assert await memory.schedules.get("nightly") is not None
        assert await memory.schedules.fire_ts("nightly") is not None
        assert not [e for e in swarmq_logs if e["event"] == "scheduler_cron_pruned"]
        entries = [e for e in swarmq_logs if e["event"] == "scheduler_cron_prune_skipped"]
        assert [e["schedule_name"] for e in entries] == ["nightly"]

    async def test_off_mode_does_nothing(
        self, memory, broker, config, lock_manager, signal_dispatcher, clock, swarmq_logs,
    ):
        leader = self._loop_with(memory, broker, config, lock_manager, signal_dispatcher, "off")
        await self._seed(memory, _static())
        await self._seed(memory, _static("legacy"))
        await self._report(memory, _static())
        await clock.advance(_DAY_MS / 1000 + 1)

        await leader.adopt_reported_crons()

        assert await memory.schedules.get("nightly") is not None
        assert not [e for e in swarmq_logs if e["event"].startswith("scheduler_cron_prune")]

    async def test_pruned_disabled_cron_comes_back_disabled(self, memory, leader, clock):
        await self._seed(memory, _static(disabled=True))
        await self._report(memory, _static())
        await clock.advance(_DAY_MS / 1000 + 1)
        await leader.adopt_reported_crons()
        assert await memory.schedules.get("nightly") is None

        await self._report(memory, _static())
        await leader.adopt_reported_crons()

        raw = await memory.schedules.get("nightly")
        assert raw is not None
        assert msgspec.json.decode(raw, type=ScheduleDefinition).disabled is True
        assert await memory.schedules.fire_ts("nightly") is None
        assert await memory.schedules.pruned_disabled("nightly") is False

    async def test_reports_outlive_a_period_longer_than_seven_days(
        self, memory, broker, config, lock_manager, signal_dispatcher, clock,
    ):
        """Sonst wäre die Meldung vor Ablauf der Frist gelöscht und der Cron
        gälte als nie gemeldet."""
        config = dataclasses.replace(config, scheduler_prune_after_s=10 * _DAY_MS / 1000)
        leader = _make_loop(
            memory, broker, config, lock_manager, ScheduleStore(memory), signal_dispatcher,
        )
        await self._seed(memory, _static())
        await self._report(memory, _static())

        await clock.advance(8 * _DAY_MS / 1000)
        await leader.adopt_reported_crons()
        await clock.advance(2 * _DAY_MS / 1000 + 1)
        await leader.adopt_reported_crons()

        assert await memory.schedules.get("nightly") is None

    async def test_malformed_definition_does_not_stop_the_prune(self, memory, leader, clock):
        await memory.schedules.add("broken", b"{not json", None)
        await self._seed(memory, _static())
        await self._report(memory, _static())
        await clock.advance(_DAY_MS / 1000 + 1)

        await leader.adopt_reported_crons()

        assert await memory.schedules.get("nightly") is None
        assert await memory.schedules.get("broken") == b"{not json"

    async def test_failed_removal_is_logged_and_the_tick_goes_on(
        self, memory, leader, clock, monkeypatch, swarmq_logs,
    ):
        real = memory.schedules.prune_cron

        async def _flaky(name, *args, **kwargs):
            if name == "first":
                raise BackendConnectionError("valkey flap")
            return await real(name, *args, **kwargs)

        monkeypatch.setattr(memory.schedules, "prune_cron", _flaky)
        for name in ("first", "second"):
            await self._seed(memory, _static(name))
            await self._report(memory, _static(name))
        await clock.advance(_DAY_MS / 1000 + 1)

        await leader.adopt_reported_crons()

        assert await memory.schedules.get("first") is not None
        assert await memory.schedules.get("second") is None
        [entry] = [e for e in swarmq_logs if e["event"] == "scheduler_cron_prune_failed"]
        assert entry["log_level"] == "warning" and entry["schedule_name"] == "first"
        assert entry["error"] == "valkey flap"

    async def test_unreadable_definitions_are_logged_and_reports_still_pruned(
        self, memory, leader, clock, monkeypatch, swarmq_logs,
    ):
        async def _down():
            raise BackendConnectionError("valkey down")

        monkeypatch.setattr(memory.schedules, "list_all", _down)
        await self._report(memory, _static("ancient"))
        await clock.advance(8 * _DAY_MS / 1000)

        await leader.adopt_reported_crons()

        [entry] = [e for e in swarmq_logs if e["event"] == "scheduler_cron_prune_failed"]
        assert entry["log_level"] == "warning" and entry["error"] == "valkey down"
        assert (await memory.schedules.cron_reports()).reports == ()
