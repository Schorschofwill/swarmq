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

import pytest

from swarmq.contracts import CronReport, CronReports
from swarmq.scheduler import ScheduleDefinition
from swarmq.scheduler.prune import plan_prune

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
