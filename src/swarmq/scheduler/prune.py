"""Cron-Aufräumen über Lebenszeichen (Plan 2026-10-08-1541, U10, KTD8).

Lebenszeichen sind die ``last_seen``-Werte der Cron-Meldungen (KTD10). Ein
statischer Cron, den mindestens ein Worker gemeldet hat und den seit
``scheduler_prune_after_s`` keiner mehr meldet, ist verwaist. Nie gemeldete
statische Crons (Altbestand, Apps vor 0.10.0) sind nur Kandidaten.
"""
from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from swarmq.contracts.coordination import CronReports
from swarmq.scheduler.definition import ScheduleDefinition


@dataclass(frozen=True, slots=True)
class PrunePlan:
    """``stale``: gemeldet, jüngste Meldung älter als die Frist — der Leader
    entfernt sie. ``orphans``: nie gemeldet — nur Kandidaten (R13)."""

    stale: tuple[str, ...]
    orphans: tuple[str, ...]


def last_seen_by_name(snapshot: CronReports) -> dict[str, int]:
    """Jüngstes ``last_seen`` je Name über alle Abdrücke."""
    last_seen: dict[str, int] = {}
    for report in snapshot.reports:
        last_seen[report.name] = max(last_seen.get(report.name, 0), report.last_seen_ms)
    return last_seen


def plan_prune(
    definitions: Iterable[ScheduleDefinition], snapshot: CronReports, *,
    prune_after_ms: int,
) -> PrunePlan:
    """Verwaiste und nie gemeldete statische Crons (KTD8). CLI-Definitionen
    bleiben unberührt; gemessen wird in Backend-Zeit (``snapshot.now_ms``)."""
    last_seen = last_seen_by_name(snapshot)
    cutoff = snapshot.now_ms - prune_after_ms
    stale: list[str] = []
    orphans: list[str] = []
    for definition in definitions:
        if definition.source != "static":
            continue
        seen = last_seen.get(definition.schedule_name)
        if seen is None:
            orphans.append(definition.schedule_name)
        elif seen < cutoff:
            stale.append(definition.schedule_name)
    return PrunePlan(stale=tuple(stale), orphans=tuple(orphans))
