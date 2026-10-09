"""Cron-Aufräumen über Lebenszeichen (Plan 2026-10-08-1541, U10, KTD8).

Lebenszeichen sind die ``last_seen``-Werte der Cron-Meldungen (KTD10). Ein
statischer Cron, den mindestens ein Worker gemeldet hat und den seit
``scheduler_prune_after_s`` keiner mehr meldet, ist verwaist. Nie gemeldete
statische Crons (Altbestand, Apps vor 0.10.0) sind nur Kandidaten.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

import msgspec

from swarmq.config import SwarmQConfig
from swarmq.contracts.coordination import CronReports, ScheduleStore
from swarmq.exceptions import BackendConnectionError
from swarmq.logging import _error_fields, get_logger
from swarmq.scheduler.definition import ScheduleDefinition

logger = get_logger(__name__)


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


def decode_definitions(raw: Mapping[str, bytes]) -> dict[str, tuple[bytes, ScheduleDefinition]]:
    """Gespeicherte Definitionen samt Rohbytes (für den atomaren Vergleich
    beim Entfernen). Nicht dekodierbare fehlen: unklare Quelle, nie anfassen
    (wie ``apply_static_definition``)."""
    definitions: dict[str, tuple[bytes, ScheduleDefinition]] = {}
    for name, payload in raw.items():
        try:
            definitions[name] = (payload, msgspec.json.decode(payload, type=ScheduleDefinition))
        except msgspec.DecodeError:
            continue
    return definitions


async def prune_orphans(store: ScheduleStore, snapshot: CronReports) -> list[str]:
    """``schedule prune --orphans`` (R13): nie gemeldete statische Crons
    entfernen; deaktivierte kommen beim nächsten Melden deaktiviert zurück.
    Liefert die entfernten Namen."""
    definitions = decode_definitions(await store.list_all())
    plan = plan_prune([d for _, d in definitions.values()], snapshot, prune_after_ms=0)
    removed: list[str] = []
    for name in plan.orphans:
        payload, definition = definitions[name]
        if await store.prune_cron(
            name, payload, stale_after_ms=None, remember_disabled=definition.disabled,
        ):
            removed.append(name)
    return removed


class SchedulerCronPrune:
    # Mixin über dem Loop-Zustand (wie ``SchedulerCronSync``): die Attribute
    # setzt ``SchedulerLoop.__init__``.
    _backend: Any
    _config: SwarmQConfig
    # Bereits geloggte (Event, Name): Kandidaten und Crons, die nur wegen
    # ``scheduler_prune=log`` bleiben, einmal je Loop, nicht in jedem Takt.
    _prune_logged: frozenset[tuple[str, str]] = frozenset()

    def _first_time(self, event: str, name: str) -> bool:
        if (event, name) in self._prune_logged:
            return False
        self._prune_logged |= {(event, name)}
        return True

    async def prune_static_crons(self, snapshot: CronReports) -> None:
        """Leader-Takt (KTD8): verwaiste statische Crons entfernen. Läuft
        mit dem Meldungs-Snapshot der Übernahme. ``scheduler_prune=off``
        schaltet es ganz ab."""
        if self._config.scheduler_prune == "off":
            return
        try:
            raw = await self._backend.schedules.list_all()
        except BackendConnectionError as exc:
            logger.warning("scheduler_cron_prune_failed", **_error_fields(exc))
            return
        definitions = decode_definitions(raw)
        prune_after_ms = int(self._config.scheduler_prune_after_s * 1000)
        plan = plan_prune(
            [d for _, d in definitions.values()], snapshot, prune_after_ms=prune_after_ms,
        )
        for name in plan.orphans:
            if self._first_time("candidate", name):
                logger.info(
                    "scheduler_cron_prune_candidate",
                    schedule_name=name,
                    hint=(
                        "no worker has ever reported this static cron (legacy "
                        "entry or an app before 0.10.0); it is never removed "
                        "automatically -- 'swarmq schedule prune --orphans' "
                        "removes it"
                    ),
                )
        for name in plan.stale:
            if self._config.scheduler_prune == "log":
                if self._first_time("skipped", name):
                    logger.info(
                        "scheduler_cron_prune_skipped",
                        schedule_name=name,
                        hint=(
                            "no worker has reported this static cron within "
                            "scheduler_prune_after_s; it stays because "
                            "scheduler_prune=log"
                        ),
                    )
                continue
            payload, definition = definitions[name]
            try:
                removed = await self._backend.schedules.prune_cron(
                    name, payload, stale_after_ms=prune_after_ms,
                    remember_disabled=definition.disabled,
                )
            except BackendConnectionError as exc:
                logger.warning(
                    "scheduler_cron_prune_failed", schedule_name=name, **_error_fields(exc),
                )
                continue
            if removed:
                logger.info("scheduler_cron_pruned", schedule_name=name)
