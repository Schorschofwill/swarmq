"""Cron-Meldungen aller Worker (Plan 2026-10-08-1541, U12, KTD10).

Jeder Worker meldet seine statischen Crons mit einem Fingerabdruck über die
aus dem Code stammenden Felder; der Leader übernimmt pro Name die jüngste
lebende Meldung. Dieses Modul hält die reinen Bausteine (Fingerabdruck,
Auswahl); Melden und Übernehmen leben im ``SchedulerLoop``.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import msgspec

from swarmq.config import normalize_timezone
from swarmq.contracts.coordination import CronReport, CronReports
from swarmq.scheduler.definition import ScheduleDefinition

if TYPE_CHECKING:
    from swarmq.task import Task


CONFLICT_AFTER_MS = 10 * 60 * 1000
"""Leben pro Name länger als diese Zeit mehrere Abdrücke, warnt der Leader
(R12)."""


REPORT_MAX_AGE_MS = 7 * 24 * 60 * 60 * 1000
"""Meldungen, deren ``last_seen`` älter ist, löscht der Leader (KTD10)."""


def live_window_ms(seen_interval_s: float) -> int:
    """Lebendigkeitsfenster einer Meldung: drei Meldetakte (KTD10)."""
    return int(3 * seen_interval_s * 1000)


def code_fields(definition: ScheduleDefinition) -> tuple[Any, ...]:
    """Die aus dem Code stammenden Felder einer statischen Definition
    (KTD10): Cron, normalisierte Zeitzone, Queue, Task-Name, dazu
    ``unique_for`` und ``unique_until`` nur, wenn ``unique_for`` gesetzt ist
    (BZ-09; ohne bleiben Felder und Abdruck wie vor 0.12). Weicht eines ab,
    übernimmt der Leader die Definition."""
    fields: tuple[Any, ...] = (definition.cron, definition.timezone, definition.queue, definition.task_name)
    if definition.unique_for is not None:
        fields += (definition.unique_for, definition.unique_until)
    return fields


def cron_fingerprint(definition: ScheduleDefinition) -> str:
    """Kurzer Hex-Abdruck über :func:`code_fields`; Felder außerhalb des
    Codes (Score, ``disabled``, letzter Fire) gehen nicht ein."""
    encoded = msgspec.json.encode(code_fields(definition))
    return hashlib.sha256(encoded).hexdigest()[:16]


def static_definitions(
    task_classes: Iterable[type[Task]],
    default_timezone: str | None,
) -> list[ScheduleDefinition]:
    """Die gewünschten statischen Definitionen der Tasks mit ``cron`` — so,
    wie ein Worker sie meldet. Die Zone ist bereits aufgelöst (KTD1): die
    eigene Zone des Tasks (auch ein ausdrückliches ``"UTC"``), sonst
    *default_timezone*; UTC wird als ``None`` gespeichert. Damit braucht der
    Leader keine fremde Config."""
    result: list[ScheduleDefinition] = []
    for task_class in task_classes:
        task_info = task_class.task_info
        if not task_info.cron:
            continue
        timezone = normalize_timezone(task_info.timezone if task_info.timezone is not None else default_timezone)
        result.append(
            ScheduleDefinition(
                schedule_name=task_info.name,
                task_name=task_info.name,
                args=(),
                kwargs={},
                queue=task_info.queues[0],
                cron=task_info.cron,
                fire_ts_ms=None,
                source="static",
                timezone=timezone,
                unique_for=task_info.unique_for,
                # Ohne ``unique_for`` wirkt ``unique_until`` nicht; der Default
                # hält Payload und Abdruck wie vor 0.12 (BZ-09).
                unique_until=(task_info.unique_until if task_info.unique_for is not None else "completion"),
            )
        )
    return result


@dataclass(frozen=True, slots=True)
class CronChoice:
    """Die gewählte Meldung eines Namens. ``conflict`` nennt die lebenden
    Abdrücke, wenn mehrere länger als :data:`CONFLICT_AFTER_MS` gleichzeitig
    leben (typisch: gleichnamige Tasks in zwei Apps), sonst ``()``."""

    report: CronReport
    conflict: tuple[str, ...] = ()


def _rank(report: CronReport) -> tuple[int, str]:
    # Jüngstes first_seen gewinnt; bei Gleichstand entscheidet der Abdruck,
    # damit die Auswahl nicht von der Lesereihenfolge abhängt.
    return report.first_seen_ms, report.fingerprint


def choose_reports(
    snapshot: CronReports,
    *,
    live_window_ms: int,
) -> list[CronChoice]:
    """Pro Name die lebende Meldung mit dem jüngsten ``first_seen`` (KTD10).
    Lebendig ist eine Meldung, deren ``last_seen`` höchstens
    *live_window_ms* vor ``snapshot.now_ms`` liegt; Namen ohne lebende
    Meldung fehlen im Ergebnis."""
    live_since = snapshot.now_ms - live_window_ms
    live: dict[str, list[CronReport]] = {}
    for report in snapshot.reports:
        if report.last_seen_ms >= live_since:
            live.setdefault(report.name, []).append(report)
    choices: list[CronChoice] = []
    for reports in live.values():
        winner = max(reports, key=_rank)
        # Alle lebenden Abdrücke leben gemeinsam seit dem jüngsten first_seen.
        overlap_ms = snapshot.now_ms - winner.first_seen_ms
        conflict = (
            tuple(sorted(r.fingerprint for r in reports)) if len(reports) > 1 and overlap_ms > CONFLICT_AFTER_MS else ()
        )
        choices.append(CronChoice(winner, conflict))
    return choices
