"""Cron-Sync (Plan U11) — verbatim: ``sync_taskinfo_crons`` gleicht
statische ``TaskInfo``-Crons mit dem Store ab (CLI-Einträge schatten mit
Warnung; geänderte Cron behält ``disabled``)."""
from __future__ import annotations

import time
from typing import Any

import msgspec

from swarmq.config import SwarmQConfig, normalize_timezone
from swarmq.exceptions import BackendConnectionError
from swarmq.logging import _error_fields, get_logger
from swarmq.scheduler.cron_time import next_fire_ms
from swarmq.scheduler.definition import ScheduleDefinition, ScheduleStore
from swarmq.task import Task

logger = get_logger(__name__)


def _code_fields(definition: ScheduleDefinition) -> tuple[Any, ...]:
    """Die aus dem Code stammenden Felder einer statischen Definition
    (Plan 2026-10-08-1541, KTD10): Cron, normalisierte Zeitzone, Queue,
    Task-Name. Weicht eines ab, übernimmt der Leader die Definition."""
    return (
        definition.cron, definition.timezone, definition.queue,
        definition.task_name,
    )


class SchedulerCronSync:
    # Mixin über dem Loop-Zustand: die Attribute setzt ``SchedulerLoop.__init__``
    # (``_backend`` trägt dort das ``SchedulerBackend``-Protocol; hier ``Any``,
    # weil ein Import aus ``loop`` einen Zyklus schüfe).
    _backend: Any
    _config: SwarmQConfig
    _store: ScheduleStore

    async def sync_taskinfo_crons(self) -> None:
        """Reconcile static ``TaskInfo(cron=...)`` declarations into the
        ``sq:sched:defs`` hash via :meth:`apply_static_definition`."""
        now_ms = int(time.time() * 1000)
        for task_class in Task._registered_tasks:
            task_info = getattr(task_class, "task_info", None)
            if task_info is None or not task_info.cron:
                continue

            # KTD1: eigene Zone des Tasks (auch ein ausdrückliches "UTC"),
            # sonst der globale Standard; gespeichert wird UTC als None.
            timezone = normalize_timezone(
                task_info.timezone
                if task_info.timezone is not None
                else self._config.scheduler_timezone
            )
            desired = ScheduleDefinition(
                schedule_name=task_info.name,
                task_name=task_info.name,
                args=(),
                kwargs={},
                queue=task_info.queues[0],
                cron=task_info.cron,
                fire_ts_ms=None,
                source="static",
                timezone=timezone,
            )
            await self.apply_static_definition(desired, now_ms)

    async def apply_static_definition(
        self, desired: ScheduleDefinition, now_ms: int,
    ) -> None:
        """Insert-or-Update einer gewünschten statischen Definition gegen
        den Bestand (Plan 2026-10-08-1541, U3/KTD12). ``desired.cron`` ist
        gesetzt; ``desired.fire_ts_ms`` wird hier aus Cron und Zone nach
        *now_ms* berechnet. Fehler bleiben auf diesen Eintrag beschränkt.

        Branches (in check order):

        1. ``get`` raises ``BackendConnectionError`` → log and skip.
        2. No existing entry → ``add_schedule`` (insert).
        3. Existing entry fails msgspec decode → log loudly, leave it
           untouched (Unit 5: the source cannot be determined, so an
           overwrite could clobber a CD11-protected CLI entry).
        4. Existing ``source="cli"`` entry → no-op with warning log
           (CD11 — CLI ownership beats code, but the shadow is
           surfaced so operators can discover it).
        5. Existing ``source="static"`` entry with the same code fields
           (:func:`_code_fields`) → no-op, no write.
        6. Otherwise → ``update_static_schedule``. The existing
           ``disabled`` flag is preserved so an operator's
           ``schedule remove`` is not silently reversed.
        """
        assert desired.cron is not None  # statische Definition
        schedule_name = desired.schedule_name
        definition = msgspec.structs.replace(
            desired,
            fire_ts_ms=next_fire_ms(desired.cron, desired.timezone, now_ms),
        )

        try:
            existing_raw = await self._backend.schedules.get(schedule_name)
        except BackendConnectionError as exc:
            logger.warning(
                "scheduler_sync_hget_failed",
                schedule_name=schedule_name,
                **_error_fields(exc),
            )
            return

        if existing_raw is None:
            # New entry — per-schedule error isolation so one
            # BackendConnectionError does not abort the full sync
            # (Unit 5).
            try:
                await self._store.add_schedule(definition)
            except BackendConnectionError as exc:
                logger.warning(
                    "scheduler_sync_add_failed",
                    schedule_name=schedule_name,
                    **_error_fields(exc),
                )
            return

        try:
            existing = msgspec.json.decode(
                existing_raw, type=ScheduleDefinition
            )
        except msgspec.DecodeError as exc:
            logger.error(
                "scheduler_sync_decode_error_skipped",
                schedule_name=schedule_name,
                **_error_fields(exc),
                hint=(
                    "run 'swarmq schedule remove --permanent <name>' "
                    "to clear the malformed entry, then redeploy"
                ),
            )
            return

        if existing.source == "cli":
            logger.warning(
                "scheduler_static_cron_shadowed_by_cli",
                schedule_name=schedule_name,
                static_cron=definition.cron,
                cli_cron=existing.cron,
            )
            return
        if _code_fields(existing) == _code_fields(definition):
            return  # Unchanged static
        propagated = msgspec.structs.replace(
            definition, disabled=existing.disabled
        )
        logger.info(
            "scheduler_static_cron_updated",
            schedule_name=schedule_name,
            old_cron=existing.cron,
            new_cron=definition.cron,
            disabled_preserved=existing.disabled,
        )
        try:
            # KTD12: ein bereits fälliger Score bleibt stehen — der Termin
            # feuert einmal mit dem neuen Inhalt; sonst wird neu geplant.
            score = await self._backend.schedules.fire_ts(schedule_name)
            due = score is not None and score <= now_ms
            if due:
                propagated = msgspec.structs.replace(
                    propagated, fire_ts_ms=score
                )
            await self._store.update_static_schedule(
                propagated, keep_fire_index=due
            )
        except BackendConnectionError as exc:
            # Per-schedule error isolation (Unit 5).
            logger.warning(
                "scheduler_sync_update_failed",
                schedule_name=schedule_name,
                **_error_fields(exc),
            )
