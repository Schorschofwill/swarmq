"""Cron-Sync (Plan U11) — verbatim: ``sync_taskinfo_crons`` gleicht
statische ``TaskInfo``-Crons mit dem Store ab (CLI-Einträge schatten mit
Warnung; geänderte Cron behält ``disabled``)."""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import msgspec
import structlog
from croniter import croniter

from swarmq.exceptions import BackendConnectionError
from swarmq.scheduler.definition import ScheduleDefinition, ScheduleStore
from swarmq.task import Task

logger = structlog.get_logger()


class SchedulerCronSync:
    # Mixin über dem Loop-Zustand: die Attribute setzt ``SchedulerLoop.__init__``
    # (``_backend`` trägt dort das ``SchedulerBackend``-Protocol; hier ``Any``,
    # weil ein Import aus ``loop`` einen Zyklus schüfe).
    _backend: Any
    _store: ScheduleStore

    async def sync_taskinfo_crons(self) -> None:
        """Reconcile static ``TaskInfo(cron=...)`` declarations into the
        ``sq:sched:defs`` hash.

        Six resolution branches (in check order):

        1. ``hget_schedule_def`` raises ``BackendConnectionError`` →
           log and skip this schedule, continue iteration.
        2. No existing entry → ``add_schedule`` (insert).
        3. Existing entry fails msgspec decode → best-effort
           ``update_static_schedule`` overwrite with the valid
           definition from code.
        4. Existing ``source="cli"`` entry → no-op with warning log
           (CD11 — CLI ownership beats code, but the shadow is
           surfaced so operators can discover it).
        5. Existing ``source="static"`` entry with the same
           cron-string → no-op (stable).
        6. Existing ``source="static"`` entry with a **different**
           cron-string → ``update_static_schedule`` (Fork 1). The
           existing ``disabled`` flag is preserved so an operator's
           ``schedule remove`` is not silently reversed.
        """
        for task_class in Task._registered_tasks:
            task_info = getattr(task_class, "task_info", None)
            if task_info is None or not task_info.cron:
                continue

            schedule_name = task_info.name
            queue = task_info.queues[0]
            now = datetime.now(UTC)
            next_dt = croniter(task_info.cron, now).get_next(datetime)
            fire_ts_ms = int(next_dt.timestamp() * 1000)

            definition = ScheduleDefinition(
                schedule_name=schedule_name,
                task_name=task_info.name,
                args=(),
                kwargs={},
                queue=queue,
                cron=task_info.cron,
                fire_ts_ms=fire_ts_ms,
                source="static",
            )

            try:
                existing_raw = await self._backend.schedules.get(
                    schedule_name
                )
            except BackendConnectionError as exc:
                logger.warning(
                    "scheduler_sync_hget_failed",
                    schedule_name=schedule_name,
                    error=str(exc),
                )
                continue

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
                        error=str(exc),
                    )
                continue

            try:
                existing = msgspec.json.decode(
                    existing_raw, type=ScheduleDefinition
                )
            except msgspec.DecodeError as exc:
                # Unit 5: do NOT silently overwrite on decode
                # error. We cannot tell if the malformed bytes were
                # source="cli" (CD11-protected) or source="static"
                # (overwriteable). Preserve operator intent by
                # leaving the entry untouched and logging loudly so
                # operators can investigate + manually remove.
                logger.error(
                    "scheduler_sync_decode_error_skipped",
                    schedule_name=schedule_name,
                    error=str(exc),
                    hint=(
                        "run 'swarmq schedule remove --permanent <name>' "
                        "to clear the malformed entry, then redeploy"
                    ),
                )
                continue

            if existing.source == "cli":
                # CD11 protection: CLI ownership beats code. Log so
                # the silent shadow is discoverable.
                logger.warning(
                    "scheduler_static_cron_shadowed_by_cli",
                    schedule_name=schedule_name,
                    static_cron=task_info.cron,
                    cli_cron=existing.cron,
                )
                continue
            if existing.cron == task_info.cron:
                continue  # Unchanged static
            # Static cron changed — propagate (Fork 1).
            # Preserve the disabled flag from the existing entry so
            # an operator's `schedule remove` intent is not silently
            # reversed by the next redeploy.
            propagated = msgspec.structs.replace(
                definition, disabled=existing.disabled
            )
            logger.info(
                "scheduler_static_cron_updated",
                schedule_name=schedule_name,
                old_cron=existing.cron,
                new_cron=task_info.cron,
                disabled_preserved=existing.disabled,
            )
            try:
                await self._store.update_static_schedule(propagated)
            except BackendConnectionError as exc:
                # Per-schedule error isolation (Unit 5).
                logger.warning(
                    "scheduler_sync_update_failed",
                    schedule_name=schedule_name,
                    error=str(exc),
                )
