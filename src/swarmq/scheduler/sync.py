"""Cron-Sync (Plan U11, seit U12 aus Meldungen): ``adopt_reported_crons``
übernimmt pro Name die jüngste lebende Cron-Meldung aller Worker
(Plan 2026-10-08-1541, KTD10) per ``apply_static_definition`` in den Store
(CLI-Einträge schatten mit Warnung; geänderte Cron behält ``disabled``)."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

import msgspec

from swarmq.config import SwarmQConfig
from swarmq.contracts.coordination import CronReports
from swarmq.exceptions import BackendConnectionError
from swarmq.logging import _error_fields, get_logger
from swarmq.scheduler.cron_time import next_fire_ms
from swarmq.scheduler.definition import ScheduleDefinition, ScheduleStore
from swarmq.scheduler.reports import (
    REPORT_MAX_AGE_MS,
    choose_reports,
    code_fields,
    live_window_ms,
)
from swarmq.signals import Signal

logger = get_logger(__name__)


def _validated(payload: bytes, now_ms: int) -> ScheduleDefinition:
    """Gemeldete Definition dekodieren und prüfen, bevor der Leader sie
    übernimmt (KTD10): Quelle ``static``, ein Cron-Ausdruck, und der nächste
    Termin lässt sich in der Zone berechnen (unbekannte Zone, kaputter
    Ausdruck). Wirft bei jedem Mangel."""
    definition = msgspec.json.decode(payload, type=ScheduleDefinition)
    if definition.source != "static":
        raise ValueError(f"reported definition has source {definition.source!r}, not 'static'")
    if definition.cron is None:
        raise ValueError("reported definition has no cron expression")
    next_fire_ms(definition.cron, definition.timezone, now_ms)
    return definition


class SchedulerCronSync:
    # Mixin über dem Loop-Zustand: die Attribute setzt ``SchedulerLoop.__init__``
    # (``_backend`` trägt dort das ``SchedulerBackend``-Protocol; hier ``Any``,
    # weil ein Import aus ``loop`` einen Zyklus schüfe).
    _backend: Any
    _config: SwarmQConfig
    _store: ScheduleStore
    _reported_names: frozenset[str] = frozenset()

    if TYPE_CHECKING:

        async def _emit(self, signal: Signal, **extra: Any) -> None: ...
        async def prune_static_crons(self, snapshot: CronReports) -> None: ...
        async def _load_known_names(self, schedules: Any) -> None: ...
        async def _observe_epoch(self) -> None: ...
        async def _maybe_recheck_valkey(self) -> None: ...

    async def _replan_if_unscored(
        self,
        existing: ScheduleDefinition,
        existing_raw: bytes,
        fire_ts_ms: int,
    ) -> None:
        """Heilung (U3): eine aktive, unveränderte Definition ohne Score im
        Fire-Index (etwa nach ``scheduler_next_fire_failed``) neu einplanen.
        Atomar gegen die gelesenen Bytes: ein ``schedule remove`` nach dem
        Lesen bleibt stehen (Review #4)."""
        name = existing.schedule_name
        healed = msgspec.structs.replace(existing, fire_ts_ms=fire_ts_ms)
        try:
            await self._backend.schedules.replan_if_unchanged(
                name,
                existing_raw,
                msgspec.json.encode(healed),
                fire_ts_ms,
            )
        except BackendConnectionError as exc:
            logger.warning(
                "scheduler_sync_update_failed",
                schedule_name=name,
                **_error_fields(exc),
            )

    async def adopt_reported_crons(self) -> None:
        """Leader-Takt (KTD10): pro Name die jüngste lebende Meldung aller
        Worker übernehmen."""
        try:
            snapshot = await self._backend.schedules.cron_reports()
        except BackendConnectionError as exc:
            logger.warning("scheduler_cron_reports_read_failed", **_error_fields(exc))
            return
        now_ms = int(time.time() * 1000)
        choices = choose_reports(
            snapshot,
            live_window_ms=live_window_ms(self._config.scheduler_seen_interval_s),
        )
        # Crons, die irgendein lebender Worker meldet: beim Fire keine
        # Registry-Warnung, auch wenn der Task in einer fremden App lebt.
        self._reported_names = frozenset(c.report.name for c in choices)
        # BZ-11: je gemeldete Namen und Stempel dieser Amtszeit.
        await self._load_known_names(self._backend.schedules)
        for choice in choices:
            if choice.conflict:
                logger.warning(
                    "scheduler_cron_conflict",
                    schedule_name=choice.report.name,
                    fingerprints=list(choice.conflict),
                    chosen=choice.report.fingerprint,
                    hint=(
                        "several definitions of this cron have been live for "
                        "more than 10 minutes; typical cause: tasks with the "
                        "same name in two apps on one Valkey"
                    ),
                )
                await self._emit(
                    Signal.SCHEDULER_CRON_CONFLICT,
                    schedule_name=choice.report.name,
                    fingerprints=list(choice.conflict),
                )
            try:
                desired = _validated(choice.report.payload, now_ms)
            except Exception as exc:
                logger.error(
                    "scheduler_cron_report_invalid",
                    schedule_name=choice.report.name,
                    fingerprint=choice.report.fingerprint,
                    **_error_fields(exc),
                )
                continue
            await self.apply_static_definition(desired, now_ms)
        await self.prune_static_crons(snapshot)
        # BZ-04: Policy und Speicher hoechstens stuendlich erneut pruefen.
        await self._maybe_recheck_valkey()
        try:
            # Meldungen überdauern die Aufräum-Frist (KTD8): sonst gälte ein
            # verstummter Cron bei langer Frist als nie gemeldet.
            await self._backend.schedules.prune_cron_reports(
                max_age_ms=max(
                    REPORT_MAX_AGE_MS,
                    int(2 * self._config.scheduler_prune_after_s * 1000),
                )
            )
        except BackendConnectionError as exc:
            logger.warning("scheduler_cron_reports_prune_failed", **_error_fields(exc))

    async def apply_static_definition(
        self,
        desired: ScheduleDefinition,
        now_ms: int,
    ) -> None:
        """Insert-or-Update einer gewünschten statischen Definition gegen
        den Bestand (Plan 2026-10-08-1541, U3/KTD12). ``desired.cron`` ist
        gesetzt; ``desired.fire_ts_ms`` wird hier aus Cron und Zone nach
        *now_ms* berechnet. Fehler bleiben auf diesen Eintrag beschränkt.

        Branches (in check order):

        1. ``get`` raises ``BackendConnectionError`` → log and skip.
        2. No existing entry → ``add_schedule`` (insert); disabled when
           the cron was disabled as it was pruned (KTD8).
        3. Existing entry fails msgspec decode → log loudly, leave it
           untouched (Unit 5: the source cannot be determined, so an
           overwrite could clobber a CD11-protected CLI entry).
        4. Existing ``source="cli"`` entry → no-op with warning log
           (CD11 — CLI ownership beats code, but the shadow is
           surfaced so operators can discover it).
        5. Existing ``source="static"`` entry with the same code fields
           (:func:`~swarmq.scheduler.reports.code_fields`) → no-op, no
           write — unless it is enabled but has no fire-index score; then
           it is planned again (healing after ``scheduler_next_fire_failed``).
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
            # BZ-04: fehlt ein bekannter Eintrag, kann Valkey Daten verloren haben.
            await self._observe_epoch()
            # New entry — per-schedule error isolation so one
            # BackendConnectionError does not abort the full sync
            # (Unit 5). A cron removed while disabled comes back disabled
            # (KTD8); the mark is dropped only after the insert.
            try:
                revived_disabled = await self._backend.schedules.pruned_disabled(schedule_name)
                if revived_disabled:
                    definition = msgspec.structs.replace(definition, disabled=True)
                await self._store.add_schedule(definition)
                if revived_disabled:
                    await self._backend.schedules.forget_pruned_disabled(schedule_name)
                    logger.warning(
                        "scheduler_cron_revived_disabled",
                        schedule_name=schedule_name,
                        hint=(
                            "this cron was disabled when it was pruned and comes back disabled; "
                            "if the name now belongs to a different cron, run "
                            "'swarmq schedule remove --permanent <name>' -- the next report "
                            "recreates it enabled"
                        ),
                    )
            except BackendConnectionError as exc:
                logger.warning(
                    "scheduler_sync_add_failed",
                    schedule_name=schedule_name,
                    **_error_fields(exc),
                )
            return

        try:
            existing = msgspec.json.decode(existing_raw, type=ScheduleDefinition)
        except msgspec.DecodeError as exc:
            logger.error(
                "scheduler_sync_decode_error_skipped",
                schedule_name=schedule_name,
                **_error_fields(exc),
                hint=("run 'swarmq schedule remove --permanent <name>' to clear the malformed entry, then redeploy"),
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
        if code_fields(existing) == code_fields(definition):
            # Unchanged static. Heilung (U3): ohne Score (etwa nach
            # ``scheduler_next_fire_failed``) plant der Leader neu ein.
            if not existing.disabled:
                assert definition.fire_ts_ms is not None  # aus der Cron berechnet
                await self._replan_if_unscored(existing, existing_raw, definition.fire_ts_ms)
            return
        propagated = msgspec.structs.replace(definition, disabled=existing.disabled)
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
                propagated = msgspec.structs.replace(propagated, fire_ts_ms=score)
            await self._store.update_static_schedule(propagated, keep_fire_index=due)
        except BackendConnectionError as exc:
            # Per-schedule error isolation (Unit 5).
            logger.warning(
                "scheduler_sync_update_failed",
                schedule_name=schedule_name,
                **_error_fields(exc),
            )
