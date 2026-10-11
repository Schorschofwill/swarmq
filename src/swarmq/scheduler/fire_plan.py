"""Fire-Planung (Plan 2026-10-08-1541, KTD12, R18): nächster Termin vor dem
Publish, fortgeschriebene Definition nach dem Fire und die Zählung der
durch Catch-up-Skip verlorenen Termine. Mixin über dem ``SchedulerLoop``."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import msgspec

from swarmq.exceptions import BackendConnectionError
from swarmq.logging import _error_fields, get_logger
from swarmq.scheduler.cron_time import iter_fire_ms, next_fire_ms
from swarmq.scheduler.definition import ScheduleDefinition, ScheduleStore
from swarmq.signals import Signal

logger = get_logger(__name__)


class SchedulerFirePlan:
    # Mixin über dem Loop-Zustand: die Attribute setzt ``SchedulerLoop.__init__``.
    _store: ScheduleStore
    _owner_token: str

    if TYPE_CHECKING:

        async def _emit(self, signal: Signal, **extra: Any) -> None: ...

    async def _advanced_definition(
        self,
        schedule_name: str,
        score_ms: int,
        definition: ScheduleDefinition,
        next_fire_ts_ms: int,
        now_ms: int,
    ) -> ScheduleDefinition | None:
        """Definition to commit after a fire (``None`` removes a one-shot);
        reports cron occurrences lost to catch-up-skip."""
        if definition.cron is None:
            return None
        # Catch-up-skip was correct but invisible —
        # a skipped daily report went unnoticed. Count the cron
        # occurrences that fell between the due score and now (they
        # will never fire) and surface them as a signal.
        skipped_count = self._count_skipped_fires(
            definition.cron,
            definition.timezone,
            score_ms,
            now_ms,
        )
        if skipped_count > 0:
            logger.warning(
                "scheduler_fires_skipped",
                schedule_name=schedule_name,
                skipped_count=skipped_count,
            )
            await self._emit(
                Signal.SCHEDULER_FIRE_SKIPPED,
                schedule_name=schedule_name,
                skipped_count=skipped_count,
            )
        return msgspec.structs.replace(
            definition,
            fire_ts_ms=next_fire_ts_ms,
            last_fire_at=now_ms,
        )

    def _plan_next_fire(
        self,
        schedule_name: str,
        definition: ScheduleDefinition,
        now_ms: int,
    ) -> int | None:
        """Next fire for a cron, ``-1`` for one-shots — computed BEFORE the
        publish (KTD12, R18). A definition whose next fire cannot be
        computed in this leader (unknown zone, broken expression) is logged
        and yields ``None``; the caller parks it (:meth:`_park_unplannable`).

        Cron semantics: expressions are evaluated in the definition's
        timezone (None = UTC) against the leader clock, with the DST rule
        of ``cron_time`` (Plan 2026-10-08-1541, KTD2). Catch-up after
        downtime is SKIP: the next fire after "now" yields exactly one
        catch-up fire, never a backlog storm.
        """
        if definition.cron is None:
            return -1
        try:
            return next_fire_ms(definition.cron, definition.timezone, now_ms)
        except Exception as exc:
            logger.error(
                "scheduler_next_fire_failed",
                schedule_name=schedule_name,
                cron=definition.cron,
                timezone=definition.timezone,
                **_error_fields(exc),
            )
            return None

    async def _park_unplannable(
        self,
        schedule_name: str,
        score_ms: int,
        definition: ScheduleDefinition,
    ) -> None:
        """Take an unplannable entry out of the fire index (``-1``, definition
        kept) — otherwise it would be due again on every tick. A failure stays
        on this entry (R18): the rest of the batch fires."""
        try:
            committed = await self._store.commit_fire(
                schedule_name,
                -1,
                definition,
                self._owner_token,
                fired_ts_ms=score_ms,
            )
        except BackendConnectionError as exc:
            logger.warning(
                "scheduler_park_failed",
                schedule_name=schedule_name,
                **_error_fields(exc),
            )
            return
        if not committed:
            logger.warning("scheduler_park_owner_mismatch", schedule_name=schedule_name)

    @staticmethod
    def _count_skipped_fires(
        cron: str,
        timezone: str | None,
        score_ms: int,
        now_ms: int,
    ) -> int:
        """Count cron occurrences in ``(score_ms, now_ms]`` — fires that
        catch-up-skip will never run.

        The due score itself is the fire we ARE running; everything
        between it and ``now_ms`` is lost to the skip semantics. Iteration
        is capped at 1000 — a week-long outage on a minutely cron does
        not need an exact count to alert on. Occurrences are computed in
        the definition's *timezone* with the same DST rule as the score.
        """
        skipped = 0
        for fire_ms in iter_fire_ms(cron, timezone, score_ms):
            if fire_ms > now_ms or skipped >= 1000:
                break
            skipped += 1
        return skipped
