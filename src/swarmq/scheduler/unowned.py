"""Zurueckhalten verwaister Crons (Plan 0.12.0, MR 11, BZ-11).

Ein statischer Cron, den irgendein Worker ab 0.12 je gemeldet hat
(``known_cron_names``), den aber kein lebender Worker mehr meldet und den der
Leader selbst nicht registriert, feuerte bis zum Aufraeumen (24 h) weiter;
jeder Fire endete als unregistrierter Task. Der Leader haelt ihn jetzt
zurueck: Commit auf ``now + scheduler_seen_interval_s`` ohne Publish.

Die Entscheidung braucht die Meldungen der aktuellen Amtszeit (ein Stempel
je Amtszeit, R15): direkt nach dem Erwerb wartet der Poll hoechstens
``FIRST_ADOPT_WAIT_MS`` auf das erste Adopt, danach gilt fail-open -- lieber
ein Fire zu viel als ein stiller Stillstand aller Crons. Mixin ueber dem
``SchedulerLoop``-Zustand; Uhr und Logger wie in ``fire.py`` ueber
``self._wall_time`` und ``self._log`` (Logger-Name ``swarmq.scheduler.loop``).
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

import msgspec

from swarmq.signals import Signal

if TYPE_CHECKING:
    from swarmq.config import SwarmQConfig
    from swarmq.scheduler.definition import ScheduleDefinition

#: Hoechstens so lange wartet der Poll nach dem Erwerb der Leader-Rolle auf
#: das erste Adopt der Amtszeit, bevor er ohne Meldungen feuert (fail-open).
FIRST_ADOPT_WAIT_MS = 5_000


class SchedulerUnowned:
    # Mixin ueber dem Loop-Zustand: die Attribute setzt ``SchedulerLoop.__init__``.
    if TYPE_CHECKING:
        _config: SwarmQConfig
        _reported_names: frozenset[str]
        _static_crons: Sequence[ScheduleDefinition]

        @property
        def _log(self) -> Any: ...

        async def _emit(self, signal: Signal, **extra: Any) -> None: ...

        async def _commit_unroutable(
            self, schedule_name: str, score_ms: int, next_ms: int, updated: ScheduleDefinition | None
        ) -> bool: ...

    #: Laufende Amtszeit (zaehlt mit jedem Erwerb der Leader-Rolle) und ihr Beginn.
    _term: int = 0
    _term_started_ms: int | None = None
    #: Amtszeit, in der die Meldungen zuletzt geladen wurden bzw. ein Adopt lief.
    _reports_term: int = -1
    _adopt_term: int = -1
    _known_names: frozenset[str] = frozenset()
    #: Zurzeit zurueckgehaltene Crons (fuer Log und Signal nur beim Uebergang).
    _held_crons: frozenset[str] = frozenset()
    _unowned_skip_logged_term: int = -1

    # ---- Amtszeit und Meldungen ----------------------------------------

    def _begin_term(self, now_ms: int) -> None:
        """Neue Amtszeit: Meldungen und Uebergaenge gelten nicht mehr."""
        self._term += 1
        self._term_started_ms = now_ms
        self._held_crons = frozenset()

    def _note_adopt_ran(self) -> None:
        self._adopt_term = self._term

    async def _load_known_names(self, schedules: Any) -> None:
        """Nach gelesenen Meldungen: die je gemeldeten Namen dazu (Q15: ein
        Store ohne ``known_cron_names`` haelt nichts zurueck)."""
        try:
            known: frozenset[str] = frozenset(await schedules.known_cron_names())
        except Exception as exc:  # noqa: BLE001 - Store ohne known_cron_names hält nichts zurück
            self._log.warning("scheduler_known_crons_unavailable", error_type=type(exc).__name__, error=str(exc))
            known = frozenset()
        self._known_names = known
        self._reports_term = self._term
        for name in sorted(self._held_crons - known):
            # Die Definition ist weg (Prune, CLI): der Cron ist nicht mehr zurueckgehalten.
            await self._release_unowned(name, task_name=None)

    def _poll_may_fire(self, now_ms: int) -> bool:
        """``False``: direkt nach dem Erwerb, solange das erste Adopt der
        Amtszeit noch aussteht (hoechstens ``FIRST_ADOPT_WAIT_MS``)."""
        if self._term in (self._reports_term, self._adopt_term) or self._term_started_ms is None:
            return True
        return now_ms - self._term_started_ms >= FIRST_ADOPT_WAIT_MS

    # ---- Entscheidung ----------------------------------------------------

    def _is_unowned(self, schedule_name: str, definition: ScheduleDefinition) -> bool:
        if self._reports_term != self._term:
            self._log_unowned_check_skipped()
            return False
        return (
            definition.source == "static"
            and definition.cron is not None
            and schedule_name in self._known_names
            and schedule_name not in self._reported_names
            and schedule_name not in {d.schedule_name for d in self._static_crons}
        )

    def _log_unowned_check_skipped(self) -> None:
        if self._unowned_skip_logged_term == self._term:
            return
        self._unowned_skip_logged_term = self._term
        self._log.warning(
            "scheduler_unowned_check_skipped",
            hint=(
                "firing without the cron reports of this term (not loaded yet or unreadable); "
                "removed crons are not held until the next adoption tick succeeds"
            ),
        )

    # ---- Zurueckhalten und Freigeben -------------------------------------

    async def _hold_unowned(
        self, schedule_name: str, score_ms: int, definition: ScheduleDefinition, now_ms: int
    ) -> None:
        """Commit auf ``now + seen_interval`` ohne Publish; Log und Signal nur
        beim Uebergang in den Zustand "zurueckgehalten". Erst der Commit, dann
        das Signal: Wer darauf den Termin liest, sieht schon den verschobenen."""
        next_ms = now_ms + int(self._config.scheduler_seen_interval_s * 1000)
        updated = msgspec.structs.replace(definition, fire_ts_ms=next_ms)
        await self._commit_unroutable(schedule_name, score_ms, next_ms, updated)
        if schedule_name not in self._held_crons:
            self._held_crons |= {schedule_name}
            self._log.warning(
                "scheduler_cron_held_unowned",
                schedule_name=schedule_name,
                task_name=definition.task_name,
                queue=definition.queue,
                hint=(
                    "no live worker reports this static cron and the leader does not register its task; "
                    "it is held (not fired) until a worker reports it again or the leader prunes it"
                ),
            )
            await self._emit_unowned(schedule_name, definition.task_name, held=True)

    async def _release_unowned(self, schedule_name: str, *, task_name: str | None) -> None:
        """Uebergang aus "zurueckgehalten" (wieder gemeldet oder entfernt)."""
        if schedule_name not in self._held_crons:
            return
        self._held_crons -= {schedule_name}
        self._log.info("scheduler_cron_unowned_released", schedule_name=schedule_name, task_name=task_name)
        await self._emit_unowned(schedule_name, task_name, held=False)

    async def _emit_unowned(self, schedule_name: str, task_name: str | None, *, held: bool) -> None:
        await self._emit(
            Signal.SCHEDULER_CRON_UNOWNED,
            schedule_name=schedule_name,
            task_name=task_name,
            held=held,
            held_count=len(self._held_crons),
        )
