"""Lebenszeichen und Neustart-Budget des Supervisors (Plan 0.12.0, MR 0;
verbatim aus ``supervisor.py``): haengende Worker ersetzen, ersetzte Worker
eskalieren und Neustarts im gleitenden Fenster begrenzen.

Mixin ueber dem ``ProcessManager``-Zustand. Uhr und Logger kommen ueber
``self._monotonic`` und ``self._log`` aus ``swarmq.process_manager.supervisor``
(Q17): ``patch("…supervisor.time")`` und der Logger-Name gelten unveraendert,
ohne Rueckimport. ``pm._maybe_restart`` und ``pm._hang_timestamps`` bleiben
Attribute des ``ProcessManager``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from swarmq.supervision import INTENT_DRAINING, INTENT_RECYCLE

if TYPE_CHECKING:
    from swarmq.process_manager.slots import SlotTable, WorkerInfo


class RestartBudget:
    # Mixin über dem Supervisor-Zustand; die Attribute setzt ``ProcessManager.__init__``.
    if TYPE_CHECKING:
        MAX_RESTARTS: int
        RESTART_WINDOW: float
        SLOT_COOLDOWN_S: float
        SHUTDOWN_TERM_S: float
        workers: dict[int, WorkerInfo]
        initial_workers: int
        shutdown_grace_s: float
        heartbeat_timeout_s: float
        hang_replacement_limit: int
        _shutting_down: bool
        _terminating: list[tuple[WorkerInfo, float]]
        _hang_timestamps: list[float]
        _slots: SlotTable
        _locked_slots_shared: Any

        @property
        def _log(self) -> Any: ...

        @property
        def _exhausted_slots(self) -> int: ...

        def _monotonic(self) -> float: ...

        def _start_worker(self, slot: int) -> WorkerInfo: ...

        def _refresh_slot(self, slot: int) -> None: ...

    # ---- Lebenszeichen (Plan 2026-09-03-001, R21/R30, KTD10)

    def _heartbeat_deadline(self, info: WorkerInfo) -> float:
        """Frist: normal ``heartbeat_timeout_s``; im Drain die Shutdown-Gnade plus SIGTERM-Frist.

        Ein recycelnder Worker bekommt mindestens ``heartbeat_timeout_s`` (F3-13, E-018):
        sonst gilt er bei kleinem ``shutdown_timeout`` zwischen zwei Beats als haengend.
        """
        intent = info.channel.intent_value() if info.channel is not None else None
        if intent == INTENT_RECYCLE:
            return max(self.shutdown_grace_s + self.SHUTDOWN_TERM_S, self.heartbeat_timeout_s)
        if intent == INTENT_DRAINING:
            return self.shutdown_grace_s + self.SHUTDOWN_TERM_S
        return self.heartbeat_timeout_s

    def _check_heartbeats(self) -> None:
        """Worker ohne frisches Lebenszeichen ersetzen (SIGTERM, spaeter SIGKILL)."""
        if self._shutting_down:
            return
        now = self._monotonic()
        terminating = {id(info) for info, _ in self._terminating}
        for info in list(self.workers.values()):
            channel = info.channel
            if channel is None or not info.process.is_alive() or id(info) in terminating:
                continue
            if not info.expected_shutdown and channel.intent_value() == INTENT_RECYCLE:
                # KTD11: Absicht → expected_shutdown zuerst, dann Ersatz, dann ack.
                # Zaehlt weder gegen MAX_RESTARTS noch als sauberes Ende.
                info.expected_shutdown = True
                replacement = self._start_worker(info.worker_id)
                channel.acknowledge_replacement()
                self._log.info(
                    "worker_recycling_replacement",
                    worker_id=info.worker_id,
                    pid=info.process.pid,
                    replacement_pid=replacement.process.pid,
                )
                continue
            last = channel.last_beat() or info.spawned_at
            stale = now - last
            if stale <= self._heartbeat_deadline(info):
                continue
            if info.expected_shutdown:  # Recycler/Scale-down im Drain eingefroren: toeten, kein Ersatz
                self._log.warning(
                    "worker_hung_during_drain",
                    worker_id=info.worker_id,
                    pid=info.process.pid,
                    stale_s=round(stale, 1),
                    deadline_s=self._heartbeat_deadline(info),
                )
                self._terminate_worker(info)
            else:
                self._replace_hung_worker(info, stale)

    def _terminate_worker(self, info: WorkerInfo) -> None:
        """SIGTERM jetzt, SIGKILL nach ``SHUTDOWN_TERM_S`` (``_escalate_terminating``)."""
        try:
            info.process.terminate()
        except ProcessLookupError:
            pass
        self._terminating.append((info, self._monotonic() + self.SHUTDOWN_TERM_S))

    def _replace_hung_worker(self, info: WorkerInfo, stale_s: float) -> None:
        now = self._monotonic()
        info.expected_shutdown = True  # der SIGKILL-Exit darf keinen zweiten Ersatz ausloesen
        self._hang_timestamps = [t for t in self._hang_timestamps if now - t <= self.RESTART_WINDOW]
        self._log.warning(
            "worker_hung",
            worker_id=info.worker_id,
            pid=info.process.pid,
            stale_s=round(stale_s, 1),
            deadline_s=self._heartbeat_deadline(info),
        )
        if len(self._hang_timestamps) >= self.hang_replacement_limit:
            self._slots.lock(info.worker_id, until=now + self.SLOT_COOLDOWN_S)  # auch mit lebendem Vorgaenger (R4)
            self._log.error(
                "hang_replacement_budget_exhausted",
                worker_id=info.worker_id,
                limit=self.hang_replacement_limit,
                window_s=self.RESTART_WINDOW,
                cooldown_s=self.SLOT_COOLDOWN_S,
                hint="a poison message keeps hanging workers; check the DLQ and the delivery limit",
            )
        else:
            self._hang_timestamps.append(now)
            self._start_worker(info.worker_id)
        self._terminate_worker(info)

    def _escalate_terminating(self) -> None:
        """SIGKILL fuer ersetzte Worker, die SIGTERM ueberlebt haben."""
        now = self._monotonic()
        remaining: list[tuple[WorkerInfo, float]] = []
        for info, deadline in self._terminating:
            if not info.process.is_alive():
                continue
            if now >= deadline:
                self._log.error("worker_hung_killed", worker_id=info.worker_id, pid=info.process.pid)
                try:
                    info.process.kill()
                except ProcessLookupError:
                    pass
                continue
            remaining.append((info, deadline))
        self._terminating = remaining

    def _maybe_restart(self, old: WorkerInfo) -> None:
        """Restart ``old`` respecting the rolling-window rate limit.

        True sliding window: prunes
        ``restart_timestamps`` to entries inside
        ``[now - RESTART_WINDOW, now]`` and blocks when the count
        hits ``MAX_RESTARTS`` — regardless of how the events are
        distributed within the window. The previous single-gap
        check allowed a full new series of 5 restarts after any
        quiet period of ``RESTART_WINDOW`` seconds.
        """
        now = self._monotonic()
        cutoff = now - self.RESTART_WINDOW
        timestamps = [t for t in old.restart_timestamps if t >= cutoff]

        if len(timestamps) >= self.MAX_RESTARTS:
            # Lock the slot for the cooldown so _current_target shrinks and
            # the backfill loop cannot respawn it with a fresh (empty)
            # restart history (BZ-27: Abkuehlzeit statt Dauersperre).
            self._slots.lock(old.worker_id, until=now + self.SLOT_COOLDOWN_S)
            self._log.error(
                "worker_restart_limit_exceeded",
                worker_id=old.worker_id,
                limit=self.MAX_RESTARTS,
                window_s=self.RESTART_WINDOW,
                cooldown_s=self.SLOT_COOLDOWN_S,
                recent_restarts=len(timestamps),
                exhausted_slots=self._exhausted_slots,
                initial_workers=self.initial_workers,
            )
            return

        timestamps.append(now)
        self._log.info(
            "worker_restarting",
            worker_id=old.worker_id,
            attempt=len(timestamps),
            limit=self.MAX_RESTARTS,
        )
        new = self._start_worker(old.worker_id)
        new.restart_timestamps = timestamps

    def _unlock_expired_slots(self) -> None:
        """BZ-27 (Plan 0.12.0, MR 17): Slots nach der Abkuehlzeit wieder freigeben
        und die Zahl gesperrter Slots an die Worker-Kanaele melden (Gauge).

        Der Slot startet mit leerer Neustart-Historie neu; stuerzt er erneut
        fuenfmal binnen des Fensters ab, sperrt ihn das Budget wieder.
        """
        for slot in self._slots.unlock_expired(self._monotonic()):
            self._refresh_slot(slot)
            self._log.info(
                "worker_slot_unlocked",
                worker_id=slot,
                cooldown_s=self.SLOT_COOLDOWN_S,
                locked_slots=self._exhausted_slots,
            )
        if self._locked_slots_shared is not None:
            self._locked_slots_shared.value = self._exhausted_slots
