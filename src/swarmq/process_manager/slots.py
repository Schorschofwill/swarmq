"""Slot- und Prozess-Buchhaltung des Supervisors (Plan 2026-09-08-001, KTD1).

Ein Slot ist die stabile Kennung ``0..N-1`` eines Worker-Platzes; Prozesse
kommen und gehen, der Slot bleibt (``WorkerInfo.worker_id``). Die Tabelle
kennt vier Zustände: frei, aktiv (ein Prozess hält den Slot), Übergang
(Alt- und Ersatzprozess halten ihn gleichzeitig) und gesperrt
(Neustart-Budget erschöpft).
"""

from __future__ import annotations

import enum
import math
import multiprocessing
import time
from dataclasses import dataclass, field

from swarmq.supervision import SupervisorChannel


class SlotState(enum.Enum):
    """Zustand eines Worker-Slots."""

    FREE = "free"
    ACTIVE = "active"
    TRANSITION = "transition"
    LOCKED = "locked"


class SlotTable:
    """Zustände der Slots ``0..size-1``."""

    def __init__(self, size: int) -> None:
        self._states: list[SlotState] = [SlotState.FREE] * size
        self._locked_until: dict[int, float] = {}

    def __len__(self) -> int:
        return len(self._states)

    def state(self, slot: int) -> SlotState:
        return self._states[slot]

    def lock(self, slot: int, until: float = math.inf) -> None:
        """Slot sperren (Budget erschöpft), bis ``until`` (monotone Zeit).

        Each locked slot reduces the static backfill target while the lock
        lasts — without this, the backfill loop respawned a fresh WorkerInfo
        (empty restart history) right after ``_maybe_restart`` declined,
        bypassing the 5-per-hour limit in an unbounded spawn storm. When ALL
        slots are locked the supervisor exits non-zero so systemd/k8s take
        over with their own backoff policy. Der Default sperrt dauerhaft; der
        Supervisor sperrt mit Abkühlzeit (BZ-27, Plan 0.12.0, MR 17), kein
        ``refresh`` hebt die Sperre vorher auf.
        """
        self._states[slot] = SlotState.LOCKED
        if math.isinf(until):
            self._locked_until.pop(slot, None)  # dauerhaft: keine Uhr hebt das auf
        else:
            self._locked_until[slot] = until

    def unlock_expired(self, now: float) -> list[int]:
        """Sperren mit abgelaufener Abkühlzeit aufheben; liefert die Slots.

        Ein entsperrter Slot ist frei; hält ihn noch ein Prozess, leitet der
        nächste ``refresh`` den Zustand neu ab.
        """
        expired = [slot for slot, until in self._locked_until.items() if until <= now]
        for slot in expired:
            del self._locked_until[slot]
            self._states[slot] = SlotState.FREE
        return expired

    def locked(self) -> int:
        return sum(1 for s in self._states if s is SlotState.LOCKED)

    def refresh(self, slot: int, *, holders: int) -> None:
        """Zustand aus der Zahl der Prozesse ableiten, die den Slot halten."""
        if self._states[slot] is SlotState.LOCKED:
            return
        if holders <= 0:
            self._states[slot] = SlotState.FREE
        elif holders == 1:
            self._states[slot] = SlotState.ACTIVE
        else:
            self._states[slot] = SlotState.TRANSITION

    def smallest_free(self) -> int | None:
        """Kleinster freier Slot; ``None``, wenn keiner frei ist."""
        for slot, state in enumerate(self._states):
            if state is SlotState.FREE:
                return slot
        return None

    def occupied(self) -> int:
        """Belegte Slots: aktiv oder Übergang, je Slot einmal gezählt."""
        return sum(1 for s in self._states if s in (SlotState.ACTIVE, SlotState.TRANSITION))


@dataclass
class WorkerInfo:
    """Per-worker bookkeeping: process handle, slot, restart state.

    ``restart_timestamps`` carries the rolling-window history across
    successive generations of a crash-then-restart slot. Each entry
    is ``time.monotonic()`` at the moment a replacement was spawned.
    Entries older than ``ProcessManager.RESTART_WINDOW`` are pruned
    in :meth:`ProcessManager._maybe_restart` so the count reflects a
    true sliding window, not a since-last-event gap.

    ``spawned_at`` is set to ``time.monotonic()`` at construction time
    and consumed by :meth:`ProcessManager._scale_down_one` to protect
    freshly spawned replacements from immediate scale-down.
    """

    process: multiprocessing.process.BaseProcess
    worker_id: int  # der Slot (KTD1), nicht eine fortlaufende Nummer
    expected_shutdown: bool = False  # True when Autoscaler scaled down
    restart_timestamps: list[float] = field(default_factory=list)
    spawned_at: float = field(default_factory=time.monotonic)
    # Plan 2026-09-03-001 (R21/R22): Lebenszeichen-/Absichts-/Bestaetigungskanal.
    channel: SupervisorChannel | None = None
