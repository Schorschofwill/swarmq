"""Lock-Wartezeit je Worker fuer die Gauges (Plan 0.12.0, MR 13, BZ-05, R11).

Das Lock-Gate meldet jeden Konflikt mit dem ersten Konfliktzeitpunkt der
Nachricht (``x-swarmq-lock-wait-since``). Der Tracker haelt je Task-ID den
juengsten Stand; die Gauges zeigen, wie viele Nachrichten dieser Worker
zuletzt zurueckgestellt hat und wie lange die aelteste schon wartet. Eine
Nachricht, die er laenger nicht sieht (sie ging an einen anderen Worker),
faellt heraus; die Zahl der Eintraege ist begrenzt.
"""

from __future__ import annotations

# Eine Lock-Rueckstellung kehrt nach hoechstens 60 s zurueck (Backoff-Deckel,
# Stufe ``1m``); drei Runden ohne Sichtung heissen: anderer Worker oder fertig.
LOCK_WAIT_STALE_S = 180.0
LOCK_WAIT_MAX_TRACKED = 10_000


class LockWaitTracker:
    def __init__(self) -> None:
        # task_id -> (erster Konflikt ms, zuletzt gesehen ms); Einfuegereihenfolge
        # = Reihenfolge der letzten Sichtung.
        self._waits: dict[str, tuple[int, int]] = {}

    def record(self, task_id: str, *, wait_since_ms: int, now_ms: int) -> None:
        self._waits.pop(task_id, None)
        self._waits[task_id] = (wait_since_ms, now_ms)
        while len(self._waits) > LOCK_WAIT_MAX_TRACKED:
            self._waits.pop(next(iter(self._waits)))

    def forget(self, task_id: str) -> None:
        self._waits.pop(task_id, None)

    def snapshot(self, *, now_ms: int) -> tuple[int, float]:
        """(Zahl der Wartenden, Alter des aeltesten in s)."""
        stale_before = now_ms - int(LOCK_WAIT_STALE_S * 1000)
        for task_id in [t for t, (_, seen) in self._waits.items() if seen < stale_before]:
            del self._waits[task_id]
        if not self._waits:
            return 0, 0.0
        oldest = min(since for since, _ in self._waits.values())
        return len(self._waits), max(now_ms - oldest, 0) / 1000.0
