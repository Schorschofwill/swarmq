"""Kanal zwischen Worker-Prozess und Supervisor (Plan 2026-09-03-001, R21/R22, KTD10/KTD11).

Drei geteilte Werte je Worker, jeder mit genau einem Schreiber: ``heartbeat``
(monotone Zeit, Worker), ``intent`` (Worker: keine / draining / Recycling)
und ``ack`` (Supervisor: Ersatz gestartet). Ein gemeinsamer Wert mit zwei
Schreibern waere ein Race. Bewusst ein stdlib-only Modul auf Paketebene, weil
Worker-Kern und Prozess-Manager es beide brauchen.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

INTENT_NONE = 0
INTENT_DRAINING = 1
INTENT_RECYCLE = 2

ACK_NONE = 0
ACK_REPLACEMENT_STARTED = 1


@dataclass(slots=True)
class SupervisorChannel:
    """``multiprocessing.Value``-Tripel; wird beim Prozessstart vererbt."""

    heartbeat: Any  # Value('d'): monotone Zeit des letzten Lebenszeichens, 0 = noch keins
    intent: Any  # Value('i'): INTENT_*
    ack: Any  # Value('i'): ACK_*

    @classmethod
    def create(cls, ctx: Any) -> SupervisorChannel:
        return cls(ctx.Value("d", 0.0), ctx.Value("i", INTENT_NONE), ctx.Value("i", ACK_NONE))

    # --- Worker-Seite
    def beat(self) -> None:
        self.heartbeat.value = time.monotonic()

    def mark_draining(self) -> None:
        if int(self.intent.value) == INTENT_NONE:
            self.intent.value = INTENT_DRAINING

    def request_recycle(self) -> None:
        self.intent.value = INTENT_RECYCLE

    def replacement_acknowledged(self) -> bool:
        return int(self.ack.value) == ACK_REPLACEMENT_STARTED

    # --- Supervisor-Seite
    def last_beat(self) -> float:
        return float(self.heartbeat.value)

    def intent_value(self) -> int:
        return int(self.intent.value)

    def acknowledge_replacement(self) -> None:
        self.ack.value = ACK_REPLACEMENT_STARTED


_current: SupervisorChannel | None = None


def set_current_channel(channel: SupervisorChannel | None) -> None:
    """Vom Worker-Entry gesetzt; ohne Supervisor bleibt es ``None`` (R31)."""
    global _current
    _current = channel


def current_channel() -> SupervisorChannel | None:
    return _current


__all__ = [
    "ACK_NONE",
    "ACK_REPLACEMENT_STARTED",
    "INTENT_DRAINING",
    "INTENT_NONE",
    "INTENT_RECYCLE",
    "SupervisorChannel",
    "current_channel",
    "set_current_channel",
]
