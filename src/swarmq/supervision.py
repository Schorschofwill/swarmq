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
    # BZ-27 (Plan 0.12.0, MR 17): gesperrte Slots, ein Wert je Supervisor fuer
    # alle Kanaele; Schreiber ist nur der Supervisor, der Worker zeigt ihn als Gauge.
    locked_slots: Any = None

    @classmethod
    def create(cls, ctx: Any, *, locked_slots: Any = None) -> SupervisorChannel:
        return cls(ctx.Value("d", 0.0), ctx.Value("i", INTENT_NONE), ctx.Value("i", ACK_NONE), locked_slots)

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

    def locked_slots_value(self) -> int | None:
        """Gesperrte Slots des Supervisors; ``None`` ohne geteilten Wert."""
        if self.locked_slots is None:
            return None
        return int(self.locked_slots.value)

    # --- Supervisor-Seite
    def last_beat(self) -> float:
        return float(self.heartbeat.value)

    def intent_value(self) -> int:
        return int(self.intent.value)

    def acknowledge_replacement(self) -> None:
        self.ack.value = ACK_REPLACEMENT_STARTED


#: Exit-Code eines Workers, dessen Recycling-Absicht der Supervisor nicht
#: bestaetigt hat (KTD11): ungleich 0 und ungleich 78, damit der bestehende
#: Restart-Pfad greift statt eines stillen Verschwindens.
# Exit-Code fuer Konfigurationsfehler (EX_CONFIG). Liegt hier, damit ``cli``
# ihn ohne Import von ``process_manager`` kennt (KTD8); ein Neustart wuerde
# denselben Fehler reproduzieren, also unterscheidet eine Orchestrierung
# damit "nicht wieder anfassen" von "gleich nochmal".
CONFIG_ERROR_EXIT_CODE = 78

# Frist zwischen SIGTERM und SIGKILL fuer ein Worker-Kind. Liegt hier statt
# im Supervisor, damit ``process_manager/entry.py`` und die Konfiguration sie
# ohne Import von ``supervisor.py`` kennen (KTD4).
SHUTDOWN_TERM_S = 5.0

RECYCLE_NOACK_EXIT_CODE = 75  # EX_TEMPFAIL


class RecycleAckTimeout(RuntimeError):
    """Der Worker wollte recyceln, der Supervisor hat den Ersatz nicht bestaetigt."""


#: Exit-Code eines Workers, dessen Pool die Kindquelle verworfen hat
#: (Schluss-Review): EX_OSERR ("cannot fork"). Ungleich 0, 75 und 78 -- der
#: Supervisor startet ihn ueber den gewoehnlichen Restart-Pfad mit frischem
#: Forkserver neu, und im Betrieb ist der Fall am Code erkennbar.
FORK_SOURCE_REJECTED_EXIT_CODE = 71


class ForkSourceRejected(RuntimeError):
    """Der CPU-Pool hat seine Kindquelle verworfen (still neu gestarteter,
    mehrfaediger Forkserver): Der Worker hat aufgehoert zu konsumieren und
    ist geordnet beendet. Ein Neustart des Prozesses bringt einen frischen
    Forkserver; eingebettet entscheidet die App."""


_current: SupervisorChannel | None = None


def set_current_channel(channel: SupervisorChannel | None) -> None:
    """Vom Worker-Entry gesetzt; ohne Supervisor bleibt es ``None`` (R31)."""
    global _current  # noqa: PLW0603 - Modul-Singleton
    _current = channel


def current_channel() -> SupervisorChannel | None:
    return _current


__all__ = [
    "ACK_NONE",
    "ACK_REPLACEMENT_STARTED",
    "FORK_SOURCE_REJECTED_EXIT_CODE",
    "INTENT_DRAINING",
    "INTENT_NONE",
    "INTENT_RECYCLE",
    "RECYCLE_NOACK_EXIT_CODE",
    "ForkSourceRejected",
    "RecycleAckTimeout",
    "SupervisorChannel",
    "current_channel",
    "set_current_channel",
]
