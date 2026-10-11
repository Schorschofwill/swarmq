"""Shutdown-Budget (BZ-18, Plan 0.12.0, MR 17).

``SWARMQ_SHUTDOWN_TIMEOUT`` ist das **Gesamtbudget ab SIGTERM**: Kubernetes
schickt nach ``terminationGracePeriodSeconds`` SIGKILL, Docker nach
``stop_grace_period``. Bis dahin muessen Drain, Sperrenfreigabe,
Leader-Uebergabe, Verbindungsschluss und -- unter dem Supervisor -- dessen
eigene SIGTERM-/SIGKILL-Eskalation fertig sein.

Alle Zeitpunkte sind Sekunden ab Beginn des Herunterfahrens. Stdlib-only auf
Paketebene, weil Supervisor und Worker sie beide brauchen (wie
``swarmq.supervision``).
"""

from __future__ import annotations

from dataclasses import dataclass

from swarmq.supervision import SHUTDOWN_TERM_S

#: Supervisor: Zeit nach dem SIGKILL fuer das Einsammeln der Prozesse; die
#: Eskalation endet bei ``total - KILL_RESERVE_S``.
KILL_RESERVE_S = 2.0
#: Worker: Reserve nach dem Drain fuer Sperren, Scheduler-Stopp und
#: Verbindungsschluss (wie bisher die Reserve der Publish-Endfrist).
CLEANUP_RESERVE_S = 5.0


@dataclass(frozen=True, slots=True)
class ShutdownBudget:
    """Phasen des Herunterfahrens, in Sekunden ab SIGTERM."""

    total: float
    #: Supervisor: SIGTERM an noch lebende Worker.
    sigterm_at: float
    #: Supervisor: SIGKILL an noch lebende Worker (Ende der Eskalation).
    sigkill_at: float
    #: Worker: bis hierhin muss der Prozess fertig sein (unter dem Supervisor
    #: dessen SIGKILL, eingebettet das Gesamtbudget).
    finish_by: float
    #: Worker: Ende des Drains und Publish-Endfrist; danach bleibt die
    #: Aufraeumreserve.
    drain_until: float


def shutdown_budget(total: float, supervised: bool) -> ShutdownBudget:
    """Teilt ``total`` in die Phasen auf.

    Bei kleinen Budgets schrumpfen die Reserven anteilig (hoechstens die
    Haelfte des jeweils verbleibenden Rests), damit nie eine Phase auf null
    faellt. ``total <= 0`` heisst sofort: alle Phasen bei 0.
    """
    total = max(0.0, float(total))
    sigkill_at = total - min(KILL_RESERVE_S, total / 2.0)
    sigterm_at = sigkill_at - min(SHUTDOWN_TERM_S, sigkill_at / 2.0)
    finish_by = sigkill_at if supervised else total
    drain_until = finish_by - min(CLEANUP_RESERVE_S, finish_by / 2.0)
    return ShutdownBudget(
        total=total,
        sigterm_at=sigterm_at,
        sigkill_at=sigkill_at,
        finish_by=finish_by,
        drain_until=drain_until,
    )


__all__ = ["CLEANUP_RESERVE_S", "KILL_RESERVE_S", "ShutdownBudget", "shutdown_budget"]
