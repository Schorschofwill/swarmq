"""Sollwert-Durchlauf des Supervisors (Plan 2026-09-16 Kontrollkanal, U5-U7).

Wie ``autoscale.py``: freie Funktionen ueber ``pm: Any``, die den Zustand des
Supervisors lesen und ihn nur ueber dessen Methoden aendern (KTD1). Der
Supervisor liest den Sollwert seiner Gruppe einmal vor dem ersten Kind (R6)
und dann je Abtastintervall (R7), prueft ihn nach R10, wendet ihn an und
meldet seine wirksamen Werte zurueck (R14, R15).
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

import structlog

from swarmq.contracts.control import Setpoint, SetpointRead, SetpointUnreadable
from swarmq.process_manager.setpoint_store import SetpointUnread, read_setpoint_sync

logger = structlog.get_logger()


def validate(sp: Setpoint, *, headroom: int, autoscaling: bool) -> str | None:
    """Pruefregeln aus R10 als reine Funktion; ``None`` heisst zulaessig.

    Start und Laufzeit nutzen dieselbe Funktion. Eine Gruppe ohne
    Autoskalierung -- feste Prozesszahl oder Rechen-Pool, die schliessen
    einander aus -- nimmt nur die feste Form ``N..N`` an (KTD8, R11).
    """
    if sp.low < 1:
        return f"lower bound {sp.low} is below 1"
    if sp.high < sp.low:
        return f"bounds are inverted: {sp.low}..{sp.high}"
    if sp.high > headroom:
        return f"upper bound {sp.high} exceeds the headroom {headroom} declared with --max-processes"
    if not autoscaling and not sp.fixed:
        return (
            f"range {sp.low}..{sp.high} needs autoscaling, but this group runs without it "
            "(fixed --processes or --pool cpu): use the fixed form N..N"
        )
    return None


@dataclass
class SetpointState:
    """Was der Supervisor ueber seinen Sollwert weiss (Zustandsdiagramm im Plan).

    ``effective`` ist das eigene Grenzenfeld aus KTD12: die Zielberechnung
    liest es, Startprozesszahl und Autoscale-Feld bleiben unangetastet.
    ``store`` unterscheidet „nicht erreichbar" (``unread``), „kein Sollwert
    hinterlegt" (``absent``) und „Sollwert gelesen" (``present``).
    """

    startup: tuple[int, int]
    effective: tuple[int, int]
    source: str = "startup"
    store: str = "unread"
    last_setpoint: Setpoint | None = None
    rejection: str | None = None
    last_sample: float = float("-inf")

    @classmethod
    def from_start(cls, startup: tuple[int, int]) -> SetpointState:
        return cls(startup=startup, effective=startup)


def enabled(pm: Any) -> bool:
    """Eingeschaltet nur mit Spielraum, Fabrik und Gruppenschluessel (R1, R2)."""
    return pm.max_processes is not None and pm._setpoint_store_factory is not None and pm.group is not None


def start_count(pm: Any) -> int:
    """Erste Kinder: Startprozesszahl, in die wirksamen Grenzen geklemmt (R6)."""
    low, high = pm.effective_bounds
    return int(max(low, min(int(pm.initial_workers), high)))


def read_at_start(pm: Any) -> None:
    """Einmal vor dem ersten Kind lesen und anwenden (R6, R8); scheitert nie (R10)."""
    if not enabled(pm):
        return
    pm._setpoint.last_sample = time.monotonic()
    apply(pm, read_setpoint_sync(pm._setpoint_store_factory, pm.group))


def apply(pm: Any, result: SetpointRead | SetpointUnread) -> bool:
    """Leseergebnis in den Zustand uebernehmen; ``True``, wenn sich die
    wirksamen Grenzen geaendert haben (R7, R9, R10)."""
    state: SetpointState = pm._setpoint
    if isinstance(result, SetpointUnread):
        state.store = "unread"
        logger.warning("setpoint_store_unreachable", group=pm.group, error=result.reason)
        return False
    if result is None:
        state.store = "absent"
        state.last_setpoint = None
        state.rejection = None
        if state.source != "startup":
            state.effective = state.startup
            state.source = "startup"
            logger.info("setpoint_cleared", group=pm.group, effective=list(state.effective))
            return True
        return False
    state.store = "present"
    if isinstance(result, SetpointUnreadable):
        state.last_setpoint = None
        state.rejection = f"unreadable: {result.reason}"
        logger.warning("setpoint_rejected", group=pm.group, reason=state.rejection)
        return False
    state.last_setpoint = result
    reason = validate(result, headroom=int(pm.max_processes), autoscaling=pm._autoscale is not None)
    if reason is not None:
        state.rejection = reason
        logger.warning("setpoint_rejected", group=pm.group, setpoint=[result.low, result.high], reason=reason)
        return False
    state.rejection = None
    bounds = (result.low, result.high)
    if bounds == state.effective and state.source == "setpoint":
        return False
    state.effective = bounds
    state.source = "setpoint"
    logger.info("setpoint_applied", group=pm.group, effective=list(bounds))
    return True
