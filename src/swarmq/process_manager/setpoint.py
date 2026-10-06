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
from datetime import UTC, datetime
from typing import Any

from swarmq.contracts.control import Setpoint, SetpointRead, SetpointUnreadable
from swarmq.logging import get_logger
from swarmq.process_manager.setpoint_store import SetpointUnread, read_setpoint_sync, write_status_sync
from swarmq.process_manager.slots import SlotState

logger = get_logger(__name__)


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
    report(pm)


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


def tick(pm: Any) -> None:
    """Ein Sollwert-Durchlauf (R7); ausserhalb des Intervalls ein No-op.

    Ruht, sobald das Herunterfahren begonnen hat (KTD2). Laeuft vor dem
    Autoskalierer und setzt dessen Abkuehl- und Leerlaufbuchhaltung zurueck,
    wenn er etwas geaendert hat (KTD3). Nach einem Fehlschlag wird erst im
    naechsten Intervall erneut gelesen (KTD5).
    """
    if pm._shutting_down or not enabled(pm):
        return
    state: SetpointState = pm._setpoint
    now = time.monotonic()
    if now - state.last_sample < pm.SETPOINT_SAMPLE_INTERVAL:
        return
    state.last_sample = now
    changed = apply(pm, read_setpoint_sync(pm._setpoint_store_factory, pm.group))
    acted = reconcile(pm)
    if changed or acted:
        pm._last_scale_decision = now
        pm._last_autoscale_sample = now
        pm._idle_since = None
    report(pm)


def _heading_slots(pm: Any) -> set[int]:
    """Slots mit mindestens einem Prozess, der nicht zum sanften Ende markiert ist."""
    return {w.worker_id for w in pm.workers.values() if not w.expected_shutdown}


def _drain_one(pm: Any) -> bool:
    """Juengsten Worker sanft beenden (Weg des Autoskalierers, ohne Mindestalter, R12)."""
    eligible = {
        pid: w for pid, w in pm.workers.items()
        if not w.expected_shutdown and pm._slots.state(w.worker_id) is not SlotState.TRANSITION
    }
    if not eligible:
        return False
    pid, info = max(eligible.items(), key=lambda kv: (kv[1].spawned_at, kv[1].worker_id))
    logger.info("setpoint_scale_down", group=pm.group, worker_id=info.worker_id, pid=pid)
    info.expected_shutdown = True
    try:
        info.process.terminate()
    except ProcessLookupError:
        pass
    return True


def reconcile(pm: Any) -> bool:
    """Worker-Zahl auf die wirksamen Grenzen bringen, Bestes-Bemuehen je
    Durchlauf (KTD11): Slots im Uebergang oder gesperrte Slots bleiben aus,
    der naechste Durchlauf zieht nach. Liefert ``True``, wenn etwas geschah."""
    low, high = pm.effective_bounds
    heading = len(_heading_slots(pm))
    # Ohne Autoskalierung zaehlen gesperrte Slots und saubere Enden wie heute
    # gegen das Ziel; mit Autoskalierung fuellt der Durchlauf bis zur Untergrenze.
    floor = low if pm._autoscale is not None else pm._current_target()
    acted = False
    while heading > high and _drain_one(pm):
        heading -= 1
        acted = True
    while heading < floor:
        slot = pm._slots.smallest_free()
        if slot is None:
            break
        logger.info("setpoint_scale_up", group=pm.group, slot=slot, effective=[low, high])
        pm._start_worker(slot)
        heading += 1
        acted = True
    return acted


def _bounds(bounds: tuple[int, int] | None) -> dict[str, int] | None:
    return None if bounds is None else {"min": bounds[0], "max": bounds[1]}


def status_ttl_s(pm: Any) -> float:
    """Verfallsdatum der Rueckmeldung (KTD7): waehrend des gesamten Abbaus wird
    nichts mehr erneuert, also Gnadenfrist plus Terminierungsfrist plus ein
    Abtastintervall -- so verschwindet die Meldung eines toten Supervisors,
    aber nicht die eines Supervisors im geordneten Abbau."""
    return float(pm.shutdown_grace_s + pm.SHUTDOWN_TERM_S + pm.SETPOINT_SAMPLE_INTERVAL)


def build_status(pm: Any) -> dict[str, Any]:
    """Rueckmeldung nach R14/R15: Schluessel, zuletzt gelesener Sollwert oder
    sein Fehlen, wirksame Grenzen samt Herkunft, Ablehnungsgrund, Slots, Zeit.
    Feldnamen wie die Protokollereignisse, damit Meldung und Log dieselben
    Begriffe tragen."""
    state: SetpointState = pm._setpoint
    last = state.last_setpoint
    low, high = state.effective
    return {
        "group": pm.group,
        "supervisor": pm.supervisor_id,
        "headroom": pm.max_processes,
        "store": state.store,
        "setpoint": _bounds((last.low, last.high) if last is not None else None),
        "effective": _bounds(state.effective),
        "source": state.source,
        "autoscaling": pm._autoscale is not None and low != high,
        "rejection": state.rejection,
        "slots": {"size": len(pm._slots), "occupied": pm._slots.occupied(), "locked": pm._slots.locked()},
        "workers": len(pm.workers),
        "reported_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "ttl_s": status_ttl_s(pm),
    }


def report(pm: Any) -> None:
    """Rueckmeldung schreiben; das Verfallsdatum wird bei jedem Aufruf erneuert (KTD7)."""
    write_status_sync(pm._setpoint_store_factory, pm.group, pm.supervisor_id, build_status(pm), ttl_s=status_ttl_s(pm))
