"""Sollwert-Speicher des Kontrollkanals (Plan 2026-09-16 Kontrollkanal, U1).

Sollwert und Rückmeldung liegen unter eigenen Präfixen im ``sq:``-Stamm
(KTD6, R17): ``sq:control:setpoint:<gruppe>`` trägt den Sollwert, den ein
steuernder Dienst schreibt; ``sq:control:status:<gruppe>:<supervisor>`` die
Rückmeldung je Supervisor. Beides ist JSON, damit ein Betreiber es ohne
Werkzeug lesen kann.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from swarmq.backend.valkey.connection import ValkeyConnection
from swarmq.contracts.control import Setpoint, SetpointRead, SetpointUnreadable
from swarmq.exceptions import BackendConnectionError


class ValkeySetpointStore(ValkeyConnection):
    """Liest Sollwerte und schreibt Rückmeldungen; Fehler werden an der
    Grenze in ``BackendConnectionError`` gewandelt (Zuschnitt wie ``locks.py``)."""

    def _setpoint_key(self, group: str) -> str:
        return self._keys.control_setpoint(group)

    def _status_key(self, group: str, supervisor_id: str) -> str:
        return self._keys.control_status(group, supervisor_id)

    async def _guarded(self, what: str, coro: Any) -> Any:
        """Wie ``_run``, aber jede fremde Ausnahme wird gewandelt (Muster
        ``LockManager``): der Durchlauf im Supervisor sieht eine Fehlerklasse."""
        try:
            return await self._run(what, coro)
        except BackendConnectionError:
            raise
        except Exception as exc:
            raise BackendConnectionError(f"{what} failed: {exc}") from exc

    async def read_setpoint(self, group: str) -> SetpointRead:
        raw = await self._guarded("setpoint read", self._require_client().get(self._setpoint_key(group)))
        if raw is None:
            return None
        return _decode_setpoint(raw)

    async def write_setpoint(self, group: str, setpoint: Setpoint) -> None:
        """Schreibseite des steuernden Dienstes (A3): ``{"min": N, "max": M}``."""
        payload = json.dumps({"min": setpoint.low, "max": setpoint.high})
        await self._guarded("setpoint write", self._require_client().set(self._setpoint_key(group), payload))

    async def clear_setpoint(self, group: str) -> None:
        """Sollwert entfernen; der Supervisor kehrt zu seinen Startargumenten zurück (R7)."""
        await self._guarded("setpoint clear", self._require_client().delete(self._setpoint_key(group)))

    async def read_status(self, group: str, supervisor_id: str) -> dict[str, Any] | None:
        """Rückmeldung eines Supervisors lesen; ``None`` wenn keine (mehr) vorliegt."""
        raw = await self._guarded("status read", self._require_client().get(self._status_key(group, supervisor_id)))
        if raw is None:
            return None
        data: dict[str, Any] = json.loads(raw)
        return data

    async def write_status(self, group: str, supervisor_id: str, status: Mapping[str, Any], *, ttl_s: float) -> None:
        """Rückmeldung eines Supervisors schreiben; das Verfallsdatum wird bei
        jedem Aufruf neu gesetzt (KTD7), damit die Meldung eines toten
        Supervisors von selbst verschwindet."""
        await self._guarded(
            "status write",
            self._require_client().set(
                self._status_key(group, supervisor_id), json.dumps(status), px=max(1, int(ttl_s * 1000)),
            ),
        )


def _decode_setpoint(raw: bytes | str) -> Setpoint | SetpointUnreadable:
    """``{"min": N, "max": M}`` deuten; alles andere ist „unlesbar" (R10),
    keine Ausnahme -- der Supervisor behandelt es wie eine Ablehnung."""
    text = raw.decode(errors="replace") if isinstance(raw, bytes) else str(raw)
    try:
        data = json.loads(text)
        low, high = data["min"], data["max"]
        if isinstance(low, bool) or isinstance(high, bool) or not isinstance(low, int) or not isinstance(high, int):
            raise TypeError("min and max must be integers")
    except (ValueError, TypeError, KeyError) as exc:
        return SetpointUnreadable(reason=f"setpoint {text!r} is not {{\"min\": N, \"max\": M}}: {exc}")
    return Setpoint(low=low, high=high)
