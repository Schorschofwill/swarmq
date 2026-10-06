"""Zugang zum Sollwert-Speicher aus dem Supervisor (Plan Kontrollkanal, U2, KTD4).

Der Supervisor hat keine Ereignisschleife und das Paket importiert keine
Backend-Implementierung (R16). Der Zugang kommt deshalb als Fabrik aus der
CLI hinter einem strukturellen Protokoll, und jede Verwendung läuft in einem
eigenen ``asyncio.run`` mit eigener Zeitgrenze -- dieselbe Form wie die
Messung der Queue-Tiefen in ``sampler.py``.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from swarmq.contracts.control import SetpointRead
from swarmq.logging import _error_fields, get_logger, sanitize_url

logger = get_logger(__name__)

#: Abtastintervall des Sollwert-Durchlaufs (KTD5): schneller als die Messung
#: der Queue-Tiefen, weil ein Sollwert eine ausdrückliche Handlung ist.
DEFAULT_SAMPLE_INTERVAL_S = 5.0
#: Zeitgrenzen je Zugriff (KTD5): Lesen und Schreiben zusammen bleiben unter
#: der Hälfte des Abtastintervalls, damit ein fehlschlagender Durchlauf die
#: Aufsichtsschleife nicht länger als nötig anhält.
READ_TIMEOUT_S = 1.0
WRITE_TIMEOUT_S = 1.0
#: Das Schließen kann unter Backend-Ausfall selbst hängen -- eigene, kurze Frist
#: wie in der Vorlage ``sampler.py``.
CLOSE_TIMEOUT_S = 1.0


@dataclass(frozen=True, slots=True)
class SetpointUnread:
    """Der Speicher war nicht erreichbar: kein Urteil über den Sollwert (R8)."""

    reason: str


class SetpointStore(Protocol):
    """Struktureller Ausschnitt des Sollwert-Speichers, den der Durchlauf braucht."""

    async def connect(self) -> None: ...

    async def close(self) -> None: ...

    async def read_setpoint(self, group: str) -> SetpointRead: ...

    async def write_status(
        self, group: str, supervisor_id: str, status: Mapping[str, Any], *, ttl_s: float,
    ) -> None: ...


SetpointStoreFactory = Callable[[], SetpointStore]


def read_setpoint_sync(factory: SetpointStoreFactory, group: str) -> SetpointRead | SetpointUnread:
    """Sollwert einer Gruppe synchron lesen; Wegwerf-Verbindung je Aufruf.

    Ein Fehler oder eine überschrittene Zeitgrenze ist kein Urteil über den
    Sollwert, sondern ``SetpointUnread`` -- der Aufrufer behält seinen Zustand.
    """
    import asyncio

    async def _run() -> SetpointRead | SetpointUnread:
        store: SetpointStore | None = None
        try:
            async with asyncio.timeout(READ_TIMEOUT_S):
                store = factory()
                await store.connect()
                return await store.read_setpoint(group)
        except Exception as exc:
            reason = f"{type(exc).__name__}: {sanitize_url(str(exc))}"
            logger.warning("setpoint_read_failed", group=group, **_sanitized_error_fields(exc))
            return SetpointUnread(reason=reason)
        finally:
            await _close_quietly(store)

    return asyncio.run(_run())


def write_status_sync(
    factory: SetpointStoreFactory,
    group: str,
    supervisor_id: str,
    status: Mapping[str, Any],
    *,
    ttl_s: float,
) -> bool:
    """Rückmeldung synchron schreiben; ``False`` bei Fehler oder Zeitüberschreitung."""
    import asyncio

    async def _run() -> bool:
        store: SetpointStore | None = None
        try:
            async with asyncio.timeout(WRITE_TIMEOUT_S):
                store = factory()
                await store.connect()
                await store.write_status(group, supervisor_id, status, ttl_s=ttl_s)
                return True
        except Exception as exc:
            logger.warning("setpoint_status_write_failed", group=group, **_sanitized_error_fields(exc))
            return False
        finally:
            await _close_quietly(store)

    return asyncio.run(_run())


def _sanitized_error_fields(exc: BaseException) -> dict[str, str]:
    """Fehlerschema (R17) ohne Zugangsdaten: Backend-Fehler tragen oft die URL."""
    fields = _error_fields(exc)
    return fields | {"error": sanitize_url(fields["error"])}


async def _close_quietly(store: SetpointStore | None) -> None:
    import asyncio

    if store is None:
        return
    try:
        async with asyncio.timeout(CLOSE_TIMEOUT_S):
            await store.close()
    except Exception:
        pass
