"""Start-Sperre bei verdraengender Valkey-Policy (Plan 0.12.0, MR 14, BZ-04).

Das Backend prueft ``maxmemory-policy`` beim Verbinden und legt das Ergebnis
als ``eviction_check`` ab. Der Worker liest es vor der Verdrahtung von
Scheduler und Koordination, ohne ``backend.valkey`` zu importieren (Q16). Ein
Backend ohne dieses Ergebnis (Fremd-Backend, Mock) laesst den Start zu (Q15),
ebenso eine nicht pruefbare Policy: dafuer warnt das Backend nur.
"""

from __future__ import annotations

from swarmq.exceptions import ConfigurationError
from swarmq.valkey_policy import EvictionCheck


def enforce_eviction_policy(backend: object, mode: str) -> None:
    """``ConfigurationError`` (Exit 78), wenn *mode* ``strict`` ist und die
    Policy nachweislich verdraengt."""
    check = getattr(backend, "eviction_check", None)
    if mode != "strict" or not isinstance(check, EvictionCheck) or check.status != "unsafe":
        return
    maxmemory = "unknown" if check.maxmemory is None else str(check.maxmemory)
    msg = (
        f"Valkey runs with maxmemory-policy {check.policy} (maxmemory {maxmemory}): it may evict swarmq keys "
        "(locks, execution guard, unique and fired markers), and tasks can run twice. Set maxmemory-policy "
        "noeviction, or set SWARMQ_VALKEY_EVICTION_CHECK=warn to start anyway."
    )
    raise ConfigurationError(msg)
