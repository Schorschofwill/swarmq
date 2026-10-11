"""Eviction-Policy von Valkey bewerten (Plan 0.12.0, MR 14, BZ-04).

Neutral und rein (Q16): ``worker`` und ``scheduler`` lesen das Ergebnis der
Pruefung, ohne ``backend.valkey`` zu importieren. Die Abfrage selbst lebt in
``swarmq.backend.valkey.environment``.

swarmq haengt an TTL-Keys, die nie verdraengt werden duerfen (Locks,
Leader-Lock, Ausfuehrungsschutz, Unique- und Fired-Marker). Sicher ist nur
``noeviction`` oder eine Instanz ohne Speichergrenze (``maxmemory`` 0): dort
verdraengt Valkey nie etwas.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

EvictionStatus = Literal["safe", "unsafe", "unknown"]

#: Werte von ``SWARMQ_VALKEY_EVICTION_CHECK``: ``strict`` (Exit 78 bei
#: verdraengender Policy), ``warn`` (nur Warnung), ``off`` (keine Pruefung).
EVICTION_CHECK_MODES = ("strict", "warn", "off")


@dataclass(frozen=True)
class EvictionCheck:
    """Ergebnis einer Pruefung: ``safe``/``unsafe`` oder ``unknown`` (nicht
    pruefbar, etwa ``CONFIG`` per ACL verboten). ``maxmemory`` ``None`` heisst
    unbekannt."""

    status: EvictionStatus
    policy: str | None = None
    maxmemory: int | None = None
    #: ``used_memory`` aus ``INFO memory``, nur bei der erneuten Pruefung des Leaders.
    used_memory: int | None = None


def assess(policy: str | None, maxmemory: int | None) -> EvictionStatus:
    """``noeviction`` oder ``maxmemory`` 0 ist sicher; eine verdraengende
    Policy mit Grenze oder unbekannter Grenze ist unsicher."""
    if policy is None:
        return "unknown"
    if policy == "noeviction" or maxmemory == 0:
        return "safe"
    return "unsafe"
