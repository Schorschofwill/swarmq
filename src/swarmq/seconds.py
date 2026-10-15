"""Ganzzahlige Sekundenfelder (E-055, F3-31).

``timedelta(...).total_seconds()`` liefert immer ``float``; valkey-py nimmt in
``EX``/``PX`` aber nur ``int`` an, und msgspec dekodiert ``3600.0`` nicht in ein
``int``-Feld. Darum werden ``result_ttl`` und ``unique_for`` normalisiert,
bevor sie irgendwo ankommen.
"""

from __future__ import annotations

import math
from typing import overload


@overload
def whole_seconds(value: None, name: str) -> None: ...
@overload
def whole_seconds(value: float, name: str) -> int: ...
def whole_seconds(value: float | None, name: str) -> int | None:
    """Ganzzahliger ``float`` wird ``int``, ein echter Bruch wird aufgerundet
    (eine TTL oder Sperre wird nie kürzer als verlangt). ``None``, ``int`` und
    ``bool`` bleiben unverändert.

    Nicht endliche Werte ergeben ``ValueError`` mit *name* (E-063), statt
    eines ``OverflowError`` aus ``math.ceil``. Bereichsprüfungen des Aufrufers
    laufen vorher auf dem Rohwert (``ceil(-0.5) == 0``).
    """
    if not isinstance(value, float):
        return value
    if not math.isfinite(value):
        msg = f"{name} must be a finite number of seconds, got {value}"
        raise ValueError(msg)
    return math.ceil(value)


def validate_unique_for(value: float) -> int:
    """Ausdrückliches ``unique_for`` prüfen und normalisieren (E-056, E-055).

    ``0`` (auch ``0.0``) und negative Werte sind ein ``ValueError``; ``None``
    zum Abschalten (E-033) prüft der Aufrufer vorher aus. Die Prüfung läuft
    auf dem Rohwert, damit ``-0.5`` nicht über ``ceil`` zu ``0`` wird.
    """
    if value == 0:
        msg = "unique_for=0 is invalid; use None to disable dedup"
        raise ValueError(msg)
    if value < 0:
        msg = f"unique_for must be > 0 (got {value})"
        raise ValueError(msg)
    return whole_seconds(value, "unique_for")
