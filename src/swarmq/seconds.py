"""Ganzzahlige Sekundenfelder (E-055, F3-31).

``timedelta(...).total_seconds()`` liefert immer ``float``; valkey-py nimmt in
``EX``/``PX`` aber nur ``int`` an, und msgspec dekodiert ``3600.0`` nicht in ein
``int``-Feld. Darum werden ``result_ttl`` und ``unique_for`` normalisiert,
bevor sie irgendwo ankommen.
"""

from __future__ import annotations

import math
from typing import overload

#: Größte Frist in Millisekunden (E-063, Nachtrag 2026-10-17; PF-11). Hier im
#: Blattmodul, damit auch ``exceptions`` (``Retry``, F4-19) sie ohne
#: Importzyklus nutzt; ``config_validation`` reicht sie weiter.
#: Gemessen am 2026-10-18 gegen den Teststack: RabbitMQ 4.3.6 nimmt eine
#: Per-Message-``expiration`` bis 315360000000 ms (zehn Jahre zu 365 Tagen)
#: an und lehnt 315360000001 mit ``PRECONDITION_FAILED`` (``value_too_large``)
#: ab; Valkey 8 nimmt ``PX`` bis 2**63-1 minus der aktuellen Unix-Zeit in ms
#: an. Die kleinere Grenze gilt für alle Fristen, weil ``retry_max_delay``,
#: ``lock_wait_max_s`` und ``dead_letter_ttl_s`` auch als Expiration enden.
#: Festgeschrieben in ``tests/integration/backend/test_deadline_bounds_valkey.py``
#: und ``tests/integration/broker/test_expiration_bounds_rabbitmq.py``.
MAX_DEADLINE_MS = 315_360_000_000
#: Dieselbe Grenze in Sekunden; ``wert * 1000`` bleibt damit <= ``MAX_DEADLINE_MS``.
MAX_DEADLINE_S = MAX_DEADLINE_MS // 1000


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
