"""Optionale Broker-Faehigkeit ``ready_depth`` (Plan 0.12.0, MR 10, Q15).

Neutral und rein: Worker-Metriken und der Autoscaler-Sampler fragen die Tiefe
der Haupt-Queue ohne Delay-Queues ab. Fehlt die Methode (Fake, Fremd-Broker),
scheitert sie oder liefert sie keine Zahl, ist das Ergebnis ``None``; der
Aufrufer faellt dann auf das Verhalten bis 0.11 zurueck. Nie ueber
``isinstance`` auf ein Protocol erkennen: ein ``MagicMock`` passt dort immer.
"""

from __future__ import annotations

from typing import Any


async def ready_depth_or_none(broker: Any, queue: str) -> int | None:
    """``broker.ready_depth(queue)`` oder ``None`` (Q15)."""
    ready = getattr(broker, "ready_depth", None)
    if ready is None:
        return None
    try:
        value = await ready(queue)
    except Exception:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value
