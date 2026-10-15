"""Pause vor ``redeliver`` (Plan 0.12.0, MR 3, BZ-29).

``redeliver`` ist ``nack(requeue=True)``: Der Broker stellt die Nachricht
sofort wieder zu. Ohne Pause verbraucht eine Zustellung, die ein Gate immer
wieder zurueckgibt (Backend-Fehlerantwort, gescheiterter DLQ-Publish, Slot
nicht verfuegbar), ihr ganzes Zustellbudget in Sekundenbruchteilen; ein
kurzer Publish-Ausfall schickte sie so sofort in die DLQ. Vor dem ``nack``
wartet der Worker deshalb ``min(0,1 s * 2^k, 5 s)``, k = Zahl der bisherigen
Zustellungen (Maximum aus ``x-delivery-count`` und ``x-acquired-count``).
Die rund 20 Runden des Default-Limits verteilen sich damit auf mehr als eine
Minute. Der Slot bleibt dabei belegt. Ein Shutdown bricht die Pause ab.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

REDELIVER_PAUSE_BASE_S = 0.1
REDELIVER_PAUSE_CAP_S = 5.0

# Zaehler der bisherigen Zustellungen (Quorum-Queues; 4.3 zaehlt den ``nack``
# nur in ``x-acquired-count``, Spike S4).
_COUNTERS = ("x-delivery-count", "x-acquired-count")

# Ab diesem Exponenten liegt die Pause sicher am Deckel; begrenzt 2^k.
_MAX_EXPONENT = 16


def _as_count(raw: Any) -> int:
    try:
        return max(int(raw), 0)
    except TypeError, ValueError, OverflowError:
        return 0


def redeliver_pause_s(headers: Mapping[str, Any]) -> float:
    """Pause vor dem ``nack`` einer Zustellung mit diesen Headern."""
    k = max(_as_count(headers.get(name)) for name in _COUNTERS)
    pause: float = REDELIVER_PAUSE_BASE_S * 2 ** min(k, _MAX_EXPONENT)
    return min(pause, REDELIVER_PAUSE_CAP_S)


async def wait_unless_shutdown(
    delay_s: float,
    shutdown: asyncio.Event,
    sleep: Callable[[float], Awaitable[None]] | None = None,
) -> None:
    """Wartet *delay_s*, endet aber sofort, sobald *shutdown* gesetzt ist."""
    if shutdown.is_set():
        return
    sleeper = asyncio.ensure_future((sleep or _sleep)(delay_s))
    stopper = asyncio.ensure_future(shutdown.wait())
    try:
        await asyncio.wait({sleeper, stopper}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for waiter in (sleeper, stopper):
            waiter.cancel()


# Zeitquelle der Pause zur Laufzeit; Unit-Tests ersetzen sie
# (``tests/unit/conftest.py``), damit keiner real wartet.
_sleep: Callable[[float], Awaitable[None]] = asyncio.sleep


__all__ = ["REDELIVER_PAUSE_BASE_S", "REDELIVER_PAUSE_CAP_S", "redeliver_pause_s", "wait_unless_shutdown"]
