"""Pollen bis zu einer Frist, ohne feste Wartezeit (Plan 0.12.0, MR 1).

Für Zustände ohne eigenes Ereignis (Queue-Tiefe, Broker-Alarm, Log-Einträge).
``interval`` ist ein Polling-Takt, keine Wartezeit, von der das Ergebnis
abhängt: Ein wahres Prädikat beendet das Warten sofort.

Das Ergebnis ist ein bool und keine Ausnahme: Beobachtungs-Tests ("tritt X
innerhalb der Frist ein?") asserten danach selbst, mit eigener Meldung.
"""

from __future__ import annotations

import asyncio
import inspect
import time
from collections.abc import Awaitable, Callable


async def wait_until(
    predicate: Callable[[], bool | Awaitable[bool]],
    timeout: float,
    interval: float = 0.05,
) -> bool:
    """Wertet ``predicate`` (sync oder async) aus, bis es wahr ist oder ``timeout`` abläuft.

    Liefert ``True``, sobald das Prädikat wahr ist, sonst ``False`` nach der Frist.
    """
    deadline = time.monotonic() + timeout
    while True:
        result = predicate()
        if inspect.isawaitable(result):
            result = await result
        if result:
            return True
        if time.monotonic() >= deadline:
            return False
        await asyncio.sleep(interval)
