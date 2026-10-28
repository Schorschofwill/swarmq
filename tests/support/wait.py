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

# Startsicherung für ``Signal.WORKER_ONLINE`` in e2e-Fixtures, keine Aussage
# über ein geprüftes Verhalten. Der Start legt Quorum-Queues an; auf einem Host
# mit mehreren Brokern unter Plattenlast dauerte ``Queue.Declare`` über 8 s
# (CI-Pipeline 441), lokal ist der Worker nach rund 0,5 s online.
WORKER_START_TIMEOUT_S = 20.0


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
