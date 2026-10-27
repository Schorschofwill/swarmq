"""Sammelpause des Log-Listeners (Plan 2026-10-27-0800, KTD1, KTD2, KTD4).

Ein Listener, der fuer jede Zeile aufwacht, nimmt der Event-Loop jedes Mal
den GIL. Mit Pause wartet er nach leerer Queue bis zu ``delay`` Sekunden
und gibt dann gesammelt aus. Gewoehnliche Zeilen wecken ihn in der Pause
nicht; ``wake`` beendet sie vorzeitig. Importiert ``logging_queue`` nicht.
"""

from __future__ import annotations

from threading import Event


class BatchPause:
    """Die Pause eines Listeners; ``delay`` 0 heisst: nie warten (R3)."""

    def __init__(self, delay: float) -> None:
        self.delay = delay
        self._wake = Event()

    def wait(self) -> None:
        """Bis zu ``delay`` warten oder bis ``wake``. Zurueckgesetzt wird erst
        danach: Ein Wecken waehrend des Ausgebens davor beendet die naechste
        Pause sofort, statt verloren zu gehen."""
        if self.delay > 0:
            self._wake.wait(self.delay)
            self._wake.clear()

    def wake(self) -> None:
        """Beendet eine laufende oder die naechste Pause."""
        self._wake.set()
