"""Worker eines Systems als eigener Prozess über dessen CLI (Plan 2026-10-24-1118, KTD13).

Der Worker läuft nicht im Runner, sondern als Kindprozess in einer eigenen
Sitzung und damit eigener Prozessgruppe (PGID = PID; ``start_new_session``,
weil uvloop ``process_group`` nicht kennt). Ein Signal an die Gruppe trifft so den Worker und
die Prozesse, die seine CLI forkt (Celery-Pool, Dramatiq- und taskiq-Kinder),
aber nie den Runner. Die Garantie-Störungen aus U9 bauen darauf auf.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal
from pathlib import Path

#: Gestartete und noch nicht gestoppte Worker dieses Prozesses.
_LAUFENDE: set[WorkerProzess] = set()


def beende_alle() -> None:
    """SIGKILL an die Gruppe jedes laufenden Workers.

    Synchron und ohne Event-Loop, damit ein Signal-Handler es aufrufen kann,
    wenn der Hänger-Wächter den Prozess einer Kombination beendet.
    """
    for worker in list(_LAUFENDE):
        if worker.pid is not None:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(worker.pid, signal.SIGKILL)


class WorkerProzess:
    """Ein gestarteter Worker-Prozess mit PID."""

    def __init__(self, kommando: list[str], *, umgebung: dict[str, str], verzeichnis: Path) -> None:
        self._kommando = kommando
        self._umgebung = umgebung
        self._verzeichnis = verzeichnis
        self._prozess: asyncio.subprocess.Process | None = None

    @property
    def pid(self) -> int | None:
        """PID des Workers, solange er gestartet ist."""
        return None if self._prozess is None else self._prozess.pid

    async def starten(self) -> int:
        """Startet den Worker im Verzeichnis des Vergleichsaufbaus und liefert seine PID.

        ``PYTHONPATH`` enthält das Verzeichnis, damit jede CLI die Module unter
        ``workers/`` findet, auch wenn sie das Arbeitsverzeichnis nicht selbst
        in ``sys.path`` legt.
        """
        pythonpath = os.pathsep.join(filter(None, [str(self._verzeichnis), os.environ.get("PYTHONPATH")]))
        self._prozess = await asyncio.create_subprocess_exec(
            *self._kommando,
            cwd=self._verzeichnis,
            env={**os.environ, "PYTHONPATH": pythonpath, **self._umgebung},
            start_new_session=True,
        )
        _LAUFENDE.add(self)
        return self._prozess.pid

    async def warten(self) -> int:
        """Wartet, bis der Worker endet, und liefert seinen Rückgabewert (negativ: Signal)."""
        if self._prozess is None:
            raise RuntimeError("Worker wurde nicht gestartet")
        return await self._prozess.wait()

    async def stoppen(self, frist: float = 10.0) -> None:
        """SIGTERM an die Gruppe, nach ``frist`` Sekunden SIGKILL.

        Danach bekommt die Gruppe noch ein SIGKILL, damit kein geforktes Kind
        den Worker überlebt und die nächste Messung stört.
        """
        if self._prozess is None:
            return
        pid = self._prozess.pid
        with contextlib.suppress(ProcessLookupError):
            os.killpg(pid, signal.SIGTERM)
        try:
            await asyncio.wait_for(self._prozess.wait(), frist)
        except TimeoutError:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(pid, signal.SIGKILL)
            await self._prozess.wait()
        with contextlib.suppress(ProcessLookupError):
            os.killpg(pid, signal.SIGKILL)
        self._prozess = None
        _LAUFENDE.discard(self)
