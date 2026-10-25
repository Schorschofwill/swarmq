"""Protokoll der Adapter und gemeinsamer Worker-Start (Plan 2026-10-24-1118, KTD13)."""

from __future__ import annotations

import asyncio
import json
import os
import sys
import uuid
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

import parallelitaet as parallelitaet_von
from garantieprofile import profil as profil_von
from worker_prozess import WorkerProzess

VERGLEICH_DIR = Path(__file__).resolve().parents[1]

# Dienste aus ``docker-compose.yml`` des Vergleichs; bewusst weder Standard-
# (5672/6379) noch Testports (5699/6399), damit kein Lauf fremde Daten trifft.
# Der Garantie-Vergleich (``stoerungen/``) lenkt beide über die Variablen auf
# seinen eigenen Stack hinter Toxiproxy; Worker erben sie.
RABBITMQ_URL = os.environ.get("VERGLEICH_RABBITMQ_URL", "amqp://guest:guest@localhost:5682/")
VALKEY_HOST = os.environ.get("VERGLEICH_VALKEY_HOST", "localhost:6389")

#: Rückgabe einer Ergebnisabfrage, solange die Task nicht fertig ist; ein Ergebnis kann ``None`` sein.
NOCH_OFFEN: Any = object()


@runtime_checkable
class TaskQueueSystem(Protocol):
    """Strukturelle Schnittstelle der Adapter.

    Der Worker jedes Systems läuft als eigener Prozess über die CLI des
    Systems, nie im Runner. ``worker_pid`` nennt ihn, solange er läuft; ein
    Signal an ihn trifft den Runner nicht (Grundlage der Störungen im Garantie-Vergleich).
    Der Runner-Prozess ist nur Erzeuger und liest die Ergebnisse.
    """

    name: str
    profil: str

    @property
    def worker_pid(self) -> int | None:
        """PID des Worker-Prozesses, solange er läuft."""
        ...

    def worker_kommando(self) -> list[str]:
        """Aufruf der CLI des Systems, der den Worker startet."""
        ...

    async def setup(self) -> None:
        """Connect to broker/backend, declare queues."""
        ...

    async def teardown(self) -> None:
        """Cleanup: stop worker, close connections, flush results."""
        ...

    async def start_worker(self) -> None:
        """Startet den Worker-Prozess."""
        ...

    async def stop_worker(self) -> None:
        """Beendet den Worker-Prozess (SIGTERM, nach Frist SIGKILL)."""
        ...

    async def enqueue(self, task_name: str, payload: dict) -> str:
        """Schedule a task, return task_id."""
        ...

    async def get_result(self, task_id: str, timeout: float = 30.0, poll_interval: float = 0.01) -> Any:
        """Poll for result. poll_interval is set by the scenario."""
        ...


def cli(name: str) -> str:
    """Pfad der CLI ``name`` in der Umgebung, in der der Runner läuft.

    Jedes System hat eine eigene Umgebung (``requirements-<system>.txt``);
    die CLI liegt neben deren Python.
    """
    return str(Path(sys.executable).with_name(name))


class ProzessWorker:
    """Gemeinsamer Teil der Adapter: Profil, frische Queue und Worker-Prozess.

    Unterklassen liefern ``worker_kommando()`` und bei Bedarf weitere
    Umgebungsvariablen über ``worker_umgebung()``. Der Worker bekommt die
    Queue und die Profileinstellungen über ``VERGLEICH_*``-Variablen; der
    Erzeuger im Runner liest dieselben Werte.
    """

    name: str
    #: Valkey-Datenbank für die Ergebnisse dieses Systems, getrennt von den anderen.
    valkey_db: int = 0

    def __init__(self, *, profil: str, parallelitaet: str = parallelitaet_von.AB_WERK) -> None:
        self.profil = profil
        self.parallelitaet = parallelitaet
        self.einstellungen: dict[str, Any] = profil_von(self.name, profil).einstellungen
        # Frische Queue je Lauf: Reste eines früheren Laufs verfälschen keine Messung.
        self.queue = f"bench-{uuid.uuid4().hex[:8]}"
        self._worker: WorkerProzess | None = None

    @property
    def backend_url(self) -> str:
        return f"redis://{VALKEY_HOST}/{self.valkey_db}"

    @property
    def worker_pid(self) -> int | None:
        return None if self._worker is None else self._worker.pid

    @property
    def worker_laeuft(self) -> bool:
        """Ob der Worker-Prozess läuft; nach einer Störung startet der Garantie-Vergleich ihn neu, wenn nicht."""
        return self._worker is not None and self._worker.laeuft

    def worker_kommando(self) -> list[str]:
        raise NotImplementedError

    def worker_umgebung(self) -> dict[str, str]:
        """Umgebung des Workers; der Erzeuger übernimmt sie vor dem Import des Worker-Moduls."""
        return {
            "VERGLEICH_QUEUE": self.queue,
            "VERGLEICH_BROKER_URL": RABBITMQ_URL,
            "VERGLEICH_BACKEND_URL": self.backend_url,
            "VERGLEICH_EINSTELLUNGEN": json.dumps(self.einstellungen),
            **parallelitaet_von.umgebung(self.name, self.parallelitaet),
        }

    @property
    def parallelitaet_argumente(self) -> list[str]:
        """CLI-Argumente, die ``worker_kommando()`` für die gewählte Parallelität anhängt."""
        return parallelitaet_von.argumente(self.name, self.parallelitaet)

    async def warte_auf_ergebnis(
        self, task_id: str, abfrage: Callable[[], Awaitable[Any]], timeout: float, poll_interval: float
    ) -> Any:
        """Ruft ``abfrage`` bis zu einem Ergebnis außer ``NOCH_OFFEN`` auf, längstens ``timeout`` Sekunden."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while True:
            ergebnis = await abfrage()
            if ergebnis is not NOCH_OFFEN:
                return ergebnis
            if loop.time() >= deadline:
                raise TimeoutError(f"Task {task_id} did not complete within {timeout}s")
            await asyncio.sleep(poll_interval)

    async def start_worker(self) -> None:
        if self._worker is not None:
            return
        self._worker = WorkerProzess(self.worker_kommando(), umgebung=self.worker_umgebung(), verzeichnis=VERGLEICH_DIR)
        await self._worker.starten()

    async def stop_worker(self) -> None:
        if self._worker is None:
            return
        await self._worker.stoppen()
        self._worker = None
