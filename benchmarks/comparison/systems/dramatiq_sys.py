"""Dramatiq-Adapter des Vergleichs.

Der Worker läuft über ``dramatiq workers.dramatiq_worker`` mit Prozessen und
Threads ab Werk (ein Prozess je CPU, acht Threads). Der Erzeuger im Runner
nutzt denselben Broker; Dramatiq ist synchron, deshalb laufen Senden und
Ergebnisabfrage in Threads (der Broker hält je Thread eine eigene Verbindung).
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

from systems.base import NOCH_OFFEN, ProzessWorker, cli


class DramatiqSystem(ProzessWorker):
    """``TaskQueueSystem`` adapter for Dramatiq (RabbitMQ, Ergebnisse in Valkey)."""

    name: str = "dramatiq"
    valkey_db = 3

    def __init__(self, *, profil: str) -> None:
        super().__init__(profil=profil)
        self._worker_modul: Any = None
        self._nachrichten: dict[str, Any] = {}

    def worker_kommando(self) -> list[str]:
        return [cli("dramatiq"), "workers.dramatiq_worker"]

    def worker_umgebung(self) -> dict[str, str]:
        umgebung = super().worker_umgebung()
        if "dramatiq_queue_prefetch" in self.einstellungen:
            umgebung["dramatiq_queue_prefetch"] = str(self.einstellungen["dramatiq_queue_prefetch"])
        return umgebung

    async def setup(self) -> None:
        os.environ.update(self.worker_umgebung())
        from workers import dramatiq_worker

        # Die Queue deklariert Dramatiq beim ersten Senden, vor dem Publish.
        self._worker_modul = dramatiq_worker

    async def teardown(self) -> None:
        await self.stop_worker()
        if self._worker_modul is not None:
            await asyncio.to_thread(self._worker_modul.result_backend.client.flushdb)
            await asyncio.to_thread(self._worker_modul.broker.close)
            self._worker_modul = None
        self._nachrichten.clear()

    async def enqueue(self, task_name: str, payload: dict) -> str:
        if self._worker_modul is None:
            raise RuntimeError("DramatiqSystem.setup() has not been called")
        aktor = self._worker_modul.AKTOREN[task_name]
        nachricht = await asyncio.to_thread(aktor.send, **payload)
        self._nachrichten[nachricht.message_id] = nachricht
        return nachricht.message_id

    async def get_result(self, task_id: str, timeout: float = 30.0, poll_interval: float = 0.01) -> Any:
        if self._worker_modul is None:
            raise RuntimeError("DramatiqSystem.setup() has not been called")
        from dramatiq.results import ResultMissing

        nachricht = self._nachrichten[task_id]
        backend = self._worker_modul.result_backend

        async def abfrage() -> Any:
            try:
                return await asyncio.to_thread(backend.get_result, nachricht, block=False)
            except ResultMissing:
                return NOCH_OFFEN

        return await self.warte_auf_ergebnis(task_id, abfrage, timeout, poll_interval)
