"""Celery-Adapter des Vergleichs.

Der Worker läuft über ``celery -A workers.celery_worker worker`` mit dem
Pool und der Parallelität ab Werk (Prefork, ein Prozess je CPU). Der
Erzeuger im Runner nutzt dieselbe App; Celery ist synchron, deshalb laufen
Publish und Ergebnisabfrage in Threads.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

from parallelitaet import AB_WERK
from systems.base import NOCH_OFFEN, ProzessWorker, cli


class CelerySystem(ProzessWorker):
    """``TaskQueueSystem`` adapter for Celery (RabbitMQ, Ergebnisse in Valkey)."""

    name: str = "celery"
    valkey_db = 2

    def __init__(self, *, profil: str, parallelitaet: str = AB_WERK) -> None:
        super().__init__(profil=profil, parallelitaet=parallelitaet)
        self._worker_modul: Any = None

    def worker_kommando(self) -> list[str]:
        return [cli("celery"), "-A", "workers.celery_worker", "worker", *self.parallelitaet_argumente]

    async def setup(self) -> None:
        os.environ.update(self.worker_umgebung())
        from workers import celery_worker

        self._worker_modul = celery_worker

    async def teardown(self) -> None:
        await self.stop_worker()
        if self._worker_modul is not None:
            app = self._worker_modul.app
            await asyncio.to_thread(app.backend.client.flushdb)
            app.close()
            self._worker_modul = None

    async def enqueue(self, task_name: str, payload: dict) -> str:
        if self._worker_modul is None:
            raise RuntimeError("CelerySystem.setup() has not been called")
        task = self._worker_modul.app.tasks[task_name]
        result = await asyncio.to_thread(task.apply_async, kwargs=payload)
        return result.id

    async def get_result(self, task_id: str, timeout: float = 30.0, poll_interval: float = 0.01) -> Any:
        """Fragt den Status in Valkey ab, wie die anderen Adapter auch (kein Pub/Sub-Warten)."""
        if self._worker_modul is None:
            raise RuntimeError("CelerySystem.setup() has not been called")
        from celery import states

        backend = self._worker_modul.app.backend

        async def abfrage() -> Any:
            meta = await asyncio.to_thread(backend.get_task_meta, task_id)
            if meta["status"] == states.SUCCESS:
                return meta["result"]
            if meta["status"] in states.PROPAGATE_STATES:
                raise RuntimeError(f"Task {task_id} failed: {meta['result']!r}")
            return NOCH_OFFEN

        return await self.warte_auf_ergebnis(task_id, abfrage, timeout, poll_interval)
