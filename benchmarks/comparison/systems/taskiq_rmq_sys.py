"""taskiq-Adapter des Vergleichs (RabbitMQ über taskiq-aio-pika, Ergebnisse in Valkey).

Der Worker läuft über ``taskiq worker workers.taskiq_rmq_worker:broker`` mit
Prozesszahl und Parallelität ab Werk. Im Runner lebt nur der Erzeuger: Er
startet den Broker ohne Lesekanal und schickt die Tasks mit ``kiq()``.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

from systems.base import ProzessWorker, cli


class TaskIQRabbitMQSystem(ProzessWorker):
    """``TaskQueueSystem`` adapter for taskiq with the RabbitMQ broker."""

    name: str = "taskiq"
    valkey_db = 1

    def __init__(self, *, profil: str) -> None:
        super().__init__(profil=profil)
        self._worker_modul: Any = None

    def worker_kommando(self) -> list[str]:
        kommando = [cli("taskiq"), "worker", "workers.taskiq_rmq_worker:broker"]
        if "ack_type" in self.einstellungen:
            kommando += ["--ack-type", self.einstellungen["ack_type"]]
        return kommando

    async def setup(self) -> None:
        os.environ.update(self.worker_umgebung())
        from workers import taskiq_rmq_worker

        # ``startup()`` deklariert die Queue vor dem Worker.
        await taskiq_rmq_worker.broker.startup()
        self._worker_modul = taskiq_rmq_worker

    async def teardown(self) -> None:
        await self.stop_worker()
        if self._worker_modul is not None:
            await self._worker_modul.broker.shutdown()
            self._worker_modul = None
        import redis.asyncio as aioredis

        conn = aioredis.from_url(self.backend_url)
        try:
            await conn.flushdb()
        finally:
            await conn.aclose()

    async def enqueue(self, task_name: str, payload: dict) -> str:
        if self._worker_modul is None:
            raise RuntimeError("TaskIQRabbitMQSystem.setup() has not been called")
        result_handle = await self._worker_modul.TASKS[task_name].kiq(**payload)
        return result_handle.task_id

    async def get_result(self, task_id: str, timeout: float = 30.0, poll_interval: float = 0.01) -> Any:
        if self._worker_modul is None:
            raise RuntimeError("TaskIQRabbitMQSystem.setup() has not been called")
        backend = self._worker_modul.broker.result_backend
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while True:
            if await backend.is_result_ready(task_id):
                result = await backend.get_result(task_id)
                if result.is_err:
                    raise RuntimeError(f"Task {task_id} failed: {result.error}")
                return result.return_value
            if loop.time() >= deadline:
                raise TimeoutError(f"Task {task_id} did not complete within {timeout}s")
            await asyncio.sleep(poll_interval)
