"""arq-Adapter des Vergleichs.

arq läuft nur auf Valkey und mit der Werkseinstellung; der Bericht weist es
als eigenen Vergleich aus (KTD13). Der Worker läuft über
``arq workers.arq_worker.WorkerSettings``.
"""

from __future__ import annotations

import os
from typing import Any

from parallelitaet import AB_WERK
from systems.base import ProzessWorker, cli


class ArqSystem(ProzessWorker):
    """``TaskQueueSystem`` adapter for arq (nur Valkey)."""

    name: str = "arq"
    valkey_db = 4

    def __init__(self, *, profil: str, parallelitaet: str = AB_WERK) -> None:
        super().__init__(profil=profil, parallelitaet=parallelitaet)
        self._pool: Any = None

    def worker_kommando(self) -> list[str]:
        return [cli("arq"), "workers.arq_worker.WorkerSettings"]

    async def setup(self) -> None:
        os.environ.update(self.worker_umgebung())
        from arq import create_pool

        from workers.arq_worker import QUEUE, REDIS_SETTINGS

        self._pool = await create_pool(REDIS_SETTINGS, default_queue_name=QUEUE)

    async def teardown(self) -> None:
        await self.stop_worker()
        if self._pool is not None:
            await self._pool.flushdb()
            await self._pool.aclose()
            self._pool = None

    async def enqueue(self, task_name: str, payload: dict) -> str:
        if self._pool is None:
            raise RuntimeError("ArqSystem.setup() has not been called")
        job = await self._pool.enqueue_job(task_name, **payload)
        return job.job_id

    async def get_result(self, task_id: str, timeout: float = 30.0, poll_interval: float = 0.01) -> Any:
        if self._pool is None:
            raise RuntimeError("ArqSystem.setup() has not been called")
        from arq.jobs import Job

        job = Job(task_id, self._pool, _queue_name=self.queue)
        return await job.result(timeout=timeout, poll_delay=poll_interval)
