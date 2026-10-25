"""SwarmQ-Adapter des Vergleichs.

Der Worker läuft über ``swarmq worker`` als eigener Prozess und lädt
``workers/swarmq_cli.py``, das die Benchmark-Tasks auf der frischen Queue
anlegt. Im Runner lebt nur der Erzeuger: eine ``SwarmQ``-Instanz für
``schedule()`` und ``get_result()``.

swarmq hat feste Garantien; die Profile ``ab_werk`` und ``stark`` sind
gleich (``garantieprofile.py``).

Task registry isolation
~~~~~~~~~~~~~~~~~~~~~~~
``Task.__init_subclass__`` appends every new subclass to the global
``Task._registered_tasks`` list.  We snapshot the registry before creating
the benchmark task classes and restore it in ``teardown()``.
"""

from __future__ import annotations

from typing import Any

from parallelitaet import AB_WERK
from systems.base import RABBITMQ_URL, VALKEY_HOST, ProzessWorker, cli


class SwarmQSystem(ProzessWorker):
    """``TaskQueueSystem`` adapter for SwarmQ."""

    name: str = "swarmq"
    valkey_db = 0

    def __init__(self, *, profil: str, parallelitaet: str = AB_WERK, queue_type: str = "quorum") -> None:
        super().__init__(profil=profil, parallelitaet=parallelitaet)
        self._queue_type = queue_type
        self._broker: Any = None
        self._backend: Any = None
        self._app: Any = None
        self._registry_snapshot: list[type] | None = None

    @property
    def backend_url(self) -> str:
        return f"valkey://{VALKEY_HOST}/{self.valkey_db}"

    def worker_kommando(self) -> list[str]:
        return [
            cli("swarmq"),
            "worker",
            "--module",
            "workers.swarmq_cli",
            "--queues",
            self.queue,
            *self.parallelitaet_argumente,
        ]

    def worker_umgebung(self) -> dict[str, str]:
        return {
            **super().worker_umgebung(),
            "SWARMQ_BROKER_URL": RABBITMQ_URL,
            "SWARMQ_BACKEND_URL": self.backend_url,
            "SWARMQ_QUEUE_TYPE": self._queue_type,
        }

    async def setup(self) -> None:
        """Erzeuger verbinden und die Queue deklarieren, bevor der Worker startet."""
        from swarmq.app import SwarmQ
        from swarmq.backend.valkey import ValkeyBackend
        from swarmq.broker.rabbitmq import RabbitMQBroker
        from swarmq.task import Task

        from workers.swarmq_worker import create_task_classes

        self._registry_snapshot = Task._registered_tasks.copy()
        create_task_classes(self.queue)

        self._broker = RabbitMQBroker(RABBITMQ_URL)
        self._backend = ValkeyBackend(self.backend_url)
        # ``SwarmQ`` setzt den Queue-Typ des Brokers aus seiner Konfiguration;
        # derselbe Typ wie im Worker, sonst lehnt RabbitMQ die zweite Deklaration ab.
        self._app = SwarmQ(self._broker, self._backend, queues=[self.queue], queue_type=self._queue_type)
        await self._broker.connect()
        await self._backend.connect()
        await self._broker.declare_queue(self.queue)

    async def teardown(self) -> None:
        """Worker stoppen, Verbindungen schließen, Ergebnisse löschen, Registry zurücksetzen."""
        from swarmq.task import Task

        await self.stop_worker()
        if self._broker is not None:
            await self._broker.close()
            self._broker = None
        if self._backend is not None:
            await self._backend.close()
            self._backend = None
        self._app = None
        await _flush(self.backend_url)
        if self._registry_snapshot is not None:
            Task._registered_tasks = self._registry_snapshot
            self._registry_snapshot = None

    async def enqueue(self, task_name: str, payload: dict) -> str:
        """Schedule a task and return its id; ``payload`` becomes keyword arguments."""
        if self._app is None:
            raise RuntimeError("SwarmQSystem.setup() has not been called")
        return await self._app.schedule(task_name, **payload)

    async def get_result(self, task_id: str, timeout: float = 30.0, poll_interval: float = 0.01) -> Any:
        if self._app is None:
            raise RuntimeError("SwarmQSystem.setup() has not been called")
        return await self._app.get_result(task_id, timeout=timeout, polling=poll_interval)


async def _flush(url: str) -> None:
    """Leert die Ergebnis-Datenbank, damit lange Läufe Valkey nicht füllen (``maxmemory``, ``noeviction``)."""
    import valkey.asyncio as valkey

    client = valkey.from_url(url)
    try:
        await client.flushdb()
    finally:
        await client.aclose()
