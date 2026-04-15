"""SwarmQ App — main application class."""

from __future__ import annotations

from typing import Any

from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.client import Client
from swarmq.config import SwarmQConfig
from swarmq.logging import setup_logging
from swarmq.registry import TaskRegistry
from swarmq.task import Task
from swarmq.worker import Worker


class SwarmQ:
    """Main SwarmQ application entry point."""

    def __init__(
        self,
        broker: RabbitMQBroker,
        backend: ValkeyBackend,
        **config_kwargs: Any,
    ) -> None:
        self.broker = broker
        self.backend = backend
        self.config = SwarmQConfig(
            broker_url=broker._url,
            backend_url=backend._url,
            **config_kwargs,
        )
        self.registry = TaskRegistry()
        for task_class in Task._registered_tasks:
            self.registry.register(task_class)
        self._log_listener = setup_logging(self.config.log_level, self.config.log_json)
        self.client = Client(broker, backend, registry=self.registry)

    async def __aenter__(self) -> SwarmQ:
        await self.broker.connect()
        try:
            await self.backend.connect()
        except BaseException:
            await self.broker.close()
            raise
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.broker.close()
        await self.backend.close()
        self._log_listener.stop()

    async def schedule(self, task_name: str, *args: Any, **kwargs: Any) -> str:
        return await self.client.schedule(task_name, *args, **kwargs)

    async def get_result(
        self, task_id: str, timeout: float = 10, polling: float = 0.2
    ) -> Any:
        return await self.client.get_result(task_id, timeout=timeout, polling=polling)

    async def run(
        self,
        task_name: str,
        *args: Any,
        timeout: float = 10,
        polling: float = 0.2,
        **kwargs: Any,
    ) -> Any:
        return await self.client.run(
            task_name, *args, timeout=timeout, polling=polling, **kwargs
        )

    async def start_worker(self) -> None:
        worker = Worker(self.broker, self.backend, self.registry, self.config)
        await worker.start()
