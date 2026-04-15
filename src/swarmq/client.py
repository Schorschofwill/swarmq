"""SwarmQ Client API."""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import ResultNotReadyError, TaskError, TaskNotFoundError
from swarmq.registry import TaskRegistry
from swarmq.serialization import decode_result, encode_json


class Client:
    """Schedule tasks and retrieve results.

    Operates in two modes:
    - With registry: validates task names against the registry and resolves
      queues from TaskInfo.
    - Without registry (Client-Modus, spec 2.6): skips validation, requires
      an explicit queue for every schedule() call.
    """

    def __init__(
        self,
        broker: RabbitMQBroker,
        backend: ValkeyBackend,
        registry: TaskRegistry | None = None,
    ) -> None:
        self._broker = broker
        self._backend = backend
        self._registry = registry

    async def schedule(
        self,
        task_name: str,
        *args: Any,
        queue: str | None = None,
        **kwargs: Any,
    ) -> str:
        """Submit a task for execution and return its task_id."""
        # --- validation (registry mode only) ---
        if self._registry is not None:
            if not self._registry.has(task_name):
                raise TaskNotFoundError(task_name)

        # --- queue resolution chain ---
        resolved_queue = queue
        if resolved_queue is None and self._registry is not None:
            entry = self._registry.get_entry(task_name)
            resolved_queue = entry.task_info.queues[0]
        if resolved_queue is None:
            raise ValueError(
                "No queue specified and no registry available to resolve one"
            )

        # --- build and publish message ---
        task_id = str(uuid.uuid7())
        message = {
            "task": {
                "id": task_id,
                "queue": resolved_queue,
                "name": task_name,
            },
            "args": list(args),
            "kwargs": kwargs,
        }
        await self._broker.publish(resolved_queue, encode_json(message))
        return task_id

    async def get_result(
        self,
        task_id: str,
        timeout: float = 10,
        polling: float = 0.2,
    ) -> Any:
        """Poll the backend for a task result."""
        if timeout == 0:
            raw = await self._backend.get_result(task_id)
            if raw is None:
                raise ResultNotReadyError(task_id)
            return self._unpack(raw)

        deadline = asyncio.get_event_loop().time() + timeout
        while True:
            raw = await self._backend.get_result(task_id)
            if raw is not None:
                return self._unpack(raw)
            if asyncio.get_event_loop().time() >= deadline:
                raise ResultNotReadyError(task_id)
            await asyncio.sleep(polling)

    async def run(
        self,
        task_name: str,
        *args: Any,
        queue: str | None = None,
        timeout: float = 10,
        polling: float = 0.2,
        **kwargs: Any,
    ) -> Any:
        """Schedule a task and wait for its result."""
        task_id = await self.schedule(task_name, *args, queue=queue, **kwargs)
        return await self.get_result(task_id, timeout=timeout, polling=polling)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _unpack(raw: bytes) -> Any:
        """Decode a raw result and return the value or raise on error."""
        data = decode_result(raw)
        if data.get("is_error"):
            raise TaskError(data.get("error_message", "unknown error"))
        return data["result"]
