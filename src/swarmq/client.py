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
        """Retrieve a task result using Pub/Sub with polling fallback.

        Flow for timeout > 0:
          1. Initial GET check (catches fast tasks / race window)
          2. Pub/Sub wait via backend.subscribe_and_wait
          3. Polling fallback (5ms interval) if Pub/Sub misses
        """
        _FALLBACK_POLL = 0.005  # 5ms polling for fallback

        # --- timeout=0: single GET, no Pub/Sub ---
        if timeout == 0:
            raw = await self._backend.get_result(task_id)
            if raw is None:
                raise ResultNotReadyError(task_id)
            return self._unpack(raw)

        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout

        # --- Step 1: Initial check ---
        raw = await self._backend.get_result(task_id)
        if raw is not None:
            return self._unpack(raw)

        # --- Step 2: Pub/Sub wait ---
        remaining = deadline - loop.time()
        if remaining > 0:
            channel = f"sq:notify:{task_id}"
            pubsub_timeout = min(1.0, remaining)
            notified = await self._backend.subscribe_and_wait(
                channel, timeout=pubsub_timeout,
            )
            if notified:
                raw = await self._backend.get_result(task_id)
                if raw is not None:
                    return self._unpack(raw)

        # --- Step 3: Polling fallback ---
        while loop.time() < deadline:
            raw = await self._backend.get_result(task_id)
            if raw is not None:
                return self._unpack(raw)
            await asyncio.sleep(_FALLBACK_POLL)

        raise ResultNotReadyError(task_id)

    async def schedule_many(
        self,
        tasks: list[tuple[str, tuple, dict]],
        queue: str | None = None,
    ) -> list[str]:
        """Submit multiple tasks for execution and return their task_ids.

        Each element of *tasks* is a (task_name, args, kwargs) tuple.
        All messages are published concurrently via asyncio.gather.
        """
        if not tasks:
            return []

        task_ids: list[str] = []
        coros = []
        for task_name, args, kwargs in tasks:
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

            task_id = str(uuid.uuid7())
            task_ids.append(task_id)
            message = {
                "task": {
                    "id": task_id,
                    "queue": resolved_queue,
                    "name": task_name,
                },
                "args": list(args),
                "kwargs": kwargs,
            }
            coros.append(self._broker.publish(resolved_queue, encode_json(message)))

        results = await asyncio.gather(*coros, return_exceptions=True)
        for r in results:
            if isinstance(r, BaseException):
                raise r

        return task_ids

    async def get_results(
        self,
        task_ids: list[str],
        timeout: float = 10,
        poll_interval: float = 0.005,
    ) -> list[Any]:
        """Retrieve results for multiple tasks using MGET-based batch polling.

        Polls every *poll_interval* seconds until all results are available
        or *timeout* is exceeded.
        """
        if not task_ids:
            return []

        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout

        while loop.time() < deadline:
            raw_list = await self._backend.get_results_batch(task_ids)
            if all(r is not None for r in raw_list):
                return [self._unpack(r) for r in raw_list]
            await asyncio.sleep(poll_interval)

        missing = [
            tid for tid, raw in zip(task_ids, raw_list)
            if raw is None
        ]
        raise ResultNotReadyError(
            f"Timeout waiting for task_ids: {missing}"
        )

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
