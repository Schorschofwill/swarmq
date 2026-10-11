"""TaskIQ adapter using RabbitMQ broker (AioPikaBroker).

Same architecture as taskiq_sys.py but with RabbitMQ as broker instead of
Redis ListQueueBroker. This enables a fair same-broker comparison against
SwarmQ (both using RabbitMQ).

Result backend remains Valkey db=1 (both systems use Valkey for results).
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from taskiq.receiver import Receiver

from workers.taskiq_rmq_worker import (
    broker,
    cpu_task,
    failing_task,
    io_task,
    noop_task,
)

logger = logging.getLogger(__name__)

_TASK_REGISTRY: dict[str, Any] = {
    "noop": noop_task,
    "noop_task": noop_task,
    "io_task": io_task,
    "cpu_task": cpu_task,
    "failing_task": failing_task,
}


class TaskIQRabbitMQSystem:
    """TaskIQ adapter with RabbitMQ broker for same-broker comparison."""

    name: str = "taskiq-rmq"

    def __init__(self) -> None:
        self._receiver: Receiver | None = None
        self._worker_task: asyncio.Task[None] | None = None
        self._finish_event: asyncio.Event | None = None

    async def setup(self) -> None:
        # Mark as worker process so startup() creates the read channel
        # needed by the Receiver/listen().
        broker.is_worker_process = True
        await broker.startup()
        logger.info("TaskIQ-RMQ broker started")

    async def teardown(self) -> None:
        try:
            await broker.shutdown()
        except Exception:
            logger.exception("Error during broker shutdown")

        try:
            import redis.asyncio as aioredis

            conn = aioredis.from_url("redis://localhost:6389/1")
            await conn.flushdb()
            await conn.aclose()
        except Exception:
            logger.debug("Could not flush Valkey db=1", exc_info=True)

    async def start_worker(self) -> None:
        if self._worker_task is not None:
            return

        self._finish_event = asyncio.Event()
        self._receiver = Receiver(broker)

        async def _run_receiver() -> None:
            assert self._receiver is not None
            try:
                await self._receiver.listen(self._finish_event)
            except asyncio.CancelledError:
                pass
            except BaseException:
                logger.exception("TaskIQ-RMQ receiver crashed")

        self._worker_task = asyncio.create_task(_run_receiver())
        await asyncio.sleep(1.0)  # RabbitMQ needs more time than Redis

    async def stop_worker(self) -> None:
        if self._worker_task is None:
            return

        if self._finish_event is not None:
            self._finish_event.set()

        try:
            await asyncio.wait_for(self._worker_task, timeout=5.0)
        except asyncio.TimeoutError, asyncio.CancelledError:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass

        self._worker_task = None
        self._receiver = None
        self._finish_event = None

    async def enqueue(self, task_name: str, payload: dict) -> str:
        task_func = _TASK_REGISTRY.get(task_name)
        if task_func is None:
            raise ValueError(f"Unknown task {task_name!r}. Available: {sorted(_TASK_REGISTRY)}")
        result_handle = await task_func.kiq(**payload)
        return result_handle.task_id

    async def get_result(self, task_id: str, timeout: float = 30.0, poll_interval: float = 0.01) -> Any:
        deadline = asyncio.get_event_loop().time() + timeout
        while True:
            is_ready = await broker.result_backend.is_result_ready(task_id)
            if is_ready:
                result = await broker.result_backend.get_result(task_id)
                if result.is_err:
                    raise RuntimeError(f"Task {task_id} failed: {result.error}")
                return result.return_value

            if asyncio.get_event_loop().time() >= deadline:
                raise TimeoutError(f"Task {task_id} did not complete within {timeout}s")

            await asyncio.sleep(poll_interval)
