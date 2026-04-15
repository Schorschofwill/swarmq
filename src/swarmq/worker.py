"""SwarmQ Worker — consumer loop that executes tasks."""

from __future__ import annotations

import asyncio
import logging

from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.config import SwarmQConfig
from swarmq.exceptions import (
    BackendConnectionError,
    ResultSerializationError,
    TaskNotFoundError,
)
from swarmq.registry import TaskRegistry
from swarmq.serialization import decode_json, encode_result

logger = logging.getLogger(__name__)


class Worker:
    def __init__(
        self,
        broker: RabbitMQBroker,
        backend: ValkeyBackend,
        registry: TaskRegistry,
        config: SwarmQConfig,
    ) -> None:
        self._broker = broker
        self._backend = backend
        self._registry = registry
        self._config = config
        self._semaphore = asyncio.Semaphore(config.concurrency)

    async def start(self) -> None:
        """Connect broker + backend, declare queues, start consuming."""
        await self._broker.connect()
        await self._backend.connect()

        for queue_name in self._config.queues:
            await self._broker.declare_queue(queue_name)
            await self._broker.consume(
                queue_name,
                self._on_message,
                prefetch_count=self._config.concurrency,
            )

    async def stop(self) -> None:
        """Close broker and backend connections."""
        await self._broker.close()
        await self._backend.close()

    async def _on_message(self, message) -> None:
        """Consume callback — spawns a task so aio-pika's dispatch loop is not blocked."""
        asyncio.create_task(self._handle_message(message))

    async def _handle_message(self, message) -> None:
        """Core message handler logic."""
        # 1-2. Deserialize message body
        try:
            payload = decode_json(message.body)
            task_meta = payload["task"]
            task_id = task_meta["id"]
            queue = task_meta["queue"]
            task_name = task_meta["name"]
            args = payload["args"]
            kwargs = payload["kwargs"]
        except Exception as exc:
            logger.error("Failed to deserialize message: %s", exc)
            await self._broker.nack(message, requeue=False)
            return

        # 3. Acquire semaphore
        await self._semaphore.acquire()
        try:
            # 4-5. Look up task in registry
            try:
                task_class = self._registry.get(task_name)
            except TaskNotFoundError:
                logger.error("Task %r not found in registry", task_name)
                await self._broker.nack(message, requeue=False)
                return

            # 6. Create task instance, run lifecycle
            task_instance = task_class()
            result_value = None
            is_error = False
            error_message = ""

            try:
                await task_instance.pre_execution()
            except Exception as exc:
                is_error = True
                error_message = str(exc)
            else:
                try:
                    result_value = await task_instance.run(*args, **kwargs)
                except Exception as exc:
                    is_error = True
                    error_message = str(exc)
                else:
                    try:
                        await task_instance.post_execution()
                    except Exception as exc:
                        is_error = True
                        error_message = str(exc)
                        result_value = None

            # Build result dict
            result_data = {
                "task": {"id": task_id, "queue": queue, "name": task_name},
                "is_error": is_error,
                "error_message": error_message,
                "result": None if is_error else result_value,
            }

            # 7. Serialize result
            try:
                result_bytes = encode_result(result_data)
            except ResultSerializationError as exc:
                logger.error("Result serialization failed: %s", exc)
                result_data = {
                    "task": {"id": task_id, "queue": queue, "name": task_name},
                    "is_error": True,
                    "error_message": str(exc),
                    "result": None,
                }
                result_bytes = encode_result(result_data)

            # 8. Resolve result_ttl
            task_info = task_class.task_info
            result_ttl = task_info.result_ttl
            if result_ttl is None:
                result_ttl = self._config.result_ttl
            if result_ttl is None:
                result_ttl = 86400

            if result_ttl == 0:
                await self._broker.ack(message)
                return

            # 9-11. Store result + ack/nack
            try:
                await self._backend.store_result(
                    task_id, result_bytes, result_ttl * 1000
                )
            except BackendConnectionError:
                logger.error(
                    "Backend connection error storing result for task %s",
                    task_id,
                )
                await self._broker.nack(message, requeue=True)
                return

            await self._broker.ack(message)

        finally:
            # 12. Release semaphore
            self._semaphore.release()
