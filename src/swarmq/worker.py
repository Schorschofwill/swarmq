"""SwarmQ Worker — consumer loop that executes tasks."""

from __future__ import annotations

import asyncio
import os
import sys

import structlog

from swarmq.backend.valkey import ValkeyBackend
from swarmq.backoff import calculate_backoff
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.config import SwarmQConfig
from swarmq.exceptions import (
    BackendConnectionError,
    NoRetry,
    ResultSerializationError,
    Retry,
    TaskNotFoundError,
)
from swarmq.logging import sanitize_url
from swarmq.registry import TaskRegistry
from swarmq.serialization import decode_json, encode_result
from swarmq.task import DeliveryMode

logger = structlog.get_logger()


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

        logger.info(
            "worker_started",
            pid=os.getpid(),
            python_version=sys.version,
            queues=self._config.queues,
            concurrency=self._config.concurrency,
            registered_tasks=sorted(self._registry.get_all()),
            broker_url=sanitize_url(self._config.broker_url),
            backend_url=sanitize_url(self._config.backend_url),
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
            logger.error("deserialize_failed", error=str(exc))
            await self._broker.nack(message, requeue=False)
            return

        # Bind structured context for all log calls during this task
        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(
            task_id=task_id,
            task_name=task_name,
            queue=queue,
            worker_id=id(self),
        )

        # 3. Acquire semaphore
        await self._semaphore.acquire()
        try:
            # 4-5. Look up task in registry
            try:
                task_class = self._registry.get(task_name)
            except TaskNotFoundError:
                logger.error("task_not_found")
                await self._broker.nack(message, requeue=False)
                return

            # 5b. FIRE_AND_FORGET: ack immediately before execution
            task_info = task_class.task_info
            if task_info.delivery is DeliveryMode.FIRE_AND_FORGET:
                await self._broker.ack(message)

            # 6. Create task instance, run lifecycle
            task_instance = task_class()
            result_value = None
            is_error = False
            error_message = ""
            task_exception = None

            try:
                await task_instance.pre_execution()
            except Exception as exc:
                is_error = True
                error_message = str(exc)
                task_exception = exc
                if task_info.delivery is DeliveryMode.FIRE_AND_FORGET:
                    logger.error("fire_and_forget_error", error=error_message)
            else:
                try:
                    result_value = await task_instance.run(*args, **kwargs)
                except Exception as exc:
                    is_error = True
                    error_message = str(exc)
                    task_exception = exc
                    if task_info.delivery is DeliveryMode.FIRE_AND_FORGET:
                        logger.error("fire_and_forget_error", error=error_message)
                else:
                    try:
                        await task_instance.post_execution()
                    except Exception as exc:
                        is_error = True
                        error_message = str(exc)
                        task_exception = exc
                        result_value = None
                        if task_info.delivery is DeliveryMode.FIRE_AND_FORGET:
                            logger.error("fire_and_forget_error", error=error_message)

            # For FIRE_AND_FORGET with errors: skip result/retry pipeline
            if task_info.delivery is DeliveryMode.FIRE_AND_FORGET and is_error:
                return

            # Retry logic (AT_LEAST_ONCE only)
            if (
                is_error
                and task_exception is not None
                and task_info.delivery is DeliveryMode.AT_LEAST_ONCE
                and task_info.max_retries > 0
            ):
                should_retry = await self._evaluate_retry(
                    message, task_info, task_exception, queue, error_message
                )
                if should_retry:
                    return

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
                logger.error("result_serialization_failed", error=str(exc))
                result_data = {
                    "task": {"id": task_id, "queue": queue, "name": task_name},
                    "is_error": True,
                    "error_message": str(exc),
                    "result": None,
                }
                result_bytes = encode_result(result_data)

            # 8. Resolve result_ttl
            result_ttl = task_info.result_ttl
            if result_ttl is None:
                if task_info.delivery is DeliveryMode.FIRE_AND_FORGET:
                    result_ttl = 0
                else:
                    result_ttl = self._config.result_ttl
            if result_ttl is None:
                result_ttl = 86400

            if result_ttl == 0:
                if task_info.delivery is DeliveryMode.AT_LEAST_ONCE:
                    await self._broker.ack(message)
                return

            # 9-11. Store result + ack/nack
            try:
                await self._backend.store_result(
                    task_id, result_bytes, result_ttl * 1000
                )
            except BackendConnectionError:
                logger.error("backend_connection_error")
                if task_info.delivery is DeliveryMode.AT_LEAST_ONCE:
                    await self._broker.nack(message, requeue=True)
                return

            if task_info.delivery is DeliveryMode.AT_LEAST_ONCE:
                await self._broker.ack(message)

        finally:
            # 12. Release semaphore
            self._semaphore.release()
            structlog.contextvars.clear_contextvars()

    async def _evaluate_retry(
        self,
        message,
        task_info,
        exc: Exception,
        queue: str,
        error_message: str,
    ) -> bool:
        """Evaluate whether to retry a failed task.

        Returns True if the message was handled (retried or sent to DLQ),
        meaning the caller should return early. Returns False if the
        caller should proceed with normal error-result storage.
        """
        # 1. NoRetry -> direct DLQ
        if isinstance(exc, NoRetry):
            await self._send_to_dlq(message, queue, error_message)
            return True

        # 2. Check retry_exceptions filter
        if task_info.retry_exceptions is not None:
            if not isinstance(exc, task_info.retry_exceptions):
                await self._send_to_dlq(message, queue, error_message)
                return True

        # 3. Read retry count from message headers
        retry_count = message.headers.get("x-swarmq-retry-count", 0)

        # 4. Check if retries exhausted
        if retry_count >= task_info.max_retries:
            await self._send_to_dlq(message, queue, error_message)
            return True

        # 5. Calculate delay
        if isinstance(exc, Retry) and exc.delay is not None:
            delay_seconds = exc.delay
        else:
            delay_seconds = calculate_backoff(
                retry_count,
                task_info.retry_backoff,
                task_info.retry_base_delay,
                self._config.retry_max_delay,
            )

        delay_ms = int(delay_seconds * 1000)

        # 6. Publish to delay queue with incremented retry count
        headers = {
            "x-swarmq-retry-count": retry_count + 1,
            "x-swarmq-traceback": error_message,
        }
        await self._broker.publish_to_delay_queue(
            queue, message.body, delay_ms=delay_ms, headers=headers
        )
        await self._broker.ack(message)
        logger.info(
            "task_retrying",
            retry_count=retry_count + 1,
            max_retries=task_info.max_retries,
            delay_ms=delay_ms,
        )
        return True

    async def _send_to_dlq(
        self,
        message,
        queue: str,
        error_message: str,
    ) -> None:
        """Store error result, publish to .XQ, and ack original message."""
        headers = {
            "x-swarmq-traceback": error_message,
        }
        await self._broker.publish_to_dead_letter_queue(
            queue, message.body, headers=headers
        )
        await self._broker.ack(message)
        logger.warning("task_sent_to_dlq", error=error_message)
