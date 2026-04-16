"""SwarmQ Worker — consumer loop that executes tasks."""

from __future__ import annotations

import asyncio
import enum
import os
import signal
import sys
import time
from collections.abc import Callable

import aio_pika
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
    SoftTimeoutError,
    TaskNotFoundError,
)
from swarmq.logging import sanitize_url
from swarmq.middleware import (
    MessageContext,
    MiddlewareChain,
    coerce_middleware_chain_factory,
)
from swarmq.registry import TaskRegistry
from swarmq.serialization import decode_json, encode_result
from swarmq.signals import Signal, SignalDispatcher
from swarmq.task import DeliveryMode, TaskInfo

logger = structlog.get_logger()


class WorkerState(enum.Enum):
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    DRAINING = "DRAINING"
    SHUTTING_DOWN = "SHUTTING_DOWN"
    STOPPED = "STOPPED"


class Worker:
    def __init__(
        self,
        broker: RabbitMQBroker,
        backend: ValkeyBackend,
        registry: TaskRegistry,
        config: SwarmQConfig,
        middleware_chain: MiddlewareChain
        | Callable[[], MiddlewareChain]
        | None = None,
        signal_dispatcher: SignalDispatcher | None = None,
    ) -> None:
        self._broker = broker
        self._backend = backend
        self._registry = registry
        self._config = config
        # Accept either a static MiddlewareChain (backward compat) or a
        # zero-arg factory callable that returns a fresh chain on each call.
        # Storing a factory lets the worker see middleware registered AFTER
        # Worker construction — e.g. when SwarmQ.add_middleware() is called
        # after start_worker() has already built the Worker instance.
        self._middleware_chain_factory = coerce_middleware_chain_factory(
            middleware_chain
        )
        self._signal_dispatcher = (
            signal_dispatcher if signal_dispatcher is not None else SignalDispatcher()
        )
        self._semaphore = asyncio.Semaphore(config.concurrency)
        self._active_tasks: set[asyncio.Task] = set()
        self._state = WorkerState.STARTING
        self._shutdown_event = asyncio.Event()

    def _get_chain(self) -> MiddlewareChain:
        """Return a live MiddlewareChain for the current message."""
        return self._middleware_chain_factory()

    async def start(self) -> None:
        """Connect broker + backend, declare queues, start consuming, block until shutdown."""
        await self._broker.connect()
        await self._backend.connect()

        for queue_name in self._config.queues:
            await self._broker.declare_queue(queue_name)
            await self._broker.consume(
                queue_name,
                self._on_message,
                prefetch_count=self._config.concurrency,
            )

        self._state = WorkerState.RUNNING

        # Register signal handlers for graceful shutdown
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGTERM, self._request_shutdown)
        loop.add_signal_handler(signal.SIGINT, self._request_shutdown)

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

        await self._signal_dispatcher.dispatch(
            Signal.WORKER_ONLINE, worker_id=str(id(self))
        )

        # Block until shutdown is requested
        await self._shutdown_event.wait()
        await self._execute_shutdown()

    async def stop(self) -> None:
        """Close broker and backend connections."""
        await self._broker.close()
        await self._backend.close()

    async def _execute_shutdown(self) -> None:
        """Execute the graceful shutdown sequence."""
        await self._signal_dispatcher.dispatch(
            Signal.WORKER_SHUTDOWN, worker_id=str(id(self))
        )

        # 1. DRAINING: stop accepting new messages
        await self._broker.cancel_consume()

        # 2. Wait for in-flight tasks to complete
        tasks_snapshot = set(self._active_tasks)
        if tasks_snapshot:
            _done, pending = await asyncio.wait(
                tasks_snapshot,
                timeout=self._config.shutdown_timeout,
            )

            # 3. SHUTTING_DOWN: cancel remaining tasks if drain timeout exceeded
            if pending:
                self._state = WorkerState.SHUTTING_DOWN
                for task in pending:
                    task.cancel()
                await asyncio.wait(pending, timeout=1)

        # 4. Close connections
        self._state = WorkerState.SHUTTING_DOWN
        await self._broker.close()
        await self._backend.close()

        # 5. STOPPED
        self._state = WorkerState.STOPPED

    def _request_shutdown(self) -> None:
        """Request a graceful shutdown. Idempotent — safe to call multiple times."""
        if self._state is not WorkerState.RUNNING:
            return
        self._state = WorkerState.DRAINING
        self._shutdown_event.set()

    async def _on_message(self, message) -> None:
        """Consume callback — spawns a task so aio-pika's dispatch loop is not blocked."""
        task = asyncio.create_task(self._handle_message(message))
        self._active_tasks.add(task)
        task.add_done_callback(self._active_tasks.discard)

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
            # 3b. Check if shutdown is in progress — requeue message
            if self._shutdown_event.is_set():
                await self._broker.nack(message, requeue=True)
                return

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

            # Build canonical message_dict for middleware + track retry_count
            retry_count = message.headers.get("x-swarmq-retry-count", 0)
            message_dict: MessageContext = {
                "task_id": task_id,
                "task_name": task_name,
                "queue": queue,
                "args": args,
                "kwargs": kwargs,
                "delivery_mode": task_info.delivery.value,
                "retry_count": retry_count,
            }

            # Resolve the middleware chain once per message so that any
            # add_middleware() calls made after Worker construction are
            # visible to every hook for this message. Chain-build cost is
            # sub-microsecond (list copy), negligible vs broker roundtrip.
            # A broken user-supplied factory must not crash the consumer
            # loop — nack without requeue so the message hits the DLQ.
            try:
                chain = self._get_chain()
            except Exception as exc:
                logger.error("middleware_chain_factory_failed", error=str(exc))
                await self._broker.nack(message, requeue=False)
                return

            # Pre-execute hooks
            await chain.run_pre_execute(message_dict)
            await self._signal_dispatcher.dispatch(
                Signal.TASK_STARTED, task_id=task_id, task_name=task_name
            )
            start_time = time.monotonic()

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
                run_coro = task_instance.run(*args, **kwargs)
                if task_info.soft_timeout is not None:
                    run_coro = asyncio.wait_for(
                        run_coro, timeout=task_info.soft_timeout
                    )

                run_task = asyncio.create_task(run_coro)

                watchdog = None
                try:
                    if task_info.hard_timeout is not None:
                        watchdog = asyncio.create_task(
                            self._hard_timeout_watchdog(
                                run_task, task_info.hard_timeout
                            )
                        )

                    try:
                        result_value = await run_task
                    except asyncio.TimeoutError:
                        soft_err = SoftTimeoutError(
                            f"soft timeout after {task_info.soft_timeout}s"
                        )
                        is_error = True
                        error_message = f"SoftTimeoutError: {soft_err}"
                        task_exception = soft_err
                        if task_info.delivery is DeliveryMode.FIRE_AND_FORGET:
                            logger.error(
                                "fire_and_forget_error", error=error_message
                            )
                    except asyncio.CancelledError:
                        hard_err_msg = (
                            f"hard timeout after {task_info.hard_timeout}s"
                        )
                        is_error = True
                        error_message = hard_err_msg
                        task_exception = RuntimeError(hard_err_msg)
                        if task_info.delivery is DeliveryMode.FIRE_AND_FORGET:
                            logger.error(
                                "fire_and_forget_error", error=error_message
                            )
                    except Exception as exc:
                        is_error = True
                        error_message = str(exc)
                        task_exception = exc
                        if task_info.delivery is DeliveryMode.FIRE_AND_FORGET:
                            logger.error(
                                "fire_and_forget_error", error=error_message
                            )
                    else:
                        try:
                            await task_instance.post_execution()
                        except Exception as exc:
                            is_error = True
                            error_message = str(exc)
                            task_exception = exc
                            result_value = None
                            if task_info.delivery is DeliveryMode.FIRE_AND_FORGET:
                                logger.error(
                                    "fire_and_forget_error", error=error_message
                                )
                finally:
                    # Outer finally: guarantees watchdog cleanup regardless of
                    # which path was taken (success, soft/hard timeout, or any
                    # other exception from run_task / post_execution). Awaiting
                    # the cancelled watchdog ensures it fully completes so no
                    # orphan task survives _handle_message.
                    if watchdog is not None and not watchdog.done():
                        watchdog.cancel()
                        try:
                            await watchdog
                        except asyncio.CancelledError:
                            pass

            # Emit post-execute / on-error hooks now that lifecycle is resolved
            duration_ms = (time.monotonic() - start_time) * 1000.0
            if is_error and task_exception is not None:
                await chain.run_on_error(
                    message_dict, exception=task_exception
                )
                await self._signal_dispatcher.dispatch(
                    Signal.TASK_FAILED,
                    task_id=task_id,
                    task_name=task_name,
                    exception=str(task_exception),
                )
            else:
                await chain.run_post_execute(
                    message_dict, result=result_value
                )
                await self._signal_dispatcher.dispatch(
                    Signal.TASK_COMPLETED,
                    task_id=task_id,
                    task_name=task_name,
                    duration_ms=duration_ms,
                )

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
                    message,
                    task_info,
                    task_exception,
                    queue,
                    task_id,
                    task_name,
                    error_message,
                    message_dict,
                    chain,
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

            try:
                await self._backend.publish_notification(task_id)
            except Exception as exc:
                logger.warning(
                    "publish_notification_failed",
                    error=str(exc),
                    task_id=task_id,
                )

            if task_info.delivery is DeliveryMode.AT_LEAST_ONCE:
                await self._broker.ack(message)

        finally:
            # 12. Release semaphore
            self._semaphore.release()
            structlog.contextvars.clear_contextvars()

    async def _hard_timeout_watchdog(
        self, run_task: asyncio.Task, timeout: float
    ) -> None:
        """Sleep for *timeout* seconds, then cancel *run_task*."""
        await asyncio.sleep(timeout)
        run_task.cancel()

    async def _evaluate_retry(
        self,
        message: aio_pika.abc.AbstractIncomingMessage,
        task_info: TaskInfo,
        exc: Exception,
        queue: str,
        task_id: str,
        task_name: str,
        error_message: str,
        message_dict: MessageContext,
        chain: MiddlewareChain,
    ) -> bool:
        """Evaluate whether to retry a failed task.

        Returns True if the message was handled (retried or sent to DLQ),
        meaning the caller should return early. Returns False if the
        caller should proceed with normal error-result storage.
        """
        # 1. NoRetry -> direct DLQ
        if isinstance(exc, NoRetry):
            await self._send_to_dlq(message, queue, task_id, task_name, error_message)
            return True

        # 2. Check retry_exceptions filter
        if task_info.retry_exceptions is not None:
            if not isinstance(exc, task_info.retry_exceptions):
                await self._send_to_dlq(message, queue, task_id, task_name, error_message)
                return True

        # 3. Read retry count from message headers
        retry_count = message.headers.get("x-swarmq-retry-count", 0)

        # 4. Check if retries exhausted
        if retry_count >= task_info.max_retries:
            await self._send_to_dlq(message, queue, task_id, task_name, error_message)
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

        # 5b. Middleware + signal hooks fire BEFORE publish_to_delay_queue.
        # Synchronize retry_count in message_dict with the on_retry parameter:
        # both represent the NEW attempt number (the retry we're about to do).
        message_dict["retry_count"] = retry_count + 1
        await chain.run_on_retry(
            message_dict,
            exception=exc,
            retry_count=retry_count + 1,
            delay=delay_seconds,
        )
        await self._signal_dispatcher.dispatch(
            Signal.TASK_RETRYING,
            task_id=task_id,
            retry_count=retry_count + 1,
            delay=delay_seconds,
        )

        # 6. Publish to delay queue with incremented retry count
        headers = {
            "x-swarmq-retry-count": retry_count + 1,
            "x-swarmq-traceback": error_message,
        }
        try:
            await self._broker.publish_to_delay_queue(
                queue, message.body, delay_ms=delay_ms, headers=headers
            )
            await self._broker.ack(message)
        except Exception as broker_exc:
            logger.error("retry_publish_failed", error=str(broker_exc))
            try:
                await self._broker.nack(message, requeue=True)
            except Exception:
                pass
            return True
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
        task_id: str,
        task_name: str,
        error_message: str,
    ) -> None:
        """Store error result in backend, publish to .XQ, and ack original message."""
        # Store error result so callers of get_result() get a response
        result_data = {
            "task": {"id": task_id, "queue": queue, "name": task_name},
            "is_error": True,
            "error_message": error_message,
            "result": None,
        }
        try:
            result_bytes = encode_result(result_data)
            result_ttl = self._config.result_ttl or 86400
            await self._backend.store_result(
                task_id, result_bytes, result_ttl * 1000
            )
            try:
                await self._backend.publish_notification(task_id)
            except Exception as exc:
                logger.warning(
                    "publish_notification_failed",
                    error=str(exc),
                    task_id=task_id,
                )
        except Exception as store_exc:
            logger.warning("dlq_result_store_failed", error=str(store_exc))

        headers = {
            "x-swarmq-traceback": error_message,
        }
        try:
            await self._broker.publish_to_dead_letter_queue(
                queue, message.body, headers=headers
            )
            await self._broker.ack(message)
        except Exception as broker_exc:
            logger.error("dlq_publish_failed", error=str(broker_exc))
            try:
                await self._broker.nack(message, requeue=True)
            except Exception:
                pass
        logger.warning("task_sent_to_dlq", error=error_message)
