"""SwarmQ Client API."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable
from typing import Any

import structlog

from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import ResultNotReadyError, TaskError, TaskNotFoundError
from swarmq.middleware import MessageContext, MiddlewareChain
from swarmq.registry import TaskRegistry
from swarmq.serialization import decode_result, encode_json
from swarmq.signals import Signal, SignalDispatcher

logger = structlog.get_logger()


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
        middleware_chain: MiddlewareChain
        | Callable[[], MiddlewareChain]
        | None = None,
        signal_dispatcher: SignalDispatcher | None = None,
    ) -> None:
        self._broker = broker
        self._backend = backend
        self._registry = registry
        # Accept either a static MiddlewareChain (backward compat) or a
        # zero-arg factory callable that returns a fresh chain on each call.
        # Storing a factory lets us see middleware registered AFTER Client
        # construction — e.g. when SwarmQ.add_middleware() is called after
        # the Client has already been built.
        self._middleware_chain_factory: Callable[[], MiddlewareChain]
        if middleware_chain is None:
            empty = MiddlewareChain([])
            self._middleware_chain_factory = lambda: empty
        elif isinstance(middleware_chain, MiddlewareChain):
            static_chain = middleware_chain
            self._middleware_chain_factory = lambda: static_chain
        elif callable(middleware_chain):
            self._middleware_chain_factory = middleware_chain
        else:
            raise TypeError(
                "middleware_chain must be a MiddlewareChain, a zero-arg "
                "callable returning a MiddlewareChain, or None"
            )
        self._signal_dispatcher = (
            signal_dispatcher if signal_dispatcher is not None else SignalDispatcher()
        )

    def _get_chain(self) -> MiddlewareChain:
        """Return a live MiddlewareChain for the current schedule() call."""
        return self._middleware_chain_factory()

    def _resolve_queue_and_delivery_mode(
        self, task_name: str, queue: str | None
    ) -> tuple[str, str]:
        """Resolve the target queue and delivery mode for *task_name*.

        Uses the registered TaskInfo when available; falls back to the
        explicit *queue* argument and ``at_least_once`` delivery otherwise.
        Raises ``ValueError`` if the queue cannot be resolved.
        """
        resolved_queue = queue
        delivery_mode = "at_least_once"
        if self._registry is not None:
            entry = self._registry.get_entry(task_name)
            if resolved_queue is None:
                resolved_queue = entry.task_info.queues[0]
            delivery_mode = entry.task_info.delivery.value
        if resolved_queue is None:
            raise ValueError(
                "No queue specified and no registry available to resolve one"
            )
        return resolved_queue, delivery_mode

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

        # --- queue resolution chain + delivery_mode resolution ---
        resolved_queue, delivery_mode = self._resolve_queue_and_delivery_mode(
            task_name, queue
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

        # Canonical message_dict for middleware hooks (worker-agnostic schema).
        message_dict: MessageContext = {
            "task_id": task_id,
            "task_name": task_name,
            "queue": resolved_queue,
            "args": list(args),
            "kwargs": kwargs,
            "delivery_mode": delivery_mode,
        }

        # Resolve the middleware chain once per schedule() call — the factory
        # sees any middlewares registered after Client construction.
        chain = self._get_chain()

        # pre_enqueue exceptions PROPAGATE — they can prevent the enqueue.
        await chain.run_pre_enqueue(message_dict)

        await self._broker.publish(resolved_queue, encode_json(message))

        # post_enqueue + TASK_ENQUEUED signal only after successful publish.
        await chain.run_post_enqueue(message_dict)
        await self._signal_dispatcher.dispatch(
            Signal.TASK_ENQUEUED,
            task_id=task_id,
            task_name=task_name,
            queue=resolved_queue,
        )
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

        # --- Step 1: Subscribe first (before initial check to close race window) ---
        channel = f"sq:notify:{task_id}"
        pubsub = self._backend.create_pubsub()
        try:
            await pubsub.subscribe(channel)
        except Exception:
            pubsub = None  # Pub/Sub unavailable, fall through to polling

        try:
            # --- Step 2: Initial check (result may already be in Valkey) ---
            raw = await self._backend.get_result(task_id)
            if raw is not None:
                return self._unpack(raw)

            # --- Step 3: Pub/Sub wait (subscription is already active) ---
            if pubsub is not None:
                remaining = deadline - loop.time()
                pubsub_timeout = min(1.0, remaining) if remaining > 0 else 0
                if pubsub_timeout > 0:
                    pub_deadline = loop.time() + pubsub_timeout
                    while loop.time() < pub_deadline:
                        try:
                            msg = await pubsub.get_message(
                                ignore_subscribe_messages=True, timeout=0.05,
                            )
                            if msg is not None:
                                raw = await self._backend.get_result(task_id)
                                if raw is not None:
                                    return self._unpack(raw)
                        except Exception:
                            break  # Pub/Sub error, fall through to polling

            # --- Step 4: Polling fallback ---
            while loop.time() < deadline:
                raw = await self._backend.get_result(task_id)
                if raw is not None:
                    return self._unpack(raw)
                await asyncio.sleep(_FALLBACK_POLL)

            raise ResultNotReadyError(task_id)
        finally:
            if pubsub is not None:
                try:
                    await pubsub.unsubscribe(channel)
                    await pubsub.aclose()
                except Exception:
                    pass

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
        message_dicts: list[MessageContext] = []
        encoded_messages: list[bytes] = []
        resolved_queues: list[str] = []
        task_names: list[str] = []

        # Resolve the middleware chain once for this whole batch so that
        # every pre_enqueue/post_enqueue hook sees the same chain instance.
        chain = self._get_chain()

        # Pass 1: validate, resolve queue, build message_dicts + encoded
        # payloads. pre_enqueue is invoked sequentially; a raise aborts the
        # whole batch BEFORE any broker.publish coroutine is created or
        # awaited.
        for task_name, args, kwargs in tasks:
            # --- validation (registry mode only) ---
            if self._registry is not None:
                if not self._registry.has(task_name):
                    raise TaskNotFoundError(task_name)

            # --- queue resolution chain + delivery_mode resolution ---
            resolved_queue, delivery_mode = self._resolve_queue_and_delivery_mode(
                task_name, queue
            )

            task_id = str(uuid.uuid7())
            task_ids.append(task_id)
            task_names.append(task_name)
            resolved_queues.append(resolved_queue)

            message = {
                "task": {
                    "id": task_id,
                    "queue": resolved_queue,
                    "name": task_name,
                },
                "args": list(args),
                "kwargs": kwargs,
            }
            encoded_messages.append(encode_json(message))

            message_dict: MessageContext = {
                "task_id": task_id,
                "task_name": task_name,
                "queue": resolved_queue,
                "args": list(args),
                "kwargs": kwargs,
                "delivery_mode": delivery_mode,
            }
            message_dicts.append(message_dict)

            # pre_enqueue: exception aborts the whole batch (propagate).
            # Coroutines for broker.publish are only created in Pass 2, so
            # nothing needs cleanup if this raises.
            await chain.run_pre_enqueue(message_dict)

        # Pass 2: publish concurrently.
        coros = [
            self._broker.publish(resolved_queues[i], encoded_messages[i])
            for i in range(len(tasks))
        ]
        results = await asyncio.gather(*coros, return_exceptions=True)
        errors = [
            (i, r) for i, r in enumerate(results) if isinstance(r, BaseException)
        ]
        if errors:
            # Log partially-submitted task_ids so they can be recovered
            submitted = [
                task_ids[i] for i, r in enumerate(results)
                if not isinstance(r, BaseException)
            ]
            if submitted:
                logger.warning(
                    "schedule_many_partial_failure",
                    submitted_task_ids=submitted,
                    failed_count=len(errors),
                )
            # Run post_enqueue + TASK_ENQUEUED only for the successful ones
            # before raising, so observers aren't left with a half-emitted state.
            for i, r in enumerate(results):
                if isinstance(r, BaseException):
                    continue
                await chain.run_post_enqueue(message_dicts[i])
                await self._signal_dispatcher.dispatch(
                    Signal.TASK_ENQUEUED,
                    task_id=task_ids[i],
                    task_name=task_names[i],
                    queue=resolved_queues[i],
                )
            raise errors[0][1]

        # Pass 3: post_enqueue + TASK_ENQUEUED for each task after gather
        # succeeded.
        for i, message_dict in enumerate(message_dicts):
            await chain.run_post_enqueue(message_dict)
            await self._signal_dispatcher.dispatch(
                Signal.TASK_ENQUEUED,
                task_id=task_ids[i],
                task_name=task_names[i],
                queue=resolved_queues[i],
            )

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
        raw_list: list[bytes | None] = [None] * len(task_ids)

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
