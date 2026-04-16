"""SwarmQ Worker — consumer loop that executes tasks."""

from __future__ import annotations

import asyncio
import enum
import inspect
import os
import signal
import sys
import time
import uuid
from collections.abc import Callable

import aio_pika
import structlog

from swarmq.backend.valkey import ValkeyBackend
from swarmq.backoff import calculate_backoff
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.config import SwarmQConfig
from swarmq.limits import RateLimiter, parse_rate_limit
from swarmq.locking import LockManager, compute_lock_ttl_ms

# Lock-conflict policy. Module-level so they sit alongside other constants
# and can be referenced from tests without a class qualifier. Mirrors the
# locking.py module-level _MIN_LOCK_TTL_SECONDS pattern.
#
# DLQ_CAP: when conflict_count reaches this value (inclusive), the message
# routes to DLQ instead of being requeued. Operator inspects the
# x-swarmq-lock-name DLQ header and runs `valkey-cli DEL sq:lock:<name>`
# to recover from a wedged holder.
LOCK_CONFLICT_DLQ_CAP = 10
# Backoff cap for lock-conflict requeue: 100 ms * 2 ** (count-1), capped at
# 60 s so an exponential ramp doesn't run away. The cap is reached around
# the 9th conflict (100ms * 2**8 ≈ 25.6s, then 51s, then DLQ at 10).
LOCK_CONFLICT_BACKOFF_BASE_MS = 100
LOCK_CONFLICT_BACKOFF_CAP_MS = 60_000

# Type alias for lock-tracking entries: (lock_name, owner_token, max_concurrency).
# Used by the per-handler acquired_locks list and the worker-wide
# _active_handler_locks registry. Single source of truth for the field
# ordering so destructuring sites are self-documenting.
_LockEntry = tuple[str, str, int]
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
        # RateLimiter needs an already-connected Valkey client; instantiated
        # in start() after backend.connect(). See plan 005 Unit 5 Lifecycle.
        self._rate_limiter: RateLimiter | None = None
        # LockManager has the same lifecycle as RateLimiter — instantiated
        # in start() once the Valkey client is live. See plan 008 Unit 4.
        self._lock_manager: LockManager | None = None
        # Backup registry for graceful-shutdown lock cleanup. Keyed by a
        # per-_handle_message UUID (NOT task_id, which can collide under
        # AT_LEAST_ONCE redelivery within the same worker). Values are the
        # list of _LockEntry tuples currently held by that handler invocation.
        # _release_acquired_locks pops the entry; _execute_shutdown drains
        # anything still here.
        self._active_handler_locks: dict[uuid.UUID, list[_LockEntry]] = {}

    def _get_chain(self) -> MiddlewareChain:
        """Return a live MiddlewareChain for the current message."""
        return self._middleware_chain_factory()

    async def _try_apply_rate_limit(
        self,
        message: aio_pika.abc.AbstractIncomingMessage,
        task_info: TaskInfo,
        queue: str,
        task_id: str,
        task_name: str,
    ) -> bool:
        """Decide whether the message must be requeued for rate-limit reasons.

        Returns ``True`` when the message was rate-limited and handled
        (republished into the delay queue and acknowledged), so the
        caller must stop processing it. Returns ``False`` when the
        message should continue through the normal pipeline (no limit
        configured, limiter not initialised, or the call was allowed).

        The whole sequence — Valkey check, signal/log, broker republish,
        ack — runs inside a single ``try`` so that a Valkey or broker
        failure cannot leave the message in a half-processed state.
        Mirrors ``_evaluate_retry``'s try/except contract around
        ``publish_to_delay_queue``.
        """
        if task_info.rate_limit is None or self._rate_limiter is None:
            return False

        config = parse_rate_limit(task_info.rate_limit)
        try:
            allowed, remaining, retry_after_ms = await self._rate_limiter.check(
                task_name, config
            )
        except Exception as exc:
            # The limiter is the only side-effect-free decision-point we
            # have. If Valkey is unreachable we cannot decide safely;
            # nack(requeue=True) so the broker hands the message to the
            # next worker that may have a healthier client. Important:
            # this runs BEFORE any FIRE_AND_FORGET ack — no data loss.
            logger.error(
                "rate_limit_check_failed",
                task_id=task_id,
                task_name=task_name,
                error=str(exc),
            )
            await self._broker.nack(message, requeue=True)
            return True

        if allowed:
            return False

        # Rate-limited. Track how often this exact message has bounced
        # so operators (and TASK_RATE_LIMITED handlers) can see when a
        # task has been stuck in the limiter loop. We don't auto-DLQ
        # here — that decision belongs to the operator and is documented
        # as a Non-Goal in plan 005. The header is the visible knob.
        original_headers = dict(message.headers) if message.headers else {}
        rl_count = int(original_headers.get("x-swarmq-rate-limit-count", 0)) + 1
        original_headers["x-swarmq-rate-limit-count"] = rl_count

        logger.info(
            "task_rate_limited",
            task_id=task_id,
            task_name=task_name,
            retry_after_ms=retry_after_ms,
            remaining=remaining,
            rate_limit_count=rl_count,
        )
        await self._signal_dispatcher.dispatch(
            Signal.TASK_RATE_LIMITED,
            task_id=task_id,
            task_name=task_name,
            retry_after_ms=retry_after_ms,
            remaining=remaining,
            rate_limit_count=rl_count,
        )

        try:
            await self._broker.publish_to_delay_queue(
                queue,
                message.body,
                delay_ms=retry_after_ms,
                headers=original_headers,
            )
            await self._broker.ack(message)
        except Exception as exc:
            # Broker failure between publish and ack would create a
            # duplicate (republished + redelivered original). nack with
            # requeue=True keeps the original in flight for retry; the
            # delay-queue copy may still arrive but the rate-limit check
            # on the next consume cycle will naturally serialize them.
            logger.error(
                "rate_limit_requeue_failed",
                task_id=task_id,
                task_name=task_name,
                error=str(exc),
            )
            await self._broker.nack(message, requeue=True)

        return True

    @staticmethod
    def _render_lock_name(
        lock_template: str,
        task_class: type,
        args: list,
        kwargs: dict,
    ) -> str:
        """Resolve format placeholders in ``lock_template`` against the
        task's run-method signature so positional and keyword args can
        both fill ``"user-{user_id}"``-style templates.

        Raises ``ValueError`` (with a wrapped TypeError / KeyError as
        cause) on render failure — caller routes the message to DLQ
        with an explanatory header. Plain non-format strings short-
        circuit before any inspect work, keeping the common case cheap.
        """
        if "{" not in lock_template:
            return lock_template
        try:
            bound = inspect.signature(task_class.run).bind(
                None, *args, **kwargs
            ).arguments
            # Drop the first parameter ('self') so {self} placeholders
            # don't accidentally pull in the bound None.
            bound.pop("self", None)
            return lock_template.format_map(bound)
        except (TypeError, KeyError) as exc:
            raise ValueError(
                f"failed to render lock name {lock_template!r}: {exc}"
            ) from exc

    async def _try_apply_lock(
        self,
        message: aio_pika.abc.AbstractIncomingMessage,
        task_info: TaskInfo,
        task_class: type,
        queue: str,
        task_id: str,
        task_name: str,
        args: list,
        kwargs: dict,
        handler_id: uuid.UUID,
        acquired_locks: list[_LockEntry],
    ) -> bool:
        """Decide whether the message must be requeued for lock-conflict
        reasons. Mirrors :meth:`_try_apply_rate_limit` shape — the four
        mandatory rules from
        ``docs/solutions/.../worker-pipeline-stage-ack-ordering-2026-04-16.md``:

        1. Runs BEFORE the FIRE_AND_FORGET ack.
        2. Whole decision-and-handoff sequence wrapped in try/except,
           mirroring ``_evaluate_retry``.
        3. Per-stage requeue counter on the message header
           (``x-swarmq-lock-conflict-count``) plus diagnostic
           ``x-swarmq-lock-name`` so operators can DEL the wedged key.
        4. Single helper returning bool — True = handled, False = continue.

        On successful acquire, the (lock_name, owner_token,
        max_concurrency) tuple is appended to ``acquired_locks`` (the
        per-handler list) AND mirrored into ``_active_handler_locks``
        (the worker-wide registry used by graceful-shutdown cleanup).
        Both writes happen inside an ``asyncio.shield`` so a
        ``CancelledError`` between Lua-acquire and registration cannot
        leave a held lock untracked.
        """
        if task_info.lock is None or self._lock_manager is None:
            return False

        # Render the lock name once. A render failure is a user
        # configuration bug — route to DLQ with a clear header, do NOT
        # requeue (it would just bounce until the cap and then DLQ
        # anyway, but with a less informative diagnostic).
        try:
            lock_name = self._render_lock_name(
                task_info.lock, task_class, args, kwargs
            )
        except ValueError as exc:
            logger.error(
                "lock_name_render_failed",
                task_id=task_id,
                task_name=task_name,
                lock_template=task_info.lock,
                error=str(exc),
            )
            original_headers = dict(message.headers) if message.headers else {}
            original_headers["x-swarmq-lock-render-error"] = str(exc)
            # Mirror the rate-limit/_evaluate_retry try/except contract:
            # broker fault between publish and ack must NOT loop-bounce a
            # message whose render error is permanent — the next delivery
            # would render-fail again, hit the DLQ-publish failure again,
            # and bounce forever (same shape as ADV-002 at the conflict cap).
            # nack(requeue=False) routes the message via the broker-side
            # x-dead-letter-exchange config (see RabbitMQBroker.declare_queue)
            # to {queue}.XQ — the message is NOT lost, but the diagnostic
            # x-swarmq-lock-render-error header that this code path tried
            # to attach will be missing. Operator inspecting the DLQ sees
            # only the original headers + RabbitMQ's x-death info.
            try:
                await self._broker.publish_to_dead_letter_queue(
                    queue, message.body, headers=original_headers,
                )
                await self._broker.ack(message)
            except Exception as broker_exc:
                logger.error(
                    "lock_render_dlq_failed",
                    task_id=task_id,
                    task_name=task_name,
                    lock_template=task_info.lock,
                    error=str(broker_exc),
                    note="message routed via broker DLX to "
                         f"{queue}.XQ without diagnostic headers",
                )
                await self._broker.nack(message, requeue=False)
            return True

        # Empty-rendered lock guard. TaskInfo rejects literal lock="" and
        # whitespace-only at construction, but a format-string template
        # like lock="{var}" with var="" renders to "" at runtime. Without
        # this check, lock_key("") returns "sq:lock:" — the bare namespace
        # prefix — which silently collides with every other empty-rendered
        # lock in the system. Same DLX-via-nack pattern as the render-fail
        # branch above.
        if not lock_name.strip():
            logger.error(
                "lock_name_rendered_empty",
                task_id=task_id,
                task_name=task_name,
                lock_template=task_info.lock,
            )
            original_headers = dict(message.headers) if message.headers else {}
            original_headers["x-swarmq-lock-render-error"] = (
                f"lock template {task_info.lock!r} rendered to empty string"
            )
            try:
                await self._broker.publish_to_dead_letter_queue(
                    queue, message.body, headers=original_headers,
                )
                await self._broker.ack(message)
            except Exception as broker_exc:
                logger.error(
                    "lock_empty_render_dlq_failed",
                    task_id=task_id,
                    task_name=task_name,
                    error=str(broker_exc),
                    note="message routed via broker DLX to "
                         f"{queue}.XQ without diagnostic headers",
                )
                await self._broker.nack(message, requeue=False)
            return True

        owner_token = uuid.uuid4().hex
        ttl_ms = compute_lock_ttl_ms(task_info.hard_timeout)

        # Acquire + register run inside asyncio.shield so a cancellation
        # between successful Lua-acquire and the in-memory registration
        # cannot leave the lock held with no Python record. The shield
        # itself cannot block KeyboardInterrupt, but SIGTERM goes through
        # the graceful-shutdown path which drains active tasks first.
        try:
            acquired = await asyncio.shield(
                self._acquire_and_register(
                    lock_name=lock_name,
                    owner_token=owner_token,
                    ttl_ms=ttl_ms,
                    max_concurrency=task_info.max_concurrency,
                    handler_id=handler_id,
                    acquired_locks=acquired_locks,
                )
            )
        except Exception as exc:
            # Same shape as rate-limit: BackendConnectionError or any
            # other failure means we cannot decide safely. nack(requeue)
            # so the broker hands the message to the next worker. Runs
            # BEFORE FIRE_AND_FORGET ack — no data loss.
            logger.error(
                "lock_acquire_failed",
                task_id=task_id,
                task_name=task_name,
                lock_name=lock_name,
                error=str(exc),
            )
            await self._broker.nack(message, requeue=True)
            return True

        if acquired:
            return False

        # Conflict: lock held by someone else (mutex) or semaphore full.
        # Track how often this exact message has bounced so operators can
        # see a wedged lock. Plan 005 set the precedent — visibility-first,
        # auto-DLQ at the cap is the safety net.
        original_headers = dict(message.headers) if message.headers else {}
        prior_count = int(original_headers.get("x-swarmq-lock-conflict-count", 0))
        new_count = prior_count + 1
        original_headers["x-swarmq-lock-conflict-count"] = new_count
        # Wedged-lock recovery: operator reads this header from the DLQ
        # message and runs `valkey-cli DEL sq:lock:<lock_name>`.
        original_headers["x-swarmq-lock-name"] = lock_name

        logger.info(
            "task_lock_conflict",
            task_id=task_id,
            task_name=task_name,
            lock_name=lock_name,
            lock_conflict_count=new_count,
        )
        await self._signal_dispatcher.dispatch(
            Signal.TASK_LOCKED,
            task_id=task_id,
            task_name=task_name,
            lock_name=lock_name,
            lock_conflict_count=new_count,
        )

        # At the cap, route to DLQ instead of requeuing — operator must
        # intervene (typically: DEL the wedged Valkey key, then republish
        # the DLQ message). This caps the bounce window; without it a
        # broken upstream could consume queue capacity indefinitely.
        # Comparison uses >= so the cap is inclusive: cap=10 means the 10th
        # conflict routes to DLQ (matches the brief's "cap 10 → DLQ"; the
        # earlier > was off-by-one and let conflict 10 requeue).
        if new_count >= LOCK_CONFLICT_DLQ_CAP:
            try:
                await self._broker.publish_to_dead_letter_queue(
                    queue, message.body, headers=original_headers,
                )
                await self._broker.ack(message)
            except Exception as exc:
                # DLQ-publish failure at the cap creates a hazard: nacking
                # with requeue lets the message bounce back at count >= cap
                # again, hit the same DLQ failure, and loop without backoff
                # (the requeue branch below is unreachable past the cap).
                # nack(requeue=False) routes the message via the broker-side
                # x-dead-letter-exchange config (see RabbitMQBroker.declare_queue)
                # to {queue}.XQ — the message is NOT lost, but the diagnostic
                # headers (x-swarmq-lock-name, x-swarmq-lock-conflict-count)
                # that this branch tried to attach will be missing. Operator
                # sees only the original headers + RabbitMQ's x-death info.
                logger.error(
                    "lock_conflict_dlq_failed",
                    task_id=task_id,
                    task_name=task_name,
                    lock_name=lock_name,
                    lock_conflict_count=new_count,
                    error=str(exc),
                    note=f"message routed via broker DLX to {queue}.XQ "
                         "without diagnostic headers; recover wedged lock "
                         f"via valkey-cli DEL sq:lock:{lock_name}",
                )
                await self._broker.nack(message, requeue=False)
            return True

        delay_ms = min(
            LOCK_CONFLICT_BACKOFF_BASE_MS * (2 ** (new_count - 1)),
            LOCK_CONFLICT_BACKOFF_CAP_MS,
        )

        try:
            await self._broker.publish_to_delay_queue(
                queue,
                message.body,
                delay_ms=delay_ms,
                headers=original_headers,
            )
            await self._broker.ack(message)
        except Exception as exc:
            # Same try/except shape as _evaluate_retry / rate-limit:
            # broker failure between publish and ack means the message
            # might double-deliver. nack(requeue=True) keeps the original
            # in flight; the next acquire will naturally serialize.
            logger.error(
                "lock_conflict_requeue_failed",
                task_id=task_id,
                task_name=task_name,
                lock_name=lock_name,
                error=str(exc),
            )
            await self._broker.nack(message, requeue=True)

        return True

    async def _acquire_and_register(
        self,
        *,
        lock_name: str,
        owner_token: str,
        ttl_ms: int,
        max_concurrency: int,
        handler_id: uuid.UUID,
        acquired_locks: list[_LockEntry],
    ) -> bool:
        """Atomic-ish acquire + dual-register helper for asyncio.shield.

        Calls ``LockManager.acquire``; on success appends to both the
        per-handler list and the worker-wide registry. The two writes
        are sync (list.append + dict assignment) so they cannot raise
        between them — the only failure mode is the acquire itself.
        """
        acquired = await self._lock_manager.acquire(
            lock_name, owner_token, ttl_ms, max_concurrency
        )
        if acquired:
            entry = (lock_name, owner_token, max_concurrency)
            acquired_locks.append(entry)
            self._active_handler_locks.setdefault(handler_id, []).append(entry)
        return acquired

    async def _release_acquired_locks(
        self,
        handler_id: uuid.UUID,
        acquired_locks: list[_LockEntry],
    ) -> None:
        """Release every lock acquired by this handler. Called in the
        ``_handle_message`` finally block. Failures are logged and
        swallowed — the task already produced its result; release
        problems must not break the worker pipeline. The TTL-recovery
        path is the safety net for any lock that ends up not released.
        """
        for lock_name, owner_token, max_concurrency in acquired_locks:
            try:
                await self._lock_manager.release(
                    lock_name, owner_token, max_concurrency
                )
            except Exception as exc:
                logger.error(
                    "lock_release_failed",
                    lock_name=lock_name,
                    error=str(exc),
                )
        # Drop the worker-wide registry entry whether release succeeded
        # or not — anything still held will time out via TTL, and we
        # don't want shutdown-cleanup to double-release.
        self._active_handler_locks.pop(handler_id, None)

    async def start(self) -> None:
        """Connect broker + backend, declare queues, start consuming, block until shutdown."""
        await self._broker.connect()
        await self._backend.connect()

        # Backend._client is available only after connect(). Instantiate
        # the rate limiter here so the registered Lua script uses the
        # live client.
        self._rate_limiter = RateLimiter(self._backend._client)
        # LockManager has the same lifecycle constraint — registered
        # AsyncScripts need a live client. See plan 008 Unit 4.
        self._lock_manager = LockManager(self._backend._client)

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

        # 3b. Release any leftover handler locks. Runs AFTER the
        # asyncio.wait above (so cancelled tasks have had a chance to
        # finish their finally and release their locks themselves) but
        # BEFORE backend.close (so LockManager.release still has a
        # working Valkey client). Whatever's still in the registry
        # corresponds to a handler whose finally never ran (typically
        # because the task was cancel()ed before reaching it).
        if self._lock_manager is not None and self._active_handler_locks:
            for handler_id, locks in list(self._active_handler_locks.items()):
                for lock_name, owner_token, max_concurrency in locks:
                    try:
                        await self._lock_manager.release(
                            lock_name, owner_token, max_concurrency
                        )
                    except Exception as exc:
                        logger.error(
                            "shutdown_lock_release_failed",
                            lock_name=lock_name,
                            error=str(exc),
                        )
            self._active_handler_locks.clear()

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

    async def _pre_start_cancel_check(
        self,
        message,
        task_id: str,
        task_name: str,
        queue: str,
    ) -> bool:
        """Check whether this message has been cancelled before execution.

        Called at the earliest point in the pipeline — after deserialize,
        before semaphore acquire, rate-limit, lock, and FAF-ack.  If the
        cancel flag is set, stores an ``is_cancelled=True`` result
        (AT_LEAST_ONCE) or skips storage (FAF with result_ttl=0),
        dispatches TASK_CANCELLED, clears the flag, acks the message.

        The helper is responsible for its own structlog cleanup when it
        returns True: the main handler's finally is not reached on this
        branch.

        Returns:
            True when the cancel was handled (caller must return
            immediately). False when no cancel flag is set and normal
            execution should proceed.
        """
        try:
            is_cancelled = await self._backend.is_cancelled(task_id)
        except BackendConnectionError:
            logger.error("cancel_check_backend_error", task_id=task_id)
            await self._broker.nack(message, requeue=True)
            structlog.contextvars.clear_contextvars()
            return True

        if not is_cancelled:
            return False

        # Cancel flag is set. Resolve DeliveryMode + result_ttl from
        # registry (unknown tasks fall back to AT_LEAST_ONCE semantics so
        # the client's get_result does not hang forever waiting for a
        # result that will never arrive).
        try:
            task_class = self._registry.get(task_name)
            task_info = task_class.task_info
            delivery = task_info.delivery
            # result_ttl falls through: task → global → 86400.
            result_ttl = task_info.result_ttl
            if result_ttl is None:
                if delivery is DeliveryMode.FIRE_AND_FORGET:
                    result_ttl = 0
                else:
                    result_ttl = self._config.result_ttl
        except TaskNotFoundError:
            delivery = DeliveryMode.AT_LEAST_ONCE
            result_ttl = self._config.result_ttl

        try:
            if result_ttl > 0:
                result_data = {
                    "task": {"id": task_id, "queue": queue, "name": task_name},
                    "is_error": False,
                    "is_cancelled": True,
                    "error_message": "",
                    "result": None,
                }
                result_bytes = encode_result(result_data)
                await self._backend.store_result(
                    task_id, result_bytes, result_ttl * 1000
                )
                try:
                    await self._backend.publish_notification(task_id)
                except Exception as exc:
                    logger.warning(
                        "publish_notification_failed",
                        task_id=task_id,
                        error=str(exc),
                    )

            # Clear the flag (log+swallow at backend boundary — Unit 3).
            await self._backend.clear_cancel_flag(task_id)

            # Ack the message.
            await self._broker.ack(message)

        except BackendConnectionError as exc:
            # Result-write failure: nack(requeue=True) so a redelivery
            # re-runs this branch.  Flag stays in Valkey (that's what
            # triggers the re-cancel on redelivery).
            logger.error(
                "cancel_store_result_failed",
                task_id=task_id,
                error=str(exc),
            )
            await self._broker.nack(message, requeue=True)
            structlog.contextvars.clear_contextvars()
            return True

        # Signal AFTER the broker ack so a handler that raises doesn't
        # prevent the message from being committed.
        await self._signal_dispatcher.dispatch(
            Signal.TASK_CANCELLED,
            task_id=task_id,
            task_name=task_name,
        )

        logger.info("task_cancelled_pre_start", task_id=task_id, task_name=task_name)
        structlog.contextvars.clear_contextvars()
        return True

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

        # 2b. Pre-start cancel check (Phase 3b.3). Runs BEFORE semaphore
        # acquire so a cancelled message doesn't waste a slot, BEFORE
        # rate-limit and lock so those stages are not entered for
        # cancelled work, and BEFORE FIRE_AND_FORGET early-ack in the
        # main try/finally. The helper is responsible for its own
        # structlog cleanup on the cancel branch — the main finally at
        # the end of this method is not reached on this branch.
        if await self._pre_start_cancel_check(
            message, task_id, task_name, queue
        ):
            return

        # 3. Acquire semaphore
        await self._semaphore.acquire()
        # Per-handler lock-tracking state. handler_id is the registry key
        # for shutdown cleanup; acquired_locks is the local source of truth
        # the finally-block uses to release. Both are scoped to this
        # _handle_message invocation — task_id alone would collide under
        # AT_LEAST_ONCE redelivery within the same worker.
        handler_id = uuid.uuid4()
        acquired_locks: list[_LockEntry] = []
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

            task_info = task_class.task_info

            # 5b. Rate limit check — runs BEFORE the FIRE_AND_FORGET ack
            # so a Valkey or broker hiccup cannot drop a message that has
            # already been acked. Rate-limited tasks never reach
            # pre_execute or middleware: they are republished into the
            # delay queue and ack'd as a single transactional unit.
            if await self._try_apply_rate_limit(
                message, task_info, queue, task_id, task_name,
            ):
                return

            # 5b'. Lock check — same FIRE_AND_FORGET-ordering discipline as
            # rate-limit. Conflict path: requeue with exponential backoff
            # until the per-message cap, then DLQ. Acquired locks are
            # released in the finally below.
            if await self._try_apply_lock(
                message, task_info, task_class, queue, task_id, task_name,
                args, kwargs, handler_id, acquired_locks,
            ):
                return

            # 5c. FIRE_AND_FORGET: ack immediately before execution.
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
            # 12a. Release any task-locks acquired by this handler. Runs
            # BEFORE semaphore release so the central registry is clean
            # before the next concurrent handler starts. Failures here
            # are logged + swallowed — TTL is the recovery safety net.
            if acquired_locks and self._lock_manager is not None:
                await self._release_acquired_locks(handler_id, acquired_locks)
            # 12b. Release semaphore
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
