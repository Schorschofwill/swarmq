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
from swarmq.headers import HEADER_ENQUEUED_AT, HEADER_UNIQUE_HASH, HEADER_UNIQUE_UNTIL
from swarmq.cancellation import (
    CANCEL_REASON_CANCEL,
    CANCEL_REASON_TIMEOUT,
    CancelReason,
    build_cancel_result_dict,
    build_error_result_dict,
    build_result_dict,
)
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
from swarmq.scheduler import ScheduleStore, SchedulerLoop
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
        # Embedded scheduler (Phase 3c). Lazy-constructed in start() after
        # the backend is connected — ScheduleStore's registered Lua
        # scripts need a live client. None means "this worker is not
        # running the scheduler loop" (only possible if explicitly
        # disabled; default is enabled).
        self._scheduler: SchedulerLoop | None = None
        # Schedfire in-flight claims. Keyed by id(message) because
        # aio_pika's IncomingMessage forbids dynamic attribute
        # assignment. Populated when _pre_start_schedfire_dedup wins
        # the inflight SETNX, read + popped by _mark_schedfire_completed
        # in the finally block (Unit 1 / H-2).
        self._schedfire_inflight_claims: dict[int, str] = {}
        # Backup registry for graceful-shutdown lock cleanup. Keyed by a
        # per-_handle_message UUID (NOT task_id, which can collide under
        # AT_LEAST_ONCE redelivery within the same worker). Values are the
        # list of _LockEntry tuples currently held by that handler invocation.
        # _release_acquired_locks pops the entry; _execute_shutdown drains
        # anything still here.
        self._active_handler_locks: dict[uuid.UUID, list[_LockEntry]] = {}
        # Phase 3b.3: detached lock-release tasks. The shield pattern
        # around _release_acquired_locks protects against run_task
        # cancellation (inner-cancel). Against graceful-shutdown
        # (outer-cancel), the shielded task is detached here so
        # _execute_shutdown can await it before closing the backend.
        # Without this set, the orphaned release would race backend.close
        # and likely fail with BackendConnectionError, leaving the lock
        # held until TTL.
        self._detached_releases: set[asyncio.Task] = set()
        # R4.5 Burst-Mode state. Initialised at start() time; the
        # monitor coroutine reads it every poll. monotonic() so wall-
        # clock jumps don't trigger a false-idle exit.
        self._last_message_received_at: float = 0.0
        self._burst_monitor_task: asyncio.Task | None = None

    def _get_chain(self) -> MiddlewareChain:
        """Return a live MiddlewareChain for the current message."""
        return self._middleware_chain_factory()

    async def _check_max_age_expired(
        self,
        message: "aio_pika.abc.AbstractIncomingMessage",
        task_info: TaskInfo,
        queue: str,
        task_id: str,
        task_name: str,
    ) -> bool:
        """R4.4 — route expired messages to the DLQ with TASK_EXPIRED.

        Returns True when the message was expired and handled; caller
        exits the main pipeline. Returns False on the happy path.

        ``max_age=None`` disables the check entirely. A missing
        ``x-swarmq-enqueued-at`` header is treated as 'not expired'
        (defensive — pre-R4.4 producers + external producers).
        """
        if task_info.max_age is None:
            return False
        headers = message.headers or {}
        enqueued_at = headers.get(HEADER_ENQUEUED_AT)
        if enqueued_at is None:
            return False
        try:
            enqueued_ts = int(enqueued_at)
        except (TypeError, ValueError):
            # Malformed header — treat as non-expiring rather than DLQ
            # on a header-shape bug. An expired task with a corrupt
            # header still gets MAX_RETRIES redeliveries + x-delivery-
            # limit as the fallback safety net.
            return False
        now = int(time.time())
        age = now - enqueued_ts
        if age < task_info.max_age:
            return False

        logger.warning(
            "task_expired",
            task_id=task_id,
            task_name=task_name,
            queue=queue,
            age_s=age,
            max_age_s=task_info.max_age,
        )
        await self._signal_dispatcher.dispatch(
            Signal.TASK_EXPIRED,
            task_id=task_id,
            task_name=task_name,
            queue=queue,
            age_s=age,
            max_age_s=task_info.max_age,
        )
        # Store an error result so callers waiting on get_result() see
        # a clean TaskError("expired") instead of an indefinite timeout.
        try:
            await self._backend.store_result(
                task_id,
                build_result_dict(
                    task_id=task_id,
                    queue=queue,
                    task_name=task_name,
                    is_error=True,
                    is_cancelled=False,
                    error_message=(
                        f"task expired: age {age}s exceeds "
                        f"max_age {task_info.max_age}s"
                    ),
                ),
                ttl_ms=(task_info.result_ttl or self._config.result_ttl) * 1000,
                op_timeout=self._config.backend_op_timeout,
            )
        except BackendConnectionError:
            # Best-effort: if the backend is down, the message still
            # routes to the DLQ for operator inspection.
            logger.warning("task_expired_result_store_failed")

        # Route to DLQ + ack. Same pattern as _send_to_dlq but inline
        # because the DLQ semantics of 'expired' differ from 'error'
        # (different signal, no retry-count bump).
        #
        # ce-review REL-02: guard the DLQ publish + ack pair. Without
        # this, a broker hiccup between store_result (succeeded above)
        # and the publish_to_dead_letter_queue call leaves the
        # TaskError('expired') result in Valkey while the original
        # message cycles through redelivery, generating duplicate
        # TASK_EXPIRED signals. nack(requeue=True) preserves the
        # message so a subsequent delivery retries the terminal path.
        try:
            await self._broker.publish_to_dead_letter_queue(
                queue, message.body, headers=dict(message.headers or {}),
            )
            await self._broker.ack(message)
        except BrokerConnectionError:
            logger.warning("task_expired_dlq_publish_failed")
            try:
                await self._broker.nack(message, requeue=True)
            except BrokerConnectionError:
                # Double-failure — let the message TTL / broker
                # redeliver-limit be the safety net.
                logger.error("task_expired_nack_also_failed")
        return True

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
                # Release AFTER successful ack (P1 #3 / ADV-001).
                await self._maybe_release_unique(message, task_id, task_info)
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
                # Fallback nack(requeue=False) still terminal (DLX
                # routes it); release after the nack to keep the
                # ack-ordering discipline uniform with the happy path.
                await self._broker.nack(message, requeue=False)
                await self._maybe_release_unique(message, task_id, task_info)
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
                await self._maybe_release_unique(message, task_id, task_info)
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
                await self._maybe_release_unique(message, task_id, task_info)
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
                await self._maybe_release_unique(message, task_id, task_info)
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
                # Terminal DLX fallback — nack first, then release.
                await self._broker.nack(message, requeue=False)
                await self._maybe_release_unique(message, task_id, task_info)
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

        # Embedded scheduler (Phase 3c). Also lazy-constructed here for
        # the same reason — ScheduleStore's registered sched_poll /
        # sched_commit scripts need the connected client. Starts both
        # the election and poll sub-coroutines; only the elected leader
        # will actually do work, standbys no-op.
        schedule_store = ScheduleStore(self._backend)
        self._scheduler = SchedulerLoop(
            backend=self._backend,
            broker=self._broker,
            config=self._config,
            lock_manager=self._lock_manager,
            store=schedule_store,
            signal_dispatcher=self._signal_dispatcher,
            worker_id=str(id(self)),
        )
        await self._scheduler.start()

        # ce-review COR-M-05: warn when a registered task declares
        # priority != "normal" but the worker is not configured for
        # priority routing. Without the warning, a publish to
        # {queue}.high is silently dropped into a queue nobody
        # consumes and get_result() times out with no diagnostic.
        if not self._config.priorities:
            priority_tasks: list[str] = []
            for task_name in self._registry.get_all():
                try:
                    ti = self._registry.get(task_name).task_info
                except Exception:  # noqa: BLE001
                    continue
                if getattr(ti, "priority", "normal") != "normal":
                    priority_tasks.append(f"{task_name}({ti.priority})")
            if priority_tasks:
                logger.warning(
                    "registered_priority_tasks_with_priorities_disabled",
                    tasks=priority_tasks[:20],
                    hint=(
                        "Enable worker priorities (SwarmQConfig.priorities="
                        "True) or change TaskInfo.priority='normal' to "
                        "avoid silent message drop into .high/.low queues "
                        "nobody consumes."
                    ),
                )

        # R4.3 Task Priorities: expand each base queue into the
        # high/normal/low triplet when priorities are enabled.
        #
        # Semantics: high-priority drain first under LIGHT load simply
        # because RabbitMQ delivers from whichever of the three queues
        # has a message. Under FULL saturation the channel-level
        # prefetch is shared fairly — true weighted prefetch (3:2:1)
        # would require separate channels per priority, which is
        # deferred to a follow-up. For v1 operators still get:
        #   * queue-level visibility of priority splits (DLQ list,
        #     queue-depth metrics per priority)
        #   * the routing contract (priority="high" → {queue}.high)
        #   * independent TTL / retry config per priority via
        #     separate TaskInfo instances if needed
        for base_queue in self._config.queues:
            if self._config.priorities:
                queue_names = [
                    f"{base_queue}.high",
                    base_queue,
                    f"{base_queue}.low",
                ]
            else:
                queue_names = [base_queue]
            for queue_name in queue_names:
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

        # R4.5 Burst-Mode: launch the idle monitor only when burst is
        # enabled. The monitor sets _shutdown_event after the queues
        # have been silent for burst_idle_timeout seconds, exiting the
        # await below and triggering the same graceful path SIGTERM uses.
        if self._config.burst_mode:
            self._last_message_received_at = time.monotonic()
            self._burst_monitor_task = asyncio.create_task(
                self._burst_monitor()
            )

        # Block until shutdown is requested
        await self._shutdown_event.wait()
        await self._execute_shutdown()

    async def _burst_monitor(self) -> None:
        """Poll the idle clock; trigger shutdown when burst-timeout elapses.

        Idle window starts at start() and resets on every _on_message
        delivery. Polls every burst_idle_timeout/4 (max 1s) so a typical
        burst worker exits within ~25% of timeout once all queues drain.
        Cancellation-safe: returns silently when the worker is already
        shutting down.
        """
        timeout = self._config.burst_idle_timeout
        poll_interval = min(1.0, max(0.1, timeout / 4))
        try:
            while not self._shutdown_event.is_set():
                idle_for = time.monotonic() - self._last_message_received_at
                if idle_for >= timeout and not self._active_tasks:
                    logger.info(
                        "burst_idle_exit",
                        idle_for_s=round(idle_for, 2),
                        burst_idle_timeout=timeout,
                    )
                    self._request_shutdown()
                    return
                await asyncio.sleep(poll_interval)
        except asyncio.CancelledError:
            # Normal during _execute_shutdown teardown.
            pass

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

        # 3b. Await detached lock-release tasks (Phase 3b.3). When a
        # handler finally block was interrupted by outer-cancel, its
        # shielded _release_acquired_locks call was detached into
        # self._detached_releases. We must await them BEFORE
        # backend.close() — otherwise the detached release would use a
        # closed Valkey client and the lock would stay held until TTL.
        if self._detached_releases:
            await asyncio.wait(
                self._detached_releases,
                timeout=max(5.0, float(self._config.shutdown_timeout)),
            )

        # 3c. Release any leftover handler locks. Runs AFTER the
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

        # 3d. Stop the embedded scheduler. Must run AFTER in-flight tasks
        # have drained (otherwise the leader could still fire during
        # drain and have its commit arrive at a closed backend) and
        # BEFORE broker/backend close (scheduler.stop() writes a handover
        # marker + releases the leader lock — both Valkey ops). If this
        # worker holds the leader lock, the stop() call emits
        # SCHEDULER_LEADER_HANDOVER and sets the transient marker so
        # the next standby emits SCHEDULER_LEADER (not FAILOVER).
        if self._scheduler is not None:
            try:
                await self._scheduler.stop()
            except Exception as exc:
                logger.error(
                    "scheduler_stop_failed", error=str(exc)
                )

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
        # R4.5 Burst-Mode: every delivered message resets the idle clock.
        # _burst_monitor uses this to decide when 'all queues idle' has
        # held long enough to warrant a clean exit.
        self._last_message_received_at = time.monotonic()
        task = asyncio.create_task(self._handle_message(message))
        self._active_tasks.add(task)
        task.add_done_callback(self._active_tasks.discard)

    async def _pre_start_schedfire_dedup(self, message) -> bool:
        """Two-marker schedfire dedup (Unit 1 / H-2 / reliability R-002).

        Three-state check:

        1. **Completed marker present** (``sq:sched:fired:<key>``) → a
           previous delivery of this fire already finished. Ack + skip.
        2. **Completed absent, in-flight SETNX succeeded**
           (``sq:sched:inflight:<key>``) → we own the run now. Caller
           proceeds to the full pipeline. The finally block in
           ``_handle_message`` writes the completed marker on exit
           regardless of success (idempotent).
        3. **Completed absent, in-flight SETNX lost** → another worker
           is currently running this fire. Ack + skip. If that worker
           crashes before writing the completed marker, the in-flight
           key expires on its short TTL (default 300s) and the next
           redelivery can re-run. That closes the silent-drop hole the
           one-marker design had (SETNX winner crash → key persists
           for 3900s → at-most-once failure mode).

        Backend errors → nack(requeue=True); we cannot safely classify
        the state and want a healthy worker to retry later.
        """
        header_value = (message.headers or {}).get("x-swarmq-schedfire")
        if not header_value:
            return False

        try:
            already_completed = await self._backend.is_schedfire_completed(
                header_value
            )
        except BackendConnectionError:
            logger.error(
                "schedfire_dedup_backend_error",
                schedfire_key=header_value,
                stage="completed_check",
            )
            await self._broker.nack(message, requeue=True)
            structlog.contextvars.clear_contextvars()
            return True

        if already_completed:
            logger.info(
                "schedfire_completed_duplicate_acked",
                schedfire_key=header_value,
            )
            await self._broker.ack(message)
            structlog.contextvars.clear_contextvars()
            return True

        inflight_ttl_ms = int(self._config.scheduler_inflight_ttl * 1000)
        try:
            claimed = await self._backend.set_schedfire_inflight(
                header_value, inflight_ttl_ms
            )
        except BackendConnectionError:
            logger.error(
                "schedfire_dedup_backend_error",
                schedfire_key=header_value,
                stage="inflight_claim",
            )
            await self._broker.nack(message, requeue=True)
            structlog.contextvars.clear_contextvars()
            return True

        if claimed:
            # This worker owns the run. Stash the key in a Worker-local
            # dict keyed by id(message) so the finally block in
            # _handle_message can look it up and write the completed
            # marker once the task finishes. (aio_pika's IncomingMessage
            # rejects dynamic attribute assignment.)
            self._schedfire_inflight_claims[id(message)] = header_value
            return False

        # Another worker is running it right now.
        logger.info(
            "schedfire_inflight_duplicate_acked",
            schedfire_key=header_value,
        )
        await self._broker.ack(message)
        structlog.contextvars.clear_contextvars()
        return True

    async def _mark_schedfire_completed(self, message) -> None:
        """Write the long-TTL completed marker after a schedfire task
        finished (success OR failure — both count as "this fire was
        processed"). Called from the ``_handle_message`` finally block.

        No-ops when the message was not a schedfire delivery or when
        this worker did not win the in-flight claim (the key attribute
        is only set on the happy path in ``_pre_start_schedfire_dedup``).
        Failures are logged and swallowed — an orphan in-flight marker
        self-heals via its short TTL."""
        # Pop from the Worker-local claims dict (see __init__). No-op
        # when the message was not a schedfire delivery we claimed.
        schedfire_key = self._schedfire_inflight_claims.pop(id(message), None)
        if not schedfire_key:
            return
        ttl_ms = int(self._config.scheduler_dedup_ttl * 1000)
        try:
            await self._backend.set_schedfire_dedup(schedfire_key, ttl_ms)
        except BackendConnectionError as exc:
            logger.warning(
                "schedfire_completed_write_failed",
                schedfire_key=schedfire_key,
                error=str(exc),
            )

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
            is_cancelled = await self._backend.is_cancelled(
                task_id, op_timeout=self._config.backend_op_timeout,
            )
        except BackendConnectionError:
            # Observability is via the structured log event only (REV-P2-7
            # scope-down). An x-swarmq-cancel-deferred-count header was
            # originally planned, but AMQP nack(requeue=True) redelivers
            # the stored message — in-memory header mutations are lost,
            # as confirmed by the Phase 3c Fork-A2 decision for the
            # sibling schedfire-dedup path. A per-message cap has no
            # defensive value against a systemic Valkey outage (every
            # message bounces the same way); operators rely on Valkey
            # up/down alerts instead.
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
        task_info: TaskInfo | None = None
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
                result_data = build_cancel_result_dict(
                    task_id=task_id, queue=queue, task_name=task_name,
                )
                result_bytes = encode_result(result_data)
                await self._backend.store_result(
                    task_id, result_bytes, result_ttl * 1000,
                    op_timeout=self._config.backend_op_timeout,
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
            await self._backend.clear_cancel_flag(
                task_id, op_timeout=self._config.backend_op_timeout,
            )

            # If this was a schedfire delivery we claimed in-flight,
            # mark it completed so a redelivery after in-flight-TTL
            # expiry does not re-run the task (Unit 1 / H-2). User-
            # initiated cancel counts as "this fire was processed."
            await self._mark_schedfire_completed(message)

            # R3.5 Unit 11: pre-start cancel is a terminal path — the
            # task never ran; release the dedup flag so a future
            # legitimate schedule() can enqueue again. task_info is
            # None when task is unregistered (fallback to completion
            # mode inside _maybe_release_unique).
            await self._maybe_release_unique(message, task_id, task_info)

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
            queue=queue,
        )

        logger.info("task_cancelled_pre_start", task_id=task_id, task_name=task_name)
        structlog.contextvars.clear_contextvars()
        return True

    @staticmethod
    def _extract_valid_unique_hash(message) -> str | None:
        """Read and validate the unique-hash header from an incoming message.

        Producer sites emit a sha256 hex digest (64 lowercase hex chars).
        Any message whose header deviates — missing, wrong type, wrong
        length, non-hex — is treated as "no flag": the worker skips
        release rather than forwarding unvalidated bytes to Valkey as a
        key. Defends against adversarial producers that could otherwise
        amplify a malformed header into a large per-message Valkey key
        (plan Risks / R3.5 ADV-005).
        """
        unique_hash = (message.headers or {}).get(HEADER_UNIQUE_HASH)
        if not isinstance(unique_hash, str) or len(unique_hash) != 64:
            return None
        # Cheap manual hex check — re.match on a 64-char string is
        # faster than compiling a regex but still O(n).
        if not all(c in "0123456789abcdef" for c in unique_hash):
            return None
        return unique_hash

    async def _maybe_release_unique(
        self, message, task_id: str, task_info: TaskInfo | None = None,
    ) -> None:
        """Release the R3.5 dedup flag if this message carries one.

        Contract:
          * No-op when the message lacks a valid ``x-swarmq-unique-hash``
            header (missing, wrong type, not a 64-char lowercase hex
            digest).
          * No-op in ``start``-mode at terminal paths — Unit 10 already
            released at pickup (owner-check would fail here anyway).
          * ``completion``-mode (default): call ``backend.release_unique``;
            owner-check protects against TTL-expiry-race (first task's
            flag was reclaimed by a second task before first finished).
          * When ``task_info`` is None (task not registered) → treat as
            completion mode. Better to attempt release than leave an
            orphan flag until TTL.

        Backend errors are swallowed inside ``release_unique`` — an
        orphan flag self-heals via TTL and a release failure must not
        break the worker's terminal-path pipeline (same discipline as
        ``clear_cancel_flag``). ``op_timeout`` bounds the Valkey call
        so the finally block can't wedge on a slow backend.
        """
        unique_hash = self._extract_valid_unique_hash(message)
        if unique_hash is None:
            return
        # Prefer the header over TaskInfo — per-call overrides from
        # Client.schedule set the header, and we want those overrides
        # to flow end-to-end. TaskInfo is the fallback when the header
        # is absent or malformed.
        header_mode = (message.headers or {}).get(HEADER_UNIQUE_UNTIL)
        mode = header_mode or (
            task_info.unique_until if task_info is not None else "completion"
        )
        if mode == "start":
            return  # Release already happened at pickup (Unit 10).
        await self._backend.release_unique(
            unique_hash, task_id,
            op_timeout=self._config.backend_op_timeout,
        )

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
            # Plan Unit 11 Risks: deserialize-fail cannot release (task_id
            # unknown). Accepted limitation — flag self-heals via TTL.
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

        # 2a. Consumer-side schedfire dedup (Phase 3c Unit 8). Runs FIRST —
        # before cancel-check, semaphore, rate-limit, lock — so a
        # duplicate scheduled fire never enters any of those stages and
        # costs no Valkey round-trips beyond the SETNX. Ack-safe
        # pre-pipeline stage, same discipline as cancel-check.
        if await self._pre_start_schedfire_dedup(message):
            return

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
        # R3.5 Unit 11 state: task_info visible in finally even if
        # lookup fails; _unique_release_pending tracks whether the
        # finally block should fire _maybe_release_unique.
        # Default TRUE = this delivery is terminal until a branch
        # proves otherwise. Non-terminal branches (nack(requeue=True),
        # delay-queue republish) MUST clear the flag to False —
        # otherwise the dedup flag gets released while a redelivered
        # copy of the message is still in flight, creating a bypass
        # window (ce-review M-03 / P2 #12: name + direction chosen
        # so that forgetting the mutation over-releases rather than
        # under-releases; over-release is owner-check-safe).
        task_info: TaskInfo | None = None
        _unique_release_pending = True
        try:
            # 3b. Check if shutdown is in progress — requeue message
            if self._shutdown_event.is_set():
                # nack(requeue=True): broker redelivers with headers
                # intact → keep the flag so redelivery can release.
                _unique_release_pending = False
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

            # R4.4 Task Expiry: check max_age BEFORE any execution
            # stages. An expired message takes the DLQ path without
            # entering rate-limit, lock, pre_execute, etc. The enqueue
            # time is stamped at publish (see Client.schedule +
            # schedule_many), so this runs on every delivery including
            # retry-after-delay-queue — each retry's clock starts from
            # the ORIGINAL enqueue, which is the intended semantic
            # (expiry is a wall-time contract, not a per-delivery one).
            if await self._check_max_age_expired(
                message, task_info, queue, task_id, task_name,
            ):
                _unique_release_pending = False
                return

            # 5a. Rate limit check — runs BEFORE the FIRE_AND_FORGET ack
            # so a Valkey or broker hiccup cannot drop a message that has
            # already been acked. Rate-limited tasks never reach
            # pre_execute or middleware: they are republished into the
            # delay queue and ack'd as a single transactional unit.
            # Any "handled" return is a requeue (delay queue or nack=True)
            # — the original message's x-swarmq-unique-hash header is
            # preserved in the republished copy (Unit 11 header
            # preservation), so the flag must stay claimed.
            if await self._try_apply_rate_limit(
                message, task_info, queue, task_id, task_name,
            ):
                _unique_release_pending = False
                return

            # 5b. Lock check — same FIRE_AND_FORGET-ordering discipline as
            # rate-limit. Conflict path: requeue with exponential backoff
            # until the per-message cap, then DLQ. Acquired locks are
            # released in the finally below.
            # Terminal DLQ sub-paths inside the helper release the flag
            # inline (render-fail, empty-render, conflict-cap); non-
            # terminal sub-paths requeue and preserve the header. Clear
            # _unique_release_pending either way to avoid double-release
            # (terminal sub-paths already released) or premature release
            # (non-terminal sub-paths still in flight).
            if await self._try_apply_lock(
                message, task_info, task_class, queue, task_id, task_name,
                args, kwargs, handler_id, acquired_locks,
            ):
                _unique_release_pending = False
                return

            # 5c. R3.5 Unit 10 (ce-review P1 #4 / ADV-002 fix): start-mode
            # dedup release happens AFTER rate-limit / lock checks and
            # BEFORE the FIRE_AND_FORGET ack. Releasing earlier would
            # break dedup semantics in the requeue case: rate-limit or
            # lock-conflict republishes the message with the header
            # intact, a second schedule() call in the requeue window
            # would win the SETNX, and both tasks would run. The flag
            # must persist across requeue-based non-terminal stages and
            # only release when the task is actually about to execute.
            # Owner-check in unique_release.lua protects against a
            # foreign-owner DEL; BackendConnectionError is swallowed
            # inside release_unique (log-only) — letting Valkey flap
            # abort pickup would turn dedup into a liveness hazard.
            unique_hash = self._extract_valid_unique_hash(message)
            header_unique_until = (message.headers or {}).get(HEADER_UNIQUE_UNTIL)
            effective_unique_until = header_unique_until or task_info.unique_until
            if unique_hash and effective_unique_until == "start":
                await self._backend.release_unique(
                    unique_hash, task_id,
                    op_timeout=self._config.backend_op_timeout,
                )

            # 5d. FIRE_AND_FORGET: ack immediately before execution.
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
                Signal.TASK_STARTED,
                task_id=task_id, task_name=task_name, queue=queue,
            )
            start_time = time.monotonic()

            # 6. Create task instance, run lifecycle
            task_instance = task_class()

            # R3.7 progress-tracking wiring. Task.update_progress relies
            # on these four instance attributes; if the task never calls
            # update_progress, setting them is a handful of wasted cycles.
            # Resolving result_ttl once here mirrors the cascade used
            # further below for the result-storage path (task_info →
            # config → 86400 fallback).
            progress_result_ttl = task_info.result_ttl
            if progress_result_ttl is None:
                if task_info.delivery is DeliveryMode.FIRE_AND_FORGET:
                    progress_result_ttl = 0
                else:
                    progress_result_ttl = self._config.result_ttl
            if progress_result_ttl in (None, 0):
                # result_ttl=0 means "don't store results" — progress
                # gets the default 24h fallback so it's still inspectable
                # during the task's run.
                progress_result_ttl = 86400
            task_instance._task_id = task_id
            task_instance._backend = self._backend
            task_instance._task_start_time = time.monotonic()
            task_instance._result_ttl_ms = progress_result_ttl * 1000

            result_value = None
            is_error = False
            is_cancelled = False
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

                # Per-handler cancel-reason box (Phase 3b.3). Both the
                # hard-timeout watchdog and the cancel monitor write via
                # set_reason_if_unset — first writer wins. The
                # CancelledError branch below reads cancel_reason[0] to
                # decide which path to take.
                cancel_reason: list[CancelReason | None] = [None]

                def set_reason_if_unset(reason: CancelReason) -> None:
                    if cancel_reason[0] is None:
                        cancel_reason[0] = reason

                watchdog = None
                monitor = asyncio.create_task(
                    self._cancel_monitor(task_id, run_task, set_reason_if_unset)
                )
                try:
                    if task_info.hard_timeout is not None:
                        watchdog = asyncio.create_task(
                            self._hard_timeout_watchdog(
                                run_task,
                                task_info.hard_timeout,
                                set_reason_if_unset,
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
                        # Discriminate user-cancel vs hard-timeout via
                        # cancel_reason box (Phase 3b.3).  The box is
                        # written BEFORE run_task.cancel() by whichever
                        # source fires first — first-writer-wins.
                        if cancel_reason[0] == CANCEL_REASON_CANCEL:
                            is_cancelled = True
                            # No task_exception / error_message: cancel
                            # bypasses the error pipeline entirely.
                        else:
                            # "timeout" or None (bare CancelledError):
                            # fall through to the existing hard-timeout
                            # path.
                            hard_err_msg = (
                                f"hard timeout after {task_info.hard_timeout}s"
                            )
                            is_error = True
                            error_message = hard_err_msg
                            task_exception = RuntimeError(hard_err_msg)
                            if task_info.delivery is DeliveryMode.FIRE_AND_FORGET:
                                logger.error(
                                    "fire_and_forget_error",
                                    error=error_message,
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
                    # Outer finally: guarantees watchdog + monitor cleanup
                    # regardless of which path was taken (success, soft/
                    # hard timeout, user cancel, or any other exception
                    # from run_task / post_execution).
                    if watchdog is not None and not watchdog.done():
                        watchdog.cancel()
                        try:
                            await watchdog
                        except asyncio.CancelledError:
                            pass
                    if not monitor.done():
                        monitor.cancel()
                        try:
                            await monitor
                        except asyncio.CancelledError:
                            pass

            # Emit post-execute / on-error / on-cancel hooks now that
            # the run lifecycle is resolved.
            duration_ms = (time.monotonic() - start_time) * 1000.0
            if is_cancelled:
                # Cancel bypasses the error/success pipeline: no
                # on_error, no post_execute hooks — cancel is not an
                # error from middleware's perspective.  Only the
                # TASK_CANCELLED signal fires.
                await self._signal_dispatcher.dispatch(
                    Signal.TASK_CANCELLED,
                    task_id=task_id,
                    task_name=task_name,
                    queue=queue,
                )
            elif is_error and task_exception is not None:
                await chain.run_on_error(
                    message_dict, exception=task_exception
                )
                await self._signal_dispatcher.dispatch(
                    Signal.TASK_FAILED,
                    task_id=task_id,
                    task_name=task_name,
                    queue=queue,
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
                    queue=queue,
                    duration_ms=duration_ms,
                )

            # For FIRE_AND_FORGET with errors: skip result/retry pipeline.
            # For FIRE_AND_FORGET cancels: also clear the flag before we
            # bail — the result-ttl-zero early-return below wouldn't
            # clear it because FAF control flow doesn't reach that
            # branch for cancelled tasks with errors.
            if task_info.delivery is DeliveryMode.FIRE_AND_FORGET and (
                is_error or is_cancelled
            ):
                if is_cancelled:
                    await self._backend.clear_cancel_flag(task_id, op_timeout=self._config.backend_op_timeout)
                return

            # Retry logic (AT_LEAST_ONCE only) — cancel bypasses retry.
            if (
                is_error
                and task_exception is not None
                and not is_cancelled
                and task_info.delivery is DeliveryMode.AT_LEAST_ONCE
                and task_info.max_retries > 0
            ):
                # Post-execution cancel guard (REV-P2-6): a cancel that
                # arrived WHILE post_execution was running or between
                # run_task.done() and this branch wouldn't have been
                # caught by the cancel-monitor (which stops polling as
                # soon as run_task.done() fires). Re-check the flag
                # here so a cancel that raced into the tail of the
                # pipeline suppresses the retry. Timeout-wrapped like
                # every other cancel-path backend call (REV-P1-3).
                try:
                    late_cancel = await self._backend.is_cancelled(
                        task_id,
                        op_timeout=self._config.backend_op_timeout,
                    )
                except BackendConnectionError:
                    # Backend hiccup at this late stage: treat as "not
                    # cancelled" and let the normal retry path decide.
                    # The next pre-start check on the redelivered
                    # message will still honor an actual cancel flag.
                    late_cancel = False

                if late_cancel:
                    logger.info(
                        "cancel_observed_post_execution",
                        task_id=task_id,
                        task_name=task_name,
                    )
                    # Flip state so downstream code treats this as a
                    # cancel rather than an error → skip retry, store
                    # is_cancelled=True result, clear flag, ack.
                    is_cancelled = True
                    is_error = False
                    error_message = ""
                    task_exception = None
                else:
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
                        # Retry path covers two outcomes:
                        #   * delay-queue republish (non-terminal —
                        #     header preserved, next delivery runs the
                        #     full pipeline again),
                        #   * _send_to_dlq (terminal — releases
                        #     inline).
                        # Either way the finally must NOT release:
                        # non-terminal would strand the redelivered
                        # message, terminal already released inline.
                        _unique_release_pending = False
                        return

            # Build result dict. is_cancelled is a new key in Phase 3b.3;
            # Client._unpack reads it via .get() so pre-R3.6 consumers
            # don't break when the key is absent.
            result_data = build_result_dict(
                task_id=task_id,
                queue=queue,
                task_name=task_name,
                is_error=is_error,
                is_cancelled=is_cancelled,
                error_message=error_message,
                result=None if (is_error or is_cancelled) else result_value,
            )

            # 7. Serialize result
            try:
                result_bytes = encode_result(result_data)
            except ResultSerializationError as exc:
                logger.error("result_serialization_failed", error=str(exc))
                result_data = build_error_result_dict(
                    task_id=task_id, queue=queue, task_name=task_name,
                    error_message=str(exc),
                )
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
                # Even with result_ttl=0 we must clear the cancel flag
                # when the task was cancelled — otherwise it lives until
                # TTL expires and a task_id reuse would silently cancel.
                if is_cancelled:
                    await self._backend.clear_cancel_flag(task_id, op_timeout=self._config.backend_op_timeout)
                return

            # 9-11. Store result + ack/nack
            try:
                await self._backend.store_result(
                    task_id, result_bytes, result_ttl * 1000
                )
            except BackendConnectionError:
                logger.error("backend_connection_error")
                if task_info.delivery is DeliveryMode.AT_LEAST_ONCE:
                    # Non-terminal — redelivery will re-run the task
                    # from scratch; keep the flag so next terminal
                    # path can release it.
                    _unique_release_pending = False
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

            # Clear the cancel flag AFTER the result is safely stored so
            # a mid-op failure leaves the flag in place and a redelivery
            # re-triggers the cancel branch.
            if is_cancelled:
                await self._backend.clear_cancel_flag(task_id, op_timeout=self._config.backend_op_timeout)

            if task_info.delivery is DeliveryMode.AT_LEAST_ONCE:
                await self._broker.ack(message)

        finally:
            # 12-pre. Write the schedfire completed marker if this message
            # was a schedfire delivery we claimed in-flight. Must run
            # BEFORE broker-level ack is reconsidered downstream — a
            # redelivery racing our in-flight key expiry must see the
            # completed marker (Unit 1 / H-2).
            await self._mark_schedfire_completed(message)

            # 12-pre-b. R3.5 Unit 11: completion-mode dedup release.
            # No-op in start-mode (Unit 10 already released at pickup),
            # no-op when the message has no x-swarmq-unique-hash header.
            # All main-body terminal paths (success, error, cancel,
            # DLQ, lock-conflict-cap, task_not_found nack=False,
            # lock-render-fail, middleware-factory-fail) flow through
            # here with _unique_release_pending still True. Non-
            # terminal paths (rate-limit requeue, lock-conflict
            # requeue, retry delay queue, store_result nack=True,
            # shutdown nack=True) also hit this finally, but they
            # preserve the x-swarmq-unique-hash header in the
            # republished message and clear _unique_release_pending
            # so the next delivery re-runs the owner-check pipeline.
            if _unique_release_pending:
                await self._maybe_release_unique(
                    message, task_id, task_info,
                )

            # 12a. Release any task-locks acquired by this handler.
            #
            # Previous implementation wrapped the release in
            # ``asyncio.ensure_future(asyncio.shield(...))`` and tracked
            # the SHIELD-wrapper task in _detached_releases. On outer-
            # cancel the shield-wrapper got cancelled, but its inner
            # coroutine kept running DETACHED and was never tracked —
            # _execute_shutdown would see an already-done wrapper and
            # close the backend while the real release was still in
            # flight, leaving the lock held (REV-P1-1).
            #
            # Fix: schedule the inner release as its own Task first and
            # track THAT task in _detached_releases. Then ``await
            # asyncio.shield(inner)`` — shield only protects the await,
            # not the task. Outer-cancel into this finally cancels the
            # wait, but the inner Task keeps running; _execute_shutdown
            # awaits it via _detached_releases before backend.close.
            if acquired_locks and self._lock_manager is not None:
                inner = asyncio.create_task(
                    self._release_acquired_locks(handler_id, acquired_locks)
                )
                self._detached_releases.add(inner)
                inner.add_done_callback(self._detached_releases.discard)
                try:
                    await asyncio.shield(inner)
                except asyncio.CancelledError:
                    # Outer-cancel reached us. ``inner`` is unaffected
                    # (shield protected it); _execute_shutdown awaits
                    # it via _detached_releases. Re-raise so the
                    # caller's cancel is observed.
                    raise
            # 12b. Release semaphore
            self._semaphore.release()
            structlog.contextvars.clear_contextvars()

    async def _hard_timeout_watchdog(
        self,
        run_task: asyncio.Task,
        timeout: float,
        set_reason_if_unset: Callable[[str], None] | None = None,
    ) -> None:
        """Sleep for *timeout* seconds, then cancel *run_task*.

        The optional *set_reason_if_unset* callback writes ``"timeout"``
        into the per-handler cancel-reason box (Phase 3b.3) so the
        CancelledError branch in _handle_message can distinguish a
        hard-timeout from a user cancel.  Omitted arg keeps the old
        behaviour for tests that instantiate the watchdog directly.
        """
        await asyncio.sleep(timeout)
        if set_reason_if_unset is not None:
            set_reason_if_unset(CANCEL_REASON_TIMEOUT)
        run_task.cancel()

    async def _cancel_monitor(
        self,
        task_id: str,
        run_task: asyncio.Task,
        set_reason_if_unset: Callable[[str], None],
    ) -> None:
        """Poll Valkey for a cancel flag and cancel *run_task* when set.

        Polling interval: ``self._config.cancel_poll_interval``.  Uses
        ``asyncio.wait(timeout=…)`` to short-circuit the sleep as soon
        as *run_task* completes — a vanilla ``asyncio.sleep`` would
        keep the coroutine alive for a full poll interval past
        run_task completion.

        Transient errors from ``backend.is_cancelled`` are logged as a
        warning and do not break the loop — the next poll re-tries.

        The caller is responsible for cancelling and awaiting this
        coroutine in its finally block.
        """
        poll_interval = self._config.cancel_poll_interval
        while not run_task.done():
            try:
                done, _ = await asyncio.wait(
                    {run_task}, timeout=poll_interval
                )
                if run_task in done:
                    # Task completed on its own — stop polling.
                    return
                if await self._backend.is_cancelled(
                    task_id, op_timeout=self._config.backend_op_timeout,
                ):
                    set_reason_if_unset(CANCEL_REASON_CANCEL)
                    run_task.cancel()
                    return
            except asyncio.CancelledError:
                # Handler finally cancels us explicitly — propagate.
                raise
            except Exception as exc:
                logger.warning(
                    "cancel_monitor_error",
                    task_id=task_id,
                    error=str(exc),
                )
                # Fall through — the next iteration's asyncio.wait
                # already waits poll_interval, so no extra sleep is
                # needed.  A prior version slept an additional
                # poll_interval here, doubling the effective latency
                # between checks during transient Valkey errors.

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
            task_name=task_name,
            queue=queue,
            retry_count=retry_count + 1,
            delay=delay_seconds,
        )

        # 6. Publish to delay queue with incremented retry count.
        # R3.5 Unit 11: merge original headers so x-swarmq-unique-hash
        # (and any future stage-specific header) survives redelivery.
        # Otherwise the flag would zombie in Valkey until TTL while the
        # retry succeeds and the terminal-release finds no header to
        # release on. Same merge pattern as rate-limit (worker.py ~208)
        # and lock-conflict requeue (worker.py ~443).
        headers = {**(dict(message.headers) if message.headers else {})}
        headers["x-swarmq-retry-count"] = retry_count + 1
        headers["x-swarmq-traceback"] = error_message
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
        result_data = build_error_result_dict(
            task_id=task_id, queue=queue, task_name=task_name,
            error_message=error_message,
        )
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

        # R3.5 Unit 11: preserve all inbound headers so a message that
        # reached DLQ via retry-exhaustion still carries its original
        # x-swarmq-unique-hash for operator introspection (inline
        # release below takes care of the actual flag cleanup).
        headers = {**(dict(message.headers) if message.headers else {})}
        headers["x-swarmq-traceback"] = error_message
        # Fetch task_info for release-mode; unregistered tasks default
        # to completion (see _maybe_release_unique contract).
        try:
            task_class = self._registry.get(task_name)
            task_info_for_release = task_class.task_info
        except TaskNotFoundError:
            task_info_for_release = None
        try:
            await self._broker.publish_to_dead_letter_queue(
                queue, message.body, headers=headers
            )
            await self._broker.ack(message)
            # R3.5 ce-review P1 #3 / ADV-001: release AFTER successful
            # ack. If ack raises, the except branch nacks with
            # requeue=True, and redelivery's terminal path must still
            # be able to release the flag. Releasing before ack would
            # make the redelivered task run a second time within the
            # dedup window.
            await self._maybe_release_unique(
                message, task_id, task_info_for_release
            )
        except Exception as broker_exc:
            logger.error("dlq_publish_failed", error=str(broker_exc))
            try:
                await self._broker.nack(message, requeue=True)
            except Exception:
                pass
            # nack(requeue=True) is non-terminal — leave the flag so
            # the redelivery's terminal path releases it. No release
            # here.
        logger.warning("task_sent_to_dlq", error=error_message)
