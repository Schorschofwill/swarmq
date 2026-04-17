"""SwarmQ Client API."""

from __future__ import annotations

import asyncio
import time
import uuid
import json as _json
from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime
from typing import Any, Literal

import structlog

from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import (
    ResultNotReadyError,
    TaskCancelledError,
    TaskError,
    TaskNotFoundError,
)
from swarmq.middleware import (
    MessageContext,
    MiddlewareChain,
    coerce_middleware_chain_factory,
)
from swarmq.registry import TaskRegistry
from swarmq.headers import HEADER_UNIQUE_HASH, HEADER_UNIQUE_UNTIL
from swarmq.progress import ProgressInfo
from swarmq.scheduler import ScheduleDefinition, ScheduleStore
from swarmq.serialization import (
    canonical_encode,
    compute_dedup_hash,
    decode_result,
    encode_json,
)
from swarmq.signals import Signal, SignalDispatcher

logger = structlog.get_logger()


# Default TTL for the cancel flag. 24h is wide enough to outlive the
# typical retry-horizon + redelivery window while still self-healing
# orphaned flags within a day (REV-P3 cleanup). Callers who want a
# tighter window (e.g. deterministic task_id systems vulnerable to
# stale-flag-zombie, see Client.cancel docstring) can override.
_DEFAULT_CANCEL_FLAG_TTL_MS = 86_400_000  # 24h


# R3.5 Unique Tasks: module-private sentinel that distinguishes
# "caller did not pass a value" from "caller explicitly passed None".
# None means "dedup explicitly disabled for this call"; _MISSING means
# "fall through to TaskInfo default (or None in Client-Modus)".
# Not exported — do NOT add to __init__.py.
#
# Dedicated singleton class so type-checkers narrow int | None | _MissingType
# unions at call sites (plain Any would collapse to Any). __slots__ keeps
# the object tiny; __new__ enforces exactly one instance so `is _MISSING`
# identity checks stay cheap.
class _MissingType:
    """Private singleton for the `_MISSING` sentinel. See module docstring."""
    __slots__ = ()
    _instance: "_MissingType | None" = None

    def __new__(cls) -> "_MissingType":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:  # noqa: D401
        return "<_MISSING>"

    def __bool__(self) -> bool:
        # Defensive: treat the sentinel as falsy in `if value:` style checks
        # that might be written before reading the docstring. Proper checks
        # use `value is _MISSING`.
        return False


_MISSING = _MissingType()


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
        backend_op_timeout: float | None = None,
    ) -> None:
        self._broker = broker
        self._backend = backend
        self._registry = registry
        # R3.5: bound every Valkey call on the enqueue hot path so a
        # hung backend cannot wedge Client.schedule. None preserves
        # pre-R3.5 behavior (no enforcement at the Client layer);
        # SwarmQ passes config.backend_op_timeout through so app-mode
        # callers inherit the same guard the Worker already uses.
        self._backend_op_timeout = backend_op_timeout
        # Accept either a static MiddlewareChain (backward compat) or a
        # zero-arg factory callable that returns a fresh chain on each call.
        # Storing a factory lets us see middleware registered AFTER Client
        # construction — e.g. when SwarmQ.add_middleware() is called after
        # the Client has already been built.
        self._middleware_chain_factory = coerce_middleware_chain_factory(
            middleware_chain
        )
        self._signal_dispatcher = (
            signal_dispatcher if signal_dispatcher is not None else SignalDispatcher()
        )
        # Lazy-init (Fork 2 / Review-Finding P2-6). ScheduleStore requires a
        # connected backend to register its Lua scripts — but Client is
        # constructed before backend.connect(). We defer the construction to
        # the first schedule(delay=|eta=) call.
        self._schedule_store: ScheduleStore | None = None

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

    async def _claim_unique_flag(
        self,
        task_name: str,
        args: tuple,
        kwargs: dict,
        task_id: str,
        ttl_ms: int,
        source: str,
        **extra_signal_payload: Any,
    ) -> tuple[str, str | None]:
        """Attempt SETNX for the given (task_name, args, kwargs) tuple.

        Returns ``(dedup_hash, existing_task_id)``:
          * ``existing_task_id is None`` → flag claimed; caller proceeds
            to publish and tag the message headers.
          * ``existing_task_id is str`` → duplicate detected; caller
            short-circuits and returns the existing owner's task_id.
            TASK_DUPLICATE_DETECTED has already been dispatched with
            the standard payload (existing_task_id, task_name,
            dedup_hash, source) plus any ``extra_signal_payload`` keys
            (e.g. batch_index for schedule_many).

        Shared helper — single source of truth for the SETNX + dup-
        signal shape used by ``Client.schedule``, ``_schedule_deferred``,
        and ``schedule_many``. Changing the signal payload now requires
        exactly one edit (plan M-01 / ce-review P2 #10).
        """
        dedup_hash = compute_dedup_hash(task_name, args, kwargs)
        existing = await self._backend.setnx_unique(
            dedup_hash, task_id, ttl_ms,
            op_timeout=self._backend_op_timeout,
        )
        if existing is None:
            return dedup_hash, None

        payload: dict[str, Any] = {
            "existing_task_id": existing,
            "task_name": task_name,
            "dedup_hash": dedup_hash,
            "source": source,
            **extra_signal_payload,
        }
        await self._signal_dispatcher.dispatch(
            Signal.TASK_DUPLICATE_DETECTED, **payload,
        )
        logger.info("swarmq.dedup.detected", **payload)
        return dedup_hash, existing

    async def _rollback_unique_claim(
        self, dedup_hash: str, task_id: str,
    ) -> None:
        """Best-effort release of a dedup flag whose enqueue failed.

        Called by P1 #1 / #2 rollback paths when SETNX succeeded but the
        subsequent publish / store insert raised. The release uses the
        owner-checked Lua script — if a racing second schedule somehow
        claimed the same cell, the DEL is a no-op. Failures are logged
        and swallowed so the original enqueue exception stays the
        visible error to the caller (TTL-backstop self-heals any leak).
        """
        try:
            await self._backend.release_unique(
                dedup_hash, task_id,
                op_timeout=self._backend_op_timeout,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "swarmq.dedup.rollback_failed",
                dedup_hash=dedup_hash,
                task_id=task_id,
                error=str(exc),
            )

    def _ensure_schedule_store(self) -> ScheduleStore:
        """Construct and cache the ScheduleStore on first deferred schedule.

        ScheduleStore needs a connected backend (for Lua script-register).
        Client is built before backend.connect() is awaited, so we defer
        the construction to the first ``schedule(delay=|eta=)`` call.
        """
        if self._schedule_store is None:
            if getattr(self._backend, "_client", None) is None:
                raise RuntimeError(
                    "Client.schedule(delay=/eta=) requires a connected "
                    "backend. Use 'async with SwarmQ(...)' or await "
                    "backend.connect() before scheduling deferred tasks."
                )
            self._schedule_store = ScheduleStore(self._backend)
        return self._schedule_store

    async def schedule(
        self,
        task_name: str,
        *args: Any,
        queue: str | None = None,
        delay: float | None = None,
        eta: datetime | None = None,
        unique_for: int | None | _MissingType = _MISSING,
        unique_until: Literal["start", "completion"] | _MissingType = _MISSING,
        **kwargs: Any,
    ) -> str:
        """Submit a task for execution and return its task_id.

        Reserved keyword-only args ``delay``, ``eta``, ``unique_for``,
        ``unique_until`` cannot collide with user payloads because they
        are reserved at ``@app.task`` registration (P1-3).

        Dedup (R3.5) is silent-by-default: a duplicate ``schedule`` call
        within ``unique_for`` seconds returns the *existing* task_id
        instead of a new one, and emits ``TASK_DUPLICATE_DETECTED``
        for observability. No exception is raised — the API shape
        stays ``schedule() -> str``.
        """
        # --- validation (registry mode only) ---
        if self._registry is not None:
            if not self._registry.has(task_name):
                raise TaskNotFoundError(task_name)

        # --- queue resolution chain + delivery_mode resolution ---
        resolved_queue, delivery_mode = self._resolve_queue_and_delivery_mode(
            task_name, queue
        )

        # --- deferred path: delay= / eta= ---
        if delay is not None and eta is not None:
            raise ValueError("Specify delay OR eta, not both")
        if delay is not None and delay < 0:
            raise ValueError(f"delay must be >= 0, got {delay!r}")
        # delay=0 is a normal immediate publish; only non-zero delay and
        # eta take the scheduler-store detour.
        if (delay is not None and delay > 0) or eta is not None:
            # Resolve R3.5 dedup up here so deferred path + immediate path
            # apply the same precedence rules (_MISSING → TaskInfo default,
            # None → dedup off, int → explicit).
            resolved_unique_for, resolved_unique_until = self._resolve_unique(
                task_name, unique_for, unique_until
            )
            return await self._schedule_deferred(
                task_name,
                list(args),
                kwargs,
                resolved_queue=resolved_queue,
                delay=delay,
                eta=eta,
                unique_for=resolved_unique_for,
                unique_until=resolved_unique_until,
            )

        # --- build and publish message ---
        task_id = str(uuid.uuid7())

        # --- R3.5 dedup resolution ---
        resolved_unique_for, resolved_unique_until = self._resolve_unique(
            task_name, unique_for, unique_until
        )
        dedup_hash: str | None = None
        headers: dict[str, Any] = {}

        if resolved_unique_for is not None:
            dedup_hash, existing = await self._claim_unique_flag(
                task_name, args, kwargs, task_id,
                ttl_ms=resolved_unique_for * 1000,
                source="client.schedule",
            )
            if existing is not None:
                return existing  # Silent dedup — signal already emitted.
            # Flag acquired — tag the message so the worker can release
            # at the right point (start-mode: pickup; completion-mode: terminal).
            headers[HEADER_UNIQUE_HASH] = dedup_hash
            headers[HEADER_UNIQUE_UNTIL] = resolved_unique_until

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

        # R3.5 ce-review P1 #1 / COR-001: if SETNX claimed the flag,
        # rollback on any downstream failure (pre_enqueue hook raises,
        # encode_json raises, broker.publish raises). Without rollback
        # the flag zombies until TTL while no message exists in the
        # broker — any subsequent same-args schedule() gets the orphan
        # task_id as "existing". Owner-check in unique_release makes
        # the rollback safe against a racing second schedule that
        # somehow won SETNX in the meantime (cannot happen atomically
        # but defense-in-depth).
        try:
            # pre_enqueue exceptions PROPAGATE — they can prevent the enqueue.
            await chain.run_pre_enqueue(message_dict)

            # broker.publish forwards **properties to aio_pika.Message(**),
            # which accepts ``headers=`` natively. Empty dict is harmless.
            await self._broker.publish(
                resolved_queue,
                encode_json(message),
                headers=headers if headers else None,
            )
        except BaseException:
            if dedup_hash is not None:
                await self._rollback_unique_claim(dedup_hash, task_id)
            raise

        # post_enqueue + TASK_ENQUEUED signal only after successful publish.
        await chain.run_post_enqueue(message_dict)
        await self._signal_dispatcher.dispatch(
            Signal.TASK_ENQUEUED,
            task_id=task_id,
            task_name=task_name,
            queue=resolved_queue,
        )
        return task_id

    def _resolve_unique(
        self,
        task_name: str,
        unique_for: int | None | _MissingType,
        unique_until: Literal["start", "completion"] | _MissingType,
    ) -> tuple[int | None, str]:
        """Resolve effective unique_for / unique_until for a schedule() call.

        Rules (origin CD1):
          - unique_for is _MISSING: look up TaskInfo default if registry
            is available; otherwise None (dedup off) in Client-Modus.
          - unique_for is None (explicit): dedup off for this call,
            even if TaskInfo sets a default.
          - unique_for is int: use this value.
        unique_until follows the same pattern with default "completion".
        """
        final_for: int | None
        final_until: str = "completion"

        if unique_for is _MISSING:
            final_for = None
            if self._registry is not None and self._registry.has(task_name):
                entry = self._registry.get_entry(task_name)
                final_for = entry.task_info.unique_for
                final_until = entry.task_info.unique_until
        else:
            final_for = unique_for

        if unique_until is _MISSING:
            # Already set from TaskInfo above (or still "completion" default
            # in Client-Modus).
            pass
        else:
            final_until = unique_until

        return final_for, final_until

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

    # ------------------------------------------------------------------
    # R3.7 Progress tracking — consumer side
    # ------------------------------------------------------------------

    async def get_progress(self, task_id: str) -> ProgressInfo | None:
        """Read the current progress HASH for *task_id*.

        Returns ``None`` when no progress has been recorded — either
        the task never called :meth:`Task.update_progress`, the TTL
        has expired, or the task_id is unknown. The caller should
        treat None as "nothing observed yet", not "task complete".

        One-shot read; use :meth:`watch_progress` for a live stream.
        """
        raw = await self._backend.get_progress(task_id)
        if raw is None:
            return None
        return ProgressInfo.from_hash(raw)

    async def watch_progress(
        self, task_id: str,
    ) -> AsyncIterator[ProgressInfo]:
        """Stream progress updates for *task_id*.

        Subscribes to the per-task Pub/Sub channel FIRST (so an update
        arriving between the initial read and subscription isn't
        missed), then yields the last known HASH state if present,
        then streams every subsequent :meth:`Task.update_progress`
        publish.

        The iterator never completes on its own — the caller must
        ``break`` when satisfied (typical: after a ProgressInfo with
        ``percent == 100.0`` or after observing a terminal signal via
        another channel).

        Cleanup happens in the finally of the underlying
        ``async_generator`` — unsubscribing from the channel on
        iterator close or caller break.
        """
        channel = self._backend._progress_channel(task_id)
        pubsub = self._backend.create_pubsub()
        try:
            await pubsub.subscribe(channel)
            # Drain the subscribe-confirmation so later get_message
            # calls don't return it as a false-positive update.
            await pubsub.get_message(
                ignore_subscribe_messages=True, timeout=0.05,
            )

            # Yield current state if present (bridges the gap between
            # 'an update was published just before subscribe landed'
            # and 'nothing has been published yet').
            initial = await self.get_progress(task_id)
            if initial is not None:
                yield initial

            while True:
                try:
                    msg = await pubsub.get_message(
                        ignore_subscribe_messages=True, timeout=0.1,
                    )
                except Exception:
                    # Pub/Sub error (disconnection, etc) — fall back to
                    # re-reading the HASH so the caller sees *some*
                    # update stream even if Pub/Sub is flaking.
                    await asyncio.sleep(0.5)
                    continue
                if msg is None or msg.get("type") != "message":
                    continue
                data = msg.get("data")
                if isinstance(data, bytes):
                    try:
                        fields = _json.loads(data)
                    except ValueError:
                        continue
                    info = ProgressInfo.from_hash(fields)
                    if info is not None:
                        yield info
        finally:
            try:
                await pubsub.unsubscribe(channel)
                await pubsub.aclose()
            except Exception:
                pass

    async def schedule_many(
        self,
        tasks: list[tuple[str, tuple, dict]],
        queue: str | None = None,
        *,
        unique_for: int | None | _MissingType = _MISSING,
        unique_until: Literal["start", "completion"] | _MissingType = _MISSING,
    ) -> list[str]:
        """Submit multiple tasks for execution and return their task_ids.

        Each element of *tasks* is a (task_name, args, kwargs) tuple.
        All messages are published concurrently via asyncio.gather.

        R3.5 dedup (Unit 8): the batch-level ``unique_for`` /
        ``unique_until`` kwargs override TaskInfo defaults for every
        element (same precedence as the immediate path — _MISSING falls
        through to TaskInfo when a registry is available, None disables
        dedup, int sets the TTL). Duplicates emit
        ``TASK_DUPLICATE_DETECTED`` with ``batch_index=i`` in the
        payload and the returned list places the existing owner's
        task_id at index ``i`` — list length always equals
        ``len(tasks)``.
        """
        if not tasks:
            return []

        # Positional result slots so dup-indices and success-indices
        # stay aligned with the input list. Dup slots are filled in
        # Pass 1; success slots overwrite their uuid7 in Pass 2 (no-op)
        # and get published / signalled.
        task_ids: list[str] = [""] * len(tasks)
        publish_indices: list[int] = []  # positions that actually publish
        message_dicts: dict[int, MessageContext] = {}
        encoded_messages: dict[int, bytes] = {}
        resolved_queues: dict[int, str] = {}
        task_names: dict[int, str] = {}
        dedup_headers: dict[int, dict[str, Any]] = {}
        # R3.5 ce-review P1 #2 / COR-002 / ADV-004: track dedup flags
        # claimed during Pass 1 so any downstream failure (Pass 1 mid-
        # loop exception, Pass 2 broker publish error) can roll them
        # back. Without this, a partial batch leaves orphan flags
        # pointing at task_ids that no broker will ever see, blocking
        # re-schedule of those args for the full unique_for window.
        # Keyed by batch index; value is (dedup_hash, task_id) for
        # owner-checked release.
        claimed_flags: dict[int, tuple[str, str]] = {}

        # Resolve the middleware chain once for this whole batch so that
        # every pre_enqueue/post_enqueue hook sees the same chain instance.
        chain = self._get_chain()

        # Pass 0 (sync): validate + generate task_ids + compute dedup
        # parameters per element. Any validation error aborts before a
        # single network call, so no rollback is needed yet.
        # Layout per index:
        #   prepped[i] = {
        #       "task_name", "args", "kwargs", "task_id", "queue",
        #       "delivery_mode", "unique_until",
        #       "dedup_hash": str | None,       # sha256 hex or None
        #       "dedup_ttl_ms": int | None,     # SETNX TTL
        #   }
        prepped: list[dict[str, Any]] = []
        for i, (task_name, args, kwargs) in enumerate(tasks):
            if self._registry is not None and not self._registry.has(task_name):
                raise TaskNotFoundError(task_name)
            resolved_queue, delivery_mode = self._resolve_queue_and_delivery_mode(
                task_name, queue,
            )
            resolved_unique_for, resolved_unique_until = self._resolve_unique(
                task_name, unique_for, unique_until,
            )
            if resolved_unique_for is not None:
                dedup_hash_precomp: str | None = compute_dedup_hash(
                    task_name, args, kwargs,
                )
                ttl_ms: int | None = resolved_unique_for * 1000
            else:
                dedup_hash_precomp = None
                ttl_ms = None
            prepped.append({
                "task_name": task_name,
                "args": args,
                "kwargs": kwargs,
                "task_id": str(uuid.uuid7()),
                "queue": resolved_queue,
                "delivery_mode": delivery_mode,
                "unique_until": resolved_unique_until,
                "dedup_hash": dedup_hash_precomp,
                "dedup_ttl_ms": ttl_ms,
            })

        # Pass 1 (parallel): fire all SETNX calls concurrently. Sequential
        # loop was the main per-batch latency cost for dedup-tagged
        # batches — N * Valkey_RTT grew linearly with batch size. Gather
        # amortises that to 1 * RTT (plan PERF-01 / ce-review P3 #22).
        setnx_tasks: dict[int, asyncio.Task[str | None]] = {}
        for i, entry in enumerate(prepped):
            if entry["dedup_hash"] is None:
                continue
            setnx_tasks[i] = asyncio.create_task(
                self._backend.setnx_unique(
                    entry["dedup_hash"], entry["task_id"],
                    entry["dedup_ttl_ms"],
                    op_timeout=self._backend_op_timeout,
                ),
            )
        setnx_results: dict[int, str | None] = {}
        setnx_errors: dict[int, BaseException] = {}
        if setnx_tasks:
            await asyncio.gather(*setnx_tasks.values(), return_exceptions=True)
            for i, t in setnx_tasks.items():
                exc = t.exception()
                if exc is None:
                    setnx_results[i] = t.result()
                else:
                    setnx_errors[i] = exc

        # Pass 2 (sequential): iterate in input order, emit per-element
        # signals / build headers / register pre_enqueue. Ordering
        # matters here: TASK_DUPLICATE_DETECTED handlers see events
        # in batch-index order which matches the sequential contract.
        # If any SETNX raised (setnx_errors non-empty) we rollback all
        # successful claims and re-raise the first error.
        try:
            if setnx_errors:
                raise next(iter(setnx_errors.values()))

            for i, entry in enumerate(prepped):
                task_name = entry["task_name"]
                args = entry["args"]
                kwargs = entry["kwargs"]
                task_id = entry["task_id"]
                resolved_queue = entry["queue"]
                delivery_mode = entry["delivery_mode"]
                dedup_hash = entry["dedup_hash"]
                headers: dict[str, Any] = {}

                if dedup_hash is not None:
                    existing = setnx_results.get(i)
                    if existing is not None:
                        # Duplicate — emit signal, fill result slot, skip.
                        payload = {
                            "existing_task_id": existing,
                            "task_name": task_name,
                            "dedup_hash": dedup_hash,
                            "source": "client.schedule_many",
                            "batch_index": i,
                        }
                        await self._signal_dispatcher.dispatch(
                            Signal.TASK_DUPLICATE_DETECTED, **payload,
                        )
                        logger.info("swarmq.dedup.detected", **payload)
                        task_ids[i] = existing
                        continue
                    # Flag claimed — record for rollback + headers.
                    headers[HEADER_UNIQUE_HASH] = dedup_hash
                    headers[HEADER_UNIQUE_UNTIL] = entry["unique_until"]
                    claimed_flags[i] = (dedup_hash, task_id)

                task_ids[i] = task_id
                publish_indices.append(i)
                task_names[i] = task_name
                resolved_queues[i] = resolved_queue
                dedup_headers[i] = headers

                message = {
                    "task": {
                        "id": task_id,
                        "queue": resolved_queue,
                        "name": task_name,
                    },
                    "args": list(args),
                    "kwargs": kwargs,
                }
                encoded_messages[i] = encode_json(message)

                message_dict: MessageContext = {
                    "task_id": task_id,
                    "task_name": task_name,
                    "queue": resolved_queue,
                    "args": list(args),
                    "kwargs": kwargs,
                    "delivery_mode": delivery_mode,
                }
                message_dicts[i] = message_dict

                # pre_enqueue: exception aborts the whole batch.
                await chain.run_pre_enqueue(message_dict)
        except BaseException:
            # Pass 2 aborted — release every flag already claimed in
            # Pass 1 regardless of which batch position failed (all
            # claims are equally orphaned since we never reach publish).
            for i, entry in enumerate(prepped):
                if entry["dedup_hash"] is None:
                    continue
                if setnx_results.get(i) is None and i in setnx_tasks:
                    # Claim succeeded in Pass 1.
                    await self._rollback_unique_claim(
                        entry["dedup_hash"], entry["task_id"],
                    )
            raise

        # Pass 2: publish concurrently. Only non-dup positions participate.
        coros = [
            self._broker.publish(
                resolved_queues[i],
                encoded_messages[i],
                headers=dedup_headers[i] if dedup_headers[i] else None,
            )
            for i in publish_indices
        ]
        results = await asyncio.gather(*coros, return_exceptions=True)
        errors = [
            (publish_indices[k], r)
            for k, r in enumerate(results) if isinstance(r, BaseException)
        ]
        if errors:
            # Log partially-submitted task_ids so they can be recovered
            submitted = [
                task_ids[publish_indices[k]]
                for k, r in enumerate(results)
                if not isinstance(r, BaseException)
            ]
            if submitted:
                logger.warning(
                    "schedule_many_partial_failure",
                    submitted_task_ids=submitted,
                    failed_count=len(errors),
                )
            # Rollback flags for elements whose publish failed (the
            # successful ones still have matching messages in the broker).
            for idx, _ in errors:
                claim = claimed_flags.get(idx)
                if claim is not None:
                    await self._rollback_unique_claim(*claim)
            # Run post_enqueue + TASK_ENQUEUED only for the successful ones
            # before raising, so observers aren't left with a half-emitted state.
            for k, r in enumerate(results):
                if isinstance(r, BaseException):
                    continue
                idx = publish_indices[k]
                await chain.run_post_enqueue(message_dicts[idx])
                await self._signal_dispatcher.dispatch(
                    Signal.TASK_ENQUEUED,
                    task_id=task_ids[idx],
                    task_name=task_names[idx],
                    queue=resolved_queues[idx],
                )
            raise errors[0][1]

        # Pass 3: post_enqueue + TASK_ENQUEUED for each non-dup task.
        # Duplicates never fire TASK_ENQUEUED (they emit
        # TASK_DUPLICATE_DETECTED in Pass 1 instead).
        for i in publish_indices:
            await chain.run_post_enqueue(message_dicts[i])
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

    async def cancel(
        self, task_id: str, ttl_ms: int = _DEFAULT_CANCEL_FLAG_TTL_MS,
    ) -> bool:
        """Request cancellation of *task_id*.

        Fire-and-forget: sets the Valkey cancel flag (with TTL) and
        returns immediately.  Does **not** wait for a worker to react.

        Args:
            task_id: the task to cancel.  Must be a non-empty string;
                format is not validated otherwise — cancellation for
                unknown IDs is harmless and self-heals via TTL.
            ttl_ms: how long the flag lives in Valkey, in milliseconds.
                Defaults to 24 hours.  The SwarmQ app wrapper passes
                ``config.result_ttl * 1000`` so the flag outlives a
                typical retry window.

        Returns:
            True when the flag was newly set, False when it already
            existed (i.e. a prior cancel() call is still in flight).

        .. warning::

           **Stale-flag zombie class** (REV-P2-5, accepted limitation).
           The cancel flag lives for ``ttl_ms`` (24h by default) in
           Valkey. If the same ``task_id`` is reused within that window
           (e.g. deterministic IDs, nack-requeue redelivery after a
           crash before clear_cancel_flag ran), the stale flag will
           cancel the next delivery too. Two mitigations if you hit
           this in production: (a) keep ``task_id`` unique per
           schedule (uuid.uuid7 defaults do this), (b) shorten
           ``ttl_ms`` to less than your expected redelivery window.
           A flag-nonce scheme is tracked for a future release if
           a real caller reports the issue.
        """
        if not task_id:
            raise ValueError("task_id must be a non-empty string")
        return await self._backend.set_cancel_flag(task_id, ttl_ms)

    async def _schedule_deferred(
        self,
        task_name: str,
        args: list[Any],
        kwargs: dict[str, Any],
        *,
        resolved_queue: str,
        delay: float | None,
        eta: datetime | None,
        unique_for: int | None = None,
        unique_until: str = "completion",
    ) -> str:
        """Write a one-shot entry to the ScheduleStore and return task_id.

        The scheduler-loop in Unit 10 poll's, publishes with
        ``x-swarmq-schedfire`` header, and commits. The worker dedupes on
        the header (Unit 8), then runs the task like any other message.

        R3.5 (Unit 7): when ``unique_for`` is set, claim the dedup flag
        BEFORE the ScheduleStore insert with TTL ``(unique_for +
        fire_offset_s) * 1000`` so the flag outlives the fire delay plus
        the whole terminal window. The producer here is authoritative —
        Scheduler._fire_one observes ``source="cli_unique"`` and skips
        its own SETNX, avoiding the double-claim race. On duplicate we
        emit ``TASK_DUPLICATE_DETECTED`` and return the existing task_id
        silently, same API shape as the immediate path.
        """
        now_ms = int(time.time() * 1000)
        if eta is not None:
            fire_ts_ms = int(eta.timestamp() * 1000)
            # Allow a 1s lenience for clock skew / scheduling latency.
            if fire_ts_ms < now_ms - 1_000:
                raise ValueError(
                    f"eta is in the past: {eta.isoformat()}. "
                    "Use delay=0 for immediate execution."
                )
        else:
            assert delay is not None  # caller-side validated
            fire_ts_ms = now_ms + int(delay * 1000)

        task_id = str(uuid.uuid7())

        # --- R3.5 Unit 7: producer-side SETNX before ScheduleStore insert ---
        dedup_hash: str | None = None
        if unique_for is not None:
            # Fire offset: how long we wait before publish. Use ceiling
            # division so sub-second delays (e.g. delay=0.9) still contribute
            # a full second of TTL padding — floor would truncate to 0 and
            # let the flag expire before the task finishes running. Clamp
            # to 0 so late-past eta does not shrink TTL below unique_for.
            fire_offset_ms = max(0, fire_ts_ms - now_ms)
            fire_offset_s = (fire_offset_ms + 999) // 1000  # ceiling division
            ttl_ms = (unique_for + int(fire_offset_s)) * 1000
            dedup_hash, existing = await self._claim_unique_flag(
                task_name, tuple(args), kwargs, task_id,
                ttl_ms=ttl_ms,
                source="client.schedule_deferred",
            )
            if existing is not None:
                return existing

        schedule_name = f"oneshot:{task_id}"
        definition = ScheduleDefinition(
            schedule_name=schedule_name,
            task_name=task_name,
            args=tuple(args),
            kwargs=kwargs,
            queue=resolved_queue,
            cron=None,
            fire_ts_ms=fire_ts_ms,
            source="cli_unique" if dedup_hash is not None else "cli",
            dedup_hash=dedup_hash,
            unique_until=unique_until,
        )

        # R3.5 ce-review P1 #1 / REL-004: rollback the dedup flag if the
        # ScheduleStore insert fails — for large delays the TTL is
        # (unique_for + fire_offset) seconds, potentially hours of
        # orphan window blocking re-schedule of the same args.
        store = self._ensure_schedule_store()
        try:
            await store.add_schedule(definition)
        except BaseException:
            if dedup_hash is not None:
                await self._rollback_unique_claim(dedup_hash, task_id)
            raise
        return task_id

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _unpack(raw: bytes) -> Any:
        """Decode a raw result and return the value or raise on cancel/error.

        Priority: is_cancelled > is_error > success. Cancelled results
        raise :class:`TaskCancelledError` even when ``is_error`` is also
        set, because cancel bypasses the normal error pipeline and the
        caller needs to distinguish a user-initiated abort from a task
        failure.
        """
        data = decode_result(raw)
        if data.get("is_cancelled", False):
            task_id = data.get("task", {}).get("id")
            raise TaskCancelledError(task_id)
        if data.get("is_error"):
            raise TaskError(data.get("error_message", "unknown error"))
        return data["result"]
