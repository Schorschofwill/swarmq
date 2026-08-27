"""SwarmQ Worker — consumer loop that executes tasks."""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

import structlog

if TYPE_CHECKING:

    from swarmq.backend.valkey import ValkeyBackend
    from swarmq.broker.rabbitmq import RabbitMQBroker
    from swarmq.health import HealthServer
    from swarmq.metrics import Metrics, MetricsServer

from swarmq.config import SwarmQConfig
from swarmq.dishka import (
    request_container,
)
from swarmq.headers import (
    HEADER_UNIQUE_HASH,
    HEADER_UNIQUE_UNTIL,
    HEADER_WORKFLOW_ID,
)
from swarmq.limits import RateLimiter
from swarmq.locking import LockManager

# Lock-conflict policy. Module-level so they sit alongside other constants
# and can be referenced from tests without a class qualifier. Mirrors the
# locking.py module-level _LOCK_TTL_BUFFER_SECONDS pattern.
#

# Type alias for lock-tracking entries: (lock_name, owner_token, max_concurrency).
# Used by the per-handler acquired_locks list and the worker-wide
# _active_handler_locks registry. Single source of truth for the field
# ordering so destructuring sites are self-documenting.
_LockEntry = tuple[str, str, int]
from swarmq.contracts import PublishReceipt
from swarmq.exceptions import (
    BackendConnectionError,
    TaskNotFoundError,
)
from swarmq.middleware import (
    MessageContext,
    MiddlewareChain,
    coerce_middleware_chain_factory,
)
from swarmq.registry import TaskRegistry
from swarmq.scheduler import SchedulerLoop
from swarmq.serialization import decode_json
from swarmq.signals import SignalDispatcher
from swarmq.task import DeliveryMode, TaskInfo
from swarmq.worker.execution import TaskExecutor
from swarmq.worker.gates import GateContext, GateDeps, route_via_dlx
from swarmq.worker.gates.cancel import CancelGate
from swarmq.worker.gates.expiry import ExpiryGate
from swarmq.worker.gates.lock import (  # noqa: F401  # Re-Export der Konstanten (Tests)
    LOCK_CONFLICT_BACKOFF_BASE_MS,
    LOCK_CONFLICT_BACKOFF_CAP_MS,
    LOCK_CONFLICT_DLQ_CAP,
    LockGate,
)
from swarmq.worker.gates.rate_limit import RateLimitGate
from swarmq.worker.gates.schedfire import SchedfireGate
from swarmq.worker.lifecycle import WorkerLifecycle, WorkerState  # Re-Export (Fassade/Tests)
from swarmq.worker.outcome import Continue, DeliveryOutcome, Redelivered, Terminal
from swarmq.worker.results import ResultTail
from swarmq.worker.retry import RetryHandler
from swarmq.workflow_orchestration import (
    WorkflowAdvancer,
)

logger = structlog.get_logger()




class Worker(WorkerLifecycle):
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
        self._active_tasks: set[asyncio.Task[None]] = set()
        self._state = WorkerState.STARTING
        self._shutdown_event = asyncio.Event()
        # RateLimiter needs an already-connected Valkey client; instantiated
        # in start() after backend.connect(). See plan 005 Unit 5 Lifecycle.
        self._rate_limiter: RateLimiter | None = None
        # LockManager has the same lifecycle as RateLimiter — instantiated
        # in start() once the Valkey client is live. See plan 008 Unit 4.
        self._lock_manager: LockManager | None = None
        # R4.1 workflow advancement (U5). Same lifecycle constraint — its
        # ChordManager registers a Lua script needing a live client. None
        # means workflow-step messages are not advanced (no live client yet).
        self._workflow_advancer: WorkflowAdvancer | None = None
        # Embedded scheduler (Phase 3c). Lazy-constructed in start() after
        # the backend is connected — ScheduleStore's registered Lua
        # scripts need a live client. None means "this worker is not
        # running the scheduler loop" (only possible if explicitly
        # disabled; default is enabled).
        self._scheduler: SchedulerLoop | None = None
        # Schedfire in-flight claims. Keyed by id(message) because
        # aio_pika's IncomingMessage forbids dynamic attribute
        # assignment. Populated when _pre_start_schedfire_dedup wins
        # the inflight SETNX, read + popped by _finalize_schedfire
        # in the finally block (Unit 1).
        self._schedfire_inflight_claims: dict[int, str] = {}
        # Backup registry for graceful-shutdown lock cleanup. Keyed by a
        # per-_handle_message UUID (NOT task_id, which can collide under
        # AT_LEAST_ONCE redelivery within the same worker). Values are the
        # list of _LockEntry tuples currently held by that handler invocation.
        # _release_acquired_locks pops the entry; _execute_shutdown drains
        # anything still here.
        self._active_handler_locks: dict[uuid.UUID, list[_LockEntry]] = {}
        # U7: die Pre-Execute-Stufen als Gates (KTD5/KTD6). Rate-Limiter und
        # Lock-Manager entstehen erst in start(), daher Getter.
        gate_deps = GateDeps(
            broker=self._broker,
            backend=self._backend,
            config=self._config,
            signals=self._signal_dispatcher,
            registry=self._registry,
            rate_limiter=lambda: self._rate_limiter,
            lock_manager=lambda: self._lock_manager,
            finalize_schedfire=lambda message, terminal: self._finalize_schedfire(message, terminal=terminal),
            maybe_release_unique=self._maybe_release_unique,
            extract_valid_workflow_id=self._extract_valid_workflow_id,
            claim_schedfire=lambda message, key: self._schedfire_inflight_claims.__setitem__(id(message), key),
            register_lock=lambda handler_id, entry: self._active_handler_locks.setdefault(handler_id, []).append(entry),
        )
        self._schedfire_gate = SchedfireGate(gate_deps)
        self._cancel_gate = CancelGate(gate_deps)
        self._expiry_gate = ExpiryGate(gate_deps)
        self._rate_limit_gate = RateLimitGate(gate_deps)
        self._lock_gate = LockGate(gate_deps)
        self._executor = TaskExecutor(gate_deps)
        self._retry_handler = RetryHandler(gate_deps, workflow_advancer=lambda: self._workflow_advancer)
        self._result_tail = ResultTail(gate_deps, workflow_advancer=lambda: self._workflow_advancer)
        # Phase 3b.3: detached lock-release tasks. The shield pattern
        # around _release_acquired_locks protects against run_task
        # cancellation (inner-cancel). Against graceful-shutdown
        # (outer-cancel), the shielded task is detached here so
        # _execute_shutdown can await it before closing the backend.
        # Without this set, the orphaned release would race backend.close
        # and likely fail with BackendConnectionError, leaving the lock
        # held until TTL.
        self._detached_releases: set[asyncio.Task[None]] = set()
        # R4.5 Burst-Mode state. Initialised at start() time; the
        # monitor coroutine reads it every poll. monotonic() so wall-
        # clock jumps don't trigger a false-idle exit.
        self._last_message_received_at: float = 0.0
        self._burst_monitor_task: asyncio.Task[None] | None = None
        # Periodic metrics sampler. Populates the
        # queue_depth / dlq_messages / worker_uptime_seconds gauges (which
        # were declared but never .set()) and emits WORKER_HEARTBEAT.
        # Started in start() only when metrics are enabled.
        self._metrics_sampler_task: asyncio.Task[None] | None = None
        self._worker_started_at: float = 0.0
        # Guards the cancel-path teardown (see start()'s finally /
        # _teardown_on_exit) so it is a no-op once _execute_shutdown has run.
        self._torn_down = False
        # R5.1/R5.2 auto-wire state. Populated in
        # start() when config.metrics_port / config.health_port are set.
        # Proper Optional typing — TYPE_CHECKING import keeps the
        # runtime dependency lazy without the type-ignore-Suppressions.
        self._metrics: Metrics | None = None
        self._metrics_server: MetricsServer | None = None
        self._health_server: HealthServer | None = None

    def _get_chain(self) -> MiddlewareChain:
        """Return a live MiddlewareChain for the current message."""
        return self._middleware_chain_factory()

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
        assert self._lock_manager is not None  # Locks existieren nur nach start()
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

    async def _on_message(self, message: Any) -> None:
        """Consume callback — spawns a task so aio-pika's dispatch loop is not blocked."""
        # R4.5 Burst-Mode: every delivered message resets the idle clock.
        # _burst_monitor uses this to decide when 'all queues idle' has
        # held long enough to warrant a clean exit.
        self._last_message_received_at = time.monotonic()
        task = asyncio.create_task(self._handle_message(message))
        self._active_tasks.add(task)
        task.add_done_callback(self._active_tasks.discard)

    async def _finalize_schedfire(self, message: Any, *, terminal: bool) -> None:
        """Finalize the in-flight schedfire claim for this message.

        Called from the ``_handle_message`` finally block (and the
        pre-start-cancel terminal path). No-ops when the message was not
        a schedfire delivery we claimed (the claim is only stashed on the
        happy path in ``_pre_start_schedfire_dedup``).

        * ``terminal=True`` (success, error→DLQ, pre-start cancel): write
          the long-TTL ``sq:sched:fired:<key>`` completed marker so any
          redelivery of this fire is ack+skipped.
        * ``terminal=False`` (retry republish, rate-limit / lock-conflict
          / shutdown / result-write requeue): the fire is being
          re-delivered, so DELETE the short-TTL in-flight key instead of
          writing the completed marker. Writing
          the completed marker here would make the redelivered/retry copy
          fail ``is_schedfire_completed`` and get silently dropped.

        Failures are logged and swallowed — an orphan in-flight marker
        self-heals via its short TTL."""
        # Pop from the Worker-local claims dict (see __init__). No-op
        # when the message was not a schedfire delivery we claimed.
        schedfire_key = self._schedfire_inflight_claims.pop(id(message), None)
        if not schedfire_key:
            return
        try:
            if terminal:
                assert self._config.scheduler_dedup_ttl is not None  # Config-Default
                ttl_ms = int(self._config.scheduler_dedup_ttl * 1000)
                await self._backend.set_schedfire_dedup(schedfire_key, ttl_ms)
            else:
                await self._backend.clear_schedfire_inflight(schedfire_key)
        except BackendConnectionError as exc:
            logger.warning(
                "schedfire_finalize_failed",
                schedfire_key=schedfire_key,
                terminal=terminal,
                error=str(exc),
            )

    @staticmethod
    def _extract_valid_unique_hash(message: Any) -> str | None:
        """Read and validate the unique-hash header from an incoming message.

        Producer sites emit a sha256 hex digest (64 lowercase hex chars).
        Any message whose header deviates — missing, wrong type, wrong
        length, non-hex — is treated as "no flag": the worker skips
        release rather than forwarding unvalidated bytes to Valkey as a
        key. Defends against adversarial producers that could otherwise
        amplify a malformed header into a large per-message Valkey key
        (plan Risks / R3.5).
        """
        unique_hash = (message.headers or {}).get(HEADER_UNIQUE_HASH)
        if not isinstance(unique_hash, str) or len(unique_hash) != 64:
            return None
        # Cheap manual hex check — re.match on a 64-char string is
        # faster than compiling a regex but still O(n).
        if not all(c in "0123456789abcdef" for c in unique_hash):
            return None
        return unique_hash

    @staticmethod
    def _extract_valid_workflow_id(message: Any) -> str | None:
        """Read and validate the workflow-id header from an incoming message.

        Workflow ids are minted as ``str(uuid.uuid7())`` (client/worker side)
        — a canonical UUID string: 36 chars, lowercase hex with dashes at the
        8-4-4-4-12 positions. Any header that deviates — missing, wrong type,
        wrong length, non-hex, misplaced dashes — is treated as "no workflow":
        the caller skips advancement rather than forwarding unvalidated bytes
        to Valkey as a workflow-state key. Mirrors ``_extract_valid_unique_hash``:
        never mark a workflow failed on a malformed id.
        """
        wf_id = (message.headers or {}).get(HEADER_WORKFLOW_ID)
        if not isinstance(wf_id, str) or len(wf_id) != 36:
            return None
        for i, c in enumerate(wf_id):
            if i in (8, 13, 18, 23):
                if c != "-":
                    return None
            elif c not in "0123456789abcdef":
                return None
        return wf_id

    async def _maybe_release_unique(
        self, message: Any, task_id: str, task_info: TaskInfo | None = None,
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

    async def _publish_workflow_step(self, queue: str, body: bytes, headers: dict[str, Any]) -> PublishReceipt:
        """Publish a next workflow step. Thin adapter so the advancer stays
        broker-agnostic; broker.publish runs on aio_pika's publisher-confirms
        channel, so the awaited call returns only after the broker confirms
        the enqueue. Seit U20 gibt er den ``PublishReceipt`` zurück, den
        ``WorkflowAdvancer.mark_enqueued`` verlangt (publish-then-mark, AE5)."""
        return await self._broker.publish(queue, body, headers=headers)

    def _resolve_workflow_queue(self, task_name: str) -> str:
        """Resolve the target queue for a workflow step from the registry,
        applying the priority suffix the same way Client does."""
        entry = self._registry.get_entry(task_name)
        queue = entry.task_info.queues[0]
        if entry.task_info.priority != "normal":
            queue = f"{queue}.{entry.task_info.priority}"
        return queue

    async def _handle_message(self, message: Any) -> None:  # noqa: C901, PLR0915  # U19 (Runner)
        """Core message handler logic."""
        # Size cap BEFORE the decode. msgspec has a
        # built-in recursion-depth guard but no size guard — a
        # compromised/misconfigured producer could otherwise make the
        # worker decode an arbitrarily large body fully into memory
        # before any validation runs. Oversized → DLQ route, no decode.
        if len(message.body) > self._config.max_message_body_bytes:
            logger.error(
                "message_body_too_large",
                body_bytes=len(message.body),
                cap_bytes=self._config.max_message_body_bytes,
                hint="raise SWARMQ_MAX_MESSAGE_BODY_BYTES if legitimate",
            )
            await route_via_dlx(self._broker, message)
            return

        # 1-2. Deserialize message body
        try:
            payload = decode_json(message.body)
            task_meta = payload["task"]
            task_id = task_meta["id"]
            queue = task_meta["queue"]
            task_name = task_meta["name"]
            args = payload["args"]
            kwargs = payload["kwargs"]
            # Validate shapes before run(*args,
            # **kwargs). A wrong-typed payload from a misbehaving producer
            # (e.g. args="hello") would otherwise unpack a string
            # character-by-character into positional args and run the task
            # with garbage instead of failing. Treat as a malformed
            # message: route to DLQ (nack requeue=False), don't redeliver.
            if not isinstance(args, list):
                raise TypeError(
                    f"task 'args' must be a list, got {type(args).__name__}"
                )
            if not isinstance(kwargs, dict):
                raise TypeError(
                    f"task 'kwargs' must be a dict, got {type(kwargs).__name__}"
                )
        except Exception as exc:
            logger.error("deserialize_failed", error=str(exc))
            # Plan Unit 11 Risks: deserialize-fail cannot release (task_id
            # unknown). Accepted limitation — flag self-heals via TTL.
            await route_via_dlx(self._broker, message)
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
        ctx = GateContext(task_id=task_id, task_name=task_name, queue=queue, args=args, kwargs=kwargs)
        if await self._schedfire_gate.apply(message, ctx) is not Continue:
            return

        # 2b. Pre-start cancel check (Phase 3b.3). Runs BEFORE semaphore
        # acquire so a cancelled message doesn't waste a slot, BEFORE
        # rate-limit and lock so those stages are not entered for
        # cancelled work, and BEFORE FIRE_AND_FORGET early-ack in the
        # main try/finally. The helper is responsible for its own
        # structlog cleanup on the cancel branch — the main finally at
        # the end of this method is not reached on this branch.
        if await self._cancel_gate.apply(message, ctx) is not Continue:
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
        ctx.handler_id = handler_id
        ctx.acquired_locks = acquired_locks
        # R3.5 Unit 11 state: task_info visible in finally even if
        # lookup fails.
        task_info: TaskInfo | None = None
        # U19 (KTD5): ein typisiertes Zustellungs-Ergebnis statt zweier
        # Booleans. Default = terminal mit Freigabe im finally; jeder
        # nicht-terminale Zweig setzt ``Redelivered`` (Dedup-Flag bleibt,
        # In-Flight-Claim wird gelöscht), Expiry setzt
        # ``Terminal.never_release()``. Die Richtung ist bewusst so gewählt,
        # dass ein vergessener Zweig über-freigibt (owner-check-sicher) statt
        # unter-freigibt.
        outcome: DeliveryOutcome = Terminal.release_in_finally()
        # FIRE_AND_FORGET: nach dem letzten Gate wird bestätigt; danach darf
        # kein Zweig mehr ein Broker-Verb auslösen (Modus acked_before_execute).
        acked_before_execute = False
        # Optional Dishka REQUEST scope. Entered just before the task
        # lifecycle (below) and torn down in the finally via the same
        # detached-shield pattern as lock release. None when DI is inactive.
        _di_scope: Any = None
        _di_token: Any = None
        try:
            # 3b. Check if shutdown is in progress — requeue message
            if self._shutdown_event.is_set():
                # nack(requeue=True): broker redelivers with headers
                # intact → keep the flag so redelivery can release.
                outcome = Redelivered
                await self._broker.redeliver(message)
                return

            # 4-5. Look up task in registry
            try:
                task_class = self._registry.get(task_name)
            except TaskNotFoundError:
                logger.error("task_not_found")
                await route_via_dlx(self._broker, message)
                return

            task_info = task_class.task_info
            ctx.task_info = task_info
            ctx.task_class = task_class

            # R4.4 Task Expiry: check max_age BEFORE any execution
            # stages. An expired message takes the DLQ path without
            # entering rate-limit, lock, pre_execute, etc. The enqueue
            # time is stamped at publish (see Client.schedule +
            # schedule_many), so this runs on every delivery including
            # retry-after-delay-queue — each retry's clock starts from
            # the ORIGINAL enqueue, which is the intended semantic
            # (expiry is a wall-time contract, not a per-delivery one).
            outcome = await self._expiry_gate.apply(message, ctx)
            if outcome is not Continue:
                return
            outcome = Terminal.release_in_finally()

            # 5a. Rate limit check — runs BEFORE the FIRE_AND_FORGET ack
            # so a Valkey or broker hiccup cannot drop a message that has
            # already been acked. Rate-limited tasks never reach
            # pre_execute or middleware: they are republished into the
            # delay queue and ack'd as a single transactional unit.
            # Any "handled" return is a requeue (delay queue or nack=True)
            # — the original message's x-swarmq-unique-hash header is
            # preserved in the republished copy (Unit 11 header
            # preservation), so the flag must stay claimed.
            outcome = await self._rate_limit_gate.apply(message, ctx)
            if outcome is not Continue:
                return
            outcome = Terminal.release_in_finally()

            # 5b. Lock check — same FIRE_AND_FORGET-ordering discipline as
            # rate-limit. Conflict path: requeue with exponential backoff
            # until the per-message cap, then DLQ. Acquired locks are
            # released in the finally below.
            # Terminal DLQ sub-paths inside the gate release the flag
            # inline (render-fail, empty-render, conflict-cap); non-
            # terminal sub-paths requeue and preserve the header (siehe
            # gates/lock.py zur Outcome-Wahl).
            outcome = await self._lock_gate.apply(message, ctx)
            if outcome is not Continue:
                return
            outcome = Terminal.release_in_finally()

            # 5c. R3.5 Unit 10: start-mode
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

            # 5d. FIRE_AND_FORGET: ack immediately before execution — ab hier
            # ist kein Broker-Verb mehr zulässig (acked_before_execute).
            if task_info.delivery is DeliveryMode.FIRE_AND_FORGET:
                await self._broker.ack(message)
                acked_before_execute = True

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
                # Nach dem FAF-Ack gibt es nichts mehr zu nacken (KTD5); vorher
                # führt der heutige DLX-Fallback über die Broker-Konfiguration.
                if not acked_before_execute:
                    await route_via_dlx(self._broker, message)
                return

            # 6. Ausführung (U8): Lifecycle, Timeouts, Cancel-Monitor,
            # Middleware-Hooks und Signale leben in worker/execution.py.
            execution = await self._executor.execute(ctx, message_dict, chain)
            is_error = execution.is_error
            is_cancelled = execution.is_cancelled
            error_message = execution.error_message
            task_exception = execution.task_exception
            _di_scope = execution.di_scope
            _di_token = execution.di_token


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
            #
            # Deliberate (user-approved) behaviour change: max_retries=0 errors now
            # route through _evaluate_retry like every other failure —
            # retry_count(0) >= max_retries(0) is immediately exhausted,
            # so _send_to_dlq runs: error result stored (get_result()
            # contract unchanged), message copied to {queue}.XQ, TASK_DLQ
            # dispatched. Previously this branch was gated on
            # max_retries > 0, which silently exempted the most common
            # "no recovery" config from DLQ inspection and alerting.
            if (
                is_error
                and task_exception is not None
                and not is_cancelled
                and task_info.delivery is DeliveryMode.AT_LEAST_ONCE
            ):
                # Post-execution cancel guard: a cancel that
                # arrived WHILE post_execution was running or between
                # run_task.done() and this branch wouldn't have been
                # caught by the cancel-monitor (which stops polling as
                # soon as run_task.done() fires). Re-check the flag
                # here so a cancel that raced into the tail of the
                # pipeline suppresses the retry. Timeout-wrapped like
                # every other cancel-path backend call.
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
                    # is_cancelled=True result, clear flag, ack. Der
                    # Ergebnis-Tail liest das ExecutionResult (U20), der
                    # DI-Teardown die lokalen Aliase — beide umstellen.
                    execution.is_cancelled = is_cancelled = True
                    execution.is_error = is_error = False
                    execution.error_message = error_message = ""
                    execution.task_exception = task_exception = None
                else:
                    # U8: Retry-Bewertung und Dead-Letter leben in
                    # worker/retry.py. Beide Wege sind fuer den finally
                    # ``Redelivered`` (Dead-Letter gibt inline frei; das
                    # In-Flight-Loeschen ist dort folgenlos).
                    outcome = await self._retry_handler.evaluate(
                        message, ctx, task_exception, error_message, message_dict, chain
                    )
                    return

            # 7.-11. Ergebnis-Tail (U20): Speichern, Benachrichtigung,
            # Cancel-Flag, Workflow-Advance-Seam und ALO-Ack leben in
            # worker/results.py.
            outcome = await self._result_tail.finish(message, ctx, execution)
            return

        finally:
            # 12-pre. Finalize the schedfire claim if this message was a
            # schedfire delivery we claimed in-flight. On a terminal
            # outcome this writes the completed marker (a redelivery
            # racing our in-flight-key expiry must see it, Unit 1);
            # on a non-terminal outcome (retry/requeue) it instead clears
            # the in-flight key so the redelivered copy can re-run rather
            # than being skipped as "completed".
            # U19: der finally liest die Outcome-Felder. ``Redelivered`` (kein
            # Terminal) → In-Flight-Claim löschen, keine Freigabe.
            terminal = outcome if isinstance(outcome, Terminal) else None
            await self._finalize_schedfire(
                message, terminal=terminal is not None and terminal.finalize_schedfire,
            )

            # 12-pre-b. R3.5 Unit 11: completion-mode dedup release.
            # No-op in start-mode (Unit 10 already released at pickup),
            # no-op when the message has no x-swarmq-unique-hash header.
            # All main-body terminal paths (success, error, cancel,
            # DLQ, lock-conflict-cap, task_not_found nack=False,
            # lock-render-fail, middleware-factory-fail) flow through
            # here with a Terminal outcome whose release_unique is True.
            # Non-terminal paths (rate-limit requeue, lock-conflict
            # requeue, retry delay queue, store_result nack=True,
            # shutdown nack=True) also hit this finally, but they
            # preserve the x-swarmq-unique-hash header in the
            # republished message and report ``Redelivered`` so the
            # next delivery re-runs the owner-check pipeline.
            if terminal is not None and terminal.release_unique:
                await self._maybe_release_unique(
                    message, task_id, task_info,
                )

            # 12-pre. Close the Dishka REQUEST scope (if opened), driving
            # __aexit__ with the captured task exception so yield-based
            # finalizers can commit on success / roll back on failure. On a
            # user-cancel (no task_exception), a CancelledError is passed so
            # finalizers treat it as an abort. Uses the same detached-shield
            # pattern as lock release: schedule the exit as its own tracked
            # Task so an outer (shutdown) cancel into this finally does not
            # abort the finalizer mid-await — _execute_shutdown drains
            # _detached_releases before closing the backend.
            if _di_scope is not None:
                if _di_token is not None:
                    request_container.reset(_di_token)
                _di_exc = (
                    asyncio.CancelledError() if is_cancelled else task_exception
                )
                _di_et = type(_di_exc) if _di_exc is not None else None
                _di_tb = _di_exc.__traceback__ if _di_exc is not None else None
                _di_exit = asyncio.create_task(
                    _di_scope.__aexit__(_di_et, _di_exc, _di_tb)
                )
                self._detached_releases.add(_di_exit)
                _di_exit.add_done_callback(self._detached_releases.discard)
                try:
                    await asyncio.shield(_di_exit)
                except asyncio.CancelledError:
                    # Outer-cancel reached us. ``_di_exit`` is unaffected
                    # (shield protected it) and tracked in
                    # _detached_releases, which _execute_shutdown awaits
                    # before backend.close. Re-raise to observe the cancel.
                    raise

            # 12a. Release any task-locks acquired by this handler.
            #
            # Previous implementation wrapped the release in
            # ``asyncio.ensure_future(asyncio.shield(...))`` and tracked
            # the SHIELD-wrapper task in _detached_releases. On outer-
            # cancel the shield-wrapper got cancelled, but its inner
            # coroutine kept running DETACHED and was never tracked —
            # _execute_shutdown would see an already-done wrapper and
            # close the backend while the real release was still in
            # flight, leaving the lock held.
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
