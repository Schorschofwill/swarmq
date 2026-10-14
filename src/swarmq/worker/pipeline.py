"""SwarmQ Worker — consumer loop that executes tasks."""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, cast, override

if TYPE_CHECKING:
    from swarmq.health import HealthServer
    from swarmq.metrics import Metrics, MetricsServer

from swarmq.config import SwarmQConfig

# Lock-conflict policy. Module-level so they sit alongside other constants
# and can be referenced from tests without a class qualifier. Mirrors the
# locking.py module-level _LOCK_TTL_BUFFER_SECONDS pattern.
#
# Type alias for lock-tracking entries: (lock_name, owner_token, max_concurrency).
# Used by the per-handler acquired_locks list and the worker-wide
from swarmq.contracts import ExecutionGuardStore, PublishReceipt
from swarmq.contracts.coordination import LockStore, LockToken, RateLimitStore
from swarmq.dishka import (
    request_container,
)
from swarmq.exceptions import (
    BackendConnectionError,
)
from swarmq.headers import HEADER_RETRY_COUNT, HEADER_UNIQUE_UNTIL
from swarmq.logging import _error_fields, _task_log_context, get_logger
from swarmq.middleware import (
    MessageContext,
    MiddlewareChain,
    coerce_middleware_chain_factory,
)
from swarmq.registry import TaskRegistry
from swarmq.scheduler import SchedulerLoop
from swarmq.signals import SignalDispatcher
from swarmq.task import DeliveryMode, Task, TaskInfo
from swarmq.worker import decode, redelivery
from swarmq.worker.execution import TaskExecutor, lifecycle_level, task_logger
from swarmq.worker.finalize import finalize_schedfire, maybe_release_unique
from swarmq.worker.gates import (
    GateContext,
    GateDeps,
    WorkerBackend,
    WorkerBroker,
    redeliver_in_place,
)
from swarmq.worker.gates.cancel import CancelGate
from swarmq.worker.gates.delivery_count import DeliveryCountGate
from swarmq.worker.gates.execution_guard import ExecutionGuard
from swarmq.worker.gates.expiry import ExpiryGate
from swarmq.worker.gates.lock import (  # noqa: F401  # Re-Export der Konstanten (Tests)
    LOCK_CONFLICT_BACKOFF_BASE_MS,  # pyright: ignore[reportUnusedImport]  # Re-Export fuer Tests
    LOCK_CONFLICT_BACKOFF_CAP_MS,  # pyright: ignore[reportUnusedImport]  # Re-Export fuer Tests
    LockGate,
    release_acquired_locks,
)
from swarmq.worker.gates.rate_limit import RateLimitGate
from swarmq.worker.gates.reject import reject_to_dead_letter
from swarmq.worker.gates.schedfire import SchedfireGate
from swarmq.worker.gates.unknown_task import lookup_task_or_route
from swarmq.worker.guarded_broker import GuardedBroker
from swarmq.worker.lifecycle import WorkerLifecycle, WorkerState  # Re-Export (Fassade/Tests)
from swarmq.worker.lock_keeper import LockKeeper
from swarmq.worker.outcome import Continue, DeliveryOutcome, Redelivered, Terminal
from swarmq.worker.results import ResultTail
from swarmq.worker.retry import RetryHandler
from swarmq.worker.settle import contain_delivery_errors
from swarmq.worker.shutdown import WorkerShutdown
from swarmq.workflow_orchestration import (
    WorkflowAdvancer,
)

logger = get_logger(__name__)


class Worker(WorkerLifecycle, WorkerShutdown):
    def __init__(
        self,
        broker: WorkerBroker,
        backend: WorkerBackend,
        registry: TaskRegistry,
        config: SwarmQConfig,
        middleware_chain: MiddlewareChain | Callable[[], MiddlewareChain] | None = None,
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
        self._middleware_chain_factory = coerce_middleware_chain_factory(middleware_chain)
        self._signal_dispatcher = signal_dispatcher if signal_dispatcher is not None else SignalDispatcher()
        self._semaphore = asyncio.Semaphore(config.concurrency)
        self._active_tasks: set[asyncio.Task[None]] = set()
        self._state = WorkerState.STARTING
        self._shutdown_event = asyncio.Event()
        # RateLimiter needs an already-connected Valkey client; instantiated
        # in start() after backend.connect(). See plan 005 Unit 5 Lifecycle.
        self._rate_limiter: RateLimitStore | None = None
        # LockManager has the same lifecycle as RateLimiter — instantiated
        # in start() once the Valkey client is live. See plan 008 Unit 4.
        self._lock_manager: LockStore | None = None
        # R4.1 workflow advancement (U5). Same lifecycle constraint — its
        # ChordManager registers a Lua script needing a live client. None
        # means workflow-step messages are not advanced (no live client yet).
        self._workflow_advancer: WorkflowAdvancer | None = None
        # Embedded scheduler (Phase 3c). Lazy-constructed in start() after
        # the backend is connected — ScheduleStore's registered Lua
        # scripts need a live client. None means "not started yet" (or
        # already torn down). With ``scheduler="off"`` the loop exists but
        # only reports and never runs for leader (P1).
        self._scheduler: SchedulerLoop | None = None
        # Backup registry for graceful-shutdown lock cleanup. Keyed by a
        # per-_handle_message UUID (NOT task_id, which can collide under
        # AT_LEAST_ONCE redelivery within the same worker). Values are the
        # list of LockTokens currently held by that handler invocation.
        # _release_acquired_locks pops the entry; _execute_shutdown drains
        # anything still here.
        self._active_handler_locks: dict[uuid.UUID, list[LockToken]] = {}
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
            finalize_schedfire=lambda ctx, terminal: self._finalize_schedfire(ctx, terminal=terminal),
            maybe_release_unique=self._maybe_release_unique,
            extract_valid_workflow_id=self._extract_valid_workflow_id,
            register_lock=lambda handler_id, entry: self._active_handler_locks.setdefault(handler_id, []).append(entry),
            # BZ-29: Pause vor ``redeliver``, endet beim Shutdown sofort.
            redeliver_pause=lambda delay_s: redelivery.wait_unless_shutdown(delay_s, self._shutdown_event),
        )
        # BZ-25 (Plan 0.12.0, MR 13): verlaengert gehaltene Locks, solange der
        # Handler lebt; endet mit der letzten Lease oder beim Schliessen.
        self._lock_keeper = LockKeeper(lambda: self._lock_manager, op_timeout_s=config.backend_op_timeout)
        gate_deps.keep_lock = lambda token, ttl_ms, ctx: self._lock_keeper.keep(
            token, ttl_ms, task_id=ctx.task_id, task_name=ctx.task_name
        )
        self._schedfire_gate = SchedfireGate(gate_deps)
        self._cancel_gate = CancelGate(gate_deps)
        self._expiry_gate = ExpiryGate(gate_deps)
        self._rate_limit_gate = RateLimitGate(gate_deps)
        self._lock_gate = LockGate(
            gate_deps,
            dead_letter=lambda d, c, m, t, h, f: self._retry_handler.dead_letter(
                d, c, m, t, extra_headers=h, on_publish_failure=f
            ),
        )
        # Am Zustelllimit stellt eine gescheiterte Workflow-Fehlmarkierung
        # nicht mehr neu zu (MR 3): auf 4.3 zaehlte der ``nack`` nicht.
        self._delivery_count_gate = DeliveryCountGate(
            gate_deps, dead_letter=lambda d, c, m, t: self._retry_handler.dead_letter(d, c, m, t, may_redeliver=False)
        )
        self._gate_deps = gate_deps  # CPU-Modus tauscht den Executor beim Start (compute.wiring)
        self._executor = TaskExecutor(gate_deps)
        self._lifecycle_level = lifecycle_level(config)  # einmal je Worker (KTD8)
        self._result_tail = ResultTail(gate_deps, workflow_advancer=lambda: self._workflow_advancer)
        self._retry_handler = RetryHandler(
            gate_deps, workflow_advancer=lambda: self._workflow_advancer, finish=self._result_tail.finish
        )
        gate_deps.fail_workflow = self._retry_handler.fail_workflow  # BZ-13: terminale Gate-Pfade
        # Ausfuehrungsschutz (KTD5): der Store entsteht in start(); Gates,
        # Retry und Ergebnis-Tail sprechen den protokollierenden Wrapper, der
        # die Marker nach dem ausgefuehrten Verb setzt.
        self._guard_store: ExecutionGuardStore | None = None
        self._execution_guard = ExecutionGuard(
            gate_deps,
            store=lambda: self._guard_store,
            dead_letter=lambda d, c, m, t: self._retry_handler.dead_letter(d, c, m, t),  # noqa: PLW0108 - spaete Bindung und nur vier Argumente weiterreichen
            on_event=self._count_guard_event,
        )
        gate_deps.broker = cast("WorkerBroker", GuardedBroker(broker, self._execution_guard))
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

    def _count_guard_event(self, event: str) -> None:
        if self._metrics is not None:
            self._metrics.count_execution_guard_event(event)

    @override
    def _get_chain(self) -> MiddlewareChain:
        """Return a live MiddlewareChain for the current message."""
        return self._middleware_chain_factory()

    async def _release_acquired_locks(
        self,
        handler_id: uuid.UUID,
        acquired_locks: list[LockToken],
    ) -> None:
        """``gates.lock.release_acquired_locks`` mit dem Logger dieses Moduls (Q17)."""
        assert self._lock_manager is not None  # Locks existieren nur nach start()
        await release_acquired_locks(
            self._lock_manager,
            self._active_handler_locks,
            handler_id,
            acquired_locks,
            log=logger,
            drop=self._lock_keeper.drop,
        )

    @override
    async def _on_message(self, message: Any) -> None:
        """Consume callback — spawns a task so aio-pika's dispatch loop is not blocked."""
        # R4.5 Burst-Mode: every delivered message resets the idle clock.
        # _burst_monitor uses this to decide when 'all queues idle' has
        # held long enough to warrant a clean exit.
        self._last_message_received_at = time.monotonic()
        task = asyncio.create_task(self._handle_message(message))
        self._active_tasks.add(task)
        task.add_done_callback(self._active_tasks.discard)

    async def _finalize_schedfire(self, ctx: GateContext, *, terminal: bool) -> bool:
        """``finalize.finalize_schedfire`` mit dem Logger dieses Moduls (Q17)."""
        return await finalize_schedfire(
            ctx, terminal=terminal, backend=self._backend, config=self._config, gate_deps=self._gate_deps, log=logger
        )

    async def _release_schedfire_claim(self, ctx: GateContext) -> None:
        await self._finalize_schedfire(ctx, terminal=False)

    @staticmethod
    def _extract_valid_unique_hash(message: Any) -> str | None:
        return decode.valid_unique_hash(message)

    @staticmethod
    def _extract_valid_workflow_id(message: Any) -> str | None:
        return decode.valid_workflow_id(message)

    async def _maybe_release_unique(self, message: Any, ctx: GateContext, task_info: TaskInfo | None = None) -> None:
        """Siehe ``finalize.maybe_release_unique``."""
        unique_hash = self._extract_valid_unique_hash(message)
        await maybe_release_unique(
            message, ctx, task_info, unique_hash=unique_hash, backend=self._backend, config=self._config
        )

    @override
    async def _publish_workflow_step(self, queue: str, body: bytes, headers: dict[str, Any]) -> PublishReceipt:
        """Publish a next workflow step. Thin adapter so the advancer stays
        broker-agnostic; broker.publish runs on aio_pika's publisher-confirms
        channel, so the awaited call returns only after the broker confirms
        the enqueue. Seit U20 gibt er den ``PublishReceipt`` zurück, den
        ``WorkflowAdvancer.mark_enqueued`` verlangt (publish-then-mark, AE5)."""
        return await self._broker.publish(queue, body, headers=headers)

    @override
    def _resolve_workflow_queue(self, task_name: str) -> str:
        """Resolve the target queue for a workflow step from the registry,
        applying the priority suffix the same way Client does."""
        entry = self._registry.get_entry(task_name)
        queue = entry.task_info.queues[0]
        if entry.task_info.priority != "normal":
            queue = f"{queue}.{entry.task_info.priority}"
        return queue

    async def _decode_and_validate(self, message: Any) -> tuple[str, str, str, list[Any], dict[str, Any]] | None:
        return await decode.decode_and_validate(self._gate_deps, message)

    async def _check_late_cancel(self, task_id: str) -> bool:
        """Post-execution cancel guard: ein Cancel, der während
        ``post_execution`` oder zwischen ``run_task.done()`` und dem
        Retry-Zweig eintraf, entgeht dem Cancel-Monitor — hier erneut
        prüfen. Backend-Hiccup zählt als „nicht storniert" (die nächste
        Zustellung honoriert das Flag im Pre-Start-Check)."""
        try:
            return await self._backend.is_cancelled(
                task_id,
                op_timeout=self._config.backend_op_timeout,
            )
        except BackendConnectionError:
            return False

    async def _teardown_di_scope(
        self,
        di_scope: Any,
        di_token: Any,
        is_cancelled: bool,
        task_exception: BaseException | None,
        teardown: list[asyncio.Task[None]],
    ) -> None:
        """Dishka-REQUEST-Scope schließen — ``__aexit__`` mit der
        eingefangenen Exception (Commit/Rollback der yield-Finalizer);
        User-Cancel übergibt CancelledError. Detached-Shield-Muster wie
        beim Lock-Release: der Exit läuft als eigener getrackter Task,
        ein Outer-Cancel in den finally bricht ihn nicht ab
        (``_execute_shutdown`` drained ``_detached_releases``). Der Task landet
        in ``teardown``: laeuft er nach einem Outer-Cancel weiter, gibt der
        ``finally`` Lock und Semaphor erst nach seinem Ende frei (KTD6)."""
        if di_token is not None:
            request_container.reset(di_token)
        _di_exc = asyncio.CancelledError() if is_cancelled else task_exception
        _di_et = type(_di_exc) if _di_exc is not None else None
        _di_tb = _di_exc.__traceback__ if _di_exc is not None else None
        inner = self._track(di_scope.__aexit__(_di_et, _di_exc, _di_tb))
        teardown.append(inner)
        await asyncio.shield(inner)

    def _track(self, call: Awaitable[None]) -> asyncio.Task[None]:
        """Als eigenen Task starten und in ``_detached_releases`` tracken: der
        Shutdown wartet vor ``backend.close`` darauf."""
        inner = asyncio.ensure_future(call)
        self._detached_releases.add(inner)
        inner.add_done_callback(self._detached_releases.discard)
        return inner

    async def _detached(self, call: Awaitable[None]) -> None:
        """Als eigener getrackter Task + geshieldeter Await (siehe
        ``_detached_lock_release``): ein Outer-Cancel bricht ihn nicht ab."""
        await asyncio.shield(self._track(call))

    async def _detached_lock_release(self, handler_id: uuid.UUID, acquired_locks: list[LockToken]) -> None:
        """Lock-Release als eigener getrackter Task + geshieldeter Await.

        Ein ``ensure_future(shield(...))``-Wrapper ließe die innere Coroutine
        bei Outer-Cancel detached und ungetrackt weiterlaufen —
        ``_execute_shutdown`` würde den Backend-Client schließen, während
        der Release noch läuft, und der Lock bliebe bis zur TTL gehalten.
        Deshalb: inneren Task tracken, nur den Await shielden; ein
        Outer-Cancel erreicht den Aufrufer, ``_execute_shutdown`` wartet
        über ``_detached_releases`` auf den inneren Task."""
        await self._detached(self._release_acquired_locks(handler_id, acquired_locks))

    async def _release_unique_on_start(self, message: Any, task_id: str, task_info: TaskInfo) -> None:
        """R3.5 Unit 10: start-mode dedup release — AFTER rate-limit/lock
        checks (a requeue republishes with the header intact; releasing
        earlier would let a second schedule() win the SETNX and run twice)
        and BEFORE the FIRE_AND_FORGET ack. Owner-check in
        unique_release.lua protects against a foreign-owner DEL;
        BackendConnectionError is swallowed inside release_unique
        (log-only) — a Valkey flap must not turn dedup into a liveness
        hazard."""
        unique_hash = self._extract_valid_unique_hash(message)
        headers: dict[str, Any] = message.headers or {}
        header_unique_until = headers.get(HEADER_UNIQUE_UNTIL)
        effective_unique_until = header_unique_until or task_info.unique_until
        if unique_hash and effective_unique_until == "start":
            await self._backend.release_unique(
                unique_hash,
                task_id,
                op_timeout=self._config.backend_op_timeout,
            )

    @staticmethod
    def _build_message_dict(ctx: GateContext, message: Any) -> MessageContext:
        """Canonical middleware payload + retry_count from the headers."""
        assert ctx.task_info is not None
        return {
            "task_id": ctx.task_id,
            "task_name": ctx.task_name,
            "queue": ctx.queue,
            "args": ctx.args,
            "kwargs": ctx.kwargs,
            "delivery_mode": ctx.task_info.delivery.value,
            "retry_count": message.headers.get(HEADER_RETRY_COUNT, 0),
        }

    async def _resolve_chain_or_route(
        self,
        message: Any,
        acked_before_execute: bool,
        *,
        task_id: str | None = None,
        task_name: str | None = None,
    ) -> Any:
        """Resolve the middleware chain once per message (add_middleware()
        after Worker construction stays visible; list-copy cost is
        negligible vs broker roundtrip). A broken user-supplied factory
        must not crash the consumer loop — DLX-route unless the FAF ack
        already happened (KTD5: no verb after acked_before_execute). Der
        Body ist dekodiert, das Signal trägt Kennung und Name (B14)."""
        try:
            return self._get_chain()
        except Exception as exc:  # noqa: BLE001 - Nutzer-Factory darf Consumer-Loop nicht stoppen
            logger.error("middleware_chain_factory_failed", **_error_fields(exc))
            if not acked_before_execute:
                await reject_to_dead_letter(
                    self._gate_deps,
                    message,
                    reason="middleware_chain_factory",
                    error=str(exc),
                    task_id=task_id,
                    task_name=task_name,
                )
            return None

    async def _pre_pipeline_gates(self, message: Any, ctx: GateContext) -> bool:
        """Ack-sichere Prä-Pipeline-Stufen VOR Semaphore und try/finally:
        2a Schedfire-Dedup (nur ein SETNX), 2b Pre-Start-Cancel, 2c Delivery-
        Count (Plan 2026-09-03-001, KTD10) — alle vor Semaphore, Rate-Limit/
        Lock und dem FIRE_AND_FORGET-Früh-Ack. Den Task-Kontext stellt der
        Scope in ``_handle_message`` wieder her (KTD13), nicht die Gates.

        Diese Gates liegen vor dem ``try/finally``: endet die Zustellung nach
        gewonnenem Schedfire-Claim hier (Zurueckstellung, Dead-Letter,
        Ausnahme eines Gates), gibt dieser Zweig den Claim frei -- sonst saehe
        die Kopie ihn als Duplikat. Ein Gate, das selbst finalisiert hat
        (Pre-Start-Cancel), hat den Claim schon vom Kontext genommen (No-op)."""
        for gate in (self._schedfire_gate, self._cancel_gate, self._delivery_count_gate):
            try:
                outcome = await gate.apply(message, ctx)
            except BaseException:
                # Ein Gate-Fehler endet ebenfalls vor dem finally: Claim
                # nicht-terminal freigeben, dann weiterwerfen. Geshieldet und
                # getrackt (wie der Lock-Release): ein zweiter Abbruch
                # (Shutdown) unterbricht die Freigabe nicht, der Shutdown
                # wartet vor ``backend.close`` auf sie.
                await self._detached(self._release_schedfire_claim(ctx))
                raise
            if outcome is not Continue:
                await self._finalize_schedfire(ctx, terminal=False)
                return False
        return True

    async def _lookup_task_or_route(self, message: Any, ctx: GateContext) -> type[Task] | DeliveryOutcome:
        """``gates.unknown_task.lookup_task_or_route`` mit dem Logger dieses Moduls (Q17)."""
        return await lookup_task_or_route(
            self._registry,
            self._gate_deps,
            message,
            ctx,
            log=logger,
            dead_letter=lambda d, c, m, t, h, f: self._retry_handler.dead_letter(
                d, c, m, t, extra_headers=h, on_publish_failure=f
            ),
        )

    def _log_context(self, message: Any, ctx: GateContext) -> Any:
        """Task-Kontext fuer alle Logaufrufe dieser Zustellung (KTD13)."""
        return _task_log_context(
            task_id=ctx.task_id,
            task_name=ctx.task_name,
            queue=ctx.queue,
            worker_id=self._config.worker_identity,
            workflow_id=ctx.workflow_id,
            attempt=decode.attempt_number(message),
        )

    async def _finalize_delivery(
        self, message: Any, outcome: DeliveryOutcome, ctx: GateContext, task_info: TaskInfo | None
    ) -> None:
        """Finally-Kopf des Runners (U19: der finally liest das Outcome).

        Schedfire: Terminal mit ``finalize_schedfire`` schreibt den
        Completed-Marker (eine Redelivery, die in den Ablauf des
        In-Flight-Keys rast, muss ihn sehen); nicht-terminal löscht den
        In-Flight-Key, damit die Kopie erneut laufen kann.

        Unique (R3.5 Unit 11, completion-mode): alle terminalen Hauptpfade
        laufen mit ``release_unique=True`` hier durch; nicht-terminale
        Pfade (Requeues) erhalten den ``x-swarmq-unique-hash``-Header in
        der Kopie und melden ``Redelivered`` — die nächste Zustellung
        durchläuft die Owner-Check-Pipeline erneut. Start-mode nach dem Startpunkt
        (``ctx.start_release_reached``) und Nachrichten ohne Header: No-op."""
        terminal = outcome if isinstance(outcome, Terminal) else None
        await self._finalize_schedfire(
            ctx,
            terminal=terminal is not None and terminal.finalize_schedfire,
        )
        if terminal is not None and terminal.release_unique:
            await self._maybe_release_unique(message, ctx, task_info)

    async def _resolve_error_path(
        self,
        message: Any,
        ctx: GateContext,
        execution: Any,
        message_dict: MessageContext,
        chain: Any,
        current_outcome: DeliveryOutcome,
    ) -> DeliveryOutcome | None:
        """FAF-Fehler-Kurzschluss und AT_LEAST_ONCE-Retry-Bewertung.

        Rückgabe: ein Outcome beendet den Hauptteil damit; ``None`` läuft
        weiter in den Ergebnis-Tail (der FAF-Kurzschluss reicht das
        aktuelle Outcome unverändert zurück).

        FIRE_AND_FORGET mit Fehler/Cancel überspringt die Result-/Retry-
        Pipeline (bei Cancel wird das Flag hier auf höchstens 1 h verkürzt — der
        result-ttl-0-Zweig des Tails wird auf diesem Pfad nie erreicht).
        Bewusste (user-approved) Verhaltensänderung von damals:
        ``max_retries=0``-Fehler laufen wie jeder Fehler durch die
        Retry-Bewertung — sofort erschöpft → DLQ mit gespeichertem
        Fehlerergebnis und ``TASK_DLQ``; der Late-Cancel-Flip stellt einen
        Cancel, der ins Pipeline-Ende raste, dem Tail als Cancel dar."""
        task_info = ctx.task_info
        assert task_info is not None
        if task_info.delivery is DeliveryMode.FIRE_AND_FORGET and (execution.is_error or execution.is_cancelled):
            if execution.is_cancelled:
                await self._backend.clear_cancel_flag(ctx.task_id, op_timeout=self._config.backend_op_timeout)
            return current_outcome

        if (
            execution.is_error
            and execution.task_exception is not None
            and not execution.is_cancelled
            and task_info.delivery is DeliveryMode.AT_LEAST_ONCE
        ):
            if await self._check_late_cancel(ctx.task_id):
                logger.info(
                    "cancel_observed_post_execution",
                    task_id=ctx.task_id,
                    task_name=ctx.task_name,
                )
                execution.is_cancelled = True
                execution.is_error = False
                execution.error_message = ""
                execution.task_exception = None
                return None
            # U8: Retry-Bewertung und Dead-Letter leben in worker/retry.py.
            # Beide Wege melden ``Redelivered`` an den finally (Dead-Letter
            # gibt inline frei; das In-Flight-Löschen ist folgenlos).
            return await self._retry_handler.evaluate(
                message, ctx, execution.task_exception, execution.error_message, message_dict, chain
            )
        return None

    async def _handle_message(self, message: Any) -> None:
        """Core message handler logic. Nichts entweicht aus der Task (B22)."""
        decoded = None
        with contain_delivery_errors():
            decoded = await self._decode_and_validate(message)
        if decoded is None:
            return
        task_id, queue, task_name, args, kwargs = decoded
        ctx = GateContext(
            task_id=task_id,
            task_name=task_name,
            queue=queue,
            args=args,
            kwargs=kwargs,
            workflow_id=self._extract_valid_workflow_id(message),
        )
        with self._log_context(message, ctx), contain_delivery_errors():
            task_logger.log(self._lifecycle_level, "task_received")
            await self._process_message(message, ctx)

    async def _process_message(self, message: Any, ctx: GateContext) -> None:
        """Gates, Ausfuehrung und Ergebnis-Tail einer dekodierten Zustellung."""
        task_id, task_name = ctx.task_id, ctx.task_name
        if not await self._pre_pipeline_gates(message, ctx):
            return

        # 3. Acquire semaphore -- bewusst vor dem ``try`` (siehe ``_acquire_slot``).
        await self._acquire_slot(ctx)
        # Per-handler lock-tracking state, scoped to this invocation —
        # task_id alone would collide under AT_LEAST_ONCE redelivery.
        # handler_id ist der Registry-Schlüssel fürs Shutdown-Cleanup.
        handler_id = ctx.handler_id = uuid.uuid4()
        acquired_locks = ctx.acquired_locks  # lokale Quelle der Wahrheit für den finally
        # R3.5 Unit 11 state: task_info visible in finally even if
        # lookup fails.
        task_info: TaskInfo | None = None
        # U19 (KTD5): typisiertes Zustellungs-Ergebnis; Default = terminal
        # mit Freigabe im finally — ein vergessener Zweig über-freigibt
        # (owner-check-sicher) statt unter-freigibt. Semantik: outcome.py.
        outcome: DeliveryOutcome = Terminal.release_in_finally()
        # FIRE_AND_FORGET: nach dem letzten Gate wird bestätigt; danach darf
        # kein Zweig mehr ein Broker-Verb auslösen (Modus acked_before_execute).
        acked_before_execute = False
        # Optional Dishka REQUEST scope. Entered just before the task
        # lifecycle (below) and torn down in the finally via the same
        # detached-shield pattern as lock release. None when DI is inactive.
        _di_scope: Any = None
        _di_token: Any = None
        # Nur gelesen, wenn ``_di_scope`` gesetzt ist (dann immer zugewiesen).
        is_cancelled = False
        task_exception: BaseException | None = None
        try:
            # 3b. Check if shutdown is in progress — requeue message
            if self._shutdown_event.is_set():
                # nack(requeue=True): broker redelivers with headers
                # intact → keep the flag so redelivery can release.
                outcome = Redelivered
                await redeliver_in_place(self._gate_deps, message, ctx)
                return

            # 4-5. Look up task in registry. Ein unbekannter Task wird
            # innerhalb der Gnadenfrist zurueckgestellt (BZ-06): das Outcome
            # des Gates gilt, sonst schriebe der finally den
            # Schedfire-Completed-Marker und gaebe das Unique-Flag frei.
            resolved = await self._lookup_task_or_route(message, ctx)
            if isinstance(resolved, DeliveryOutcome):
                outcome = resolved
                return
            task_class = resolved

            task_info = task_class.task_info
            ctx.task_info = task_info
            ctx.task_class = task_class

            # 4a–5b. Gate-Sequenz in fester Reihenfolge (der Ordnungstest
            # pinnt sie): Expiry (R4.4 — max_age VOR allen Stufen; die
            # Enqueue-Zeit stammt vom Publish, jede Redelivery misst gegen
            # das ORIGINAL — Expiry ist ein Wanduhr-Vertrag), dann
            # Rate-Limit und Lock (beide VOR dem FIRE_AND_FORGET-Ack; ihre
            # Requeue-Kopien erhalten den x-swarmq-unique-hash-Header, das
            # Flag bleibt beansprucht; die Lock-DLQ-Unterpfade geben inline
            # frei — siehe gates/rate_limit.py und gates/lock.py).
            # Jedes „behandelt" beendet den Hauptteil mit dem Gate-Outcome;
            # nach jedem Continue fällt das Outcome auf den Terminal-Default
            # zurück (KTD5-Richtung: über- statt unter-freigeben).
            for gate in (self._execution_guard, self._expiry_gate, self._rate_limit_gate, self._lock_gate):
                outcome = await gate.apply(message, ctx)
                if outcome is not Continue:
                    return
                outcome = Terminal.release_in_finally()

            await self._release_unique_on_start(message, task_id, task_info)
            ctx.start_release_reached = True  # FS-13: keine zweite Freigabe

            # 5d. FIRE_AND_FORGET: ack immediately before execution — ab hier
            # ist kein Broker-Verb mehr zulässig (acked_before_execute).
            if task_info.delivery is DeliveryMode.FIRE_AND_FORGET:
                await self._broker.ack(message)
                acked_before_execute = True

            message_dict = self._build_message_dict(ctx, message)
            chain = await self._resolve_chain_or_route(
                message,
                acked_before_execute,
                task_id=task_id,
                task_name=task_name,
            )
            if chain is None:
                return

            # 6. Ausführung (U8): Lifecycle, Timeouts, Cancel-Monitor,
            # Middleware-Hooks und Signale leben in worker/execution.py.
            execution = await self._executor.execute(ctx, message_dict, chain)
            is_cancelled = execution.is_cancelled
            task_exception = execution.task_exception
            _di_scope = execution.di_scope
            _di_token = execution.di_token

            error_outcome = await self._resolve_error_path(message, ctx, execution, message_dict, chain, outcome)
            # Der Late-Cancel-Flip mutiert das ExecutionResult — die lokalen
            # Aliase (DI-Teardown im finally) nachziehen.
            is_cancelled = execution.is_cancelled
            task_exception = execution.task_exception
            if error_outcome is not None:
                outcome = error_outcome
                return

            # 7.-11. Ergebnis-Tail (U20): Speichern, Benachrichtigung,
            # Cancel-Flag, Workflow-Advance-Seam und ALO-Ack leben in
            # worker/results.py.
            outcome = await self._result_tail.finish(message, ctx, execution)
            return

        finally:
            # FS-04 (KTD6): Kaskade in fester Reihenfolge Settle/Finalize ->
            # DI-Teardown -> Lock-Freigabe -> Semaphor. Jeder spaetere Schritt
            # laeuft auch, wenn ein frueherer scheitert oder abgebrochen wird
            # (Dishka-``ExitError``, ``BaseException`` aus einem Finalizer,
            # aeusserer Abbruch); eine spaetere Ausnahme ersetzt eine fruehere
            # (die fruehere haengt in ``__context__``).
            teardown: list[asyncio.Task[None]] = []
            try:
                await self._settle_and_teardown(
                    message, outcome, ctx, task_info, _di_scope, _di_token, is_cancelled, task_exception, teardown
                )
            finally:
                running = [task for task in teardown if not task.done()]
                if running:
                    # Ein aeusserer Abbruch traf den geshieldeten DI-Teardown,
                    # dessen Finalizer (z. B. DB-Commit) noch laeuft: Lock und
                    # Semaphor erst nach seinem Ende freigeben (KTD6), als
                    # getrackter Nachlauf -- der Abbruch propagiert sofort.
                    _ = self._track(self._release_after_teardown(running[0], handler_id, acquired_locks))
                else:
                    await self._release_locks_and_slot(handler_id, acquired_locks)

    async def _acquire_slot(self, ctx: GateContext) -> None:
        """Semaphor-Platz nehmen, vor dem ``try/finally`` der Zustellung: ein
        Abbruch hier darf den ``finally`` nicht erreichen (dessen
        Terminal-Default schriebe den Schedfire-Completed-Marker). Wie bei
        einer Gate-Ausnahme wird der Schedfire-Claim nicht-terminal
        freigegeben, sonst saehe die Neuzustellung ihn als Duplikat (FS-03
        Teil 2). Geshieldet und getrackt: der Shutdown wartet vor
        ``backend.close`` auf die Freigabe."""
        try:
            await self._semaphore.acquire()
        except BaseException:
            await self._detached(self._release_schedfire_claim(ctx))
            raise

    async def _settle_and_teardown(
        self,
        message: Any,
        outcome: DeliveryOutcome,
        ctx: GateContext,
        task_info: TaskInfo | None,
        di_scope: Any,
        di_token: Any,
        is_cancelled: bool,
        task_exception: BaseException | None,
        teardown: list[asyncio.Task[None]],
    ) -> None:
        """Erste Stufen der ``finally``-Kaskade: Settle/Finalize, danach -- auch
        wenn das scheitert oder abgebrochen wird -- der DI-Teardown (FS-04)."""
        try:
            # 12-pre. Finalize the schedfire claim if this message was a
            # schedfire delivery we claimed in-flight. On a terminal
            # outcome this writes the completed marker (a redelivery
            # racing our in-flight-key expiry must see it, Unit 1);
            # on a non-terminal outcome (retry/requeue) it instead clears
            # the in-flight key so the redelivered copy can re-run rather
            # than being skipped as "completed".
            # U19: der finally liest die Outcome-Felder. ``Redelivered`` (kein
            # Terminal) → In-Flight-Claim löschen, keine Freigabe.
            # KTD5: hat kein Verb den Running-Marker abgeschlossen (kein Verb,
            # Ausnahme, Abbruch), nur freigeben -- geshieldet wie der Lock-Release
            # und VOR dem Finalisieren (ein Abbruch dort liesse es sonst aus).
            # Der Regelfall (Verb hat abgeschlossen, Schutz aus) braucht keinen Task.
            if self._execution_guard.lease_of(message) is not None:
                await self._detached(self._execution_guard.settle(message))
            await self._finalize_delivery(message, outcome, ctx, task_info)
        finally:
            # 12-pre. Close the Dishka REQUEST scope (if opened), driving
            # __aexit__ with the captured task exception so yield-based
            # finalizers can commit on success / roll back on failure. On a
            # user-cancel (no task_exception), a CancelledError is passed so
            # finalizers treat it as an abort. Uses the same detached-shield
            # pattern as lock release: schedule the exit as its own tracked
            # Task so an outer (shutdown) cancel into this finally does not
            # abort the finalizer mid-await — _execute_shutdown drains
            # _detached_releases before closing the backend.
            if di_scope is not None:
                await self._teardown_di_scope(di_scope, di_token, is_cancelled, task_exception, teardown)

    async def _release_after_teardown(
        self, teardown: asyncio.Task[None], handler_id: uuid.UUID, acquired_locks: list[LockToken]
    ) -> None:
        """Nachlauf nach einem abgebrochenen DI-Teardown: erst dessen Ende
        abwarten (seine Ausnahme bleibt geschluckt wie bisher -- ``shield``
        holt sie ab), dann Lock und Semaphor freigeben. Hat der Shutdown die
        Locks inzwischen selbst abgeraeumt (Registry-Eintrag weg), nicht noch
        einmal freigeben -- sonst Fehlalarm ``task_lock_lost``."""
        try:
            await asyncio.wait([teardown])
        finally:
            if handler_id not in self._active_handler_locks:
                acquired_locks = []
            await self._release_locks_and_slot(handler_id, acquired_locks)

    async def _release_locks_and_slot(self, handler_id: uuid.UUID, acquired_locks: list[LockToken]) -> None:
        """Letzte Stufen der ``finally``-Kaskade: Lock-Freigabe, danach -- auch
        wenn sie scheitert oder abgebrochen wird -- der Semaphor (FS-04)."""
        try:
            # 12a. Release any task-locks acquired by this handler
            # (detached-shield pattern, see ``_detached_lock_release``).
            if acquired_locks and self._lock_manager is not None:
                await self._detached_lock_release(handler_id, acquired_locks)
        finally:
            # 12b. Release semaphore
            self._semaphore.release()
