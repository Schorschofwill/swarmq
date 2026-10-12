"""Worker-Lifecycle (Plan U9) — verbatim aus ``pipeline.py``: ``start``
(Verbinden, Koordinationsobjekte über die Fassade, Observability-Autowire mit
Teilstart-Rollback, Queue-Deklaration/Consume, Signal-Handler), Burst-Monitor,
Metrics-Sampler, ``stop``/graceful Shutdown und der Crash-Teardown mit dem
Detached-Shield-Idiom (``docs/solutions/patterns/worker-cancel-safe-teardown-
detached-tasks-2026-06-01.md``).

Als Mixin geschnitten: der Zustand (``_state``, ``_shutdown_event``,
``_active_tasks``, ``_detached_releases``, …) lebt weiterhin im
``Worker``-Konstruktor (``pipeline.py``); die ``self``-Referenzen bleiben
unverändert — reine Verschiebung (KTD14).
"""

from __future__ import annotations

import asyncio
import enum
import os
import signal
import sys
import time
import uuid
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

from swarmq.logging import _error_fields, get_logger, sanitize_url
from swarmq.ready_depth import ready_depth_or_none
from swarmq.scheduler import SchedulerLoop, ScheduleStore
from swarmq.scheduler.reports import static_definitions
from swarmq.serialization import decode_result
from swarmq.shutdown_budget import CLEANUP_RESERVE_S
from swarmq.signals import Signal
from swarmq.supervision import ForkSourceRejected, RecycleAckTimeout, current_channel
from swarmq.worker.eviction_gate import enforce_eviction_policy
from swarmq.worker.prefetch import prefetch_limit, warn_prefetch_over_consumer_timeout
from swarmq.workflow_orchestration import (
    WorkflowAdvancer,
    WorkflowStepEnqueuer,
    workflow_step_task_id,
)

if TYPE_CHECKING:
    from swarmq.config import SwarmQConfig
    from swarmq.contracts.coordination import ExecutionGuardStore, LockStore, LockToken, RateLimitStore
    from swarmq.health import HealthServer
    from swarmq.metrics import Metrics, MetricsServer
    from swarmq.middleware import MiddlewareChain
    from swarmq.registry import TaskRegistry
    from swarmq.signals import SignalDispatcher
    from swarmq.worker.execution import TaskExecutor
    from swarmq.worker.gates import GateDeps, WorkerBackend, WorkerBroker
    from swarmq.worker.gates.execution_guard import ExecutionGuard
    from swarmq.worker.gates.lock import LockGate

logger = get_logger(__name__)

# Lifecycle-Signale (WORKER_ONLINE/HEARTBEAT/SHUTDOWN) laufen mit gedeckeltem
# Handler-Timeout, damit ein haengender Handler den Shutdown nicht blockiert.
_LIFECYCLE_DISPATCH_TIMEOUT_S = 10.0
# Rest der Gnadenfrist, den das Herunterfahren NACH dem Drain noch braucht:
# Sperren freigeben, Scheduler stoppen, Broker- und Backend-Verbindung
# schliessen (Plan 2026-08-28-001, KTD9). Seit BZ-18 (Plan 0.12.0, MR 17)
# gilt sie fuer Drain und Publish-Endfrist gemeinsam und lebt in
# ``swarmq.shutdown_budget``; dieser Name bleibt als Alias.
_SHUTDOWN_PUBLISH_RESERVE_S = CLEANUP_RESERVE_S
# Untergrenze je Aufraeumschritt im Herunterfahren (BZ-18): auch mit
# ausgeschoepftem Budget bekommt jeder Schritt diesen Versuch.
_SHUTDOWN_STEP_MIN_S = 1.0


class WorkerState(enum.Enum):
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    DRAINING = "DRAINING"
    SHUTTING_DOWN = "SHUTTING_DOWN"
    STOPPED = "STOPPED"


class WorkerLifecycle:
    # Mixin über dem Worker-Zustand (U9); die Attribute setzt
    # ``Worker.__init__`` (pipeline.py), die Methoden-Stubs zeigen dorthin.
    if TYPE_CHECKING:
        _broker: WorkerBroker
        _backend: WorkerBackend
        _registry: TaskRegistry
        _config: SwarmQConfig
        _signal_dispatcher: SignalDispatcher
        _shutdown_event: asyncio.Event
        _active_tasks: set[asyncio.Task[None]]
        _active_handler_locks: dict[uuid.UUID, list[LockToken]]
        _detached_releases: set[asyncio.Task[None]]
        _workflow_advancer: WorkflowAdvancer | None
        _scheduler: SchedulerLoop | None
        _rate_limiter: RateLimitStore | None
        _lock_manager: LockStore | None
        _guard_store: ExecutionGuardStore | None
        _execution_guard: ExecutionGuard
        _metrics: Metrics | None
        _metrics_server: MetricsServer | None
        _health_server: HealthServer | None
        _burst_monitor_task: asyncio.Task[None] | None
        _recycle_monitor_task: asyncio.Task[None] | None
        _tasks_done: int
        _recycle_ack_missing: bool
        _drain_timeout_override: float | None
        _metrics_sampler_task: asyncio.Task[None] | None
        _lock_gate: LockGate
        _executor: TaskExecutor
        _gate_deps: GateDeps

        async def _on_message(self, message: Any) -> None: ...

        async def _publish_workflow_step(self, queue: str, body: bytes, headers: dict[str, Any]) -> Any: ...

        def _resolve_workflow_queue(self, task_name: str) -> str: ...

        def _get_chain(self) -> MiddlewareChain: ...

        # ``WorkerShutdown`` (shutdown.py, Plan 0.12.0 MR 0)
        def _request_shutdown(self) -> None: ...

        async def _execute_shutdown(self) -> None: ...

        async def _teardown_on_exit(self) -> None: ...

    async def _wire_coordination(self) -> None:
        """Verträge/Advancer/Scheduler nach ``connect()`` verdrahten."""
        # Die Koordinations-Verträge kommen seit U15 von der Fassade —
        # die Properties existieren erst nach connect() (registrierte
        # Lua-Skripte brauchen den lebenden Client).
        self._rate_limiter = self._backend.rate_limit
        self._lock_manager = self._backend.locks
        # KTD5: ein Backend ohne Ausfuehrungsschutz (Dritt-Store) laesst ihn aus.
        self._guard_store = getattr(self._backend, "execution_guard", None)

        # R4.1 workflow advancer (U5) — der ChordStore-Vertrag kommt
        # ebenfalls von der Fassade (chord_complete.lua, lebender Client).
        # U11 (KTD7): Publisher-Seam des Advancers — Folgeschritte laufen
        # durch dieselbe Middleware-Kette und dieselben Signale wie
        # ``Client.schedule``. Die Kette wird je Front frisch geholt, damit
        # spaeter registrierte Middleware sichtbar bleibt.
        publisher = WorkflowStepEnqueuer(
            publish=self._publish_workflow_step,
            dispatch=self._signal_dispatcher.dispatch,
            get_chain=self._get_chain,
        )
        self._workflow_advancer = WorkflowAdvancer(
            self._backend,
            self._backend.chords,
            publisher=publisher,
            resolve_queue=self._resolve_workflow_queue,
            make_task_id=workflow_step_task_id,
            unpack=lambda raw: decode_result(raw)["result"],
            ttl_ms=self._config.result_ttl * 1000,
        )

        # Embedded scheduler (Phase 3c). Also lazy-constructed here for
        # the same reason — ScheduleStore's registered sched_poll /
        # sched_commit scripts need the connected client. Starts both
        # the election and poll sub-coroutines; only the elected leader
        # will actually do work, standbys no-op.
        schedule_store = ScheduleStore(self._backend)
        candidate = self._config.scheduler == "on"
        if not candidate:
            logger.warning(
                "scheduler_disabled",
                hint="this worker does not run for scheduler leader; crons, delay/eta tasks and "
                "oneshots fire only while another worker on the same Valkey and namespace is leader",
            )
        self._scheduler = SchedulerLoop(
            backend=self._backend,
            broker=self._broker,
            config=self._config,
            # U11: der Loop spricht den LockStore-Vertrag (Token-API);
            # der Worker-Lock-Pfad nutzt weiter den LockManager oben.
            lock_manager=self._backend.locks,
            store=schedule_store,
            signal_dispatcher=self._signal_dispatcher,
            worker_id=self._config.worker_identity,
            # KTD10: die eigenen statischen Crons (Registry dieses Workers,
            # Zone aufgelöst) werden gemeldet, auch wenn er nie Leader wird.
            static_crons=static_definitions(
                self._registry.get_all().values(),
                self._config.scheduler_timezone,
            ),
            # BZ-14: die konsumierten Queues (mit Prioritaeten je Band).
            consumed_queues=self._config.consumed_queues,
            candidate=candidate,
        )
        await self._scheduler.start()

    async def _start_observability(self) -> None:
        """Metrics-/Health-Server mit Teilstart-Rollback hochfahren."""
        # Auto-wire Prometheus metrics + health
        # endpoints when config flags set. Operator no longer needs
        # to construct Metrics + install_signal_collectors + MetricsServer
        # manually — the default wiring is opt-in via metrics_port /
        # health_port config.
        #
        # The partial-start cleanup path
        # wraps the whole observability bring-up. A failure after
        # scheduler.start() OR after metrics_server.start() now unwinds
        # whatever previously succeeded before re-raising, so a port
        # conflict on --health-port no longer orphans the metrics
        # socket. Scheduler + broker-consume cleanup happens in
        # _execute_shutdown which is NOT reached on this path, so we
        # call a dedicated _rollback_partial_start_observability helper.
        try:
            if self._config.metrics_port is not None:
                from swarmq.metrics import (
                    Metrics,
                    MetricsServer,
                    install_signal_collectors,
                )

                self._metrics = Metrics()
                install_signal_collectors(
                    self._signal_dispatcher,
                    self._metrics,
                )
                self._metrics_server = MetricsServer(
                    self._metrics,
                    host=self._config.metrics_host,
                    port=self._config.metrics_port,
                )
                await self._start_with_bind_retry(self._metrics_server.start)
            if self._config.health_port is not None:
                from swarmq.health import HealthChecker, HealthServer

                checker = HealthChecker(
                    self._broker,
                    self._backend,
                    checks=self._readiness_checks(),
                )
                self._health_server = HealthServer(
                    checker,
                    host=self._config.health_host,
                    port=self._config.health_port,
                )
                await self._start_with_bind_retry(self._health_server.start)
        except Exception:
            # The scheduler has already started; its
            # leader-election + poll tasks would leak as "Task was
            # destroyed but it is pending!" on loop teardown without
            # this rollback.
            if self._metrics_server is not None:
                try:
                    await self._metrics_server.stop()
                except Exception:
                    logger.exception("rollback_metrics_stop_failed")
            if self._health_server is not None:
                try:
                    await self._health_server.stop()
                except Exception:
                    logger.exception("rollback_health_stop_failed")
            if self._scheduler is not None:
                try:
                    await self._scheduler.stop()
                except Exception:
                    logger.exception("rollback_scheduler_stop_failed")
            raise

    # ---- CPU-Modus (Plan 2026-09-03-001, U7): Verdrahtung lebt in compute.wiring,
    # damit dieses Modul und pipeline.py unter dem 800-Zeilen-Guard bleiben.

    async def _start_executor(self) -> None:
        from swarmq.worker.compute.wiring import start_compute_executor

        await start_compute_executor(self)

    async def _close_executor(self) -> None:
        from swarmq.worker.compute.wiring import close_compute_executor

        await close_compute_executor(self)

    def _warn_hard_timeout_over_consumer_timeout(self) -> None:
        from swarmq.worker.compute.wiring import warn_hard_timeout_over_consumer_timeout

        warn_hard_timeout_over_consumer_timeout(self._registry, self._config.consumer_timeout_s)

    def _readiness_checks(self) -> dict[str, Any]:
        from swarmq.health import consumer_check

        checks: dict[str, Any] = {}
        if self._config.pool == "cpu":
            from swarmq.worker.compute.wiring import forkserver_ready

            checks["compute"] = lambda: forkserver_ready(self)
        # U6 (R2): jede konsumierte Queue braucht einen aktiven Consumer. Ein
        # Fremdbroker ohne ``consumer_gaps`` bekommt den Check nicht.
        gaps = getattr(self._broker, "consumer_gaps", None)
        if gaps is not None:
            checks["consumers"] = consumer_check(
                gaps, self._config.consumed_queues, grace_s=self._config.consumer_ready_grace_s
            )
        return checks

    # ---- Selbst-Recycling des IO-Workers (Plan 2026-09-03-001, R22/R31, KTD11):
    # Logik in worker/recycling.py, damit dieses Modul unter dem 800-Zeilen-Guard bleibt.

    def _start_recycle_monitor(self) -> None:
        from swarmq.worker.recycling import start_recycle_monitor

        start_recycle_monitor(self)

    async def _request_recycle(self, reason: str, value: float) -> None:
        from swarmq.worker.recycling import request_recycle

        await request_recycle(self, reason, value)

    async def _start_with_bind_retry(self, start: Callable[[], Awaitable[None]]) -> None:
        from swarmq.worker import recycling

        # KTD4: das Budget kommt aus der Konfiguration — das Entry leitet es
        # aus beiden Freigabefristen ab, damit ein Ersatz nicht in einen
        # Bind-Fehler und damit ins Neustart-Budget laeuft.
        await recycling.start_with_bind_retry(start, budget_s=self._config.bind_retry_budget_s)

    def _warn_without_delivery_limit(self) -> None:
        """R21: ohne Quorum-Queue gibt es kein Delivery-Limit — eine Giftnachricht
        koennte Worker endlos ersetzen."""
        if self._config.queue_type != "quorum":
            logger.warning(
                "no_delivery_limit_for_queue_type",
                queue_type=self._config.queue_type,
                hint=(
                    "x-delivery-limit exists only on quorum queues; with the supervisor "
                    "replacing hung workers, a blocking message is redelivered without bound"
                ),
            )

    def _warn_priority_tasks_without_routing(self) -> None:
        # Warn when a registered task declares
        # priority != "normal" but the worker is not configured for
        # priority routing. Without a declared {queue}.high the broker
        # returns such a publish and the producer gets UnroutableMessage
        # (BZ-03, 0.12); before 0.12 it was dropped silently.
        if not self._config.priorities:
            priority_tasks: list[str] = []
            for task_name in self._registry.get_all():
                try:
                    ti = self._registry.get(task_name).task_info
                except Exception:  # noqa: BLE001, S112 - nicht ladbarer Task wird bei der Warnung uebersprungen
                    continue
                if getattr(ti, "priority", "normal") != "normal":
                    priority_tasks.append(f"{task_name}({ti.priority})")
            if priority_tasks:
                logger.warning(
                    "registered_priority_tasks_with_priorities_disabled",
                    tasks=priority_tasks[:20],
                    hint=(
                        "Enable worker priorities (SwarmQConfig.priorities="
                        "True) or change TaskInfo.priority='normal'; without "
                        "a declared .high/.low queue, publishing these tasks "
                        "raises UnroutableMessage."
                    ),
                )

    async def _declare_and_consume_queues(self) -> None:
        # R4.3: mit Prioritaeten das Tripel je Basis-Queue (SwarmQConfig.consumed_queues).
        for queue_name in self._config.consumed_queues:
            await self._broker.declare_queue(queue_name)
            await self._broker.consume(
                queue_name,
                self._on_message,
                concurrency=self._config.concurrency,
            )
        await self._report_queue_declarations()

    async def _report_queue_declarations(self) -> None:
        # Plan 0.12.0, MR 8 (BZ-19): hoechstens eine Warnung zu at-most-once-
        # Bestandsqueues. Optionale Broker-Faehigkeit (Q15): fehlt sie oder
        # scheitert sie, startet der Worker trotzdem.
        report = getattr(self._broker, "report_queue_declarations", None)
        if report is None:
            return
        try:
            await report()
        except Exception as exc:  # noqa: BLE001 - Bericht best effort, Worker startet trotzdem
            logger.debug("queue_declaration_report_failed", **_error_fields(exc))

    def _start_consumer_watch(self) -> None:
        # U6 (R1, KTD3): verlorene Consumer selbst neu anmelden. Optional am
        # Broker wie ``set_publish_deadline``; ``cancel_consume`` stoppt ihn.
        start = getattr(self._broker, "start_consumer_watch", None)
        if start is not None:
            count = self._metrics.count_consumer_reregistration if self._metrics is not None else None
            start(self._config.consumer_watch_interval_s, count)

    def _start_background_monitors(self) -> None:
        self._start_recycle_monitor()
        # R4.5 Burst-Mode: launch the idle monitor only when burst is
        # enabled. The monitor sets _shutdown_event after the queues
        # have been silent for burst_idle_timeout seconds, exiting the
        # await below and triggering the same graceful path SIGTERM uses.
        if self._config.burst_mode:
            self._last_message_received_at = time.monotonic()
            self._burst_monitor_task = asyncio.create_task(self._burst_monitor())

        # Start the metrics sampler when metrics are
        # enabled so the queue_depth / dlq_messages / uptime gauges are
        # actually populated and WORKER_HEARTBEAT is emitted.
        if self._metrics is not None and self._metrics.enabled:
            self._worker_started_at = time.monotonic()
            self._metrics_sampler_task = asyncio.create_task(self._metrics_sampler())

    async def start(self) -> None:
        """Connect broker + backend, declare queues, start consuming, block until shutdown."""
        await self._broker.connect()
        await self._backend.connect()
        # The try/finally makes the cancel and error paths safe from the
        # moment the scheduler starts (inside _wire_coordination): a plain
        # cancel of this coroutine (tests, ProcessManager reload, embedded
        # use) or a startup failure (queue declaration, executor) would
        # otherwise skip _execute_shutdown and leak the scheduler
        # election/poll tasks — which keep renewing the leader lock for the
        # life of the process — plus the broker connection tasks. This used
        # to start only after WORKER_ONLINE (0.9.0 fix).
        try:
            # BZ-04 (Plan 0.12.0, MR 14): vor Scheduler und Koordination.
            enforce_eviction_policy(self._backend, self._config.valkey_eviction_check)
            await self._wire_coordination()
            await self._start_observability()
            self._warn_priority_tasks_without_routing()
            self._warn_hard_timeout_over_consumer_timeout()
            # BZ-20 (Plan 0.12.0, MR 18): Prefetch gilt je Queue-Consumer.
            warn_prefetch_over_consumer_timeout(
                self._registry,
                self._config.consumed_queues,
                self._config.concurrency,
                self._config.consumer_timeout_s,
            )
            self._warn_without_delivery_limit()
            await self._start_executor()
            await self._declare_and_consume_queues()
            self._start_consumer_watch()
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
                Signal.WORKER_ONLINE,
                handler_timeout=_LIFECYCLE_DISPATCH_TIMEOUT_S,
                worker_id=self._config.worker_identity,
            )
            self._start_background_monitors()
            # Block until shutdown is requested.
            await self._shutdown_event.wait()
            await self._execute_shutdown()
            if getattr(self, "_recycle_ack_missing", False):
                # KTD11: ohne Bestaetigung Exit != 0, damit der Supervisor den
                # bestehenden Restart-Pfad nimmt statt den Slot zu verlieren.
                raise RecycleAckTimeout("supervisor did not acknowledge the recycle request")
            if (rejected := getattr(self, "_fork_source_rejected", None)) is not None:
                # Schluss-Review: verworfene Kindquelle (compute.wiring) -> Exit 71.
                raise ForkSourceRejected(f"compute fork source rejected: {rejected}")
        finally:
            await self._teardown_on_exit()

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

    async def _metrics_sampler(self) -> None:
        """Periodically populate the gauges that the signal-driven
        collectors cannot, and emit the periodic worker heartbeat.

        queue_depth, dlq_messages and
        worker_uptime_seconds are declared in Metrics but were never
        ``.set()``, so backlog growth, DLQ filling and crash-looping
        workers were invisible to Prometheus. This sampler closes the
        "looks instrumented but isn't" trap. Per-queue broker errors are
        logged and skipped (a transient passive-declare failure must not
        kill the sampler). Cancellation-safe. Only started when metrics
        are enabled.
        """
        interval = self._config.metrics_sample_interval
        try:
            while not self._shutdown_event.is_set():
                # One failing iteration must not kill the loop — an
                # unobserved sampler death froze the gauges on their last
                # value with zero operator signal (the exact "looks
                # instrumented but isn't" trap again).
                try:
                    await self._sample_metrics_once()
                except Exception as exc:  # noqa: BLE001 - Fehler wird geloggt, Schleife laeuft weiter
                    logger.warning(
                        "metrics_sampler_iteration_failed",
                        **_error_fields(exc),
                    )
                await asyncio.sleep(interval)
        except asyncio.CancelledError:
            # Normal during _execute_shutdown teardown.
            pass

    async def _sample_metrics_once(self) -> None:
        """One metrics-sampler iteration (extracted for testability):
        set the uptime / queue-depth / dlq gauges and emit the heartbeat.
        Per-queue broker errors are logged and skipped."""
        assert self._metrics is not None  # Sampler startet nur mit Metrics
        worker_id = self._config.worker_identity
        now = time.monotonic()
        uptime = now - self._worker_started_at
        self._metrics.worker_uptime_seconds.labels(
            worker_id=worker_id,
        ).set(uptime)
        # Dieselben Labels wie ``swarmq_delay_tiers_unavailable``: die
        # konsumierten Queues, mit Prioritaeten also je Band (Review MR 10, P2-6).
        for queue in self._config.consumed_queues:
            try:
                depth = await self._broker.queue_depth(queue)
                self._metrics.queue_depth.labels(
                    queue=queue,
                    kind="pending",
                ).set(depth)
                # Plan 0.12.0, MR 10: nur die Delay-Queues (Q15: ohne
                # ``ready_depth`` bleibt die Serie aus).
                ready = await ready_depth_or_none(self._broker, queue)
                if ready is not None:
                    self._metrics.queue_depth.labels(queue=queue, kind="delayed").set(max(0, depth - ready))
                else:
                    # Scheitert ``ready_depth`` in diesem Takt, bliebe der
                    # letzte Wert stehen: die Serie faellt aus, statt zu
                    # luegen (Review MR 10, Hinweis).
                    self._metrics.queue_depth.remove(queue, "delayed")
                dlq = await self._broker.dead_letter_depth(queue)
                self._metrics.dlq_messages.labels(queue=queue).set(dlq)
            except Exception as exc:  # noqa: BLE001 - bewusst breit, Sampler darf nicht sterben
                # Broad on purpose. Catching only
                # BrokerConnectionError let any other error (Timeout,
                # ChannelInvalidStateError, decode glitch) propagate and
                # kill the sampler task silently.
                logger.warning(
                    "metrics_sample_failed",
                    queue=queue,
                    **_error_fields(exc),
                )
        self._sample_delay_tiers_unavailable()
        # Plan 0.12.0, MR 18 (BZ-20): hoechstens gehaltene unbestaetigte Nachrichten.
        self._metrics.worker_prefetch_limit.labels(worker_id=worker_id).set(
            prefetch_limit(self._config.consumed_queues, self._config.concurrency)
        )
        # Plan 0.12.0, MR 13 (BZ-05, R11): Lock-Wartezeit je Worker.
        waiting, oldest_s = self._lock_gate.waits.snapshot(now_ms=int(time.time() * 1000))
        self._metrics.lock_waits.labels(worker_id=worker_id).set(waiting)
        self._metrics.lock_wait_oldest_seconds.labels(worker_id=worker_id).set(oldest_s)
        # Plan 0.12.0, MR 17 (BZ-27): gesperrte Slots des Supervisors.
        channel = current_channel()
        locked = None if channel is None else channel.locked_slots_value()
        if locked is not None:
            self._metrics.supervisor_locked_slots.labels(worker_id=worker_id).set(locked)
        # Revive WORKER_HEARTBEAT (declared but never dispatched): a
        # per-worker liveness tick complementing the shallow /health probe.
        await self._signal_dispatcher.dispatch(
            Signal.WORKER_HEARTBEAT,
            handler_timeout=_LIFECYCLE_DISPATCH_TIMEOUT_S,
            worker_id=worker_id,
            uptime_seconds=uptime,
        )

    def _sample_delay_tiers_unavailable(self) -> None:
        # Plan 0.12.0, MR 10 (BZ-01): optionale Broker-Faehigkeit (Q15).
        assert self._metrics is not None
        count = getattr(self._broker, "delay_tiers_unavailable", None)
        if count is None:
            return
        for queue in self._config.consumed_queues:
            try:
                value = count(queue)
            except Exception:  # noqa: BLE001 - Fremd-Broker, Metrik best effort
                return
            if isinstance(value, int) and not isinstance(value, bool):
                self._metrics.delay_tiers_unavailable.labels(queue=queue).set(value)

    async def stop(self) -> None:
        """Close broker and backend connections."""
        await self._broker.close()
        await self._backend.close()
