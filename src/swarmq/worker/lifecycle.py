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
import contextlib
import enum
import os
import signal
import sys
import time
import uuid
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

from swarmq.dishka import _Missing, get_registered_container
from swarmq.logging import _error_fields, get_logger, sanitize_url
from swarmq.scheduler import SchedulerLoop, ScheduleStore
from swarmq.scheduler.reports import static_definitions
from swarmq.serialization import decode_result
from swarmq.signals import Signal
from swarmq.supervision import ForkSourceRejected, RecycleAckTimeout, current_channel
from swarmq.workflow_orchestration import (
    WorkflowAdvancer,
    WorkflowStepEnqueuer,
    workflow_step_task_id,
)

if TYPE_CHECKING:
    from swarmq.config import SwarmQConfig
    from swarmq.contracts.coordination import LockStore, LockToken, RateLimitStore
    from swarmq.health import HealthServer
    from swarmq.metrics import Metrics, MetricsServer
    from swarmq.middleware import MiddlewareChain
    from swarmq.registry import TaskRegistry
    from swarmq.signals import SignalDispatcher
    from swarmq.worker.execution import TaskExecutor
    from swarmq.worker.gates import GateDeps, WorkerBackend, WorkerBroker

logger = get_logger(__name__)

# Lifecycle-Signale (WORKER_ONLINE/HEARTBEAT/SHUTDOWN) laufen mit gedeckeltem
# Handler-Timeout, damit ein haengender Handler den Shutdown nicht blockiert.
_LIFECYCLE_DISPATCH_TIMEOUT_S = 10.0
# Rest der Gnadenfrist, den das Herunterfahren NACH dem Drain noch braucht:
# Sperren freigeben, Scheduler stoppen, Broker- und Backend-Verbindung
# schliessen. Ohne diese Reserve faellt die Publish-Endfrist exakt mit der
# Eskalation des Supervisors zusammen — dessen Gnadenfrist ist derselbe
# ``shutdown_timeout``, den der Drain bereits als eigenes Limit verbraucht
# (siehe src/swarmq/process_manager/run.py) — und der Deckel brächte im
# Herunterfahren gar nichts (Plan 2026-08-28-001, KTD9).
_SHUTDOWN_PUBLISH_RESERVE_S = 5.0



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
        _metrics: Metrics | None
        _metrics_server: MetricsServer | None
        _health_server: HealthServer | None
        _burst_monitor_task: asyncio.Task[None] | None
        _recycle_monitor_task: asyncio.Task[None] | None
        _tasks_done: int
        _recycle_ack_missing: bool
        _drain_timeout_override: float | None
        _metrics_sampler_task: asyncio.Task[None] | None
        _executor: TaskExecutor
        _gate_deps: GateDeps

        async def _on_message(self, message: Any) -> None: ...

        async def _publish_workflow_step(
            self, queue: str, body: bytes, headers: dict[str, Any]
        ) -> Any: ...

        def _resolve_workflow_queue(self, task_name: str) -> str: ...

        def _get_chain(self) -> MiddlewareChain: ...

    async def _wire_coordination(self) -> None:
        """Verträge/Advancer/Scheduler nach ``connect()`` verdrahten."""
        # Die Koordinations-Verträge kommen seit U15 von der Fassade —
        # die Properties existieren erst nach connect() (registrierte
        # Lua-Skripte brauchen den lebenden Client).
        self._rate_limiter = self._backend.rate_limit
        self._lock_manager = self._backend.locks

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
                    self._signal_dispatcher, self._metrics,
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
                    self._broker, self._backend, compute_check=self._compute_readiness_check(),
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

    def _compute_readiness_check(self) -> Any:
        if self._config.pool != "cpu":
            return None
        from swarmq.worker.compute.wiring import forkserver_ready

        return lambda: forkserver_ready(self)

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
        await recycling.start_with_bind_retry(
            start, budget_s=self._config.bind_retry_budget_s
        )

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
        # priority routing. Without the warning, a publish to
        # {queue}.high is silently dropped into a queue nobody
        # consumes and get_result() times out with no diagnostic.
        if not self._config.priorities:
            priority_tasks: list[str] = []
            for task_name in self._registry.get_all():
                try:
                    ti = self._registry.get(task_name).task_info
                except Exception:
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

    async def _declare_and_consume_queues(self) -> None:
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
                    concurrency=self._config.concurrency,
                )

    def _start_background_monitors(self) -> None:
        self._start_recycle_monitor()
        # R4.5 Burst-Mode: launch the idle monitor only when burst is
        # enabled. The monitor sets _shutdown_event after the queues
        # have been silent for burst_idle_timeout seconds, exiting the
        # await below and triggering the same graceful path SIGTERM uses.
        if self._config.burst_mode:
            self._last_message_received_at = time.monotonic()
            self._burst_monitor_task = asyncio.create_task(
                self._burst_monitor()
            )

        # Start the metrics sampler when metrics are
        # enabled so the queue_depth / dlq_messages / uptime gauges are
        # actually populated and WORKER_HEARTBEAT is emitted.
        if self._metrics is not None and self._metrics.enabled:
            self._worker_started_at = time.monotonic()
            self._metrics_sampler_task = asyncio.create_task(
                self._metrics_sampler()
            )

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
            await self._wire_coordination()
            await self._start_observability()
            self._warn_priority_tasks_without_routing()
            self._warn_hard_timeout_over_consumer_timeout()
            self._warn_without_delivery_limit()
            await self._start_executor()
            await self._declare_and_consume_queues()
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
                except Exception as exc:
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
        for queue in self._config.queues:
            try:
                depth = await self._broker.queue_depth(queue)
                self._metrics.queue_depth.labels(
                    queue=queue, kind="pending",
                ).set(depth)
                dlq = await self._broker.dead_letter_depth(queue)
                self._metrics.dlq_messages.labels(queue=queue).set(dlq)
            except Exception as exc:
                # Broad on purpose. Catching only
                # BrokerConnectionError let any other error (Timeout,
                # ChannelInvalidStateError, decode glitch) propagate and
                # kill the sampler task silently.
                logger.warning(
                    "metrics_sample_failed",
                    queue=queue,
                    **_error_fields(exc),
                )
        # Revive WORKER_HEARTBEAT (declared but never dispatched): a
        # per-worker liveness tick complementing the shallow /health probe.
        await self._signal_dispatcher.dispatch(
            Signal.WORKER_HEARTBEAT,
            handler_timeout=_LIFECYCLE_DISPATCH_TIMEOUT_S,
            worker_id=worker_id,
            uptime_seconds=uptime,
        )

    async def stop(self) -> None:
        """Close broker and backend connections."""
        await self._broker.close()
        await self._backend.close()

    async def _stop_http_surfaces(self) -> None:
        # Stop auto-wired HTTP surfaces first so scrapers / probes
        # get a clean connection-close rather than hitting a worker
        # in mid-drain. Idempotent when not started.
        if self._metrics_server is not None:
            try:
                await self._metrics_server.stop()
            except Exception:
                logger.exception("metrics_server_stop_failed")
        if self._health_server is not None:
            try:
                await self._health_server.stop()
            except Exception:
                logger.exception("health_server_stop_failed")

    async def _drain_in_flight(self) -> None:
        # 1. DRAINING: stop accepting new messages. cancel_consume now
        # isolates per-consumer failures and raises only after attempting
        # every consumer + clearing its registry; guard it so a partial
        # cancel cannot abort the rest of the graceful-shutdown sequence
        # (in-flight drain, lock release, scheduler stop). 2026-06-02 audit.
        try:
            await self._broker.cancel_consume()
        except Exception as exc:
            logger.warning("cancel_consume_partial_failure", **_error_fields(exc))

        # 2. Wait for in-flight tasks to complete
        tasks_snapshot = set(self._active_tasks)
        if tasks_snapshot:
            # KTD11/R22: ein Recycling-Drain wartet ohne erzwungenen Abbruch
            # (Frist aus dem groessten hard_timeout), damit ein gesunder
            # Langlaeufer keinen Retry verliert; SIGTERM behaelt shutdown_timeout.
            drain_timeout = getattr(self, "_drain_timeout_override", None) or self._config.shutdown_timeout
            _done, pending = await asyncio.wait(
                tasks_snapshot,
                timeout=drain_timeout,
            )

            # 3. SHUTTING_DOWN: cancel remaining tasks if drain timeout exceeded
            if pending:
                self._state = WorkerState.SHUTTING_DOWN
                for task in pending:
                    task.cancel()
                await asyncio.wait(pending, timeout=1)

    async def _release_leftover_locks(self) -> None:
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
            for handler_id, locks in list(self._active_handler_locks.items()):  # noqa: B007
                for token in locks:
                    try:
                        await self._lock_manager.release(token)
                    except Exception as exc:
                        # KTD11: die Lock-TTL heilt den Fall selbst -> WARNING.
                        logger.warning(
                            "shutdown_lock_release_failed",
                            lock_name=token.name,
                            **_error_fields(exc),
                        )
            self._active_handler_locks.clear()

    async def _stop_scheduler_and_di(self) -> None:
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
                # KTD11: Leader-Lock und Marker laufen per TTL ab -> WARNING.
                logger.warning("scheduler_stop_failed", **_error_fields(exc))

        # 3e. Close the Dishka APP container (if one was registered). Runs
        # AFTER the in-flight drain + detached-release wait (so live REQUEST
        # scopes have finalized) and only when DI is active — no-op otherwise.
        # APP-scoped resources (connection pools etc.) are torn down here; a
        # close() failure is logged, not raised, so it cannot abort shutdown.
        _di_container = get_registered_container()
        if not isinstance(_di_container, _Missing):
            try:
                await _di_container.close()
            except Exception:
                logger.exception("dishka_container_close_failed")

    def _shutdown_publish_budget(self) -> float:
        """Wieviel Zeit ein Publish im Herunterfahren noch bekommt (R12, KTD9).

        Die Reserve deckt den Rest der Sequenz nach dem Drain: Sperren
        freigeben, Scheduler stoppen, Verbindungen schliessen. Sie wird
        gedeckelt auf die Haelfte der Gnadenfrist, damit eine kurz
        konfigurierte Frist nicht auf ein Nullbudget faellt — dann scheiterte
        im Drain jeder Retry- und Dead-Letter-Publish rein rechnerisch, und
        alle laufenden Tasks kaemen als Redelivery zurueck statt sauber
        verzoegert oder aussortiert zu werden.
        """
        grace = max(0.0, float(self._config.shutdown_timeout))
        reserve = min(_SHUTDOWN_PUBLISH_RESERVE_S, grace / 2.0)
        return max(0.0, grace - reserve)

    def _set_publish_deadline(self, at: float | None) -> None:
        """Setzt die Endfrist, sofern der Broker sie kennt.

        ``Publisher.set_publish_deadline`` kam erst mit diesem Plan dazu. Ein
        vorher geschriebener Fremdbroker wuerde hier sonst mit AttributeError
        die gesamte Abbausequenz abbrechen — vor Sperrenfreigabe und
        Verbindungsschluss. Die Frist ist eine Verbesserung, kein Muss.
        """
        setter = getattr(self._broker, "set_publish_deadline", None)
        if setter is None:
            return
        setter(at)

    async def _execute_shutdown(self) -> None:
        """Execute the graceful shutdown sequence."""
        # Die Uhr des Supervisors laeuft ab SIGTERM, nicht ab hier: Der
        # Signal-Dispatch (bis zu _LIFECYCLE_DISPATCH_TIMEOUT_S) und das
        # Stoppen der HTTP-Oberflaechen zaehlen bereits gegen dieselbe
        # Gnadenfrist. Ohne diesen frueh genommenen Zeitpunkt laege die
        # Endfrist hinter der Eskalation des Supervisors (R12, KTD9).
        started_at = asyncio.get_running_loop().time()
        try:
            # Bounded — a hanging handler must not stall
            # the drain/lock-release/close sequence below.
            await self._signal_dispatcher.dispatch(
                Signal.WORKER_SHUTDOWN,
                handler_timeout=_LIFECYCLE_DISPATCH_TIMEOUT_S,
                worker_id=self._config.worker_identity,
            )
            await self._stop_http_surfaces()
            # Ab hier darf kein Publish mehr laenger laufen als das
            # verbleibende Budget. Auch bereits laufende werden nachgezogen.
            self._set_publish_deadline(started_at + self._shutdown_publish_budget())
            await self._drain_in_flight()
            await self._close_executor()
            await self._release_leftover_locks()
            await self._stop_scheduler_and_di()
            # 4. Close connections
            self._state = WorkerState.SHUTTING_DOWN
            await self._broker.close()
            await self._backend.close()
        finally:
            # Immer zuruecknehmen, auch wenn oben etwas geworfen hat: Ein
            # eingebetteter Nutzer darf ueber dieselbe Instanz weiter Tasks
            # einreihen, und eine liegengebliebene, laengst verstrichene
            # Endfrist liesse danach jeden Publish sofort scheitern (R12).
            self._set_publish_deadline(None)

        # 5. STOPPED
        self._state = WorkerState.STOPPED
        # Graceful teardown done — start()'s finally _teardown_on_exit is now
        # a no-op (it only covers the cancel/exception path).
        self._torn_down = True

    async def _teardown_on_exit(self) -> None:
        """Crash-path teardown, run from start()'s finally on every exit.

        No-op once _execute_shutdown has completed (graceful path). On the
        cancel/exception path it tears down what start() spun up so nothing
        orphans: the burst monitor, the scheduler's election/poll sub-tasks
        (the root of the leak), the SIGTERM/SIGINT handlers, and the broker +
        backend connections.

        Runs the work as a shielded child task so it completes even while
        start() is itself being cancelled — the established detached-shield
        pattern (cf. _detached_releases / the DI scope-exit in
        _handle_message): the first CancelledError is absorbed and the child
        is awaited to completion (retrieving its result, so no "Future
        exception was never retrieved"), then the cancellation is re-raised.
        """
        if self._torn_down:
            return
        self._torn_down = True
        teardown = asyncio.create_task(self._abort_teardown())
        try:
            await asyncio.shield(teardown)
        except asyncio.CancelledError:
            await teardown
            raise

    async def _abort_teardown(self) -> None:
        """Best-effort, non-graceful cleanup of the resources start() itself
        spun up. Each step is independently guarded so one failure cannot leave
        a later resource leaked. NOT the graceful path: the scheduler is aborted
        (no HANDOVER / lock-release — a crashed leader's lock expires by TTL).

        Deliberately does NOT close the broker/backend: those connections are
        owned by the caller / the SwarmQ context manager (``__aexit__`` closes
        them), and a caller that cancels start_worker() may keep using them
        (e.g. to read a cancel flag). Closing them here would pull the client
        out from under such code. The aio_pika connection tasks are therefore
        reaped at context-exit, not here."""
        if self._burst_monitor_task is not None and not self._burst_monitor_task.done():
            self._burst_monitor_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._burst_monitor_task
        recycle_task = getattr(self, "_recycle_monitor_task", None)
        if recycle_task is not None and not recycle_task.done():
            recycle_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await recycle_task
        # Der Compute-Pool gehoert dem Worker: seine Kinder duerfen einen
        # abgebrochenen start_worker() nicht ueberleben (Tests, --reload,
        # eingebettet). Im IO-Modus ein No-op. Siehe
        # docs/solutions/patterns/worker-cancel-safe-teardown-detached-tasks-2026-06-01.md
        with contextlib.suppress(Exception):
            await self._close_executor()

        if (
            self._metrics_sampler_task is not None
            and not self._metrics_sampler_task.done()
        ):
            self._metrics_sampler_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._metrics_sampler_task

        # The leak root: cancel + await the scheduler election/poll sub-tasks.
        if self._scheduler is not None:
            with contextlib.suppress(Exception):
                await self._scheduler.abort()

        # Drop the process-wide signal handlers this worker installed.
        with contextlib.suppress(Exception):
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.remove_signal_handler(sig)

    def _request_shutdown(self) -> None:
        """Request a graceful shutdown. Idempotent — safe to call multiple times."""
        if self._state is not WorkerState.RUNNING:
            return
        self._state = WorkerState.DRAINING
        channel = current_channel()
        if channel is not None:
            channel.mark_draining()  # R21/KTD10: laengere Lebenszeichen-Frist im Drain
        self._shutdown_event.set()

