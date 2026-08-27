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
from typing import TYPE_CHECKING, Any

import structlog

from swarmq.backend.valkey.chords import ChordManager
from swarmq.dishka import _Missing, get_registered_container
from swarmq.limits import RateLimiter
from swarmq.locking import LockManager
from swarmq.logging import sanitize_url
from swarmq.scheduler import SchedulerLoop, ScheduleStore
from swarmq.serialization import decode_result
from swarmq.signals import Signal
from swarmq.workflow_orchestration import WorkflowAdvancer, workflow_step_task_id

if TYPE_CHECKING:
    from swarmq.backend.valkey import ValkeyBackend
    from swarmq.broker.rabbitmq import RabbitMQBroker
    from swarmq.config import SwarmQConfig
    from swarmq.health import HealthServer
    from swarmq.metrics import Metrics, MetricsServer
    from swarmq.registry import TaskRegistry
    from swarmq.signals import SignalDispatcher

logger = structlog.get_logger()

# Lifecycle-Signale (WORKER_ONLINE/HEARTBEAT/SHUTDOWN) laufen mit gedeckeltem
# Handler-Timeout, damit ein haengender Handler den Shutdown nicht blockiert.
_LIFECYCLE_DISPATCH_TIMEOUT_S = 10.0



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
        _broker: RabbitMQBroker
        _backend: ValkeyBackend
        _registry: TaskRegistry
        _config: SwarmQConfig
        _signal_dispatcher: SignalDispatcher
        _shutdown_event: asyncio.Event
        _active_tasks: set[asyncio.Task[None]]
        _active_handler_locks: dict[uuid.UUID, list[tuple[str, str, int]]]
        _detached_releases: set[asyncio.Task[None]]
        _workflow_advancer: WorkflowAdvancer | None
        _scheduler: SchedulerLoop | None
        _rate_limiter: RateLimiter | None
        _lock_manager: LockManager | None
        _metrics: Metrics | None
        _metrics_server: MetricsServer | None
        _health_server: HealthServer | None
        _burst_monitor_task: asyncio.Task[None] | None
        _metrics_sampler_task: asyncio.Task[None] | None

        async def _on_message(self, message: Any) -> None: ...

        async def _publish_workflow_step(
            self, queue: str, body: bytes, headers: dict[str, Any]
        ) -> Any: ...

        def _resolve_workflow_queue(self, task_name: str) -> str: ...

    async def start(self) -> None:  # noqa: C901, PLR0915  # U9 (worker/lifecycle.py)
        """Connect broker + backend, declare queues, start consuming, block until shutdown."""
        await self._broker.connect()
        await self._backend.connect()

        # Backend.client is available only after connect(). Instantiate
        # the rate limiter here so the registered Lua script uses the
        # live client.
        self._rate_limiter = RateLimiter(self._backend.client)
        # LockManager has the same lifecycle constraint — registered
        # AsyncScripts need a live client. See plan 008 Unit 4.
        self._lock_manager = LockManager(self._backend.client)

        # R4.1 workflow advancer (U5) — ChordManager registers chord_complete.lua
        # against the live client, same as RateLimiter/LockManager above.
        self._workflow_advancer = WorkflowAdvancer(
            self._backend,
            ChordManager(self._backend.client),
            publish=self._publish_workflow_step,
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
            worker_id=str(id(self)),
        )
        await self._scheduler.start()

        # ce-review REL-06: auto-wire Prometheus metrics + health
        # endpoints when config flags set. Operator no longer needs
        # to construct Metrics + install_signal_collectors + MetricsServer
        # manually — the default wiring is opt-in via metrics_port /
        # health_port config.
        #
        # ce-review COR-001/REL-F-01: the partial-start cleanup path
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
                await self._metrics_server.start()
            if self._config.health_port is not None:
                from swarmq.health import HealthChecker, HealthServer
                checker = HealthChecker(self._broker, self._backend)
                self._health_server = HealthServer(
                    checker,
                    host=self._config.health_host,
                    port=self._config.health_port,
                )
                await self._health_server.start()
        except Exception:
            # COR-002 tail: the scheduler has already started; its
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
            worker_id=str(id(self)),
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

        # 2026-06-02 audit P1-3: start the metrics sampler when metrics are
        # enabled so the queue_depth / dlq_messages / uptime gauges are
        # actually populated and WORKER_HEARTBEAT is emitted.
        if self._metrics is not None and self._metrics.enabled:
            self._worker_started_at = time.monotonic()
            self._metrics_sampler_task = asyncio.create_task(
                self._metrics_sampler()
            )

        # Block until shutdown is requested. The try/finally makes the cancel
        # path safe: a plain cancel of this coroutine (tests, ProcessManager
        # reload, embedded use) would otherwise skip _execute_shutdown and
        # leak the scheduler election/poll tasks + broker connection tasks.
        try:
            await self._shutdown_event.wait()
            await self._execute_shutdown()
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

        2026-06-02 audit P1-3: queue_depth, dlq_messages and
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
                # Härtung R2 P2: one failing iteration must not kill the
                # loop — an unobserved sampler death froze the gauges on
                # their last value with zero operator signal (the exact
                # "looks instrumented but isn't" trap P1-3 closed).
                try:
                    await self._sample_metrics_once()
                except Exception as exc:
                    logger.warning(
                        "metrics_sampler_iteration_failed",
                        error=repr(exc),
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
        worker_id = str(id(self))
        now = time.monotonic()
        uptime = now - self._worker_started_at
        self._metrics.worker_uptime_seconds.labels(
            worker_id=worker_id,
        ).set(uptime)
        for queue in self._config.queues:
            try:
                depth = await self._broker.get_queue_depth(queue)
                self._metrics.queue_depth.labels(
                    queue=queue, kind="pending",
                ).set(depth)
                dlq = await self._broker.get_dlq_depth(queue)
                self._metrics.dlq_messages.labels(queue=queue).set(dlq)
            except Exception as exc:
                # Härtung R2 P2: broad on purpose. Catching only
                # BrokerConnectionError let any other error (Timeout,
                # ChannelInvalidStateError, decode glitch) propagate and
                # kill the sampler task silently.
                logger.warning(
                    "metrics_sample_failed",
                    queue=queue,
                    error=repr(exc),
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

    async def _execute_shutdown(self) -> None:  # noqa: C901  # U9 (worker/lifecycle.py)
        """Execute the graceful shutdown sequence."""
        # Härtung R2 P1-9: bounded — a hanging handler must not stall
        # the drain/lock-release/close sequence below.
        await self._signal_dispatcher.dispatch(
            Signal.WORKER_SHUTDOWN,
            handler_timeout=_LIFECYCLE_DISPATCH_TIMEOUT_S,
            worker_id=str(id(self)),
        )

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

        # 1. DRAINING: stop accepting new messages. cancel_consume now
        # isolates per-consumer failures and raises only after attempting
        # every consumer + clearing its registry; guard it so a partial
        # cancel cannot abort the rest of the graceful-shutdown sequence
        # (in-flight drain, lock release, scheduler stop). 2026-06-02 audit.
        try:
            await self._broker.cancel_consume()
        except Exception:
            logger.warning("cancel_consume_partial_failure")

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
            for handler_id, locks in list(self._active_handler_locks.items()):  # noqa: B007
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

        # 4. Close connections
        self._state = WorkerState.SHUTTING_DOWN
        await self._broker.close()
        await self._backend.close()

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
        self._shutdown_event.set()

