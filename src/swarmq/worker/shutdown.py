"""Herunterfahren des Workers (Plan 0.12.0, MR 0; verbatim aus ``lifecycle.py``).

Graceful Shutdown (``_execute_shutdown`` mit Drain, Sperrenfreigabe,
Scheduler-Stopp und Publish-Endfrist), der Crash-Teardown mit dem
Detached-Shield-Idiom und ``_request_shutdown``.

Als Mixin geschnitten wie ``WorkerLifecycle``: der Zustand lebt im
``Worker``-Konstruktor (``pipeline.py``). Logger und Konstanten werden ueber
``swarmq.worker.lifecycle`` gelesen (Q17), damit Logger-Name und
``patch("swarmq.worker.lifecycle.…")`` unveraendert gelten.
"""

from __future__ import annotations

import asyncio
import contextlib
import signal
import uuid
from typing import TYPE_CHECKING

from swarmq.dishka import _Missing, get_registered_container
from swarmq.logging import _error_fields
from swarmq.signals import Signal
from swarmq.supervision import current_channel
from swarmq.worker import lifecycle as _lc
from swarmq.worker.lifecycle import WorkerState

if TYPE_CHECKING:
    from swarmq.config import SwarmQConfig
    from swarmq.contracts.coordination import LockStore, LockToken
    from swarmq.health import HealthServer
    from swarmq.metrics import MetricsServer
    from swarmq.scheduler import SchedulerLoop
    from swarmq.signals import SignalDispatcher
    from swarmq.worker.gates import WorkerBackend, WorkerBroker
    from swarmq.worker.gates.execution_guard import ExecutionGuard


class WorkerShutdown:
    # Mixin über dem Worker-Zustand; die Attribute setzt ``Worker.__init__``
    # (pipeline.py), die Methoden-Stubs zeigen auf ``WorkerLifecycle``.
    if TYPE_CHECKING:
        _state: WorkerState
        _torn_down: bool
        _broker: WorkerBroker
        _backend: WorkerBackend
        _config: SwarmQConfig
        _signal_dispatcher: SignalDispatcher
        _shutdown_event: asyncio.Event
        _active_tasks: set[asyncio.Task[None]]
        _active_handler_locks: dict[uuid.UUID, list[LockToken]]
        _detached_releases: set[asyncio.Task[None]]
        _scheduler: SchedulerLoop | None
        _lock_manager: LockStore | None
        _execution_guard: ExecutionGuard
        _metrics_server: MetricsServer | None
        _health_server: HealthServer | None
        _burst_monitor_task: asyncio.Task[None] | None
        _metrics_sampler_task: asyncio.Task[None] | None

        async def _close_executor(self) -> None: ...

    async def _stop_http_surfaces(self) -> None:
        # Stop auto-wired HTTP surfaces first so scrapers / probes
        # get a clean connection-close rather than hitting a worker
        # in mid-drain. Idempotent when not started.
        if self._metrics_server is not None:
            try:
                await self._metrics_server.stop()
            except Exception:
                _lc.logger.exception("metrics_server_stop_failed")
        if self._health_server is not None:
            try:
                await self._health_server.stop()
            except Exception:
                _lc.logger.exception("health_server_stop_failed")

    async def _drain_in_flight(self) -> None:
        # 1. DRAINING: stop accepting new messages. cancel_consume now
        # isolates per-consumer failures and raises only after attempting
        # every consumer + clearing its registry; guard it so a partial
        # cancel cannot abort the rest of the graceful-shutdown sequence
        # (in-flight drain, lock release, scheduler stop). 2026-06-02 audit.
        try:
            await self._broker.cancel_consume()
        except Exception as exc:
            _lc.logger.warning("cancel_consume_partial_failure", **_error_fields(exc))

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
        # Nachgeholte Marker-Abschluesse des Ausfuehrungsschutzes: ein letzter,
        # begrenzter Versuch, solange Valkey noch offen ist.
        await self._execution_guard.drain()

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
                        _lc.logger.warning(
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
                _lc.logger.warning("scheduler_stop_failed", **_error_fields(exc))

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
                _lc.logger.exception("dishka_container_close_failed")

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
        reserve = min(_lc._SHUTDOWN_PUBLISH_RESERVE_S, grace / 2.0)
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
                handler_timeout=_lc._LIFECYCLE_DISPATCH_TIMEOUT_S,
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

        if self._metrics_sampler_task is not None and not self._metrics_sampler_task.done():
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
