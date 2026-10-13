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
from collections.abc import Awaitable
from typing import TYPE_CHECKING

from swarmq.dishka import _Missing, get_registered_container
from swarmq.logging import _error_fields
from swarmq.shutdown_budget import ShutdownBudget, shutdown_budget
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
    from swarmq.worker.lock_keeper import LockKeeper


class WorkerShutdown:
    # Mixin über dem Worker-Zustand; die Attribute setzt ``Worker.__init__``
    # (pipeline.py), die Methoden-Stubs zeigen auf ``WorkerLifecycle``.
    if TYPE_CHECKING:
        _state: WorkerState
        _torn_down: bool
        _shutdown_end: float | None  # BZ-18: gesetzt in ``_execute_shutdown``
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
        _lock_keeper: LockKeeper
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
            except Exception:  # noqa: BLE001 - Teardown best effort
                _lc.logger.exception("metrics_server_stop_failed")
        if self._health_server is not None:
            try:
                await self._health_server.stop()
            except Exception:  # noqa: BLE001 - Teardown best effort
                _lc.logger.exception("health_server_stop_failed")

    async def _drain_in_flight(self, drain_until: float | None = None) -> None:
        # 1. DRAINING: stop accepting new messages. cancel_consume now
        # isolates per-consumer failures and raises only after attempting
        # every consumer + clearing its registry; guard it so a partial
        # cancel cannot abort the rest of the graceful-shutdown sequence
        # (in-flight drain, lock release, scheduler stop). 2026-06-02 audit.
        try:
            await self._broker.cancel_consume()
        except Exception as exc:  # noqa: BLE001 - Teilfehler darf Shutdown nicht abbrechen
            _lc.logger.warning("cancel_consume_partial_failure", **_error_fields(exc))

        # 2. Wait for in-flight tasks to complete
        tasks_snapshot = set(self._active_tasks)
        if tasks_snapshot:
            # KTD11/R22: ein Recycling-Drain wartet ohne erzwungenen Abbruch
            # (Frist aus dem groessten hard_timeout), damit ein gesunder
            # Langlaeufer keinen Retry verliert; SIGTERM behaelt shutdown_timeout.
            # BZ-18 (Plan 0.12.0, MR 17): sonst endet der Drain bei ``drain_until``
            # (Loop-Zeit), damit Sperren, Leader-Uebergabe und Verbindungsschluss
            # noch im Gesamtbudget ab SIGTERM liegen.
            drain_timeout = getattr(self, "_drain_timeout_override", None)
            if not drain_timeout:
                drain_timeout = float(self._config.shutdown_timeout)
                if drain_until is not None:
                    drain_timeout = max(0.0, drain_until - asyncio.get_running_loop().time())
            _done, pending = await asyncio.wait(
                tasks_snapshot,
                timeout=drain_timeout,
            )

            # 3. SHUTTING_DOWN: cancel remaining tasks if drain timeout exceeded
            if pending:
                self._state = WorkerState.SHUTTING_DOWN
                for task in pending:
                    task.cancel()
                await asyncio.wait(pending, timeout=min(1.0, self._step_timeout()))

    async def _release_leftover_locks(self) -> None:
        # 3b. Await detached lock-release tasks (Phase 3b.3). When a
        # handler finally block was interrupted by outer-cancel, its
        # shielded _release_acquired_locks call was detached into
        # self._detached_releases. We must await them BEFORE
        # backend.close() — otherwise the detached release would use a
        # closed Valkey client and the lock would stay held until TTL.
        if self._detached_releases:
            # BZ-18: im Budget, nicht mehr mindestens 5 s.
            await asyncio.wait(self._detached_releases, timeout=self._step_timeout())
        # Nachgeholte Marker-Abschluesse des Ausfuehrungsschutzes: ein letzter,
        # begrenzter Versuch, solange Valkey noch offen ist.
        await self._bounded_step("execution_guard_drain", self._execution_guard.drain())

        # 3c. Release any leftover handler locks. Runs AFTER the
        # asyncio.wait above (so cancelled tasks have had a chance to
        # finish their finally and release their locks themselves) but
        # BEFORE backend.close (so LockManager.release still has a
        # working Valkey client). Whatever's still in the registry
        # corresponds to a handler whose finally never ran (typically
        # because the task was cancel()ed before reaching it).
        if self._lock_manager is not None and self._active_handler_locks:
            for locks in list(self._active_handler_locks.values()):
                for token in locks:
                    kept = self._lock_keeper.drop(token)
                    task_fields = {} if kept is None else {"task_id": kept.task_id, "task_name": kept.task_name}
                    try:
                        released = await asyncio.wait_for(self._lock_manager.release(token), self._step_timeout())
                    except Exception as exc:  # noqa: BLE001 - Lock-TTL heilt den Fall selbst
                        # KTD11: die Lock-TTL heilt den Fall selbst -> WARNING.
                        _lc.logger.warning(
                            "shutdown_lock_release_failed",
                            lock_name=token.name,
                            **_error_fields(exc),
                        )
                        continue
                    if released is False:
                        # BZ-04 (Plan 0.12.0, MR 13): wie ``release_acquired_locks``.
                        _lc.logger.warning(
                            "task_lock_lost",
                            lock_name=token.name,
                            **task_fields,
                            stage="release",
                            hint="the lock was gone or held by another owner at release; "
                            "a second holder may have run in parallel",
                        )
            self._active_handler_locks.clear()
        # BZ-25: der Keeper endet, bevor das Backend schliesst.
        self._lock_keeper.close()

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
                await asyncio.wait_for(self._scheduler.stop(), self._step_timeout())
            except Exception as exc:  # noqa: BLE001 - Leader-Lock laeuft per TTL ab
                # KTD11: Leader-Lock und Marker laufen per TTL ab -> WARNING.
                _lc.logger.warning("scheduler_stop_failed", **_error_fields(exc))
                if isinstance(exc, TimeoutError):
                    # BZ-18: keine Wahl-/Poll-Schleife ueberleben lassen.
                    with contextlib.suppress(Exception):
                        await asyncio.wait_for(self._scheduler.abort(), self._step_timeout())

        # 3e. Close the Dishka APP container (if one was registered). Runs
        # AFTER the in-flight drain + detached-release wait (so live REQUEST
        # scopes have finalized) and only when DI is active — no-op otherwise.
        # APP-scoped resources (connection pools etc.) are torn down here; a
        # close() failure is logged, not raised, so it cannot abort shutdown.
        _di_container = get_registered_container()
        if not isinstance(_di_container, _Missing):
            try:
                await self._bounded_step("di_container_close", _di_container.close())
            except Exception:  # noqa: BLE001 - Teardown best effort
                _lc.logger.exception("dishka_container_close_failed")

    async def _bounded_step(self, step: str, awaitable: Awaitable[object]) -> None:
        """BZ-18: einen Aufraeumschritt mit Zeitlimit ausfuehren; bei
        Ueberschreitung WARNING ``shutdown_step_timeout`` und weiter."""
        timeout = self._step_timeout()
        try:
            await asyncio.wait_for(awaitable, timeout)
        except TimeoutError:
            _lc.logger.warning("shutdown_step_timeout", step=step, timeout_s=round(timeout, 3))

    def _step_timeout(self) -> float:
        """BZ-18: Zeitlimit eines Aufraeumschritts -- der Rest bis zum Ende des
        Budgets, mindestens ``_SHUTDOWN_STEP_MIN_S`` (ein ausgeschoepftes
        Budget ueberspringt keinen Schritt)."""
        end = getattr(self, "_shutdown_end", None)
        remaining = float(self._config.shutdown_timeout) if end is None else end - asyncio.get_running_loop().time()
        return max(remaining, _lc._SHUTDOWN_STEP_MIN_S)

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
        return self._shutdown_budget().drain_until

    def _shutdown_budget(self) -> ShutdownBudget:
        """BZ-18: ``shutdown_timeout`` als Gesamtbudget ab SIGTERM; unter dem
        Supervisor (Kanal gesetzt) endet es mit dessen SIGKILL."""
        return shutdown_budget(float(self._config.shutdown_timeout), supervised=current_channel() is not None)

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
            budget = self._shutdown_budget()
            self._shutdown_end = started_at + budget.finish_by
            self._set_publish_deadline(started_at + budget.drain_until)
            await self._drain_in_flight(started_at + budget.drain_until)
            await self._bounded_step("executor_close", self._close_executor())
            await self._release_leftover_locks()
            await self._stop_scheduler_and_di()
            # 4. Close connections
            self._state = WorkerState.SHUTTING_DOWN
            # BZ-18: auch der Verbindungsschluss (Broker: zwei Verbindungen,
            # MR 16) liegt im Budget; das Backend schliesst nach dem Broker.
            await self._bounded_step("broker_close", self._broker.close())
            await self._bounded_step("backend_close", self._backend.close())
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

        # BZ-25 (Review MR 13): keine Keeper-Schleife ueberleben lassen; die
        # Locks laufen mit ihrer Rest-TTL ab.
        with contextlib.suppress(Exception):
            self._lock_keeper.close()

        # The leak root: cancel + await the scheduler election/poll sub-tasks.
        if self._scheduler is not None:
            with contextlib.suppress(Exception):
                await self._scheduler.abort()

        # Nachholversuche des Ausfuehrungsschutzes: ein letzter, begrenzter
        # Versuch, danach keine weiteren. Ohne das liefen sie (auch die von
        # Handlern, die erst nach dem Abbruch abschliessen) gegen das
        # geschlossene Backend weiter, bis der Event-Loop schliesst.
        with contextlib.suppress(Exception):
            await self._bounded_step("execution_guard_drain", self._execution_guard.drain())

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
