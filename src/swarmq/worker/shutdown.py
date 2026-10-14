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
from typing import TYPE_CHECKING, Any, cast

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


#: ``cancel_consume``-Aufrufe, deren Warten der Drain aufgegeben hat (FS-09):
#: die Referenz haelt sie bis zum Ende am Leben, ihr Ausgang wird geloggt.
_LATE_CONSUME_CANCELS: set[asyncio.Future[None]] = set()


def _collect_late_consume_cancel(cancelling: asyncio.Future[None]) -> None:
    _LATE_CONSUME_CANCELS.discard(cancelling)
    if cancelling.cancelled():
        return
    exc = cancelling.exception()
    if isinstance(exc, Exception):
        _lc.logger.warning("cancel_consume_partial_failure", late=True, **_error_fields(exc))


class WorkerShutdown:
    # Mixin über dem Worker-Zustand; die Attribute setzt ``Worker.__init__``
    # (pipeline.py), die Methoden-Stubs zeigen auf ``WorkerLifecycle``.
    if TYPE_CHECKING:
        _state: WorkerState
        _torn_down: bool
        _shutdown_end: float | None  # BZ-18: gesetzt in ``_execute_shutdown``
        _drain_timeout_override: float | None
        _recycle_sigterm_at: float | None  # FS-30: erstes SIGTERM im Recycling-Drain
        _recycle_interrupt: asyncio.Event | None  # FS-30: weckt den laufenden Drain
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

    async def _drain_in_flight(self, drain_until: float) -> None:
        # ``drain_until`` ist das Drain-Ende aus ``_drain_end`` (Loop-Zeit,
        # FS-02 Teil b): beim Recycling die eigene Frist, sonst das Budget.
        # 1. DRAINING: stop accepting new messages.
        drain_until = await self._cancel_consume(drain_until)

        # 2. Wait for in-flight tasks to complete. Neu gestartete Handler
        # (Zustellung an einen spaet angemeldeten Consumer oder nach einem
        # aufgegebenen ``cancel_consume``) gehoeren mit in den Drain: Sonst
        # liefen sie ungesehen in Sperrenfreigabe und ``broker.close()``
        # (Review U10).
        pending: set[asyncio.Task[None]] = set()
        tasks_snapshot = set(self._active_tasks)
        while tasks_snapshot and not pending:
            pending = await self._wait_for_drain(tasks_snapshot, drain_until)
            tasks_snapshot = set(self._active_tasks) - pending

        # 3. SHUTTING_DOWN: cancel remaining tasks if drain timeout exceeded
        if pending:
            pending |= set(self._active_tasks)
            self._state = WorkerState.SHUTTING_DOWN
            for task in pending:
                task.cancel()
            await asyncio.wait(pending, timeout=min(1.0, self._step_timeout()))

    async def _cancel_consume(self, drain_until: float) -> float:
        """Consumer abmelden, begrenzt bis zum Drain-Ende (FS-09, KTD16).

        Das Warten endet spaetestens bei *drain_until* (mindestens
        ``_SHUTDOWN_STEP_MIN_S``); ein SIGTERM im Recycling-Drain zieht es wie
        den Drain selbst auf den Anker vor (FS-30, Review U10). Liefert das so
        gueltige Drain-Ende. Das Abmelden selbst wird dabei nicht abgebrochen:
        Ein abgebrochenes ``basic.cancel`` schloesse in aiormq den
        Consume-Kanal, und alle laufenden Handler kaemen als Neuzustellung
        zurueck. Es laeuft weiter, bis der Broker antwortet, die Notbremse des
        Waechters greift, ``broker.close()`` den Kanal schliesst oder der
        Kanalbezug bei getrennter Verbindung aufgibt.

        ``cancel_consume`` isoliert Fehler je Consumer und wirft erst danach;
        ein Teilfehler darf den Rest der Sequenz (Drain, Sperrenfreigabe,
        Scheduler-Stopp) nicht abbrechen (Audit 2026-06-02).
        """
        loop = asyncio.get_running_loop()
        started = loop.time()
        cancelling = asyncio.ensure_future(self._broker.cancel_consume())
        pending, drain_until = await self._wait_anchored(
            {cancelling}, drain_until, not_before=started + _lc._SHUTDOWN_STEP_MIN_S
        )
        if pending:
            timeout = max(drain_until - started, _lc._SHUTDOWN_STEP_MIN_S)
            _lc.logger.warning("shutdown_step_timeout", step="cancel_consume", timeout_s=round(timeout, 3))
            _LATE_CONSUME_CANCELS.add(cancelling)
            cancelling.add_done_callback(_collect_late_consume_cancel)
            return drain_until
        try:
            cancelling.result()
        except Exception as exc:  # noqa: BLE001 - Teilfehler darf Shutdown nicht abbrechen
            _lc.logger.warning("cancel_consume_partial_failure", **_error_fields(exc))
        return drain_until

    async def _wait_for_drain(self, tasks: set[asyncio.Task[None]], drain_until: float) -> set[asyncio.Task[None]]:
        """Bis *drain_until* auf *tasks* warten; liefert die noch laufenden."""
        pending, _drain_until = await self._wait_anchored(set(tasks), drain_until)
        return cast("set[asyncio.Task[None]]", pending)

    async def _wait_anchored(
        self, futures: set[asyncio.Future[Any]], drain_until: float, *, not_before: float | None = None
    ) -> tuple[set[asyncio.Future[Any]], float]:
        """Bis *drain_until* (nicht vor *not_before*) auf *futures* warten.

        Liefert die noch laufenden und das gueltige Drain-Ende. FS-30 (KTD4):
        Ein SIGTERM im Recycling-Drain weckt das Warten
        (``_recycle_interrupt``) und zieht das Drain-Ende samt
        Publish-Endfrist nach vorn. Der Weck-Task wird im ``finally``
        gestrichen (kein Task-Leck).
        """
        loop = asyncio.get_running_loop()
        interrupt = asyncio.Event()
        self._recycle_interrupt = interrupt
        if getattr(self, "_recycle_sigterm_at", None) is not None:
            interrupt.set()
        waiter: asyncio.Task[bool] | None = asyncio.create_task(interrupt.wait())
        pending = set(futures)
        try:
            while pending:
                watched: set[asyncio.Future[Any]] = set(pending)
                if waiter is not None:
                    watched.add(waiter)
                end = drain_until if not_before is None else max(drain_until, not_before)
                done, _ = await asyncio.wait(
                    watched, timeout=max(0.0, end - loop.time()), return_when=asyncio.FIRST_COMPLETED
                )
                if not done:
                    break
                pending.difference_update(done)
                if waiter is not None and waiter in done:
                    waiter = None
                    anchored = self._anchored_drain_end(drain_until)
                    if anchored < drain_until:
                        drain_until = anchored
                        self._set_publish_deadline(drain_until)
        finally:
            self._recycle_interrupt = None
            if waiter is not None:
                waiter.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await waiter
        return pending, drain_until

    def _anchored_drain_end(self, drain_end: float) -> float:
        """KTD4: nach einem SIGTERM im Recycling-Drain endet der Drain
        spaetestens ``drain_until`` nach diesem Signal; nur nach vorn."""
        anchor: float | None = getattr(self, "_recycle_sigterm_at", None)
        if anchor is None:
            return drain_end
        return min(drain_end, anchor + self._shutdown_budget().drain_until)

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
        # Den Eintrag je Handler VOR der Freigabe entnehmen (wie
        # ``release_acquired_locks``): endet ein Nachlauf
        # (``_release_after_teardown``), waehrend diese Schleife noch
        # freigibt, faende er ihn sonst vor und gaebe denselben Token ein
        # zweites Mal frei -- Fehlalarm ``task_lock_lost``.
        if self._lock_manager is not None and self._active_handler_locks:
            for handler_id in list(self._active_handler_locks):
                for token in self._active_handler_locks.pop(handler_id, []):
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

    def _drain_end(self, started_at: float, budget: ShutdownBudget) -> float:
        """Drain-Ende in Loop-Zeit ab ``started_at`` (FS-02 Teil b, KTD3).

        Die einzige Quelle fuer Drain-Wartezeit und Publish-Endfrist: Ein
        Recycling-Drain wartet ohne erzwungenen Abbruch seine eigene Frist ab
        (KTD11/R22, ``_drain_timeout_override``), damit ein gesunder
        Langlaeufer keinen Retry verliert; SIGTERM endet bei ``drain_until``
        (BZ-18), damit Sperren, Leader-Uebergabe und Verbindungsschluss noch
        im Gesamtbudget ab SIGTERM liegen. ``0``/``None`` heisst: kein Override.
        """
        override: float | None = getattr(self, "_drain_timeout_override", None)
        if override:
            return self._anchored_drain_end(started_at + override)
        return started_at + budget.drain_until

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
            drain_end = self._drain_end(started_at, budget)
            self._set_publish_deadline(drain_end)
            await self._drain_in_flight(drain_end)
            drain_end = self._drain_end(started_at, budget)  # ein SIGTERM kann es vorgezogen haben
            if drain_end > started_at + budget.drain_until:
                # Nur ein Recycling-Drain darf ueber das Budget hinaus warten.
                # Danach bekaemen die Aufraeumschritte sonst nur noch die
                # Untergrenze: Reserve ab Drain-Ende, nie kuerzer als das
                # urspruengliche Ende (FS-02 Teil b, KTD3). Ohne Recycling
                # bleibt es bei ``finish_by`` (E-018, SIGKILL des Supervisors).
                self._shutdown_end = max(
                    self._shutdown_end,
                    asyncio.get_running_loop().time() + (budget.finish_by - budget.drain_until),
                )
            anchor: float | None = getattr(self, "_recycle_sigterm_at", None)
            if anchor is not None:
                # FS-30 (KTD4): nach einem SIGTERM im Recycling endet auch das
                # Aufraeumen im Budget ab diesem Signal (E-018, SIGKILL).
                self._shutdown_end = min(self._shutdown_end, anchor + budget.finish_by)
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
        """Request a graceful shutdown. Idempotent — safe to call multiple times.

        Im laufenden Recycling-Drain ist der Aufruf ein SIGTERM: Er setzt das
        Shutdown-Budget ab diesem Signal in Kraft (FS-30, KTD4).
        """
        if self._state is not WorkerState.RUNNING:
            if getattr(self, "_drain_timeout_override", None):
                self._interrupt_recycling_drain()
            return
        self._state = WorkerState.DRAINING
        channel = current_channel()
        if channel is not None:
            channel.mark_draining()  # R21/KTD10: laengere Lebenszeichen-Frist im Drain
        self._shutdown_event.set()

    def _interrupt_recycling_drain(self) -> None:
        """Erstes SIGTERM im Recycling-Drain: Anker setzen und den Drain wecken.

        Ein zweites SIGTERM (Eskalation des Supervisors) ankert nicht neu,
        sonst laege das Drain-Ende hinter dessen SIGKILL (``fix-r-flow.md``
        Kritisch 3).
        """
        if getattr(self, "_recycle_sigterm_at", None) is not None:
            return
        self._recycle_sigterm_at = asyncio.get_running_loop().time()
        interrupt: asyncio.Event | None = getattr(self, "_recycle_interrupt", None)
        if interrupt is not None:
            interrupt.set()
