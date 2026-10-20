"""Supervisor (Plan U21): ``ProcessManager`` (Restart-Budget, Autoscale,
SIGHUP-Reload mit eingefrorenem argv/env) und der injizierte
``QueueDepthFn``-Seam. Slot-Tabelle und ``WorkerInfo`` liegen in ``slots``
(Plan 2026-09-08-001, KTD1), Signal-Blöcke und das Kindziel in
``worker_target``."""

from __future__ import annotations

import logging
import multiprocessing
import os
import signal
import socket
import sys
import time
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, override

if TYPE_CHECKING:  # pragma: no cover — import-time only
    from swarmq.hot_reload import FileWatcher

from swarmq.logging import _error_fields, get_logger
from swarmq.process_manager import autoscale, setpoint
from swarmq.process_manager.entry import CONFIG_ERROR_EXIT_CODE
from swarmq.process_manager.reload import ReloadController, reexec
from swarmq.process_manager.restart_budget import RestartBudget
from swarmq.process_manager.setpoint_store import DEFAULT_SAMPLE_INTERVAL_S, SetpointStoreFactory
from swarmq.process_manager.slots import SlotState, SlotTable, WorkerInfo
from swarmq.process_manager.worker_target import block_signals, run_worker_entry, unblock_signals
from swarmq.shutdown_budget import shutdown_budget
from swarmq.supervision import (
    FORK_SOURCE_REJECTED_EXIT_CODE,
    RECYCLE_NOACK_EXIT_CODE,
    SupervisorChannel,
)
from swarmq.supervision import (
    SHUTDOWN_TERM_S as _SHUTDOWN_TERM_S,
)

logger = get_logger(__name__)


QueueDepthFn = Callable[[], int]


class ProcessManager(RestartBudget):
    """Supervises multi-process workers with auto-restart + autoscaling.

    Parameters
    ----------
    num_workers:
        Initial worker count. With autoscaling, serves as the warm
        starting point clamped to ``[min, max]``.
    worker_entry:
        Callable executed inside each spawned subprocess. Must accept
        ``(worker_id: int, shutdown_event, *worker_args)``; ``worker_id``
        is the slot ``0..N-1`` and repeats across replacements (KTD1).
    worker_args:
        Extra positional arguments passed to ``worker_entry`` after
        ``worker_id`` and ``shutdown_event``.
    autoscale:
        ``(min_workers, max_workers)`` tuple or ``None``. When set,
        the supervisor polls ``queue_depth_fn`` every
        :attr:`AUTOSCALE_SAMPLE_INTERVAL` seconds and adjusts the
        worker count.
    queue_depth_fn:
        Synchronous callable returning the total pending-message
        count across all watched queues. Required when
        ``autoscale`` is set.
    """

    # Rolling-window restart limit (roadmap: 5 per hour).
    MAX_RESTARTS = 5
    RESTART_WINDOW = 3600.0
    # BZ-27 (Plan 0.12.0, MR 17): ein gesperrter Slot laeuft nach dieser
    # Abkuehlzeit wieder an, statt dauerhaft zu fehlen.
    SLOT_COOLDOWN_S = 3600.0

    # Parent-loop tick.
    MONITOR_INTERVAL = 0.5

    # Gesamtbudget des Herunterfahrens ab SIGTERM (Default; pro Instanz ueber
    # ``shutdown_grace_s`` = ``SwarmQConfig.shutdown_timeout``). Seit BZ-18
    # (Plan 0.12.0, MR 17) liegen Drain, SIGTERM-Phase, SIGKILL und Einsammeln
    # darin (``swarmq.shutdown_budget``); ``SHUTDOWN_TERM_S`` bleibt die
    # SIGTERM-Frist ersetzter Worker im laufenden Betrieb.
    SHUTDOWN_GRACE_S = 30.0
    SHUTDOWN_TERM_S = _SHUTDOWN_TERM_S  # lebt seit U3 in ``supervision`` (KTD4)

    # Autoscaling knobs.
    SCALE_UP_THRESHOLD = 50  # pending tasks per active worker
    SCALE_DOWN_IDLE_S = 60.0  # sustained idle window before scale-down
    SCALE_COOLDOWN_S = 30.0  # minimum gap between scale actions
    AUTOSCALE_SAMPLE_INTERVAL = 10.0
    # Minimum worker age before the autoscaler may pick it for
    # scale-down. Literal 30.0, matching
    # max(AUTOSCALE_SAMPLE_INTERVAL=10, SCALE_COOLDOWN_S=30) at the
    # current defaults — protects the crash → _maybe_restart →
    # _autoscale_tick churn path where a freshly spawned replacement
    # would otherwise be eligible on the very next tick. Must stay
    # < SCALE_DOWN_IDLE_S so steady-state scale-down is unaffected.
    # If you subclass and change AUTOSCALE_SAMPLE_INTERVAL or
    # SCALE_COOLDOWN_S, override SCALE_MIN_AGE_S to stay
    # >= max(AUTOSCALE_SAMPLE_INTERVAL, SCALE_COOLDOWN_S).
    SCALE_MIN_AGE_S = 30.0
    # Kontrollkanal (KTD5): Abtastintervall des Sollwert-Durchlaufs.
    SETPOINT_SAMPLE_INTERVAL = DEFAULT_SAMPLE_INTERVAL_S

    def __init__(
        self,
        num_workers: int,
        worker_entry: Callable[..., None],
        worker_args: tuple[Any, ...] = (),
        *,
        autoscale: tuple[int, int] | None = None,
        queue_depth_fn: QueueDepthFn | None = None,
        shutdown_grace_s: float | None = None,
        reload: bool = False,
        watch_dirs: tuple[str, ...] = (),
        heartbeat_timeout_s: float = 60.0,
        delivery_limit: int = 20,
        max_processes: int | None = None,
        group: str | None = None,
        setpoint_store_factory: SetpointStoreFactory | None = None,
        supervisor_id: str | None = None,
    ) -> None:
        if autoscale is not None:
            min_w, max_w = autoscale
            if min_w < 1 or max_w < min_w:
                raise ValueError(
                    f"invalid autoscale bounds (min={min_w}, max={max_w}); require min >= 1 and max >= min",
                )
            if queue_depth_fn is None:
                raise ValueError(
                    "queue_depth_fn is required when autoscale is set",
                )
            num_workers = max(min_w, min(num_workers, max_w))

        self.initial_workers = num_workers
        # Kontrollkanal (Plan 2026-09-16, R1/R5): erklaerter Spielraum (``None``
        # = Kanal aus) und der von der CLI fertig gebildete Gruppenschluessel.
        self.max_processes = max_processes
        self.group = group
        self._setpoint_store_factory = setpoint_store_factory
        self.supervisor_id = supervisor_id or f"{socket.gethostname()}:p{os.getpid()}"
        # KTD12: wirksame Grenzen; die Startargumente bleiben als Rueckfall.
        self._setpoint = setpoint.SetpointState.from_start(
            autoscale if autoscale is not None else (num_workers, num_workers),
        )
        self._worker_entry = worker_entry
        self._worker_args = worker_args
        self._autoscale = autoscale
        self._queue_depth_fn = queue_depth_fn
        # Per-instance override of the total shutdown budget (BZ-18: the
        # whole sequence from SIGTERM, not only phase 1). Without this, any
        # task running longer than the class default was killed
        # mid-execution regardless of SwarmQConfig.shutdown_timeout.
        # E-018/F3-42: 0 heisst "sofort abbrechen" -- ``shutdown_budget`` legt
        # dann alle Phasen auf 0, SIGTERM und SIGKILL folgen ohne Wartezeit.
        if shutdown_grace_s is not None:
            if shutdown_grace_s < 0:
                raise ValueError(
                    f"shutdown_grace_s must be >= 0 (got {shutdown_grace_s})",
                )
            self.shutdown_grace_s = float(shutdown_grace_s)
        else:
            self.shutdown_grace_s = self.SHUTDOWN_GRACE_S

        self._ctx = multiprocessing.get_context("spawn")
        self.shutdown_event = self._ctx.Event()
        self.workers: dict[int, WorkerInfo] = {}
        # KTD1: Slots 0..N-1 (N = --processes bzw. Autoscale-Obergrenze); mit
        # Kontrollkanal der erklaerte Spielraum (R4) -- die Tabelle waechst
        # zur Laufzeit nie.
        if max_processes is not None:
            slot_count = max_processes
        elif autoscale is not None:
            slot_count = autoscale[1]
        else:
            slot_count = num_workers
        self._slots = SlotTable(slot_count)
        # BZ-27 (Plan 0.12.0, MR 17): Zahl gesperrter Slots fuer die Worker-Gauge;
        # lazy beim ersten Start ueber ``self._ctx`` angelegt.
        self._locked_slots_shared: Any = None
        self._shutting_down = False
        # BZ-18: Ende des Gesamtbudgets (monoton), gesetzt beim Herunterfahren.
        self._shutdown_deadline: float | None = None
        # KTD8: vom Handler hinterlegt und nie geloescht -- ``reexec`` prueft
        # es direkt vor ``execvpe``; geloggt wird es genau einmal.
        self._shutdown_signal: str | None = None
        self._shutdown_signal_logged = False
        self._reexec_holds_signals = False  # ``reload.reexec`` vor ``execve``
        self._reload_requested = False
        self.reloader = ReloadController()  # KTD8; ``run`` setzt ihn mit --log-config
        # R20/KTD9 (Plan 2026-09-03-001): sauber beendete Worker (Exit 0 ohne
        # expected_shutdown, z. B. --burst) werden nicht nachgefuellt; sind
        # alle so beendet, endet der Supervisor mit 0.
        self._clean_exits = 0
        # R21/R30 (Plan 2026-09-03-001): Lebenszeichen-Frist und eigenes
        # Ersatz-Budget (2 x Delivery-Limit je Stunde), getrennt von MAX_RESTARTS.
        self.heartbeat_timeout_s = float(heartbeat_timeout_s)
        self.hang_replacement_limit = 2 * int(delivery_limit)
        self._hang_timestamps: list[float] = []
        self._terminating: list[tuple[WorkerInfo, float]] = []
        # R29 (Plan 2026-09-03-001): ein Worker endete mit
        # CONFIG_ERROR_EXIT_CODE — kein Neustart, der Supervisor endet mit 1.
        self._config_error = False

        # Autoscale state. Initialised to -inf ("never sampled / never
        # scaled") rather than 0.0 so the FIRST tick always samples and the
        # first scale decision is never blocked by cooldown — independent of
        # the absolute time.monotonic() magnitude. With 0.0, a process started
        # within AUTOSCALE_SAMPLE_INTERVAL of system boot (small monotonic
        # clock, e.g. a fresh container) would have its first sample wrongly
        # gated by `now - 0.0 < interval`.
        self._last_scale_decision = float("-inf")
        self._last_autoscale_sample = float("-inf")
        self._idle_since: float | None = None

        # Freeze argv AND environment at
        # supervisor start. SIGHUP reload uses these snapshots so
        # in-process mutation (buggy 3rd-party lib rewriting sys.argv
        # or mutating os.environ['PYTHONPATH']) cannot change the
        # re-exec vector. Both are shallow copies; argv is list[str]
        # (strings are immutable) and env is dict[str, str].
        # orig_argv statt argv: behaelt ``python -m`` und Flags (argv[0] waere __main__.py).
        self._startup_argv: list[str] = list(sys.orig_argv[1:])
        self._startup_env: dict[str, str] = dict(os.environ)

        # R5.3 Unit 6: optional file-watch hot-reload trigger. The
        # watcher lives in a daemon thread and fires SIGHUP at our own
        # PID — the existing _on_reload_signal handler does the work.
        self._reload: bool = bool(reload)
        self._watch_dirs: tuple[str, ...] = tuple(watch_dirs)
        # TYPE_CHECKING import gives mypy/IDEs full
        # coverage on .start()/.stop() call sites without forcing
        # watchfiles onto the import path at runtime.
        self._file_watcher: FileWatcher | None = None

    # ------------------------------------------------------------------
    # Public entry-point
    # ------------------------------------------------------------------

    def run(self) -> int:
        """Blocking supervisor entry-point. Returns a POSIX exit code.

        On ``SIGHUP``, returns after graceful-shutdown and re-execs
        via ``os.execvpe(sys.executable, [sys.executable] +
        self._startup_argv, self._startup_env)`` — the call does not
        return. The argv and env snapshots were captured in
        :meth:`__init__` to defeat in-process mutation of either
        trust surface.
        """
        logger.info(
            "process_manager_start",
            pid=os.getpid(),
            workers=self.initial_workers,
            autoscale=self._autoscale,
        )
        setpoint.read_at_start(self)  # R6: vor dem ersten Kind, scheitert nie
        block_signals()
        self._start_all_workers()
        self._setup_signals()
        unblock_signals()

        # R5.3 Unit 6: arm the file-watcher AFTER _setup_signals so the
        # SIGHUP handler is guaranteed to be installed by the time the
        # watcher can fire its first trigger. Kept lazy-imported so the
        # `watchfiles` optional dep is not required on the import path.
        if self._reload:
            logger.warning(
                "hot_reload_enabled",
                reason="development feature — do not use in production",
                watch_dirs=list(self._watch_dirs),
            )
            from swarmq.hot_reload import FileWatcher

            self._file_watcher = FileWatcher(
                watch_dirs=self._watch_dirs,
                trigger=lambda: os.kill(os.getpid(), signal.SIGHUP),
                is_busy=lambda: self._reload_requested or self.reloader.busy,
            )
            self._file_watcher.start()

        try:
            retcode = self._monitor_loop()
        finally:
            self.reloader.abort()  # KTD8: laufende Probe beenden, kein Reload
            self._log_shutdown_signal()
            if self._file_watcher is not None:
                try:
                    self._file_watcher.stop()
                except Exception as exc:
                    logger.warning("file_watcher_stop_failed", **_error_fields(exc), exc_info=True)
            self._shutdown_all_workers()
            self._collect_zombies()
            self._log_shutdown_signal()  # SIGTERM waehrend des Worker-Stopps

        if self._reload_requested:
            return reexec(self, retcode)  # kehrt nur ohne Reload zurueck
        return retcode

    # ------------------------------------------------------------------
    # Signal handling
    # ------------------------------------------------------------------

    def _log_shutdown_signal(self) -> None:
        """Das vom Handler hinterlegte Signal genau einmal loggen."""
        if self._shutdown_signal is not None and not self._shutdown_signal_logged:
            self._shutdown_signal_logged = True
            logger.info("process_manager_shutdown_signal", signal=self._shutdown_signal)

    def _setup_signals(self) -> None:
        # Deliberately NO SIGCHLD handler. A previous
        # `waitpid(-1, WNOHANG)` reaper raced multiprocessing's own
        # per-pid waitpid: whoever reaped first won, and when the
        # handler won, Popen.poll() got ECHILD and returned None →
        # is_alive() stayed True forever for dead workers (no restart,
        # shutdown always waited the full grace period). Reaping is
        # owned exclusively by the multiprocessing API — _check_dead_
        # workers/is_alive()/join() reap within one MONITOR_INTERVAL.
        signal.signal(signal.SIGINT, self._on_shutdown_signal)
        signal.signal(signal.SIGTERM, self._on_shutdown_signal)
        signal.signal(signal.SIGHUP, self._on_reload_signal)

    def _on_shutdown_signal(self, signum: int, _frame: object) -> None:
        # KTD8: kein Loggen im Handler (Mutex der Log-Queue); ``run`` schreibt es.
        self._shutdown_signal = signal.Signals(signum).name
        self._shutting_down = True
        # KTD8: SIGTERM gewinnt immer, auch gegen einen beschlossenen Reload.
        self._reload_requested = False
        self.reloader.cancel()
        self.shutdown_event.set()
        if self._reexec_holds_signals:
            # ``reload.reexec`` prueft womoeglich nicht mehr: das Signal an den
            # blockierten Hauptthread zurueck, anhaengig ueber ``execve``
            # hinweg. Kommt es ohne ``execve`` noch einmal, ist das harmlos.
            signal.raise_signal(signum)

    def _on_reload_signal(self, _signum: int, _frame: object) -> None:
        # KTD8: nur vormerken; Probe und Reload treibt der Monitor-Loop.
        if not self._shutting_down:  # Szenario E: SIGTERM gewinnt
            self.reloader.request()

    # ------------------------------------------------------------------
    # Worker lifecycle
    # ------------------------------------------------------------------

    @override
    def _start_worker(self, slot: int) -> WorkerInfo:
        """Spawn one worker subprocess for ``slot`` and register it."""
        if self._locked_slots_shared is None:  # BZ-27: ein Wert fuer alle Kanaele
            self._locked_slots_shared = self._ctx.Value("i", self._slots.locked())
        channel = SupervisorChannel.create(self._ctx, locked_slots=self._locked_slots_shared)
        p = self._ctx.Process(
            target=run_worker_entry,
            args=(self._worker_entry, slot, self.shutdown_event, *self._worker_args, channel),
            name=f"swarmq-worker-{slot}",
            daemon=False,
        )
        p.start()
        info = WorkerInfo(process=p, worker_id=slot, channel=channel, spawned_at=time.monotonic())
        # dict key is PID — unique across restarts within a session.
        self.workers[p.pid or slot] = info
        self._refresh_slot(slot)
        logger.info("worker_spawned", worker_id=slot, pid=p.pid, slot_state=self._slots.state(slot).value)
        return info

    def slot_state(self, slot: int) -> SlotState:
        """Zustand des Slots (frei, aktiv, Uebergang, gesperrt)."""
        return self._slots.state(slot)

    @property
    @override
    def _exhausted_slots(self) -> int:
        """Gesperrte Slots (Budget erschoepft); Begruendung bei ``SlotTable.lock``."""
        return self._slots.locked()

    @override
    def _refresh_slot(self, slot: int) -> None:
        """Slot-Zustand aus den Prozessen ableiten, die ihn noch halten."""
        holders = sum(1 for w in self.workers.values() if w.worker_id == slot)
        self._slots.refresh(slot, holders=holders)

    @property
    def effective_bounds(self) -> tuple[int, int]:
        """Wirksame Unter- und Obergrenze (KTD12): Sollwert oder Startargumente."""
        return self._setpoint.effective

    def _start_all_workers(self) -> None:
        for slot in range(setpoint.start_count(self)):
            self._start_worker(slot)

    @staticmethod
    def _wait_for_exit(candidates: list[WorkerInfo], until: float) -> None:
        """Warten, bis alle beendet sind oder die monotone Zeit ``until`` erreicht ist."""
        while (now := time.monotonic()) < until:
            if not any(w.process.is_alive() for w in candidates):
                break
            time.sleep(min(0.3, until - now))

    def _signal_survivors(self, event: str, action: str, level: int) -> list[WorkerInfo]:
        """SIGTERM/SIGKILL an noch lebende Worker; liefert die Adressaten.

        ``level`` ist die Stufe des Events (PF-8, E-018): Das SIGTERM am
        geplanten Drain-Ende ist ein regulärer Schritt des Budgets (INFO),
        erst das SIGKILL zeigt ein Hängen (ERROR).
        """
        alive = [w for w in self.workers.values() if w.process.is_alive()]
        if not alive:
            return []
        logger.log(level, event, count=len(alive))
        for info in alive:
            try:
                getattr(info.process, action)()
            except ProcessLookupError:
                continue
        return alive

    def _shutdown_all_workers(self) -> None:
        """Three-phase shutdown: event → SIGTERM → SIGKILL, im Gesamtbudget.

        BZ-18 (Plan 0.12.0, MR 17): ``shutdown_grace_s`` ist das Gesamtbudget
        ab SIGTERM (``SWARMQ_SHUTDOWN_TIMEOUT``). Die Eskalation endet bei
        ``total - 2 s``, das Einsammeln bei ``total`` -- vor dem SIGKILL der
        Orchestrierung, wenn deren Frist mindestens so lang ist.
        """
        if not self.workers:
            return
        self.shutdown_event.set()
        started = time.monotonic()
        budget = shutdown_budget(self.shutdown_grace_s, supervised=True)
        self._shutdown_deadline = started + budget.total

        # Phase 1: cooperative exit via shutdown_event.
        self._wait_for_exit(list(self.workers.values()), started + budget.sigterm_at)

        # Phase 2: SIGTERM.
        alive = self._signal_survivors("shutdown_sigterm_escalation", "terminate", logging.INFO)
        if alive:
            self._wait_for_exit(alive, started + budget.sigkill_at)

        # Phase 3: SIGKILL.
        self._signal_survivors("shutdown_sigkill_escalation", "kill", logging.ERROR)
        self._join_all(timeout_per_worker=2.0)

    def _join_all(self, timeout_per_worker: float) -> None:
        """Prozesse einsammeln, gemeinsam hoechstens bis ``_shutdown_deadline``."""
        deadline = self._shutdown_deadline
        for info in list(self.workers.values()):
            timeout = timeout_per_worker
            if deadline is not None:
                timeout = min(timeout, max(0.0, deadline - time.monotonic()))
            try:
                info.process.join(timeout=timeout)
            except Exception:  # noqa: BLE001, S110 - Join best effort beim Warten auf Worker
                pass

    def _collect_zombies(self) -> None:
        self._join_all(timeout_per_worker=1.0)

    # ------------------------------------------------------------------
    # Monitor loop + restart
    # ------------------------------------------------------------------

    def _monitor_loop(self) -> int:
        """Check worker health, backfill to target count, autoscale."""
        while not self._shutting_down:
            self._check_heartbeats()
            self._escalate_terminating()
            self._check_dead_workers()
            self._unlock_expired_slots()
            if self._config_error and not self._reload:
                # R20: derselbe Code wie das Kind. Mit 1 sah eine
                # Orchestrierung nur "irgendein Fehler" und startete neu.
                # Unter --reload wartet der Loop stattdessen auf die naechste
                # Dateiaenderung (E-057), siehe unten.
                self._shutdown_all_workers()
                return CONFIG_ERROR_EXIT_CODE

            # All restart budgets burned and no live
            # worker left → exit non-zero instead of spinning (or, pre-
            # fix, storming fresh spawns). systemd/k8s restart us with
            # their own backoff; staying alive here would just hide the
            # outage from the orchestrator.
            if (
                self._autoscale is None
                and not self.workers
                and self._exhausted_slots + self._clean_exits >= self.initial_workers
                and self._exhausted_slots == 0
            ):
                logger.info(
                    "all_workers_exited_clean",
                    initial_workers=self.initial_workers,
                    hint="every worker ended with exit code 0 (burst drain); exiting 0",
                )
                return 0
            if (
                self._autoscale is None
                and self._exhausted_slots + self._clean_exits >= self.initial_workers
                and not self.workers
            ):
                logger.error(
                    "all_worker_slots_exhausted",
                    initial_workers=self.initial_workers,
                    limit=self.MAX_RESTARTS,
                    window_s=self.RESTART_WINDOW,
                    hint=(
                        "every worker slot hit the crash-restart limit; "
                        "exiting so the service manager applies its own "
                        "backoff/restart policy"
                    ),
                )
                return 1

            if self.reloader.step():  # KTD8: Probe starten bzw. pollen, nie blockierend
                logger.info("process_manager_reload_requested")
                self._reload_requested = True
                self._shutting_down = True
                self.shutdown_event.set()
                break
            if self._config_error:
                # E-057 (--reload): kein Nachfuellen und kein Skalieren, bis
                # die naechste Dateiaenderung den Reload ausloest; der
                # startet alle Worker neu.
                time.sleep(self.MONITOR_INTERVAL)
                continue
            setpoint.tick(self)  # Kontrollkanal: vor Nachfuellen und Autoskalierer (KTD3)
            # Backfill to the current target (initial count without
            # autoscale; between min..max with autoscale).
            target = self._current_target()
            while self._slots.occupied() < target and not self._shutting_down:
                slot = self._slots.smallest_free()
                if slot is None:  # nur gesperrte Slots uebrig (R4)
                    break
                self._start_worker(slot)

            if self._autoscale is not None:
                self._autoscale_tick()

            time.sleep(self.MONITOR_INTERVAL)
        return 0

    def _current_target(self) -> int:
        """Target worker count at this instant.

        Without autoscale: ``initial_workers`` minus permanently
        exhausted slots (backfilling an exhausted
        slot would reset its restart history and bypass the rate
        limit).
        With autoscale: the currently occupied slots (a slot in
        transition counts once, R2) since ``_autoscale_tick`` adjusts
        independently. The backfill loop therefore starts nothing under
        ``--autoscale``: a crashed worker is replaced by ``_maybe_restart``
        alone, and a slot whose worker drains stays occupied until it
        exits (E-084).
        """
        if self._autoscale is None:
            return max(0, self.effective_bounds[0] - self._exhausted_slots - self._clean_exits)
        return self._slots.occupied()

    def _check_dead_workers(self) -> None:
        """Detect exited workers and decide whether to restart."""
        dead: list[tuple[int, WorkerInfo]] = []
        for pid, info in list(self.workers.items()):
            if not info.process.is_alive():
                dead.append((pid, info))

        for pid, info in dead:
            exitcode = info.process.exitcode
            try:
                info.process.join(timeout=1)
            except Exception:  # noqa: BLE001, S110 - Join best effort, Prozess ist bereits beendet
                pass
            del self.workers[pid]
            self._refresh_slot(info.worker_id)  # KTD1: Uebergang → aktiv / frei; Sperre bleibt

            log = logger.info if exitcode == 0 or info.expected_shutdown else logger.warning  # KTD11
            log(
                "worker_died",
                worker_id=info.worker_id,
                pid=pid,
                exitcode=exitcode,
                expected=info.expected_shutdown,
                slot_state=self._slots.state(info.worker_id).value,
            )

            if self._shutting_down:
                continue
            if info.expected_shutdown:
                # Autoscaler scaled this worker down — do not restart.
                continue
            if exitcode == 0:
                # Clean exit (burst-drain, etc.): also no restart, and the
                # slot is retired so the backfill does not respawn it (KTD9).
                self._clean_exits += 1
                continue
            if self._handle_exit_code(info, pid, exitcode):
                continue
            self._maybe_restart(info)

    def _handle_exit_code(self, info: WorkerInfo, pid: int, exitcode: int | None) -> bool:
        """Logsaetze zu den benannten Exit-Codes; ``True`` = kein Neustart
        (R20).

        78: Ein Neustart wuerde denselben Konfigurationsfehler reproduzieren,
        also genau ein Startversuch. 75: Das Kind bekam seine
        Recycling-Bestaetigung nicht — Neustart wie bei jedem Absturz, aber
        mit eigenem Logsatz, sonst ist der Fall im Betrieb nicht von einem
        Crash zu unterscheiden. 71: Der CPU-Pool hat seine Kindquelle
        verworfen -- ebenso, der Neustart bringt einen frischen Forkserver.
        """
        if exitcode == CONFIG_ERROR_EXIT_CODE:
            logger.error(
                "worker_config_error",
                worker_id=info.worker_id,
                pid=pid,
                exitcode=exitcode,
                hint=(
                    "fix the code or configuration; workers restart after the next file change or SIGHUP (--reload)"
                    if self._reload
                    else "fix the configuration and start again; no restart attempted"
                ),
            )
            self._config_error = True
            return True
        if exitcode == RECYCLE_NOACK_EXIT_CODE:
            logger.warning(
                "worker_recycle_noack",
                worker_id=info.worker_id,
                pid=pid,
                exitcode=exitcode,
            )
        if exitcode == FORK_SOURCE_REJECTED_EXIT_CODE:  # Schluss-Review: frischer Forkserver
            logger.warning("worker_fork_source_rejected", worker_id=info.worker_id, pid=pid, exitcode=exitcode)
        return False

    # ---- Uhr und Logger fuer ``restart_budget.py`` (Plan 0.12.0, MR 0, Q17)

    @override
    def _monotonic(self) -> float:
        """Liest ``time`` dieses Moduls, damit ``patch("…supervisor.time")`` greift."""
        return time.monotonic()

    @property
    @override
    def _log(self) -> Any:
        """Logger ``swarmq.process_manager.supervisor`` (Name und Patch-Pfad bleiben)."""
        return logger

    # ------------------------------------------------------------------
    # R4.7 Autoscaling — Entscheidungen in ``process_manager/autoscale.py``
    # ------------------------------------------------------------------

    def _autoscale_tick(self) -> None:
        autoscale.tick(self)

    def _scale_down_one(self) -> None:
        autoscale.scale_down_one(self)
