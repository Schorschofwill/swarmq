"""Supervisor (Plan U21): ``ProcessManager`` (Restart-Budget, Autoscale,
SIGHUP-Reload mit eingefrorenem argv/env), Signal-Blöcke und der injizierte
``QueueDepthFn``-Seam. Slot-Tabelle und ``WorkerInfo`` liegen in ``slots``
(Plan 2026-09-08-001, KTD1)."""

from __future__ import annotations

import multiprocessing
import os
import signal
import socket
import sys
import time
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover — import-time only
    from swarmq.hot_reload import FileWatcher

import structlog

from swarmq.process_manager import autoscale, setpoint
from swarmq.process_manager.entry import CONFIG_ERROR_EXIT_CODE
from swarmq.process_manager.setpoint_store import DEFAULT_SAMPLE_INTERVAL_S, SetpointStoreFactory
from swarmq.process_manager.slots import SlotState, SlotTable, WorkerInfo
from swarmq.supervision import (
    INTENT_DRAINING,
    INTENT_RECYCLE,
    RECYCLE_NOACK_EXIT_CODE,
    SupervisorChannel,
)
from swarmq.supervision import (
    SHUTDOWN_TERM_S as _SHUTDOWN_TERM_S,
)

logger = structlog.get_logger()


_HANDLED_SIGNALS = (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)


def _block_signals() -> None:
    """Block ``_HANDLED_SIGNALS`` on the current thread."""
    try:
        signal.pthread_sigmask(signal.SIG_BLOCK, _HANDLED_SIGNALS)
    except (AttributeError, OSError):
        # Non-POSIX platforms: best effort; supervisor still works.
        pass


def _unblock_signals() -> None:
    """Unblock ``_HANDLED_SIGNALS`` on the current thread."""
    try:
        signal.pthread_sigmask(signal.SIG_UNBLOCK, _HANDLED_SIGNALS)
    except (AttributeError, OSError):
        pass


#: ``prctl``-Option, mit der ein Kind sich beim Tod seines Elternprozesses ein
#: Signal bestellt (Linux). Dieselbe Mechanik nutzt das Rechenkind (KTD3 des
#: CPU-Plans), dort mit SIGKILL; ein Worker bekommt SIGTERM und drainiert.
_PR_SET_PDEATHSIG = 1


def _bind_to_supervisor() -> bool:
    """Das Kind stirbt mit dem Supervisor -- auch wenn dieser per SIGKILL endet.

    Ohne dieses Band lebte ein Worker weiter, dessen Supervisor hart beendet
    wurde (Test-Teardown, OOM-Killer, ``docker kill``): reparented, unbeachtet,
    und mit einem Scheduler-Heartbeat, der den Leader-Lock am Leben hielt und
    jeden spaeteren Leader in derselben Datenbank blockierte (Notiz
    e2e-scheduler-leader-flake). Liefert ``True``, wenn ``prctl`` das Signal
    gesetzt hat; ohne ``prctl`` (Nicht-Linux) bleibt es beim bisherigen
    Verhalten.

    Stirbt der Supervisor zwischen ``fork`` und ``prctl``, kommt kein Signal
    mehr: dann ist der Elternprozess des Kindes nicht mehr der, der es
    gestartet hat, und das Kind beendet sich, statt als Waise anzulaufen.
    """
    try:
        import ctypes

        libc = ctypes.CDLL(None, use_errno=True)
        armed = int(libc.prctl(_PR_SET_PDEATHSIG, int(signal.SIGTERM), 0, 0, 0)) == 0
    except (AttributeError, OSError):
        return False
    parent = multiprocessing.parent_process()
    if parent is not None and os.getppid() != parent.pid:
        raise SystemExit(0)
    return armed


def _run_worker_entry(entry: Callable[..., Any], *args: Any) -> Any:
    """Ziel des Kindprozesses: geerbte Signalblockade aufheben, dann den Entry rufen.

    Der Supervisor blockiert ``_HANDLED_SIGNALS``, waehrend er Kinder startet;
    ``spawn`` vererbt die Maske. swarmqs eigener Entry hob sie selbst auf, ein
    fremder Entry (der Betreiber gibt dem ``ProcessManager`` seinen eigenen)
    erbte sie -- das SIGTERM, das sein Shutdown-Watcher auf die eigene PID
    schickt, blieb anhaengig, und der Container endete erst per SIGKILL nach
    dem Stop-Timeout (B15 des Loadsuite-Backlogs). Danach bindet sich das Kind
    per Parent-Death-Signal an den Supervisor (:func:`_bind_to_supervisor`).
    Modulfunktion, damit ``spawn`` sie picklen kann.
    """
    _unblock_signals()
    _bind_to_supervisor()
    return entry(*args)


QueueDepthFn = Callable[[], int]


class ProcessManager:
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

    # Parent-loop tick.
    MONITOR_INTERVAL = 0.5

    # Graceful-shutdown phases (defaults — overridable per-instance via
    # ``shutdown_grace_s`` to align with ``SwarmQConfig.shutdown_timeout``
    # so a subprocess actually gets its configured drain budget).
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
                    f"invalid autoscale bounds (min={min_w}, max={max_w}); "
                    "require min >= 1 and max >= min",
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
        # Per-instance override of the phase-1 shutdown budget.
        # Without this, any task running longer
        # than the class default was killed mid-execution regardless
        # of the configured SwarmQConfig.shutdown_timeout.
        if shutdown_grace_s is not None:
            if shutdown_grace_s <= 0:
                raise ValueError(
                    f"shutdown_grace_s must be > 0 (got {shutdown_grace_s})",
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
        self._shutting_down = False
        self._reload_requested = False
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
        self._startup_argv: list[str] = list(sys.argv)
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
        _block_signals()
        self._start_all_workers()
        self._setup_signals()
        _unblock_signals()

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
                is_busy=lambda: self._reload_requested,
            )
            self._file_watcher.start()

        try:
            retcode = self._monitor_loop()
        finally:
            if self._file_watcher is not None:
                try:
                    self._file_watcher.stop()
                except Exception:
                    logger.warning("file_watcher_stop_failed", exc_info=True)
            self._shutdown_all_workers()
            self._collect_zombies()

        if self._reload_requested:
            # Orphan-guard: if any child
            # survived Phase-3 SIGKILL (typically D-state: NFS stall,
            # dying disk), os.execvp would replace the parent image
            # and orphan the child to init/systemd. Prefer clean
            # no-reload exit over orphan.
            alive = [
                w for w in self.workers.values() if w.process.is_alive()
            ]
            if alive:
                logger.warning(
                    "reload_aborted_alive_children",
                    alive_count=len(alive),
                    alive_pids=[w.process.pid for w in alive],
                    hint=(
                        "re-send SIGHUP once all workers have exited; "
                        "a D-state process (uninterruptible kernel wait) "
                        "may require host-level intervention"
                    ),
                )
                # Return 0 (clean exit): systemd / k8s must NOT
                # restart us — operator needs to act.
                return 0
            try:
                logger.info("process_manager_reload_execvp")
                # Use the argv AND env
                # snapshots captured in __init__. execvpe closes the
                # env side of the trust boundary so in-process
                # os.environ mutation (e.g., a library flipping
                # PYTHONPATH) cannot influence the re-exec'd
                # interpreter's import resolution.
                os.execvpe(
                    sys.executable,
                    [sys.executable, *self._startup_argv],
                    self._startup_env,
                )
            except OSError as exc:
                logger.error("reload_execvp_failed", error=str(exc))
                return 1

        return retcode

    # ------------------------------------------------------------------
    # Signal handling
    # ------------------------------------------------------------------

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
        sig_name = signal.Signals(signum).name
        logger.info("process_manager_shutdown_signal", signal=sig_name)
        self._shutting_down = True
        self.shutdown_event.set()

    def _on_reload_signal(self, _signum: int, _frame: object) -> None:
        logger.info("process_manager_reload_requested")
        self._reload_requested = True
        self._shutting_down = True
        self.shutdown_event.set()

    # ------------------------------------------------------------------
    # Worker lifecycle
    # ------------------------------------------------------------------

    def _start_worker(self, slot: int) -> WorkerInfo:
        """Spawn one worker subprocess for ``slot`` and register it."""
        channel = SupervisorChannel.create(self._ctx)
        p = self._ctx.Process(
            target=_run_worker_entry,
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
    def _exhausted_slots(self) -> int:
        """Gesperrte Slots (Budget erschoepft); Begruendung bei ``SlotTable.lock``."""
        return self._slots.locked()

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
    def _wait_for_exit(candidates: list[WorkerInfo], grace_s: float) -> None:
        deadline = time.monotonic() + grace_s
        while time.monotonic() < deadline:
            if not any(w.process.is_alive() for w in candidates):
                break
            time.sleep(0.3)

    def _signal_survivors(self, event: str, action: str) -> list[WorkerInfo]:
        """SIGTERM/SIGKILL an noch lebende Worker; liefert die Adressaten."""
        alive = [w for w in self.workers.values() if w.process.is_alive()]
        if not alive:
            return []
        log = logger.warning if action == "terminate" else logger.error
        log(event, count=len(alive))
        for info in alive:
            try:
                getattr(info.process, action)()
            except ProcessLookupError:
                continue
        return alive

    def _shutdown_all_workers(self) -> None:
        """Three-phase shutdown: event → SIGTERM → SIGKILL."""
        if not self.workers:
            return
        self.shutdown_event.set()

        # Phase 1: cooperative exit via shutdown_event.
        self._wait_for_exit(list(self.workers.values()), self.shutdown_grace_s)

        # Phase 2: SIGTERM.
        alive = self._signal_survivors("shutdown_sigterm_escalation", "terminate")
        if alive:
            self._wait_for_exit(alive, self.SHUTDOWN_TERM_S)

        # Phase 3: SIGKILL.
        self._signal_survivors("shutdown_sigkill_escalation", "kill")

        for info in self.workers.values():
            try:
                info.process.join(timeout=2)
            except Exception:
                pass

    def _collect_zombies(self) -> None:
        for info in list(self.workers.values()):
            try:
                info.process.join(timeout=1)
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Monitor loop + restart
    # ------------------------------------------------------------------

    def _monitor_loop(self) -> int:
        """Check worker health, backfill to target count, autoscale."""
        while not self._shutting_down:
            self._check_heartbeats()
            self._escalate_terminating()
            self._check_dead_workers()
            if self._config_error:
                # R20: derselbe Code wie das Kind. Mit 1 sah eine
                # Orchestrierung nur "irgendein Fehler" und startete neu.
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
        independently; backfill only replaces crashed workers (not
        those we scaled down deliberately).
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
            except Exception:
                pass
            del self.workers[pid]
            self._refresh_slot(info.worker_id)  # KTD1: Uebergang → aktiv / frei; Sperre bleibt

            logger.warning(
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
        """Logsaetze zu den beiden benannten Exit-Codes; ``True`` = kein
        Neustart (R20).

        78: Ein Neustart wuerde denselben Konfigurationsfehler reproduzieren,
        also genau ein Startversuch. 75: Das Kind bekam seine
        Recycling-Bestaetigung nicht — Neustart wie bei jedem Absturz, aber
        mit eigenem Logsatz, sonst ist der Fall im Betrieb nicht von einem
        Crash zu unterscheiden.
        """
        if exitcode == CONFIG_ERROR_EXIT_CODE:
            logger.error(
                "worker_config_error", worker_id=info.worker_id, pid=pid, exitcode=exitcode,
                hint="fix the configuration and start again; no restart attempted",
            )
            self._config_error = True
            return True
        if exitcode == RECYCLE_NOACK_EXIT_CODE:
            logger.warning(
                "worker_recycle_noack", worker_id=info.worker_id, pid=pid, exitcode=exitcode,
            )
        return False

    # ---- Lebenszeichen (Plan 2026-09-03-001, R21/R30, KTD10)

    def _heartbeat_deadline(self, info: WorkerInfo) -> float:
        """Frist: normal ``heartbeat_timeout_s``; im Drain die Shutdown-Gnade plus SIGTERM-Frist."""
        if info.channel is not None and info.channel.intent_value() in (INTENT_DRAINING, INTENT_RECYCLE):
            return self.shutdown_grace_s + self.SHUTDOWN_TERM_S
        return self.heartbeat_timeout_s

    def _check_heartbeats(self) -> None:
        """Worker ohne frisches Lebenszeichen ersetzen (SIGTERM, spaeter SIGKILL)."""
        if self._shutting_down:
            return
        now = time.monotonic()
        terminating = {id(info) for info, _ in self._terminating}
        for info in list(self.workers.values()):
            channel = info.channel
            if channel is None or not info.process.is_alive() or id(info) in terminating:
                continue
            if not info.expected_shutdown and channel.intent_value() == INTENT_RECYCLE:
                # KTD11: Absicht → expected_shutdown zuerst, dann Ersatz, dann ack.
                # Zaehlt weder gegen MAX_RESTARTS noch als sauberes Ende.
                info.expected_shutdown = True
                replacement = self._start_worker(info.worker_id)
                channel.acknowledge_replacement()
                logger.info(
                    "worker_recycling_replacement",
                    worker_id=info.worker_id, pid=info.process.pid, replacement_pid=replacement.process.pid,
                )
                continue
            last = channel.last_beat() or info.spawned_at
            stale = now - last
            if stale <= self._heartbeat_deadline(info):
                continue
            if info.expected_shutdown:  # Recycler/Scale-down im Drain eingefroren: toeten, kein Ersatz
                logger.warning(
                    "worker_hung_during_drain", worker_id=info.worker_id, pid=info.process.pid,
                    stale_s=round(stale, 1), deadline_s=self._heartbeat_deadline(info),
                )
                self._terminate_worker(info)
            else:
                self._replace_hung_worker(info, stale)

    def _terminate_worker(self, info: WorkerInfo) -> None:
        """SIGTERM jetzt, SIGKILL nach ``SHUTDOWN_TERM_S`` (``_escalate_terminating``)."""
        try:
            info.process.terminate()
        except ProcessLookupError:
            pass
        self._terminating.append((info, time.monotonic() + self.SHUTDOWN_TERM_S))

    def _replace_hung_worker(self, info: WorkerInfo, stale_s: float) -> None:
        now = time.monotonic()
        info.expected_shutdown = True  # der SIGKILL-Exit darf keinen zweiten Ersatz ausloesen
        self._hang_timestamps = [t for t in self._hang_timestamps if now - t <= self.RESTART_WINDOW]
        logger.warning(
            "worker_hung",
            worker_id=info.worker_id, pid=info.process.pid, stale_s=round(stale_s, 1),
            deadline_s=self._heartbeat_deadline(info),
        )
        if len(self._hang_timestamps) >= self.hang_replacement_limit:
            self._slots.lock(info.worker_id)  # auch mit lebendem Vorgaenger (R4)
            logger.error(
                "hang_replacement_budget_exhausted",
                limit=self.hang_replacement_limit, window_s=self.RESTART_WINDOW,
                hint="a poison message keeps hanging workers; check the DLQ and the delivery limit",
            )
        else:
            self._hang_timestamps.append(now)
            self._start_worker(info.worker_id)
        self._terminate_worker(info)

    def _escalate_terminating(self) -> None:
        """SIGKILL fuer ersetzte Worker, die SIGTERM ueberlebt haben."""
        now = time.monotonic()
        remaining: list[tuple[WorkerInfo, float]] = []
        for info, deadline in self._terminating:
            if not info.process.is_alive():
                continue
            if now >= deadline:
                logger.error("worker_hung_killed", worker_id=info.worker_id, pid=info.process.pid)
                try:
                    info.process.kill()
                except ProcessLookupError:
                    pass
                continue
            remaining.append((info, deadline))
        self._terminating = remaining

    def _maybe_restart(self, old: WorkerInfo) -> None:
        """Restart ``old`` respecting the rolling-window rate limit.

        True sliding window: prunes
        ``restart_timestamps`` to entries inside
        ``[now - RESTART_WINDOW, now]`` and blocks when the count
        hits ``MAX_RESTARTS`` — regardless of how the events are
        distributed within the window. The previous single-gap
        check allowed a full new series of 5 restarts after any
        quiet period of ``RESTART_WINDOW`` seconds.
        """
        now = time.monotonic()
        cutoff = now - self.RESTART_WINDOW
        timestamps = [t for t in old.restart_timestamps if t >= cutoff]

        if len(timestamps) >= self.MAX_RESTARTS:
            # Lock the slot permanently so _current_target shrinks and the
            # backfill loop cannot respawn it with a fresh (empty) restart
            # history.
            self._slots.lock(old.worker_id)
            logger.error(
                "worker_restart_limit_exceeded",
                worker_id=old.worker_id,
                limit=self.MAX_RESTARTS,
                window_s=self.RESTART_WINDOW,
                recent_restarts=len(timestamps),
                exhausted_slots=self._exhausted_slots,
                initial_workers=self.initial_workers,
            )
            return

        timestamps.append(now)
        logger.info(
            "worker_restarting",
            worker_id=old.worker_id,
            attempt=len(timestamps),
            limit=self.MAX_RESTARTS,
        )
        new = self._start_worker(old.worker_id)
        new.restart_timestamps = timestamps

    # ------------------------------------------------------------------
    # R4.7 Autoscaling — Entscheidungen in ``process_manager/autoscale.py``
    # ------------------------------------------------------------------

    def _autoscale_tick(self) -> None:
        autoscale.tick(self)

    def _scale_down_one(self) -> None:
        autoscale.scale_down_one(self)
