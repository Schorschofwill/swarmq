"""Multi-process worker supervisor (R4.6 + R4.7).

Spawns N worker subprocesses via ``multiprocessing.get_context("spawn")``,
monitors them, restarts on crash with a rolling-window rate limit
(5 restarts per hour per worker), supports SIGHUP reload via
``os.execvp``, and — when autoscaling is enabled — adjusts the worker
count based on queue depth (R4.7).

Design
------

The parent process runs no asyncio event loop. It uses plain POSIX
signal handlers and ``time.sleep``-based polling. This keeps the
supervisor robust against event-loop hangs inside any single worker.

Workers use ``spawn`` (not ``fork``) so that aio_pika's robust
connection, the Valkey connection pool, and any module-level resources
are freshly constructed in the child — fork would duplicate file
descriptors and leak half-closed sockets.

The parent ``shutdown_event`` is a ``multiprocessing.Event``. Each
worker polls it in a background coroutine and triggers the existing
Worker graceful-shutdown flow when set. Scale-down sends ``SIGTERM``
to one specific process (not the shared event) so the rest keep
running.

Reference: ``prototypes/worker_prototype.py`` (design blueprint).
"""

from __future__ import annotations

import multiprocessing
import os
import signal
import sys
import time
from dataclasses import dataclass, field
from typing import Callable

import structlog

logger = structlog.get_logger()

# Signals the parent captures. Blocked before spawning children so
# child processes inherit the blocked state, then unblocked on the
# parent after children are up (Dramatiq pattern).
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


@dataclass
class WorkerInfo:
    """Per-worker bookkeeping: process handle, logical id, restart state."""

    process: multiprocessing.Process
    worker_id: int
    restart_count: int = 0
    last_restart: float = field(default_factory=time.monotonic)
    expected_shutdown: bool = False  # True when Autoscaler scaled down


# Queue-depth sampling signature: returns the total pending count
# across all configured queues. May raise — the supervisor swallows
# and logs.
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
        ``(worker_id: int, shutdown_event, *worker_args)``.
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

    # Graceful-shutdown phases.
    SHUTDOWN_GRACE_S = 10.0
    SHUTDOWN_TERM_S = 5.0

    # Autoscaling knobs.
    SCALE_UP_THRESHOLD = 50  # pending tasks per active worker
    SCALE_DOWN_IDLE_S = 60.0  # sustained idle window before scale-down
    SCALE_COOLDOWN_S = 30.0  # minimum gap between scale actions
    AUTOSCALE_SAMPLE_INTERVAL = 10.0

    def __init__(
        self,
        num_workers: int,
        worker_entry: Callable[..., None],
        worker_args: tuple = (),
        *,
        autoscale: tuple[int, int] | None = None,
        queue_depth_fn: QueueDepthFn | None = None,
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
        self._worker_entry = worker_entry
        self._worker_args = worker_args
        self._autoscale = autoscale
        self._queue_depth_fn = queue_depth_fn

        self._ctx = multiprocessing.get_context("spawn")
        self.shutdown_event = self._ctx.Event()
        self.workers: dict[int, WorkerInfo] = {}
        self._shutting_down = False
        self._reload_requested = False
        self._next_worker_id = 0

        # Autoscale state.
        self._last_scale_decision = 0.0
        self._last_autoscale_sample = 0.0
        self._idle_since: float | None = None

    # ------------------------------------------------------------------
    # Public entry-point
    # ------------------------------------------------------------------

    def run(self) -> int:
        """Blocking supervisor entry-point. Returns a POSIX exit code.

        On ``SIGHUP``, returns after graceful-shutdown and re-execs
        via ``os.execvp(sys.executable, [sys.executable] + sys.argv)``
        — the call does not return.
        """
        logger.info(
            "process_manager_start",
            pid=os.getpid(),
            workers=self.initial_workers,
            autoscale=self._autoscale,
        )
        _block_signals()
        self._start_all_workers()
        self._setup_signals()
        _unblock_signals()

        try:
            retcode = self._monitor_loop()
        finally:
            self._shutdown_all_workers()
            self._collect_zombies()

        if self._reload_requested:
            logger.info("process_manager_reload_execvp")
            os.execvp(sys.executable, [sys.executable] + sys.argv)  # noqa: S606

        return retcode

    # ------------------------------------------------------------------
    # Signal handling
    # ------------------------------------------------------------------

    def _setup_signals(self) -> None:
        signal.signal(signal.SIGINT, self._on_shutdown_signal)
        signal.signal(signal.SIGTERM, self._on_shutdown_signal)
        signal.signal(signal.SIGHUP, self._on_reload_signal)
        # SIGCHLD reaps zombies between monitor ticks.
        signal.signal(signal.SIGCHLD, self._on_sigchld)

    def _on_shutdown_signal(self, signum: int, _frame) -> None:
        sig_name = signal.Signals(signum).name
        logger.info("process_manager_shutdown_signal", signal=sig_name)
        self._shutting_down = True
        self.shutdown_event.set()

    def _on_reload_signal(self, _signum: int, _frame) -> None:
        logger.info("process_manager_reload_requested")
        self._reload_requested = True
        self._shutting_down = True
        self.shutdown_event.set()

    def _on_sigchld(self, _signum: int, _frame) -> None:
        # Non-blocking reap; monitor_loop handles bookkeeping.
        while True:
            try:
                pid, _ = os.waitpid(-1, os.WNOHANG)
                if pid == 0:
                    break
            except ChildProcessError:
                break

    # ------------------------------------------------------------------
    # Worker lifecycle
    # ------------------------------------------------------------------

    def _start_worker(self) -> WorkerInfo:
        """Spawn one worker subprocess and register it."""
        wid = self._next_worker_id
        self._next_worker_id += 1

        p = self._ctx.Process(
            target=self._worker_entry,
            args=(wid, self.shutdown_event, *self._worker_args),
            name=f"swarmq-worker-{wid}",
            daemon=False,
        )
        p.start()
        info = WorkerInfo(process=p, worker_id=wid)
        # dict key is PID — unique across restarts within a session.
        self.workers[p.pid or wid] = info
        logger.info("worker_spawned", worker_id=wid, pid=p.pid)
        return info

    def _start_all_workers(self) -> None:
        for _ in range(self.initial_workers):
            self._start_worker()

    def _shutdown_all_workers(self) -> None:
        """Three-phase shutdown: event → SIGTERM → SIGKILL."""
        if not self.workers:
            return
        self.shutdown_event.set()

        # Phase 1: cooperative exit via shutdown_event.
        deadline = time.monotonic() + self.SHUTDOWN_GRACE_S
        while time.monotonic() < deadline:
            if not any(w.process.is_alive() for w in self.workers.values()):
                break
            time.sleep(0.3)

        # Phase 2: SIGTERM.
        alive = [w for w in self.workers.values() if w.process.is_alive()]
        if alive:
            logger.warning("shutdown_sigterm_escalation", count=len(alive))
            for info in alive:
                try:
                    info.process.terminate()
                except ProcessLookupError:
                    continue
            deadline = time.monotonic() + self.SHUTDOWN_TERM_S
            while time.monotonic() < deadline:
                if not any(w.process.is_alive() for w in alive):
                    break
                time.sleep(0.3)

        # Phase 3: SIGKILL.
        alive = [w for w in self.workers.values() if w.process.is_alive()]
        if alive:
            logger.error("shutdown_sigkill_escalation", count=len(alive))
            for info in alive:
                try:
                    info.process.kill()
                except ProcessLookupError:
                    continue

        for info in self.workers.values():
            try:
                info.process.join(timeout=2)
            except Exception:  # noqa: BLE001
                pass

    def _collect_zombies(self) -> None:
        for info in list(self.workers.values()):
            try:
                info.process.join(timeout=1)
            except Exception:  # noqa: BLE001
                pass

    # ------------------------------------------------------------------
    # Monitor loop + restart
    # ------------------------------------------------------------------

    def _monitor_loop(self) -> int:
        """Check worker health, backfill to target count, autoscale."""
        while not self._shutting_down:
            self._check_dead_workers()

            # Backfill to the current target (initial count without
            # autoscale; between min..max with autoscale).
            target = self._current_target()
            while (
                len(self.workers) < target
                and not self._shutting_down
            ):
                self._start_worker()

            if self._autoscale is not None:
                self._autoscale_tick()

            time.sleep(self.MONITOR_INTERVAL)
        return 0

    def _current_target(self) -> int:
        """Target worker count at this instant.

        Without autoscale: ``initial_workers`` (static).
        With autoscale: current ``len(self.workers)`` since
        ``_autoscale_tick`` adjusts independently; backfill only
        replaces crashed workers (not those we scaled down
        deliberately).
        """
        if self._autoscale is None:
            return self.initial_workers
        return len(self.workers)

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
            except Exception:  # noqa: BLE001
                pass
            del self.workers[pid]

            logger.warning(
                "worker_died",
                worker_id=info.worker_id,
                pid=pid,
                exitcode=exitcode,
                expected=info.expected_shutdown,
            )

            if self._shutting_down:
                continue
            if info.expected_shutdown:
                # Autoscaler scaled this worker down — do not restart.
                continue
            if exitcode == 0:
                # Clean exit (burst-drain, etc.): also no restart.
                continue
            self._maybe_restart(info)

    def _maybe_restart(self, old: WorkerInfo) -> None:
        """Restart ``old`` respecting the rolling-window rate limit."""
        now = time.monotonic()
        if now - old.last_restart > self.RESTART_WINDOW:
            restart_count = 0
        else:
            restart_count = old.restart_count + 1

        if restart_count >= self.MAX_RESTARTS:
            logger.error(
                "worker_restart_limit_exceeded",
                worker_id=old.worker_id,
                limit=self.MAX_RESTARTS,
                window_s=self.RESTART_WINDOW,
            )
            return

        logger.info(
            "worker_restarting",
            worker_id=old.worker_id,
            attempt=restart_count + 1,
            limit=self.MAX_RESTARTS,
        )
        new = self._start_worker()
        new.restart_count = restart_count
        new.last_restart = now

    # ------------------------------------------------------------------
    # R4.7 Autoscaling
    # ------------------------------------------------------------------

    def _autoscale_tick(self) -> None:
        """One autoscale decision (no-op outside the sample interval).

        Sampling uses ``_queue_depth_fn`` which is synchronous — the
        caller adapts async broker APIs into a sync wrapper (see
        :func:`make_queue_depth_sampler`).
        """
        assert self._autoscale is not None
        assert self._queue_depth_fn is not None

        now = time.monotonic()
        if now - self._last_autoscale_sample < self.AUTOSCALE_SAMPLE_INTERVAL:
            return
        self._last_autoscale_sample = now

        if now - self._last_scale_decision < self.SCALE_COOLDOWN_S:
            return

        try:
            depth = self._queue_depth_fn()
        except Exception as exc:  # noqa: BLE001
            logger.warning("autoscale_sample_failed", error=str(exc))
            return

        min_w, max_w = self._autoscale
        active = len(self.workers)

        # Scale up: enough backlog to justify another worker.
        if active < max_w and depth > active * self.SCALE_UP_THRESHOLD:
            logger.info(
                "autoscale_up",
                depth=depth, active=active, max_workers=max_w,
            )
            self._start_worker()
            self._last_scale_decision = now
            self._idle_since = None
            return

        # Scale down: sustained idle.
        if active > min_w and depth == 0:
            if self._idle_since is None:
                self._idle_since = now
                return
            if now - self._idle_since >= self.SCALE_DOWN_IDLE_S:
                self._scale_down_one()
                self._last_scale_decision = now
                self._idle_since = None
        else:
            self._idle_since = None

    def _scale_down_one(self) -> None:
        """Tell the youngest worker to drain + exit (SIGTERM)."""
        if not self.workers:
            return
        # Youngest first — oldest workers have accumulated warm caches.
        target_pid, info = max(
            self.workers.items(), key=lambda kv: kv[1].worker_id,
        )
        logger.info(
            "autoscale_down",
            worker_id=info.worker_id, pid=target_pid,
        )
        info.expected_shutdown = True
        try:
            info.process.terminate()
        except ProcessLookupError:
            # Already gone — bookkeeping will clean up in _check_dead.
            pass
