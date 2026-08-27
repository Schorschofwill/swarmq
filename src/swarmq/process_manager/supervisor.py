"""Supervisor (Plan U21) — verbatim: ``ProcessManager`` (Restart-Budget,
Autoscale, SIGHUP-Reload mit eingefrorenem argv/env), Signal-Blöcke,
``WorkerInfo`` und der injizierte ``QueueDepthFn``-Seam."""

from __future__ import annotations

import multiprocessing
import os
import signal
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover — import-time only
    from swarmq.hot_reload import FileWatcher

import structlog

from swarmq.logging import sanitize_url

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


@dataclass
class WorkerInfo:
    """Per-worker bookkeeping: process handle, logical id, restart state.

    ``restart_timestamps`` carries the rolling-window history across
    successive generations of a crash-then-restart slot. Each entry
    is ``time.monotonic()`` at the moment a replacement was spawned.
    Entries older than ``ProcessManager.RESTART_WINDOW`` are pruned
    in :meth:`ProcessManager._maybe_restart` so the count reflects a
    true sliding window, not a since-last-event gap.

    ``spawned_at`` is set to ``time.monotonic()`` at construction time
    and consumed by :meth:`ProcessManager._scale_down_one` to protect
    freshly spawned replacements from immediate scale-down (ce-review
    C-03 / Unit 5).
    """

    process: multiprocessing.Process
    worker_id: int
    expected_shutdown: bool = False  # True when Autoscaler scaled down
    restart_timestamps: list[float] = field(default_factory=list)
    spawned_at: float = field(default_factory=time.monotonic)


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

    # Graceful-shutdown phases (defaults — overridable per-instance via
    # ``shutdown_grace_s`` to align with ``SwarmQConfig.shutdown_timeout``
    # so a subprocess actually gets its configured drain budget.
    # ce-review REL-01).
    SHUTDOWN_GRACE_S = 30.0
    SHUTDOWN_TERM_S = 5.0

    # Autoscaling knobs.
    SCALE_UP_THRESHOLD = 50  # pending tasks per active worker
    SCALE_DOWN_IDLE_S = 60.0  # sustained idle window before scale-down
    SCALE_COOLDOWN_S = 30.0  # minimum gap between scale actions
    AUTOSCALE_SAMPLE_INTERVAL = 10.0
    # Minimum worker age before the autoscaler may pick it for
    # scale-down (ce-review C-03 / Unit 5). Literal 30.0, matching
    # max(AUTOSCALE_SAMPLE_INTERVAL=10, SCALE_COOLDOWN_S=30) at the
    # current defaults — protects the crash → _maybe_restart →
    # _autoscale_tick churn path where a freshly spawned replacement
    # would otherwise be eligible on the very next tick. Must stay
    # < SCALE_DOWN_IDLE_S so steady-state scale-down is unaffected.
    # If you subclass and change AUTOSCALE_SAMPLE_INTERVAL or
    # SCALE_COOLDOWN_S, override SCALE_MIN_AGE_S to stay
    # >= max(AUTOSCALE_SAMPLE_INTERVAL, SCALE_COOLDOWN_S).
    SCALE_MIN_AGE_S = 30.0

    def __init__(
        self,
        num_workers: int,
        worker_entry: Callable[..., None],
        worker_args: tuple = (),
        *,
        autoscale: tuple[int, int] | None = None,
        queue_depth_fn: QueueDepthFn | None = None,
        shutdown_grace_s: float | None = None,
        reload: bool = False,
        watch_dirs: tuple[str, ...] = (),
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
        # Per-instance override of the phase-1 shutdown budget
        # (ce-review REL-01). Without this, any task running longer
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
        self._shutting_down = False
        self._reload_requested = False
        self._next_worker_id = 0
        # Härtung R2 P0-1: count of slots whose crash-restart budget is
        # exhausted. Each such slot permanently reduces the static
        # backfill target — without this, the backfill loop respawned a
        # fresh WorkerInfo (empty restart history) right after
        # _maybe_restart declined, bypassing the 5-per-hour limit in an
        # unbounded spawn storm. When ALL slots are exhausted the
        # supervisor exits non-zero so systemd/k8s take over with their
        # own backoff policy.
        self._exhausted_slots = 0

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

        # ce-review SEC-01 / Unit 6: freeze argv AND environment at
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
        # ce-review #5: TYPE_CHECKING import gives mypy/IDEs full
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
            # ce-review REL-03 / Unit 4: orphan-guard. If any child
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
                # ce-review SEC-01 / Unit 6: use the argv AND env
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
        # Härtung R2 P1-1: deliberately NO SIGCHLD handler. A previous
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

    def _shutdown_all_workers(self) -> None:  # noqa: C901  # U21 (supervisor.py)
        """Three-phase shutdown: event → SIGTERM → SIGKILL."""
        if not self.workers:
            return
        self.shutdown_event.set()

        # Phase 1: cooperative exit via shutdown_event.
        deadline = time.monotonic() + self.shutdown_grace_s
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
            self._check_dead_workers()

            # Härtung R2 P0-1: all restart budgets burned and no live
            # worker left → exit non-zero instead of spinning (or, pre-
            # fix, storming fresh spawns). systemd/k8s restart us with
            # their own backoff; staying alive here would just hide the
            # outage from the orchestrator.
            if (
                self._autoscale is None
                and self._exhausted_slots >= self.initial_workers
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

        Without autoscale: ``initial_workers`` minus permanently
        exhausted slots (Härtung R2 P0-1 — backfilling an exhausted
        slot would reset its restart history and bypass the rate
        limit).
        With autoscale: current ``len(self.workers)`` since
        ``_autoscale_tick`` adjusts independently; backfill only
        replaces crashed workers (not those we scaled down
        deliberately).
        """
        if self._autoscale is None:
            return max(0, self.initial_workers - self._exhausted_slots)
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
            except Exception:
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
        """Restart ``old`` respecting the rolling-window rate limit.

        ce-review C-01 / REL-02: true sliding window. Prunes
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
            # Härtung R2 P0-1: mark the slot as permanently exhausted so
            # _current_target shrinks and the backfill loop cannot
            # respawn it with a fresh (empty) restart history.
            self._exhausted_slots += 1
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
        new = self._start_worker()
        new.restart_timestamps = timestamps

    # ------------------------------------------------------------------
    # R4.7 Autoscaling
    # ------------------------------------------------------------------

    def _autoscale_tick(self) -> None:
        """One autoscale decision (no-op outside the sample interval).

        Sampling uses ``_queue_depth_fn`` which is synchronous — the
        caller adapts async broker APIs into a sync wrapper (see
        :func:`make_queue_depth_sampler`).
        """
        # ce-review M-7: use explicit checks instead of asserts so
        # the invariant survives python -O deployments. Constructor
        # already enforces these, but a subtle refactor could later
        # violate them.
        if self._autoscale is None or self._queue_depth_fn is None:
            return

        now = time.monotonic()
        if now - self._last_autoscale_sample < self.AUTOSCALE_SAMPLE_INTERVAL:
            return

        # ce-review C-02: check cooldown BEFORE advancing the sample
        # timestamp. Otherwise a cooldown early-return consumes the
        # sample slot, extending the effective dead-time by up to
        # AUTOSCALE_SAMPLE_INTERVAL beyond the documented cooldown.
        if now - self._last_scale_decision < self.SCALE_COOLDOWN_S:
            return

        self._last_autoscale_sample = now

        try:
            depth = self._queue_depth_fn()
        except Exception as exc:
            # ce-review SEC-03: sanitize_url strips embedded AMQP
            # credentials before the exception text reaches structlog.
            logger.warning(
                "autoscale_sample_failed", error=sanitize_url(str(exc)),
            )
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
        """Tell the youngest eligible worker to drain + exit (SIGTERM).

        ce-review C-03 / Unit 5: workers younger than
        :attr:`SCALE_MIN_AGE_S` are excluded. Without this, a
        crash → ``_maybe_restart`` → ``_autoscale_tick`` sequence
        would spawn a replacement and immediately kill it, producing
        pointless process churn.
        """
        if not self.workers:
            return
        now = time.monotonic()
        eligible = {
            pid: w for pid, w in self.workers.items()
            if (now - w.spawned_at) >= self.SCALE_MIN_AGE_S
        }
        if not eligible:
            logger.debug(
                "autoscale_down_deferred_all_young",
                total=len(self.workers),
                min_age_s=self.SCALE_MIN_AGE_S,
            )
            return
        # Youngest (highest worker_id) among the eligible — oldest
        # workers have warmer caches.
        target_pid, info = max(
            eligible.items(), key=lambda kv: kv[1].worker_id,
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
