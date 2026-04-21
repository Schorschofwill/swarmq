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

import dataclasses
import importlib.util
import multiprocessing
from importlib.machinery import ModuleSpec
import os
import signal
import sys
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Callable, TypedDict

if TYPE_CHECKING:  # pragma: no cover — import-time only
    from swarmq.hot_reload import FileWatcher

import structlog

from swarmq.exceptions import ConfigurationError
from swarmq.logging import sanitize_url

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


# Queue-depth sampling signature: returns the total pending count
# across all configured queues. May raise — the supervisor swallows
# and logs.
QueueDepthFn = Callable[[], int]


_VALID_LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR")
_PICKLE_SAFE_TYPES = (str, int, float, bool, tuple, type(None))


@dataclasses.dataclass(frozen=True)
class WorkerConfig:
    """Pickle-safe configuration bundle for a spawned worker.

    Crosses the multiprocessing spawn boundary as the sole payload
    inside ``worker_args``. Collapses the former 11-parameter
    :func:`worker_subprocess_entry` signature into one object so a
    new CLI flag is a single-site edit (add the field, set it in
    :func:`run_multi_process_worker`).

    ``queues`` is ``tuple[str, ...]`` — not ``list[str]`` — so the
    ``frozen=True`` immutability promise is not violated by in-place
    list mutation. ``__post_init__`` runtime-enforces a pickle-safe
    type allowlist so the invariant survives future programmatic
    callers that bypass argparse validation.

    The allowlist check is shallow: tuple/list field contents are
    not recursively type-checked. In production that is fine because
    ``queues`` is always built from argparse's ``list[str]`` output
    via ``tuple(args.queues)``. A future programmatic caller passing
    ``queues=(lambda: None,)`` would still fail, but at
    ``pickle.dumps`` time, not at construction.
    """

    module: str
    queues: tuple[str, ...]
    concurrency: int
    log_level: str
    log_format: str
    burst: bool
    burst_idle_timeout: float
    priorities: bool
    metrics_port: int | None
    health_port: int | None
    # R5.3: pipeline-uniform fields. Watcher lives in the supervisor
    # (not the worker subprocess), but routing them through WorkerConfig
    # avoids the supervisor-only-flag drift that R4.6-H Unit 1 removed.
    reload: bool = False
    watch_dirs: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for f in dataclasses.fields(self):
            value = getattr(self, f.name)
            if not isinstance(value, _PICKLE_SAFE_TYPES):
                raise TypeError(
                    f"WorkerConfig.{f.name} must be pickle-safe; "
                    f"got {type(value).__name__}",
                )
        if self.log_level not in _VALID_LOG_LEVELS:
            raise ValueError(
                f"WorkerConfig.log_level must be one of "
                f"{_VALID_LOG_LEVELS}; got {self.log_level!r}",
            )


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

        # Autoscale state.
        self._last_scale_decision = 0.0
        self._last_autoscale_sample = 0.0
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
                os.execvpe(  # noqa: S606
                    sys.executable,
                    [sys.executable] + self._startup_argv,
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
            logger.error(
                "worker_restart_limit_exceeded",
                worker_id=old.worker_id,
                limit=self.MAX_RESTARTS,
                window_s=self.RESTART_WINDOW,
                recent_restarts=len(timestamps),
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
        except Exception as exc:  # noqa: BLE001
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


# ---------------------------------------------------------------------------
# Worker subprocess entry + CLI bridge
# ---------------------------------------------------------------------------
#
# These helpers sit in this module (not cli.py) so they are importable
# from a spawned subprocess without side-effects. A spawn-started
# process re-imports the module that defines its ``target`` — keeping
# the entry here avoids re-running argparse during child import.


def worker_subprocess_entry(
    worker_id: int,
    shutdown_event,
    cfg: WorkerConfig,
) -> None:
    """Run a single worker inside a spawned subprocess.

    The parent supervisor calls this via ``multiprocessing.Process``.
    ``cfg`` crosses the pickle boundary as the sole typed payload;
    we expand it into an ``argparse.Namespace`` for the existing
    ``run_worker_async`` path, which remains unchanged.

    A background asyncio task polls ``shutdown_event`` (a
    ``multiprocessing.Event``) and triggers graceful shutdown by
    raising SIGTERM on our own PID — this re-uses the Worker's
    existing signal-driven shutdown flow without coupling to its
    private API.
    """
    import asyncio
    import argparse as _argparse
    import logging as _stdlib_logging
    import os
    import signal as _signal

    # ce-review M-3 / Unit 7: import a public symbol so a rename in
    # cli.py is detectable via tests rather than a silent subprocess
    # crash at runtime. Spawn re-imports modules fresh — the import
    # executes in the child, not the parent. The win is symbol-
    # stability (public vs private underscore name), not earlier
    # error surface.
    from swarmq.cli import run_worker_async

    # Logging is fresh-configured per subprocess. structlog picks up
    # its global config on first use, but stdlib logging is
    # per-process — reset level so child inherits parent's verbosity.
    _stdlib_logging.basicConfig(level=cfg.log_level)

    # asdict converts tuple → list (dataclasses has no tuple-preservation);
    # Namespace callers expect list-typed queues so we re-wrap explicitly.
    kwargs = dataclasses.asdict(cfg)
    kwargs["queues"] = list(kwargs["queues"])
    # R5.3 ce-review #18: reload/watch_dirs are supervisor-only state.
    # They ride along on WorkerConfig for pipeline-uniformity (R4.6-H
    # Unit 1), but the worker subprocess must not construct a watcher
    # — that would spawn N reload pipelines, one per worker, each
    # firing SIGHUP at its own PID (default action: terminate). Drop
    # them explicitly so a future developer adding `if ns.reload: ...`
    # inside run_worker_async cannot silently regress.
    kwargs.pop("reload", None)
    kwargs.pop("watch_dirs", None)
    ns = _argparse.Namespace(**kwargs)

    async def _run() -> None:
        async def _watch_shutdown() -> None:
            # Poll the cross-process event; when set, raise SIGTERM
            # on ourselves so the Worker's R2.5 graceful-shutdown
            # handler fires naturally.
            while True:
                if shutdown_event.is_set():
                    os.kill(os.getpid(), _signal.SIGTERM)
                    return
                await asyncio.sleep(0.3)

        watcher = asyncio.create_task(_watch_shutdown())
        try:
            await run_worker_async(ns)
        finally:
            watcher.cancel()

    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        return


#: Total wall-clock budget for one queue-depth sample cycle including
#: connect + all get_queue_depth calls + close. Beyond this, we give
#: up for this tick and let the next one retry — otherwise a stuck
#: AMQP connect would block the monitor loop for the full TCP timeout
#: (~2 min) and stall crash detection + autoscale decisions. ce-review
#: REL-05.
SAMPLE_TIMEOUT_S = 5.0


def _make_queue_depth_sampler(
    broker_url: str, queues: list[str],
) -> Callable[[], int]:
    """Build a **synchronous** sampler the ProcessManager can call.

    The supervisor has no asyncio loop; our broker API is async. We
    open a throwaway connection each call, sum depth across all
    configured queues, and close. 10s cadence makes this cheap.
    Wrapped in :data:`SAMPLE_TIMEOUT_S` so a stuck broker does not
    hang the supervisor (ce-review REL-05).
    """
    import asyncio

    from swarmq.broker.rabbitmq import RabbitMQBroker

    def sample() -> int:
        async def _run() -> int:
            broker = RabbitMQBroker(broker_url)
            try:
                async with asyncio.timeout(SAMPLE_TIMEOUT_S):
                    await broker.connect()
                    total = 0
                    for q in queues:
                        total += await broker.get_queue_depth(q)
                    return total
            finally:
                # Close may itself hang under broker failure — bound
                # it separately so the outer timeout does not leak
                # into the next sampler call.
                try:
                    async with asyncio.timeout(1.0):
                        await broker.close()
                except (TimeoutError, Exception):  # noqa: BLE001
                    pass

        return asyncio.run(_run())

    return sample


def _validate_module_importable(module: str) -> ModuleSpec:
    """Pre-flight check: raise ConfigurationError if ``module`` cannot
    be resolved (ce-review REL-07 / Unit 2).

    Runs in the PARENT before the supervisor starts, so a typo or
    a filesystem-level missing module fails once with a clear error
    instead of looping through the 5-per-hour subprocess restart cap.

    Known limitations (find_spec is not a Sandbox — dokumentiert im
    Plan unter Unit 2). Die folgenden Fälle laufen weiterhin in den
    Subprocess-Restart-Loop, weil find_spec das Modul NICHT lädt:

    - fehlende transitive Dependency (tasks.py existiert, importiert
      aber ein nicht installiertes Paket)
    - kaputte C-Extension / ABI-Mismatch
    - env-abhängige Top-Level-Imports mit Parent/Child-Divergenz
    - Zirkuläre Imports, die partiell resolven
    - Namespace-Package-Shadowing über PYTHONPATH-Order

    Für diese Fälle greift das Restart-Rate-Limit (5/h). Operator soll
    ``swarmq inspect --module ...`` nutzen um Import-Fehler vorab zu
    diagnostizieren.

    ce-review SEC-FINDSPEC-01: note that resolving ``pkg.sub`` via
    find_spec imports and executes ``pkg/__init__.py`` in THIS
    process (the supervisor). For most operators that is harmless;
    for packages with side-effectful top-level code (thread spawn,
    signal-handler installation) the effects are visible in the
    supervisor's process state. The check runs only at startup
    (inside ``run_multi_process_worker``), not on SIGHUP-reload.
    """
    try:
        spec = importlib.util.find_spec(module)
    except (ImportError, ValueError) as exc:
        # ImportError covers ModuleNotFoundError + the "no package
        # specified for '.relative'" case; ValueError handles some
        # malformed dotted paths depending on Python version.
        raise ConfigurationError(
            f"--module {module!r} cannot be imported: {exc}",
        ) from exc
    # ``spec.origin is None`` für Namespace-Packages ohne __init__.py —
    # find_spec liefert einen spec, aber import_module wird scheitern.
    if spec is None or spec.origin is None:
        raise ConfigurationError(
            f"--module {module!r} resolves to no loadable spec — "
            "check spelling, PYTHONPATH, and __init__.py presence",
        )
    # R5.3 ce-review #7: return the spec so run_multi_process_worker
    # can hand it to resolve_watch_dirs instead of calling find_spec
    # a second time (which would re-execute pkg/__init__.py in the
    # edge case where it invalidates its own sys.modules entry).
    return spec


class WorkerConfigCheckResult(TypedDict):
    """Return shape for :func:`check_worker_config`.

    ce-review pass 2 #20: TypedDict so mypy narrows field accesses
    instead of treating values as ``object``. Mirrors the ``--check``
    JSON payload keys but drops ``ok``/``error``/``error_type`` —
    those are CLI-level framing (success implies ok=True; raise on
    failure).
    """

    module: str
    module_origin: str | None
    reload: bool
    watch_dirs: list[str] | None
    warnings: list[str]


def check_worker_config(
    *,
    module: str,
    reload: bool,
    watch_dir: list[str] | tuple[str, ...] | None,
    burst: bool = False,
) -> WorkerConfigCheckResult:
    """Run the pre-flight validation for ``swarmq worker`` and return
    a machine-readable summary dict.

    Scope mirrors ``run_multi_process_worker``'s pre-supervisor checks
    so a successful ``check_worker_config`` result implies the
    supervisor would get past pre-flight (R5.3 ce-review #10 / #23):

    - ``module`` is importable (``find_spec``-based).
    - When ``reload=True``: ``watchfiles`` is installed and watch-dirs
      resolve to a non-empty tuple (stdlib + ``[reload]`` extra).
    - When ``reload=True`` and ``burst=True``: a non-fatal warning is
      recorded; the combination is ambiguous in intent but not
      rejected.

    Does **not** connect to broker or backend. Raises
    :class:`ConfigurationError` on any validation failure.

    ``stdout`` is captured while importing the module to protect
    machine-readable callers (``--check``) against ``print()`` calls
    inside ``pkg/__init__.py`` (R5.3 ce-review #23 / adv ADV-04).

    Returned dict has a fixed shape with every key always present —
    callers can rely on ``dict["watch_dirs"]`` being accessible
    regardless of ``reload`` flag. Unused fields are ``None``.
    """
    import contextlib
    import io

    from swarmq.hot_reload import (
        check_watchfiles_available,
        resolve_watch_dirs,
    )

    warnings: list[str] = []

    # ce-review #23 / ADV-04: protect JSON-on-stdout callers against
    # module-init print() contaminating the payload. Side-effects
    # captured here are discarded on success; on failure the captured
    # text is forwarded to stderr so an operator debugging a bad
    # --module sees it.
    capture = io.StringIO()
    try:
        with contextlib.redirect_stdout(capture):
            module_spec = _validate_module_importable(module)
    except Exception:
        # Forward whatever the module printed to stderr before re-raising
        # so humans still get the context.
        sys.stderr.write(capture.getvalue())
        raise

    watch_dirs_result: list[str] | None = None
    if reload:
        check_watchfiles_available()
        resolved = resolve_watch_dirs(
            module, tuple(watch_dir or ()), spec=module_spec,
        )
        watch_dirs_result = list(resolved)

        if burst:
            warnings.append(
                "reload_burst_conflict: --reload and --burst are not "
                "designed to be combined; burst drains-and-exits while "
                "reload re-execs the supervisor. Pick one per invocation."
            )

    return {
        "module": module,
        "module_origin": module_spec.origin,
        "reload": reload,
        "watch_dirs": watch_dirs_result,
        "warnings": warnings,
    }


def run_multi_process_worker(args) -> int:
    """CLI entry for ``swarmq worker --processes N`` / ``--autoscale``.

    Called from :func:`swarmq.cli.main` when the user opted into the
    multi-process path. Synchronous (the supervisor runs in the
    parent process without an event loop).
    """
    broker_url = os.environ.get("SWARMQ_BROKER_URL", "")
    backend_url = os.environ.get("SWARMQ_BACKEND_URL", "")
    if not broker_url or not backend_url:
        raise ConfigurationError(
            "SWARMQ_BROKER_URL and SWARMQ_BACKEND_URL must both be set",
        )

    reload_enabled = bool(getattr(args, "reload", False))
    watch_dirs_raw = getattr(args, "watch_dir", None) or ()

    # R5.3: --watch-dir without --reload is almost certainly an operator
    # typo. Warn (don't error) so the worker still starts, but surface
    # the ignored list so it's not silently dropped.
    if watch_dirs_raw and not reload_enabled:
        logger.warning(
            "watch_dir_ignored_without_reload",
            watch_dirs=list(watch_dirs_raw),
        )

    # ce-review Unit 2 + #10: single pre-flight via the public
    # validator; check_worker_config runs the same sequence (find_spec,
    # watchfiles availability, watch-dir resolution) that
    # `swarmq worker --check` uses. Fails fast on any misconfiguration
    # before the supervisor starts spawning subprocesses.
    check_result = check_worker_config(
        module=args.module,
        reload=reload_enabled,
        watch_dir=list(watch_dirs_raw) or None,
        burst=bool(getattr(args, "burst", False)),
    )
    for warning in check_result["warnings"] or []:
        logger.warning("worker_config_warning", warning=warning)

    resolved_watch_dirs: tuple[str, ...] = tuple(
        check_result["watch_dirs"] or (),
    )

    queue_depth_fn: QueueDepthFn | None = None
    if args.autoscale is not None:
        queue_depth_fn = _make_queue_depth_sampler(broker_url, list(args.queues))

    # ce-review REL-01: resolve SwarmQConfig.shutdown_timeout once in
    # the parent and pass it as the phase-1 grace so a subprocess's
    # graceful-shutdown flow has the budget it was configured for.
    from swarmq.config import SwarmQConfig
    sq_config = SwarmQConfig(broker_url=broker_url, backend_url=backend_url)
    shutdown_grace_s = float(sq_config.shutdown_timeout)

    worker_cfg = WorkerConfig(
        module=args.module,
        queues=tuple(args.queues),
        concurrency=args.concurrency,
        log_level=args.log_level,
        log_format=args.log_format,
        burst=args.burst,
        burst_idle_timeout=args.burst_idle_timeout,
        priorities=args.priorities,
        metrics_port=args.metrics_port,
        health_port=args.health_port,
        reload=reload_enabled,
        watch_dirs=resolved_watch_dirs,
    )

    pm = ProcessManager(
        num_workers=args.processes,
        worker_entry=worker_subprocess_entry,
        worker_args=(worker_cfg,),
        autoscale=args.autoscale,
        queue_depth_fn=queue_depth_fn,
        shutdown_grace_s=shutdown_grace_s,
        reload=reload_enabled,
        watch_dirs=resolved_watch_dirs,
    )
    return pm.run()
