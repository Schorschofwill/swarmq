"""File-watch-based SIGHUP trigger for R5.3 hot reload.

Runs a ``watchfiles``-backed watcher in a daemon thread. On change,
fires a caller-supplied ``trigger`` (in production:
``os.kill(os.getpid(), SIGHUP)``) unless a caller-supplied
``is_busy`` callable reports a reload is already in flight — the
existing SIGHUP handler in :class:`swarmq.process_manager.ProcessManager`
is idempotent at the flag level but emits duplicate log events on
double-entry, so re-entrancy is suppressed at the watcher layer.

The watcher is decoupled from :class:`ProcessManager` on purpose:
it only knows about paths, a trigger, and a busy-check. That makes
it straightforwardly testable with a fake iterator and keeps the
coupling one-directional (supervisor depends on watcher, never the
other way round).

``watchfiles`` is an optional dependency. :func:`check_watchfiles_available`
performs the pre-flight check so ``--reload`` without the extra
fails fast in ``run_multi_process_worker`` with a concrete install
hint rather than crashing inside the daemon thread.
"""

from __future__ import annotations

import importlib.util
import os
import sys
import threading
from collections.abc import Callable, Iterable
from importlib.machinery import ModuleSpec
from pathlib import Path
from typing import TYPE_CHECKING, Any

import structlog

from swarmq.exceptions import ConfigurationError

if TYPE_CHECKING:  # pragma: no cover — import-time only
    from watchfiles import BaseFilter

__all__ = [
    "FileWatcher",
    "SwarmQFilter",
    "check_watchfiles_available",
    "resolve_watch_dirs",
]

logger = structlog.get_logger()


def check_watchfiles_available() -> None:
    """Raise :class:`ConfigurationError` if ``watchfiles`` is missing.

    Called from :func:`swarmq.process_manager.run_multi_process_worker`
    before the supervisor starts, so a missing optional dep fails with
    a user-actionable message instead of a late crash in the daemon
    thread.
    """
    try:
        import watchfiles  # noqa: F401
    except ImportError as exc:
        raise ConfigurationError(
            "--reload: watchfiles is not installed. "
            "Install with:  pip install 'swarmq[reload]'"
        ) from exc


def resolve_watch_dirs(
    module: str,
    explicit_dirs: Iterable[str],
    *,
    spec: ModuleSpec | None = None,
) -> tuple[str, ...]:
    """Return the directories to watch for ``--reload``.

    If ``explicit_dirs`` is non-empty, it wins verbatim — the operator
    opted into a specific scope. Duplicates are collapsed in-order
    (``dict.fromkeys``) so repeated ``--watch-dir`` entries for the
    same absolute path do not register the same inotify watch twice.

    Otherwise the module is resolved via
    :func:`importlib.util.find_spec` and its ``origin`` parent is
    returned. Namespace packages (``spec.origin is None``) raise
    :class:`ConfigurationError`. When the auto-resolved path lives
    inside ``sys.prefix`` (site-packages / stdlib), a WARNING is
    logged — watching the framework itself is almost always a
    footgun (reload storms on ``pip install -e .``).

    Pass ``spec`` when the caller already resolved the module (e.g.
    ``_validate_module_importable``) — this avoids a second
    ``find_spec`` call that would re-execute ``pkg/__init__.py`` in
    edge cases that invalidate their own ``sys.modules`` entry.

    .. warning::
        The caller is responsible for ensuring ``spec`` corresponds
        to ``module``. No cross-validation is performed; a mismatched
        spec silently resolves the wrong watch directory.
    """
    explicit = tuple(dict.fromkeys(explicit_dirs))
    if explicit:
        return explicit

    if spec is None:
        try:
            spec = importlib.util.find_spec(module)
        except (ImportError, ValueError) as exc:
            raise ConfigurationError(
                f"--reload: cannot resolve watch directory for module "
                f"{module!r} ({exc}). Pass --watch-dir explicitly."
            ) from exc
    if spec is None or spec.origin is None:
        raise ConfigurationError(
            f"--reload: cannot auto-detect watch directory for module "
            f"{module!r} (namespace package or missing origin). "
            f"Pass --watch-dir explicitly."
        )
    if spec.origin in {"frozen", "built-in"}:
        raise ConfigurationError(
            f"--reload: module {module!r} is a {spec.origin} module — "
            "no source file to watch. Pass --watch-dir explicitly."
        )

    resolved = str(Path(spec.origin).parent)
    # Reload-storm guard: warn when the auto-resolved
    # dir lives under sys.prefix (installed package) or sys.exec_prefix
    # (stdlib). Operator still gets what they asked for; this is a
    # loud hint that they probably want --watch-dir instead.
    for root in {sys.prefix, sys.exec_prefix}:
        try:
            Path(resolved).relative_to(root)
        except ValueError:
            continue
        logger.warning(
            "watch_dir_inside_venv_or_stdlib",
            module=module,
            resolved_dir=resolved,
            sys_root=root,
            hint="reload-storms possible on pip install; use --watch-dir",
        )
        break
    return (resolved,)


class SwarmQFilter:
    """Watchfiles filter wrapping ``PythonFilter`` plus SwarmQ-specific
    editor-noise exclusions.

    Uses composition (not inheritance) over ``watchfiles.PythonFilter``
    so the lazy import stays inside ``__init__``. ``watchfiles`` is
    an optional dependency; constructing ``SwarmQFilter`` without the
    ``[reload]`` extra raises ``ImportError``.

    ``_IGNORED_SUFFIXES`` only matches at path-end, so ``tasks.py.bak``
    is rejected but ``tasks.bak.py`` passes through — the latter is a
    legitimate Python file name (``PythonFilter`` accepts ``.py``), and
    filtering on infix substrings would reject every file under a
    directory literally named ``bak``.
    """

    _IGNORED_BASENAMES = frozenset({"4913"})  # Vim probe-file
    _IGNORED_SUFFIXES = (".tmp", ".bak")

    def __init__(self) -> None:
        """Instantiate the filter.

        :raises ImportError: when ``watchfiles`` is not installed —
            install the optional extra via ``pip install 'swarmq[reload]'``.
        """
        from watchfiles import PythonFilter

        self._python_filter = PythonFilter()

    def __call__(self, change: Any, path: str) -> bool:
        basename = path.rsplit("/", 1)[-1]
        if basename in self._IGNORED_BASENAMES:
            return False
        if path.endswith(self._IGNORED_SUFFIXES):
            return False
        return self._python_filter(change, path)


class FileWatcher:
    """Daemon-thread wrapper around ``watchfiles.watch``.

    Not thread-safe to start twice; construct a fresh instance if you
    need to re-arm after :meth:`stop`.
    """

    def __init__(
        self,
        watch_dirs: Iterable[str],
        trigger: Callable[[], None],
        is_busy: Callable[[], bool],
        file_filter: BaseFilter | None = None,
        debounce_ms: int = 1600,
        step_ms: int = 50,
    ) -> None:
        self._dirs = tuple(watch_dirs)
        if not self._dirs:
            raise ValueError("FileWatcher requires at least one watch directory")
        self._trigger = trigger
        self._is_busy = is_busy
        self._filter = file_filter
        self._debounce_ms = debounce_ms
        self._step_ms = step_ms
        self._stop_event = threading.Event()
        # Public-observable completion signal so tests
        # (and operators) can wait for watcher exit without reaching
        # into the private thread handle.
        self._exit_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None:
            raise RuntimeError("FileWatcher already started")
        # Assign self._thread only AFTER .start() returns
        # so is_running() never reports True while the thread has not
        # actually been scheduled yet. If .start() raises (rare — OS
        # thread-limit), _thread stays None and the watcher is cleanly
        # re-armable.
        thread = threading.Thread(
            target=self._run,
            name="swarmq-file-watcher",
            daemon=True,
        )
        thread.start()
        self._thread = thread
        logger.info(
            "watch_started",
            watch_dirs=list(self._dirs),
            debounce_ms=self._debounce_ms,
        )

    def stop(self, timeout: float = 2.0) -> None:
        self._stop_event.set()
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=timeout)
            # Surface the silent-abandonment case: daemon=True
            # guarantees the process can still exit, but the operator
            # deserves to know the watcher did not stop cleanly (inotify
            # stuck in kernel wait, Rust runtime poll > budget).
            if self._thread.is_alive():
                logger.warning(
                    "watch_thread_stop_timeout",
                    timeout_s=timeout,
                    hint="daemon thread will be killed by process exit",
                )
        logger.info("watch_stopped")

    def is_running(self) -> bool:
        """Return True while the watcher thread is still alive.

        Useful for tests and for operators who want to detect a
        crashed watcher without consulting the stdlib logging output.
        """
        return self._thread is not None and not self._exit_event.is_set()

    def wait_until_stopped(self, timeout: float | None = None) -> bool:
        """Block until the watcher thread exits (or ``timeout`` lapses).

        Returns ``True`` if the thread exited within the timeout,
        ``False`` on timeout. Safe to call before :meth:`start` —
        returns ``True`` immediately in that case because the
        watcher is definitionally not running.

        Passing ``timeout=None`` (the default) blocks indefinitely,
        matching the :meth:`threading.Event.wait` convention.
        """
        if self._thread is None:
            return True
        return self._exit_event.wait(timeout=timeout)

    def _run(self) -> None:
        # The try/finally sets _exit_event so
        # callers observing is_running() / wait_until_stopped() see the
        # transition without racing against the thread handle.
        try:
            try:
                from watchfiles import watch
            except ImportError:  # pragma: no cover — pre-flight guards this
                logger.error("watchfiles_import_failed")
                return

            # Re-validate watch dirs just before arming.
            # Parse-time validation in argparse cannot catch NFS unmount /
            # bind-mount removal / tmpfs eviction that happens between
            # argument parsing and supervisor start. Fail loud with the
            # specific path rather than dying inside watchfiles with a
            # generic OSError.
            missing = [d for d in self._dirs if not os.path.isdir(d)]
            if missing:
                logger.error(
                    "watch_dir_missing",
                    missing=missing,
                    hint="path vanished between --watch-dir parse and watcher start",
                )
                return

            file_filter = self._filter or SwarmQFilter()
            try:
                for changes in watch(
                    *self._dirs,
                    stop_event=self._stop_event,
                    watch_filter=file_filter,
                    debounce=self._debounce_ms,
                    step=self._step_ms,
                    rust_timeout=1000,
                    # Default True lets KeyboardInterrupt
                    # (BaseException) escape our `except Exception` — thread
                    # dies silently. We install our own SIGINT handler at
                    # the supervisor level; let the Rust layer forward them
                    # via stop_event instead of raising into Python.
                    raise_interrupt=False,
                ):
                    if self._stop_event.is_set():
                        return
                    if self._is_busy():
                        logger.debug(
                            "reload_skip_reentrant",
                            changed_count=len(changes),
                        )
                        continue
                    logger.info(
                        "reload_triggered",
                        changed_count=len(changes),
                        changed_sample=[p for _, p in list(changes)[:5]],
                    )
                    try:
                        self._trigger()
                    except Exception:
                        logger.exception("reload_trigger_failed")
            except Exception:
                # logger.exception is the codebase convention
                # for top-level exception boundaries (see worker.py).
                logger.exception("watch_thread_crashed")
        finally:
            # Set the public exit event so callers
            # observing is_running() / wait_until_stopped() see the
            # transition without racing against the thread handle.
            self._exit_event.set()
