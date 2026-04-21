"""File-watch-based SIGHUP trigger for R5.3 hot reload.

Runs a ``watchfiles``-backed watcher in a daemon thread. On change,
fires a caller-supplied ``trigger`` (in production:
``os.kill(os.getpid(), SIGHUP)``) unless a caller-supplied
``is_busy`` callable reports a reload is already in flight — the
existing SIGHUP handler in :class:`swarmq.process_manager.ProcessManager`
is not idempotent, so re-entrancy is suppressed at the watcher layer.

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

import threading
from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING, Any

import structlog

if TYPE_CHECKING:  # pragma: no cover — import-time only
    from watchfiles import BaseFilter

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
        from swarmq.exceptions import ConfigurationError

        raise ConfigurationError(
            "--reload requires the optional 'watchfiles' dependency. "
            "Install with:  pip install 'swarmq[reload]'"
        ) from exc


def resolve_watch_dirs(
    module: str, explicit_dirs: Iterable[str]
) -> tuple[str, ...]:
    """Return the directories to watch for ``--reload``.

    If ``explicit_dirs`` is non-empty, it wins verbatim — the operator
    opted into a specific scope. Otherwise the module is resolved via
    :func:`importlib.util.find_spec` and its ``origin`` parent is
    returned. Namespace packages (``spec.origin is None``) raise
    :class:`ConfigurationError` because there is no single directory
    that unambiguously covers them.
    """
    explicit = tuple(explicit_dirs)
    if explicit:
        return explicit

    import importlib.util
    from pathlib import Path

    from swarmq.exceptions import ConfigurationError

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
    # Frozen ("frozen") and built-in ("built-in") modules have no
    # on-disk source to watch. Bail out loudly rather than returning
    # ``Path("frozen").parent == Path(".")``.
    if spec.origin in {"frozen", "built-in"}:
        raise ConfigurationError(
            f"--reload: module {module!r} is a {spec.origin} module — "
            "no source file to watch. Pass --watch-dir explicitly."
        )
    return (str(Path(spec.origin).parent),)


def _default_filter() -> BaseFilter:
    """Construct the SwarmQ file filter (lazy — watchfiles is optional)."""
    from watchfiles import PythonFilter

    class SwarmQFilter(PythonFilter):
        """PythonFilter + extras for editor-noise we saw in the wild."""

        _IGNORED_BASENAMES = frozenset({"4913"})  # Vim probe-file
        _IGNORED_SUFFIXES = (".tmp", ".bak")

        def __call__(self, change: Any, path: str) -> bool:
            basename = path.rsplit("/", 1)[-1]
            if basename in self._IGNORED_BASENAMES:
                return False
            if path.endswith(self._IGNORED_SUFFIXES):
                return False
            return super().__call__(change, path)

    return SwarmQFilter()


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
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None:
            raise RuntimeError("FileWatcher already started")
        self._thread = threading.Thread(
            target=self._run,
            name="swarmq-file-watcher",
            daemon=True,
        )
        self._thread.start()
        logger.info(
            "watch_started",
            watch_dirs=list(self._dirs),
            debounce_ms=self._debounce_ms,
        )

    def stop(self, timeout: float = 2.0) -> None:
        self._stop_event.set()
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=timeout)
        logger.info("watch_stopped")

    def _run(self) -> None:
        try:
            from watchfiles import watch
        except ImportError:  # pragma: no cover — pre-flight guards this
            logger.error("watchfiles_import_failed")
            return

        file_filter = self._filter or _default_filter()
        try:
            for changes in watch(
                *self._dirs,
                stop_event=self._stop_event,
                watch_filter=file_filter,
                debounce=self._debounce_ms,
                step=self._step_ms,
                rust_timeout=1000,
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
            logger.warning("watch_thread_crashed", exc_info=True)
