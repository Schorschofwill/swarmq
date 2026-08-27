"""Subprozess-Entry (Plan U21) — verbatim: ``worker_subprocess_entry``
(läuft im Kind; importiert die CLI lazy, KTD7-Allowlist-Kante)."""

from __future__ import annotations

import dataclasses
import os
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover — import-time only
    pass

import structlog

from swarmq.process_manager.config import WorkerConfig

logger = structlog.get_logger()


def worker_subprocess_entry(
    worker_id: int,
    shutdown_event: Any,  # multiprocessing.synchronize.Event (spawn-Kontext)
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
    import argparse as _argparse
    import asyncio
    import logging as _stdlib_logging
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
