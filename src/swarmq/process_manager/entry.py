"""Subprozess-Entry (Plan U21) — verbatim: ``worker_subprocess_entry``
(läuft im Kind; importiert die CLI lazy, KTD7-Allowlist-Kante)."""

from __future__ import annotations

import dataclasses
import os
import signal as _signal
import sys
import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover — import-time only
    pass

import structlog

from swarmq.exceptions import ConfigurationError
from swarmq.process_manager.config import WorkerConfig
from swarmq.supervision import (
    RECYCLE_NOACK_EXIT_CODE,
    RecycleAckTimeout,
    SupervisorChannel,
    set_current_channel,
)

logger = structlog.get_logger()


#: Exit-Code fuer Konfigurationsfehler im Worker-Subprozess (R29, KTD8).
#: Entspricht ``EX_CONFIG`` aus sysexits.h; der Supervisor startet einen so
#: beendeten Worker nicht neu, sondern endet selbst mit 1.
CONFIG_ERROR_EXIT_CODE = 78


def _unblock_inherited_signals() -> None:
    """Die vom Supervisor geerbte Signalblockade aufheben.

    Der Supervisor blockiert SIGINT/SIGTERM/SIGHUP, waehrend er Worker startet
    (Signal-Race, ``docs/worker-architecture.md`` 3.4); ``spawn`` vererbt die
    Maske. Bliebe sie stehen, laege das SIGTERM, das der Watcher auf die eigene
    PID schickt, fuer immer nur an, und der Worker endete erst per SIGKILL.
    """
    try:
        _signal.pthread_sigmask(_signal.SIG_UNBLOCK, {_signal.SIGINT, _signal.SIGTERM, _signal.SIGHUP})
    except (AttributeError, OSError):  # pragma: no cover - Nicht-POSIX
        pass


def worker_subprocess_entry(
    worker_id: int,
    shutdown_event: Any,  # multiprocessing.synchronize.Event (spawn-Kontext)
    cfg: WorkerConfig,
    channel: SupervisorChannel | None = None,
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

    # Import a public symbol so a rename in
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
    # reload/watch_dirs are supervisor-only state.
    # They ride along on WorkerConfig for pipeline-uniformity (R4.6-H
    # Unit 1), but the worker subprocess must not construct a watcher
    # — that would spawn N reload pipelines, one per worker, each
    # firing SIGHUP at its own PID (default action: terminate). Drop
    # them explicitly so a future developer adding `if ns.reload: ...`
    # inside run_worker_async cannot silently regress.
    kwargs.pop("reload", None)
    kwargs.pop("watch_dirs", None)
    # KTD2: der Slot ist das Einzige, was dieses Kind von seinen Geschwistern
    # unterscheidet — er reist ueber die Namespace in die ``SwarmQConfig``.
    # ``WorkerConfig`` bleibt das geteilte, slot-unabhaengige Objekt.
    kwargs["worker_slot"] = worker_id
    ns = _argparse.Namespace(**kwargs)

    set_current_channel(channel)
    beat_interval = float(cfg.heartbeat_interval_s or 5.0)
    _unblock_inherited_signals()

    async def _run() -> None:
        async def _watch_shutdown() -> None:
            # Poll the cross-process event; when set, raise SIGTERM
            # on ourselves so the Worker's R2.5 graceful-shutdown
            # handler fires naturally.
            # R21/KTD10: das Lebenszeichen kommt aus demselben Tick — ein
            # blockierter Loop schreibt nicht mehr, genau das erkennt der
            # Supervisor. Geschrieben wird alle ``heartbeat_interval_s``,
            # auch waehrend des Drains.
            next_beat = 0.0
            while True:
                now = time.monotonic()
                if channel is not None and now >= next_beat:
                    channel.beat()
                    next_beat = now + beat_interval
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
    except RecycleAckTimeout as exc:
        sys.stderr.write(f"swarmq worker: {exc}\n")
        raise SystemExit(RECYCLE_NOACK_EXIT_CODE) from None
    except ConfigurationError as exc:
        # R29: ein Konfigurationsfehler ist kein Absturz. Ein Neustart
        # aendert nichts, also meldet der Worker ihn mit eigenem Exit-Code
        # und der Supervisor beendet den Lauf statt fuenfmal neu zu starten.
        sys.stderr.write(f"swarmq worker: {exc}\n")
        raise SystemExit(CONFIG_ERROR_EXIT_CODE) from None
