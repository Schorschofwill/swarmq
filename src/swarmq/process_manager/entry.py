"""Subprozess-Entry (Plan U21) — verbatim: ``worker_subprocess_entry``
(läuft im Kind; importiert die CLI lazy, KTD7-Allowlist-Kante)."""

from __future__ import annotations

import dataclasses
import os
import signal as _signal
import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover — import-time only
    pass

from swarmq.exceptions import ConfigurationError
from swarmq.logging import configure_logging, get_logger, shutdown_logging
from swarmq.logging_settings import LoggingSettings
from swarmq.process_manager.config import WorkerConfig
from swarmq.supervision import (
    CONFIG_ERROR_EXIT_CODE as _CONFIG_ERROR_EXIT_CODE,
)
from swarmq.supervision import (
    RECYCLE_NOACK_EXIT_CODE,
    SHUTDOWN_TERM_S,
    RecycleAckTimeout,
    SupervisorChannel,
    set_current_channel,
)

logger = get_logger(__name__)


#: Exit-Code fuer Konfigurationsfehler im Worker-Subprozess (R29, KTD8).
#: Entspricht ``EX_CONFIG`` aus sysexits.h; der Supervisor startet einen so
#: beendeten Worker nicht neu, sondern endet selbst mit 1.
# Re-Export: die Konstante lebt seit U4 in ``supervision`` (KTD8).
CONFIG_ERROR_EXIT_CODE = _CONFIG_ERROR_EXIT_CODE


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


def _logging_settings(cfg: WorkerConfig) -> LoggingSettings:
    """Die aufgeloesten Settings; Bestandsaufrufer ohne sie bekommen das
    eingebaute Logging mit ``log_level``/``log_format`` wie bisher."""
    if cfg.logging_settings is not None:
        return cfg.logging_settings
    return LoggingSettings(level=cfg.log_level, format=cfg.log_format)


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

    # Import a public symbol so a rename in
    # cli.py is detectable via tests rather than a silent subprocess
    # crash at runtime. Spawn re-imports modules fresh — the import
    # executes in the child, not the parent. The win is symbol-
    # stability (public vs private underscore name), not earlier
    # error surface.
    from swarmq.cli import run_worker_async

    # KTD4: Logging als ERSTES und genau einmal einrichten, vor dem
    # App-Import in ``run_worker_async``, aus den im Supervisor aufgeloesten
    # Settings (KTD7) -- das Kind liest kein Env. Auch ein
    # Konfigurationsfehler vor dem Bau der App erscheint so im gewaehlten
    # Format. ``SwarmQ`` richtet danach nichts mehr ein.
    configure_logging(_logging_settings(cfg))

    # Flach statt ``dataclasses.asdict``: das wandelte ``logging_settings``
    # in ein Dict. Namespace callers expect list-typed queues.
    kwargs = {f.name: getattr(cfg, f.name) for f in dataclasses.fields(cfg)}
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
    # KTD4: Der Ersatz eines Slots bindet denselben Port wie sein Vorgaenger.
    # Der haelt ihn bis zum SIGKILL nach SHUTDOWN_TERM_S, ein recycelnder bis
    # zum Ende seines WORKER_SHUTDOWN-Dispatchs (gedeckelt auf
    # _LIFECYCLE_DISPATCH_TIMEOUT_S). Beides plus Reserve, sonst laeuft der
    # Ersatz in einen Bind-Fehler und damit ins Neustart-Budget.
    from swarmq.worker.lifecycle import _LIFECYCLE_DISPATCH_TIMEOUT_S

    kwargs["bind_retry_budget_s"] = max(SHUTDOWN_TERM_S, _LIFECYCLE_DISPATCH_TIMEOUT_S) + 2.0
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
        # U7 (R15): strukturiert im gewaehlten Format statt Rohtext, und vor
        # dem Exit geschrieben.
        logger.error(
            "worker_recycle_ack_timeout",
            worker_id=worker_id,
            error_type=type(exc).__name__,
            error=f"{type(exc).__name__}: {exc}",
        )
        raise SystemExit(RECYCLE_NOACK_EXIT_CODE) from None
    except (ConfigurationError, ModuleNotFoundError) as exc:
        # R29: ein Konfigurationsfehler ist kein Absturz. Ein Neustart
        # aendert nichts, also meldet der Worker ihn mit eigenem Exit-Code
        # und der Supervisor beendet den Lauf statt fuenfmal neu zu starten.
        # R21/U4: als strukturierter Logsatz im konfigurierten Format -- eine
        # rohe stderr-Zeile war in einer JSON-Pipeline eine kaputte Zeile,
        # und zwar genau im Fehlerfall, wo sie gelesen werden muss. Ein
        # ``ModuleNotFoundError`` (etwa eine fehlende Abhaengigkeit des
        # Task-Moduls) gehoert in dieselbe Klasse: er endete bisher mit
        # Traceback und fuenf Neustarts.
        logger.error(
            "worker_config_error",
            source="worker",
            worker_id=worker_id,
            error=f"{type(exc).__name__}: {exc}",
            hint="fix the configuration and start again; no restart attempted",
        )
        raise SystemExit(CONFIG_ERROR_EXIT_CODE) from None
    finally:
        # R15: an jedem Exit-Pfad leeren. Ohne das endet der Prozess, bevor
        # der Listener-Thread die letzten Zeilen geschrieben hat -- der
        # Supervisor saehe im Fehlerfall nur den Exit-Code.
        shutdown_logging()
