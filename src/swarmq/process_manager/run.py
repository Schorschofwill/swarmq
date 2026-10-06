"""Einstieg (Plan U21) — verbatim: ``run_multi_process_worker`` und die
konfigurierte Variante (CLI reicht die Sampler-Factory durch)."""

from __future__ import annotations

import argparse
import os
import socket
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover — import-time only
    pass

from collections.abc import Callable

from swarmq.exceptions import ConfigurationError, LoggingConfigError
from swarmq.logging import _error_fields, get_logger, load_log_config
from swarmq.logging_file import probe_dict_config
from swarmq.logging_settings import LoggingSettings, resolve_logging_settings
from swarmq.process_manager.check import check_worker_config
from swarmq.process_manager.config import WorkerConfig
from swarmq.process_manager.entry import worker_subprocess_entry
from swarmq.process_manager.sampler import AdminFactory, _make_queue_depth_sampler
from swarmq.process_manager.setpoint_store import SetpointStoreFactory
from swarmq.process_manager.supervisor import ProcessManager, QueueDepthFn

logger = get_logger(__name__)


def run_multi_process_worker(
    args: argparse.Namespace,
    *,
    admin_factory: AdminFactory | None = None,
    setpoint_store_factory: SetpointStoreFactory | None = None,
    logging_settings: LoggingSettings | None = None,
) -> int:
    """CLI entry for ``swarmq worker --processes N`` / ``--autoscale``.

    Called from :func:`swarmq.cli.main` when the user opted into the
    multi-process path. Synchronous (the supervisor runs in the
    parent process without an event loop).

    ``logging_settings`` sind die von der CLI einmal aufgeloesten
    Einstellungen (Plan 2026-10-06-0831, KTD7); ohne sie loest diese
    Funktion sie selbst aus ``args`` und Env auf.
    """
    broker_url = os.environ.get("SWARMQ_BROKER_URL", "")
    backend_url = os.environ.get("SWARMQ_BACKEND_URL", "")
    if not broker_url or not backend_url:
        raise ConfigurationError(
            "SWARMQ_BROKER_URL and SWARMQ_BACKEND_URL must both be set",
        )

    # Das Logging konfiguriert seit U4 der Worker-Zweig der CLI, bevor
    # irgendetwas den Fehler werfen kann (KTD8) -- und er flusht es auch.
    # Vorher stoppte diese Funktion ihren Listener im ``finally``: ein
    # Konfigurationsfehler, den der Aufrufer danach loggte, blieb in der
    # Queue eines gestoppten Listeners liegen und erreichte niemanden.
    return _run_multi_process_worker_configured(
        args, admin_factory=admin_factory, setpoint_store_factory=setpoint_store_factory,
        logging_settings=logging_settings or resolve_logging_settings(args, os.environ, None),
    )


def log_config_reload_check(settings: LoggingSettings) -> Callable[[], bool] | None:
    """Pruefung vor einem Hot-Reload (Plan 2026-10-06-0831, U7, KTD9).

    Der Reload ist ein ``execvpe`` des Supervisors, der ``--log-config`` dann
    neu laedt. Ist die Datei inzwischen ungueltig, bricht der Reload ab, bevor
    ein Worker stoppt: ``log_config_reload_failed`` statt Exit 78.
    """
    path = settings.log_config
    if path is None:
        return None

    def _check() -> bool:
        try:
            probe_dict_config(path, load_log_config(path))
        except LoggingConfigError as exc:
            logger.error(
                "log_config_reload_failed",
                path=path,
                **_error_fields(exc),
                hint="fix the logging config and send SIGHUP again; workers keep running",
            )
            return False
        return True

    return _check


def resolve_group_key(label: str | None, queues: list[str] | tuple[str, ...]) -> str:
    """Gruppenschluessel des Kontrollkanals (R5, KD4): das Label, ersatzweise
    die sortierte Queue-Menge. Nur innerhalb eines Backends eindeutig."""
    if label:
        return label
    return ",".join(sorted(set(queues)))


def _run_multi_process_worker_configured(
    args: argparse.Namespace,
    *,
    admin_factory: AdminFactory | None = None,
    setpoint_store_factory: SetpointStoreFactory | None = None,
    logging_settings: LoggingSettings,
) -> int:
    """Body of :func:`run_multi_process_worker` after logging setup."""
    broker_url = os.environ["SWARMQ_BROKER_URL"]
    backend_url = os.environ["SWARMQ_BACKEND_URL"]

    reload_enabled = bool(getattr(args, "reload", False))
    watch_dirs_raw = getattr(args, "watch_dir", None) or ()
    max_processes: int | None = getattr(args, "max_processes", None)
    group_key = resolve_group_key(getattr(args, "group", None), args.queues)

    # R5.3: --watch-dir without --reload is almost certainly an operator
    # typo. Warn (don't error) so the worker still starts, but surface
    # the ignored list so it's not silently dropped.
    if watch_dirs_raw and not reload_enabled:
        logger.warning(
            "watch_dir_ignored_without_reload",
            watch_dirs=list(watch_dirs_raw),
        )

    # Single pre-flight via the public
    # validator; check_worker_config runs the same sequence (find_spec,
    # watchfiles availability, watch-dir resolution) that
    # `swarmq worker --check` uses. Fails fast on any misconfiguration
    # before the supervisor starts spawning subprocesses.
    check_result = check_worker_config(
        module=args.module,
        reload=reload_enabled,
        watch_dir=list(watch_dirs_raw) or None,
        burst=bool(getattr(args, "burst", False)),
        # Der Port-Guard (R5) braucht die angeforderte Topologie; ohne diese
        # Argumente liefe er mit seinen Defaults und griffe nie.
        processes=int(getattr(args, "processes", 1) or 1),
        autoscale=args.autoscale,
        recycling=bool(os.environ.get("SWARMQ_RECYCLE_RSS_MB") or os.environ.get("SWARMQ_RECYCLE_MAX_TASKS")),
        metrics_port=args.metrics_port,
        health_port=args.health_port,
        # Kontrollkanal (U3): der Spielraum muss Startprozesszahl und
        # Autoscale-Obergrenze decken; im Rechen-Pool zaehlt nur eine
        # ausdrueckliche Nebenlaeufigkeit gegen die CPU-Zuteilung.
        pool=getattr(args, "pool", "io") or "io",
        concurrency=args.concurrency if getattr(args, "concurrency_source", "explicit") == "explicit" else None,
        max_processes=max_processes,
    )
    for warning in check_result["warnings"] or []:
        logger.warning("worker_config_warning", warning=warning)

    resolved_watch_dirs: tuple[str, ...] = tuple(
        check_result["watch_dirs"] or (),
    )

    queue_depth_fn: QueueDepthFn | None = None
    if args.autoscale is not None:
        if admin_factory is None:
            # U21 (KTD6): die Broker-Implementierung kommt als Factory aus
            # der CLI; ohne Factory kann der Autoscale-Sampler nicht messen.
            raise ConfigurationError(
                "--autoscale requires a broker admin factory (CLI wiring)"
            )
        queue_depth_fn = _make_queue_depth_sampler(admin_factory, list(args.queues))

    # Resolve SwarmQConfig.shutdown_timeout once in
    # the parent and pass it as the phase-1 grace so a subprocess's
    # graceful-shutdown flow has the budget it was configured for.
    from swarmq.config import SwarmQConfig
    # Mit den aufgeloesten Settings ueberspringt SwarmQConfig die Logging-Env:
    # ein ungueltiges SWARMQ_LOG_FORMAT, das ``--log-format`` schon
    # ueberstimmt hat, braeche sonst hier mit 78 ab.
    sq_config = SwarmQConfig(
        broker_url=broker_url, backend_url=backend_url, logging_settings=logging_settings,
    )
    shutdown_grace_s = float(sq_config.shutdown_timeout)

    worker_cfg = WorkerConfig(
        module=args.module,
        queues=tuple(args.queues),
        concurrency=args.concurrency,
        log_level=logging_settings.level,
        log_format=logging_settings.format,
        burst=args.burst,
        burst_idle_timeout=args.burst_idle_timeout,
        priorities=args.priorities,
        metrics_port=args.metrics_port,
        health_port=args.health_port,
        reload=reload_enabled,
        watch_dirs=resolved_watch_dirs,
        pool=getattr(args, "pool", "io") or "io",
        heartbeat_interval_s=sq_config.heartbeat_interval_s,
        configure=getattr(args, "configure", None),
        max_processes=max_processes,
        group=group_key,
        logging_settings=logging_settings,
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
        heartbeat_timeout_s=sq_config.heartbeat_timeout_s,
        delivery_limit=sq_config.delivery_limit,
        max_processes=max_processes,
        group=group_key,
        # Kontrollkanal (R2): ohne Spielraum keine Fabrik, also kein
        # Backend-Zugriff beim Start und kein neuer Fehlerfall.
        setpoint_store_factory=setpoint_store_factory if max_processes is not None else None,
        supervisor_id=f"{sq_config.worker_id or socket.gethostname()}:p{os.getpid()}",
    )
    pm.reload_precheck = log_config_reload_check(logging_settings)
    return pm.run()
