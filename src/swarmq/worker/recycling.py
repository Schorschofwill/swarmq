"""Selbst-Recycling des IO-Workers (Plan 2026-09-03-001, R22/R31, KTD11, KTD15).

Ausgelagert aus ``worker/lifecycle.py`` (800-Zeilen-Guard). Der Worker zaehlt
terminale Task-Signale, misst nach jedem Taskende seinen RSS und meldet bei
Schwellenueberschreitung die Recycling-Absicht ueber den Supervisor-Kanal.
Erst nach der Bestaetigung (Ersatz gestartet) drainiert er — ohne erzwungenen
Abbruch, mit Frist aus dem groessten ``hard_timeout``. Ohne Bestaetigung
endet er mit Exit ungleich 0 (``RecycleAckTimeout``), ohne Supervisor mit
Exit 0 fuer den aeusseren Prozessmanager (R31).
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from typing import Any

from swarmq.logging import get_logger
from swarmq.signals import Signal
from swarmq.supervision import current_channel
from swarmq.worker.compute.child import rss_mb

logger = get_logger(__name__)

RECYCLE_ACK_TIMEOUT_S = 10.0
RECYCLE_DRAIN_BUFFER_S = 30.0
_DISPATCH_TIMEOUT_S = 5.0
_TERMINAL_SIGNALS = (Signal.TASK_COMPLETED, Signal.TASK_FAILED, Signal.TASK_CANCELLED)


def recycling_enabled(config: Any) -> bool:
    return config.pool == "io" and (config.recycle_rss_mb is not None or config.recycle_max_tasks is not None)


def start_recycle_monitor(worker: Any) -> None:
    """Zaehler und Monitor-Task anlegen; No-op ohne gesetzte Schwellen."""
    worker._tasks_done = 0
    worker._recycle_ack_missing = False
    worker._drain_timeout_override = None
    worker._recycle_monitor_task = None
    worker._recycle_started_names = set()
    if not recycling_enabled(worker._config):
        return
    if current_channel() is None:
        logger.warning(
            "recycling_without_supervisor",
            hint="recycle thresholds are set but no supervisor channel exists; "
            "a recycling ends this process with exit 0 for the outer process manager",
        )

    async def _count(**_kw: Any) -> None:
        worker._tasks_done += 1

    async def _note_started(*, task_name: str, **_kw: Any) -> None:
        # Ein per ``schedule(queue=...)`` umgeleiteter Task laeuft hier, ohne
        # dass seine deklarierten Queues konsumiert werden (FS-30, Review U9).
        worker._recycle_started_names.add(task_name)

    for terminal in _TERMINAL_SIGNALS:
        worker._signal_dispatcher.on_signal(terminal)(_count)
    worker._signal_dispatcher.on_signal(Signal.TASK_STARTED)(_note_started)
    worker._recycle_monitor_task = asyncio.create_task(recycle_monitor(worker))


async def recycle_monitor(worker: Any) -> None:
    """Nach jedem Taskende RSS und Taskzahl gegen die Schwellen pruefen (R22)."""
    cfg = worker._config
    last_seen = 0
    while not worker._shutdown_event.is_set():
        await asyncio.sleep(0.25)
        if worker._shutdown_event.is_set():
            return  # SIGTERM im Schlaf: kein Recycling (sonst unnoetiger Ersatz)
        if worker._tasks_done == last_seen:
            continue
        last_seen = worker._tasks_done
        rss = rss_mb()
        if cfg.recycle_rss_mb is not None and rss is not None and rss > cfg.recycle_rss_mb:
            await worker._request_recycle("rss", rss)
            return
        if cfg.recycle_max_tasks is not None and worker._tasks_done >= cfg.recycle_max_tasks:
            await worker._request_recycle("tasks", float(worker._tasks_done))
            return


def recycle_drain_timeout(worker: Any) -> float:
    """Frist des Recycling-Drains (R22, FS-30, KTD4 Plan 2026-10-14-0748).

    Groesster ``hard_timeout`` aller registrierten Tasks plus Puffer -- auch
    auf fremden Queues, weil ``schedule(queue=...)`` jeden Task auf eine
    konsumierte Queue umleiten kann. Kann hier ein Task ohne ``hard_timeout``
    laufen (deklariert auf ``consumed_queues`` oder hier gestartet), wartet
    der Drain mindestens ``consumer_timeout_s`` -- danach hat RabbitMQ jede
    vor dem Recycling zugestellte Nachricht ohnehin zurueckgenommen. Sonst
    bleibt es bei ``shutdown_timeout``.
    """
    consumed = set(worker._config.consumed_queues)
    started: set[str] = getattr(worker, "_recycle_started_names", set())
    infos = [ti for cls in worker._registry.get_all().values() if (ti := getattr(cls, "task_info", None)) is not None]
    hard_timeouts = [ti.hard_timeout for ti in infos if ti.hard_timeout is not None]
    timeout = float(max(hard_timeouts)) + RECYCLE_DRAIN_BUFFER_S if hard_timeouts else None
    if any(ti.hard_timeout is None and (consumed.intersection(ti.queues) or ti.name in started) for ti in infos):
        timeout = max(timeout or 0.0, float(worker._config.consumer_timeout_s))
    return float(worker._config.shutdown_timeout) if timeout is None else timeout


async def request_recycle(worker: Any, reason: str, value: float) -> None:
    """Absicht melden, Bestaetigung abwarten, dann graceful drainen (KTD11)."""
    logger.info("worker_recycling", reason=reason, value=round(value, 1), tasks_done=worker._tasks_done)
    await worker._signal_dispatcher.dispatch(
        Signal.WORKER_RECYCLING,
        handler_timeout=_DISPATCH_TIMEOUT_S,
        worker_id=worker._config.worker_identity,
        kind="worker",
        slot=None,
        reason=reason,
        value=value,
    )
    channel = current_channel()
    if channel is None:
        logger.warning("recycle_without_supervisor_exit0", reason=reason)
    else:
        channel.request_recycle()
        deadline = time.monotonic() + RECYCLE_ACK_TIMEOUT_S
        while not channel.replacement_acknowledged():
            if worker._shutdown_event.is_set():
                break  # SIGTERM: ein Supervisor im Abbau bestaetigt nie (kein Exit 75)
            if time.monotonic() >= deadline:
                worker._recycle_ack_missing = True
                logger.error("recycle_ack_missing", timeout_s=RECYCLE_ACK_TIMEOUT_S)
                break
            await asyncio.sleep(0.1)
    if worker._shutdown_event.is_set():
        # SIGTERM kam waehrend der Bestaetigung: es gilt das Shutdown-Budget,
        # kein Recycling-Drain (FS-30, KTD4).
        logger.info("worker_recycling_superseded_by_shutdown", reason=reason)
        return
    worker._drain_timeout_override = recycle_drain_timeout(worker)
    worker._request_shutdown()


async def start_with_bind_retry(start: Callable[[], Awaitable[None]], *, budget_s: float = 5.0) -> None:
    """KTD15: beim rollierenden Recycling haelt der alte Worker den Port noch kurz."""
    deadline = time.monotonic() + budget_s
    while True:
        try:
            await start()
        except RuntimeError as exc:
            if "bind failed" not in str(exc) or time.monotonic() >= deadline:
                raise
            await asyncio.sleep(0.25)
        else:
            return


__all__ = [
    "RECYCLE_ACK_TIMEOUT_S",
    "RECYCLE_DRAIN_BUFFER_S",
    "recycle_drain_timeout",
    "recycle_monitor",
    "recycling_enabled",
    "request_recycle",
    "start_recycle_monitor",
    "start_with_bind_retry",
]
