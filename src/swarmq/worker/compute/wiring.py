"""Verdrahtung des CPU-Modus in den Worker (U7: R1, R2, R11, R19; KTD1).

Ausgelagert aus ``worker/lifecycle.py`` und ``worker/pipeline.py``, die am
800-Zeilen-Guard stehen. Der Worker ruft drei Hooks: Executor starten (nach
Broker-Connect, vor dem Konsumieren), Executor schliessen (nach dem Drain,
vor dem Broker-Close) und die ``consumer_timeout``-Startwarnung.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import structlog

from swarmq.signals import Signal
from swarmq.worker.compute.executor import ComputeExecutor
from swarmq.worker.compute.forkserver import ForkserverHandle, forkserver_pid, start_forkserver
from swarmq.worker.compute.pool import SlotPool

logger = structlog.get_logger(__name__)

_RECYCLE_DISPATCH_TIMEOUT_S = 5.0  # wie worker.recycling fuer WORKER_RECYCLING des IO-Workers


# ``set_forkserver_preload`` ERSETZT die Standardliste ``["__main__"]`` von multiprocessing.
# Fehlt ``__main__``, importiert jedes frische Kind das Hauptmodul des Loop-Prozesses samt
# aio_pika/valkey neu (gemessen ~45 ms je Kind); fehlt die Kind-Laufzeit, kommen child.py,
# envelope.py und swarmq.logging je Kind dazu (~25 ms). Mit beidem im Preload liegt Spawn
# bis Ready bei ~8 ms (Messnachweis M0, docs/cpu-worker-machbarkeit.md). ``__main__`` wird
# wie bei multiprocessing ueblich als ``__mp_main__`` importiert, der ``if __name__``-Guard
# des Hauptmoduls greift; alle Module rechnen beim Import nichts (R14/R17).
_RUNTIME_PRELOAD = ("swarmq.logging", "swarmq.worker.compute.child")


def _main_module_name() -> str:
    """Name, unter dem der Forkserver das Hauptmodul vorab laedt.

    Ein per Datei gestartetes Programm (Konsolenskript, Supervisor-Kind) kommt als
    ``"__main__"`` und wird ueber den Pfad importiert. Bei ``python -m paket.modul``
    laedt jedes Kind das Hauptmodul dagegen ueber seinen Namen nach — also den
    echten Namen vorab laden. ``paket.__main__`` laedt multiprocessing nie nach.
    """
    spec = getattr(sys.modules.get("__main__"), "__spec__", None)
    name = getattr(spec, "name", None)
    if not name or name == "__main__" or name.endswith(".__main__"):
        return "__main__"
    return str(name)


def preload_modules(registry: Any) -> list[str]:
    """R17: Hauptmodul, Kind-Laufzeit und die Module der registrierten Tasks sind der Forkserver-Preload."""
    fixed = (_main_module_name(), *_RUNTIME_PRELOAD)
    modules = {cls.__module__ for cls in registry.get_all().values()}
    return [*fixed, *sorted(m for m in modules if m and m != "__main__" and m not in fixed)]


async def start_compute_executor(worker: Any) -> ComputeExecutor | None:
    """Forkserver + SlotPool + ComputeExecutor aufbauen und einhaengen (KTD1)."""
    cfg = worker._config
    if cfg.pool != "cpu":
        return None
    preload = preload_modules(worker._registry)
    handle: ForkserverHandle = start_forkserver(
        preload,
        thread_limit=cfg.compute_thread_limit,
        malloc_trim_threshold=cfg.malloc_trim_threshold,
        malloc_arena_max=cfg.malloc_arena_max,
    )
    worker_id = str(id(worker))

    async def on_recycle(slot: int, reason: str, value: float) -> None:
        await worker._signal_dispatcher.dispatch(
            Signal.WORKER_RECYCLING,
            worker_id=worker_id, kind="compute_child", slot=slot, reason=reason, value=value,
            handler_timeout=_RECYCLE_DISPATCH_TIMEOUT_S,  # ein haengender Handler darf den Pool nicht aufhalten
        )

    pool = SlotPool(
        handle.context,
        cfg.concurrency,
        recycle_rss_mb=cfg.recycle_rss_mb,
        recycle_max_tasks=cfg.recycle_max_tasks,
        end_grace_s=float(cfg.shutdown_timeout),
        on_recycle=on_recycle,
        restart_check=handle.restarted,
    )
    executor = ComputeExecutor(worker._gate_deps, pool, worker_id=worker_id)
    pool.set_progress_sink(executor.progress_sink)
    worker._compute_handle = handle
    worker._executor = executor
    await executor.start()
    logger.info(
        "compute_pool_started",
        slots=cfg.concurrency,
        recycle_rss_mb=cfg.recycle_rss_mb,
        recycle_max_tasks=cfg.recycle_max_tasks,
        forkserver_pid=handle.pid,
        preload=preload,
    )
    return executor


async def close_compute_executor(worker: Any) -> None:
    executor = getattr(worker, "_executor", None)
    if isinstance(executor, ComputeExecutor):
        await executor.aclose()


def warn_hard_timeout_over_consumer_timeout(registry: Any, consumer_timeout_s: float) -> None:
    """R19: ``hard_timeout`` ueber dem Broker-``consumer_timeout`` verliert die Zustellung."""
    offenders = [
        f"{name}({ti.hard_timeout}s)"
        for name, cls in registry.get_all().items()
        if (ti := getattr(cls, "task_info", None)) is not None
        and ti.hard_timeout is not None
        and ti.hard_timeout > consumer_timeout_s
    ]
    if offenders:
        logger.warning(
            "hard_timeout_exceeds_consumer_timeout",
            tasks=offenders[:20],
            consumer_timeout_s=consumer_timeout_s,
            hint=(
                "RabbitMQ closes the channel when a delivery stays unacked longer "
                "than consumer_timeout; raise the broker's consumer_timeout (and "
                "SWARMQ_CONSUMER_TIMEOUT_S) or lower hard_timeout."
            ),
        )


async def forkserver_ready(worker: Any, proc_root: Path = Path("/proc")) -> tuple[bool, str | None]:
    """``/ready`` im CPU-Modus: der Forkserver lebt (KTD4, M5)."""
    handle = getattr(worker, "_compute_handle", None)
    if handle is None:
        return False, "compute pool not started"
    if not getattr(getattr(worker, "_executor", None), "pool_healthy", True):
        return False, "compute pool unhealthy: repeated ready-handshake failures or every slot blocked"
    pid = forkserver_pid()
    if pid is None:
        return True, None  # nicht pruefbar (KTD14) — nicht als Ausfall melden
    if (proc_root / str(pid)).exists():
        return True, None
    return False, "forkserver not running"


__all__ = [
    "close_compute_executor",
    "forkserver_ready",
    "preload_modules",
    "start_compute_executor",
    "warn_hard_timeout_over_consumer_timeout",
]
