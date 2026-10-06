"""Forkserver-Bootstrap fuer den CPU-Modus (U3: R2, R5, R14, R15, R16, R17, KTD3, KTD4).

Reihenfolge ist Teil des Vertrags: Thread-Limits und MALLOC-Variablen
muessen in der Umgebung des Loop-Prozesses stehen, *bevor* der Forkserver
startet — OpenBLAS liest die Threadzahl beim Laden der Bibliothek, glibc die
Malloc-Tunables beim Prozessstart, und beide passieren im Forkserver, nicht
im Kind (Befund E7, Auflage 12). Der Preload importiert die Task-Module und
darf nie rechnen: hat der Forkserver nach dem Aufwaermen mehr als einen
Thread, ist die Fork-Quelle unsicher und der Start bricht ab (Auflage 1).
"""

from __future__ import annotations

import multiprocessing
import os
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from swarmq.exceptions import ConfigurationError
from swarmq.logging import get_logger

logger = get_logger(__name__)

_PROC_ROOT = Path("/proc")

#: Bibliotheken, die ueber OpenMP/BLAS eigene Threadpools aufspannen (R15).
THREAD_LIMIT_VARS: tuple[str, ...] = (
    "OMP_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "MKL_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
    "MAGICK_THREAD_LIMIT",
)


def _noop() -> None:  # pragma: no cover - laeuft nur im Aufwaerm-Kind
    return None


@dataclass(frozen=True, slots=True)
class ComputeEnvironment:
    """Was der Bootstrap in ``os.environ`` gesetzt hat (fuer das Startlog)."""

    applied: dict[str, str] = field(default_factory=dict)
    kept_from_operator: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class ForkserverHandle:
    """Laufender Forkserver samt Kontext und Pruefergebnis."""

    context: Any  # multiprocessing.context.ForkServerContext
    preload: tuple[str, ...]
    pid: int | None
    threads: int | None
    proc_root: Path = _PROC_ROOT

    def restarted(self) -> bool:
        """KTD4: hat ``ensure_running()`` den Forkserver still neu gestartet?

        Vergleicht die aktuelle Forkserver-PID mit der beim Bootstrap
        gemerkten. Ein Neustart fuehrt den Preload erneut aus, ohne dass die
        Thread-Pruefung wiederholt wurde — der Aufrufer prueft dann nach.
        """
        current = forkserver_pid()
        if current is None or current == self.pid:
            return False
        logger.warning(
            "forkserver_restarted", previous_pid=self.pid, current_pid=current
        )
        self.pid = current
        self.threads = check_forkserver_threads(current, proc_root=self.proc_root)
        return True


def configure_compute_environment(
    *,
    thread_limit: int,
    malloc_trim_threshold: int | None,
    malloc_arena_max: int | None,
    environ: dict[str, str] | os._Environ[str] | None = None,
) -> ComputeEnvironment:
    """R15/R16: Variablen setzen, ohne Betreiber-Werte zu ueberschreiben."""
    env = os.environ if environ is None else environ
    wanted: dict[str, str] = {var: str(thread_limit) for var in THREAD_LIMIT_VARS}
    if malloc_trim_threshold is not None:
        wanted["MALLOC_TRIM_THRESHOLD_"] = str(malloc_trim_threshold)
    if malloc_arena_max is not None:
        wanted["MALLOC_ARENA_MAX"] = str(malloc_arena_max)
    applied: dict[str, str] = {}
    kept: dict[str, str] = {}
    for var, value in wanted.items():
        if var in env and env[var] != "":
            kept[var] = env[var]
        else:
            env[var] = value
            applied[var] = value
    return ComputeEnvironment(applied=applied, kept_from_operator=kept)


def forkserver_pid() -> int | None:
    """PID des laufenden Forkservers — ueber ein privates multiprocessing-Detail.

    KTD14: die stdlib bietet keine oeffentliche API; faellt das Attribut
    weg, liefert die Funktion ``None`` und die Pruefungen degradieren zu
    Warnungen statt zu brechen.
    """
    try:
        from multiprocessing import forkserver as _fs

        pid = _fs._forkserver._forkserver_pid  # type: ignore[attr-defined]
    except (ImportError, AttributeError):
        return None
    return int(pid) if pid is not None else None


def check_forkserver_threads(pid: int, *, proc_root: Path = _PROC_ROOT) -> int | None:
    """``Threads:`` aus ``/proc/<pid>/status``; ``None`` ohne ``/proc``."""
    try:
        text = (proc_root / str(pid) / "status").read_text(encoding="ascii")
    except (OSError, UnicodeDecodeError):
        return None
    for line in text.splitlines():
        if line.startswith("Threads:"):
            try:
                return int(line.split()[1])
            except (IndexError, ValueError):
                return None
    return None


def _require_forkserver_support(available: Sequence[str]) -> None:
    if os.name != "posix" or "forkserver" not in available:
        raise ConfigurationError(
            "--pool cpu requires the multiprocessing 'forkserver' start method, "
            f"which this platform does not offer (available: {sorted(available)})."
        )


def start_forkserver(
    preload: Sequence[str],
    *,
    thread_limit: int = 1,
    malloc_trim_threshold: int | None = None,
    malloc_arena_max: int | None = None,
    context_factory: Any = multiprocessing.get_context,
    available_methods: Sequence[str] | None = None,
    proc_root: Path = _PROC_ROOT,
) -> ForkserverHandle:
    """Umgebung setzen, Forkserver mit Preload starten, aufwaermen, pruefen.

    Reihenfolge: Umgebung (R15/R16) → Kontext + Preload (R17: der Preload
    importiert, der Container entsteht kalt) → Aufwaerm-Fork, der nichts
    rechnet → Thread-Pruefung (R14). Mehr als ein Thread bricht den Start
    mit ``ConfigurationError`` ab (Exit 78 ueber U2); ohne ``/proc`` wird
    die Pruefung mit Warnung uebersprungen (KTD14).
    """
    methods = (
        list(available_methods)
        if available_methods is not None
        else multiprocessing.get_all_start_methods()
    )
    _require_forkserver_support(methods)
    env = configure_compute_environment(
        thread_limit=thread_limit,
        malloc_trim_threshold=malloc_trim_threshold,
        malloc_arena_max=malloc_arena_max,
    )
    ctx = context_factory("forkserver")
    ctx.set_forkserver_preload(list(preload))

    warm = ctx.Process(target=_noop, name="swarmq-forkserver-warmup")
    warm.start()
    warm.join(timeout=30)
    if warm.is_alive():  # pragma: no cover - Haenger im Aufwaermen
        warm.kill()
        raise ConfigurationError(
            "forkserver warm-up child did not finish within 30 s; the preload "
            f"modules {list(preload)!r} probably block at import time."
        )

    pid = forkserver_pid()
    threads = check_forkserver_threads(pid, proc_root=proc_root) if pid is not None else None
    if threads is None:
        logger.warning(
            "forkserver_thread_check_skipped",
            pid=pid,
            hint="no /proc or no forkserver pid available; cannot verify single-threaded preload",
        )
    elif threads != 1:
        raise ConfigurationError(
            f"forkserver has {threads} threads after preloading {list(preload)!r}; "
            "the preload must import but never compute (a warm-up computation "
            "starts BLAS/OpenMP threads and makes the fork source unsafe). "
            "Remove the computation from module import time."
        )
    logger.info(
        "forkserver_started",
        pid=pid,
        threads=threads,
        preload=list(preload),
        env_applied=env.applied,
        env_from_operator=env.kept_from_operator,
    )
    return ForkserverHandle(
        context=ctx, preload=tuple(preload), pid=pid, threads=threads, proc_root=proc_root
    )
