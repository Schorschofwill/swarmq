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
import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

from swarmq.exceptions import ConfigurationError
from swarmq.logging import LISTENER_THREAD_NAME, get_logger

logger = get_logger(__name__)

_PROC_ROOT = Path("/proc")

#: Erstes Preload-Modul (U8, Plan 2026-10-06-1811 U6/KTD6): markiert den
#: Forkserver als Fork-Quelle, bevor ein App-Modul importiert wird.
PRELOAD_MODULE = "swarmq.worker.compute.preload"

#: Umgebung des Forkservers, gesetzt von ``wiring.prepare_forkserver_env``,
#: gelesen von ``preload`` -- die Umgebung ist der einzige Weg dorthin.
#: Name des per ``python -m`` gestarteten Hauptmoduls (U8, KTD10).
MAIN_MODULE_ENV = "SWARMQ_MAIN_MODULE"
#: ``"1"``, wenn swarmq das Logging der Kinder konfiguriert (Plan
#: 2026-10-06-1811, U9, KTD9): Dann haengt der Preload den Sentinel an Root.
SENTINEL_ENV = "SWARMQ_FORKSERVER_LOG_SENTINEL"

#: Bibliotheken, die ueber OpenMP/BLAS eigene Threadpools aufspannen (R15).
THREAD_LIMIT_VARS: tuple[str, ...] = (
    "OMP_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "MKL_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
    "MAGICK_THREAD_LIMIT",
)


class ForkSourceUnsafe(RuntimeError):
    """Ein still neu gestarteter Forkserver hat mehr als einen Thread
    (Plan 2026-10-06-1811, KTD7): Der Pool verwirft diese Kindquelle."""


def _noop() -> None:  # pragma: no cover - laeuft nur im Aufwaerm-Kind
    return None


@dataclass(frozen=True, slots=True)
class ComputeEnvironment:
    """Was der Bootstrap in ``os.environ`` gesetzt hat (fuer das Startlog)."""

    applied: dict[str, str] = field(default_factory=dict)
    kept_from_operator: dict[str, str] = field(default_factory=dict)


def ensure_forkserver_running() -> None:
    """Startet einen toten Forkserver neu -- wie es ``Process.start`` sonst
    still taete (``multiprocessing.forkserver.ensure_running``)."""
    from multiprocessing import forkserver as _fs

    _fs.ensure_running()


@dataclass(slots=True)
class ForkserverHandle:
    """Laufender Forkserver samt Kontext und Pruefergebnis."""

    context: Any  # multiprocessing.context.ForkServerContext
    preload: tuple[str, ...]
    pid: int | None
    threads: int | None
    proc_root: Path = _PROC_ROOT
    ensure_running: Callable[[], None] = ensure_forkserver_running

    def restarted(self) -> bool:
        """KTD4: hat ``ensure_running()`` den Forkserver still neu gestartet?

        Startet einen toten Forkserver selbst neu, bevor der Pool das
        naechste Kind forkt (Schluss-Review): ``Process.start`` holte das
        sonst still nach, und das erste Kind des neuen Forkservers liefe
        ungeprueft. Vergleicht dann die aktuelle Forkserver-PID mit der beim
        Bootstrap gemerkten. Ein Neustart fuehrt den Preload erneut aus; die
        Thread-Pruefung laeuft dann erneut (Plan 2026-10-06-1811, KTD7).
        Mehr als ein Thread: ``forkserver_restarted_multithreaded`` und
        ``ForkSourceUnsafe``.
        """
        self.ensure_running()
        current = forkserver_pid()
        if current is None or current == self.pid:
            return False
        logger.warning("forkserver_restarted", previous_pid=self.pid, current_pid=current)
        self.pid = current
        self.threads = threads = check_forkserver_threads(current, proc_root=self.proc_root)
        if threads is not None and threads != 1:
            names = forkserver_thread_names(current, proc_root=self.proc_root)
            hint = _thread_hint(names)
            logger.error(
                "forkserver_restarted_multithreaded",
                pid=current,
                threads=threads,
                thread_names=names,
                hint=hint,
            )
            raise ForkSourceUnsafe(
                f"restarted forkserver has {threads} threads {names!r} after preloading {list(self.preload)!r}; {hint}"
            )
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

        pid = cast("int | None", _fs._forkserver._forkserver_pid)  # type: ignore[attr-defined]  # pyright: ignore[reportAttributeAccessIssue]  # privates stdlib-Detail (KTD14)
    except ImportError, AttributeError:
        return None
    return int(pid) if pid is not None else None


def check_forkserver_threads(pid: int, *, proc_root: Path = _PROC_ROOT) -> int | None:
    """``Threads:`` aus ``/proc/<pid>/status``; ``None`` ohne ``/proc``."""
    try:
        text = (proc_root / str(pid) / "status").read_text(encoding="ascii")
    except OSError, UnicodeDecodeError:
        return None
    for line in text.splitlines():
        if line.startswith("Threads:"):
            try:
                return int(line.split()[1])
            except IndexError, ValueError:
                return None
    return None


def forkserver_thread_names(pid: int, *, proc_root: Path = _PROC_ROOT) -> list[str]:
    """Thread-Namen aus ``/proc/<pid>/task/*/comm`` (auf 15 Zeichen
    gekuerzt); leer ohne ``/proc`` (Plan 2026-10-06-1811, KTD7)."""
    try:
        tasks = sorted((proc_root / str(pid) / "task").iterdir(), key=lambda p: p.name)
    except OSError:
        return []
    names: list[str] = []
    for task in tasks:
        try:
            names.append((task / "comm").read_text(encoding="utf-8").strip())
        except OSError, UnicodeDecodeError:
            continue
    return names


_HINT_OPTIN = (
    f"the swarmq opt-in log queue started its listener thread ({LISTENER_THREAD_NAME!r}): "
    "a swarmq.* record was logged at import time while SwarmQ(log_queue=True) or "
    "SWARMQ_LOG_QUEUE=1 was active. Start the worker with the swarmq command or "
    "'python -m', where the forkserver preload keeps that record synchronous, "
    "or log after startup instead of at import time."
)
_HINT_APP_QUEUE_LISTENER = (
    "an application logging QueueListener was started at import time (its "
    "thread runs QueueListener._monitor). Start it lazily or in the "
    "--configure hook, which runs in the worker process, not in the forkserver."
)
_HINT_BLAS = (
    "the preload must import but never compute (a warm-up computation "
    "starts BLAS/OpenMP threads and makes the fork source unsafe). "
    "Remove the computation from module import time."
)


#: Standardname eines stdlib-``QueueListener``-Threads, ``Thread-N (_monitor)``,
#: in ``comm`` auf 15 Zeichen gekuerzt.
_STDLIB_LISTENER = re.compile(r"Thread-\d+ \((?P<target>_m.*)")


def _is_stdlib_queue_listener(name: str) -> bool:
    match = _STDLIB_LISTENER.fullmatch(name)
    return match is not None and "_monitor)".startswith(match["target"])


def _thread_hint(names: Sequence[str]) -> str:
    """Hinweis nach dem Verursacher der zusaetzlichen Threads (KTD7)."""
    if LISTENER_THREAD_NAME in names:
        return _HINT_OPTIN
    if any(_is_stdlib_queue_listener(name) for name in names):
        return _HINT_APP_QUEUE_LISTENER
    return _HINT_BLAS


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
    ensure_running: Callable[[], None] = ensure_forkserver_running,
) -> ForkserverHandle:
    """Umgebung setzen, Forkserver mit Preload starten, aufwaermen, pruefen.

    Reihenfolge: Umgebung (R15/R16) → Kontext + Preload (R17: der Preload
    importiert, der Container entsteht kalt) → Aufwaerm-Fork, der nichts
    rechnet → Thread-Pruefung (R14). Mehr als ein Thread bricht den Start
    mit ``ConfigurationError`` ab (Exit 78 ueber U2); ohne ``/proc`` wird
    die Pruefung mit Warnung uebersprungen (KTD14).
    """
    methods = list(available_methods) if available_methods is not None else multiprocessing.get_all_start_methods()
    _require_forkserver_support(methods)
    env = configure_compute_environment(
        thread_limit=thread_limit,
        malloc_trim_threshold=malloc_trim_threshold,
        malloc_arena_max=malloc_arena_max,
    )
    # KTD6: das Preload-Modul zuerst, auch fuer Aufrufer ohne es.
    preload = [PRELOAD_MODULE, *(m for m in preload if m != PRELOAD_MODULE)]
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
        assert pid is not None
        names = forkserver_thread_names(pid, proc_root=proc_root)
        raise ConfigurationError(
            f"forkserver has {threads} threads {names!r} after preloading {list(preload)!r}; {_thread_hint(names)}"
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
        context=ctx,
        preload=tuple(preload),
        pid=pid,
        threads=threads,
        proc_root=proc_root,
        ensure_running=ensure_running,
    )
