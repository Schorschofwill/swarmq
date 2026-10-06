"""Worker-Konfiguration (Plan U21) — verbatim: ``WorkerConfig`` mit
Pickle-Safety-Validierung (Subprozess-Übergabe)."""

from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover — import-time only
    pass

import structlog

from swarmq.logging_settings import VALID_LOG_LEVELS as _VALID_LOG_LEVELS
from swarmq.logging_settings import LoggingSettings

logger = structlog.get_logger()


# ``LoggingSettings`` ist selbst eingefroren und besteht nur aus diesen
# Typen; die geparste dictConfig reist darin als JSON-String (KTD9).
_PICKLE_SAFE_TYPES = (str, int, float, bool, tuple, type(None), LoggingSettings)


@dataclasses.dataclass(frozen=True)
class WorkerConfig:
    """Pickle-safe configuration bundle for a spawned worker.

    Crosses the multiprocessing spawn boundary as the sole payload
    inside ``worker_args``. Collapses the former 11-parameter
    :func:`worker_subprocess_entry` signature into one object so a
    new CLI flag is a single-site edit (add the field, set it in
    :func:`run_multi_process_worker`).

    ``queues`` is ``tuple[str, ...]`` — not ``list[str]`` — so the
    ``frozen=True`` immutability promise is not violated by in-place
    list mutation. ``__post_init__`` runtime-enforces a pickle-safe
    type allowlist so the invariant survives future programmatic
    callers that bypass argparse validation.

    The allowlist check is shallow: tuple/list field contents are
    not recursively type-checked. In production that is fine because
    ``queues`` is always built from argparse's ``list[str]`` output
    via ``tuple(args.queues)``. A future programmatic caller passing
    ``queues=(lambda: None,)`` would still fail, but at
    ``pickle.dumps`` time, not at construction.
    """

    module: str
    queues: tuple[str, ...]
    concurrency: int
    log_level: str
    log_format: str
    burst: bool
    burst_idle_timeout: float
    priorities: bool
    metrics_port: int | None
    health_port: int | None
    # R5.3: pipeline-uniform fields. Watcher lives in the supervisor
    # (not the worker subprocess), but routing them through WorkerConfig
    # avoids the supervisor-only-flag drift that R4.6-H Unit 1 removed.
    reload: bool = False
    watch_dirs: tuple[str, ...] = ()
    # Plan 2026-09-03-001: Pool-Modus und Schwellen; ``None`` heisst
    # "SwarmQConfig-Voreinstellung", damit Bestandsaufrufer unveraendert bleiben.
    pool: str = "io"
    recycle_rss_mb: int | None = None
    recycle_max_tasks: int | None = None
    heartbeat_interval_s: float | None = None
    heartbeat_timeout_s: float | None = None
    consumer_timeout_s: float | None = None
    malloc_trim_threshold: int | None = None
    malloc_arena_max: int | None = None
    compute_thread_limit: int | None = None
    # U5 (Plan 2026-09-08-001, R9/R10): ``MODULE:CALLABLE``. Der String reist
    # als Feld ins Kind und wird erst dort aufgeloest -- der Supervisor
    # importiert kein Anwendungsmodul (KTD5).
    configure: str | None = None
    # Kontrollkanal (Plan 2026-09-16, R1/R5): der erklaerte Spielraum schaltet
    # den Kanal ein; ``group`` ist der fertig gebildete Gruppenschluessel.
    max_processes: int | None = None
    group: str | None = None
    # Plan 2026-10-06 (KTD7): die einmal im Supervisor aufgeloesten
    # Logging-Einstellungen; Kinder lesen kein Env mehr. ``None`` haelt
    # Bestandsaufrufer unveraendert, bis die Einstiegspunkte sie setzen (U7).
    logging_settings: LoggingSettings | None = None

    def __post_init__(self) -> None:
        for f in dataclasses.fields(self):
            value = getattr(self, f.name)
            if not isinstance(value, _PICKLE_SAFE_TYPES):
                raise TypeError(
                    f"WorkerConfig.{f.name} must be pickle-safe; "
                    f"got {type(value).__name__}",
                )
        if self.log_level not in _VALID_LOG_LEVELS:
            raise ValueError(
                f"WorkerConfig.log_level must be one of "
                f"{_VALID_LOG_LEVELS}; got {self.log_level!r}",
            )
