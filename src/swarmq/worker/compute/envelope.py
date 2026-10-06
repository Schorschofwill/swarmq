"""Job- und Ergebnis-Envelope zwischen Loop-Prozess und Rechenkind (U4/U6, KTD5).

Zum Kind reist nur, was der Task-Koerper braucht — nie die aio_pika-Nachricht.
Zurueck kommt entweder der rohe Rueckgabewert (der Loop-Prozess kodiert ihn
wie heute im Result-Tail, so dass ein nicht serialisierbarer Wert genau wie
heute als gespeichertes Fehlerergebnis ohne Retry landet) oder eine
Exception: picklebar unveraendert, sonst als Modul, Qualname, Text und
Traceback, die der Loop-Prozess per Import rekonstruiert, damit ``Retry``,
``NoRetry`` und ``retry_exceptions`` ihre Wirkung behalten.
"""

from __future__ import annotations

import importlib
import pickle
import traceback
from dataclasses import dataclass
from typing import Any


class RemoteTaskError(RuntimeError):
    """Eine Exception aus dem Rechenkind, deren Typ sich nicht rekonstruieren liess."""

    def __init__(self, message: str, *, remote_type: str) -> None:
        super().__init__(message)
        self.remote_type = remote_type


class UnpicklableResult:
    """Platzhalter fuer einen Rueckgabewert, der die Prozessgrenze nicht ueberquert.

    Absichtlich nicht JSON-kodierbar: der Result-Tail des Loop-Prozesses
    faellt damit in denselben ``ResultSerializationError``-Pfad wie bei einem
    nicht serialisierbaren Wert im IO-Modus — gespeichertes Fehlerergebnis,
    kein Retry.
    """

    __slots__ = ("reason",)

    def __init__(self, reason: str) -> None:
        self.reason = reason

    def __repr__(self) -> str:
        return f"UnpicklableResult({self.reason!r})"


@dataclass(frozen=True, slots=True)
class ComputeJob:
    """Minimaler Auftrag an ein Rechenkind."""

    task_id: str
    task_name: str
    queue: str
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    retry_count: int = 0
    result_ttl_ms: int = 86_400_000
    log_level: str = "INFO"
    log_json: bool = True
    # Plan 2026-10-06 (R13/KTD7/KTD9): die fuers Kind relevanten, bereits
    # aufgeloesten Logging-Einstellungen. Die dictConfig reist als geprueft
    # geparster JSON-String -- das Kind liest die Datei nie; ``log_config``
    # nennt nur den Pfad fuer Fehlermeldungen.
    log_level_explicit: bool = False
    log_config: str | None = None
    log_config_json: str | None = None
    configure_logging: bool = True
    logger_levels: tuple[tuple[str, str], ...] = ()
    log_queue_size: int = 10_000
    worker_id: str = ""
    workflow_id: str | None = None  # U10 (KTD13): Logkontext im Kind


@dataclass(frozen=True, slots=True)
class RemoteExceptionInfo:
    module: str
    qualname: str
    message: str
    traceback_text: str


@dataclass(slots=True)
class ResultEnvelope:
    """Antwort des Kindes: genau einer von ``value``, ``exception``, ``remote_exception``."""

    task_id: str
    ok: bool
    value: Any = None
    exception: BaseException | None = None
    remote_exception: RemoteExceptionInfo | None = None
    rss_mb: float | None = None
    duration_s: float = 0.0
    slot: int | None = None
    # PID des rechnenden Kinds: ``compute_child`` der Lebenszyklus-Events (U11).
    pid: int | None = None

    def error(self) -> BaseException | None:
        """Die Exception fuer den Fehlerpfad des Loop-Prozesses (KTD5)."""
        if self.ok:
            return None
        if self.exception is not None:
            return self.exception
        if self.remote_exception is not None:
            return reconstruct_exception(self.remote_exception)
        return RemoteTaskError("compute child reported an error without detail", remote_type="unknown")


def _is_picklable(obj: object) -> bool:
    """Hin- UND Rueckweg pruefen: eine Exception mit eigener ``__init__``-Signatur
    pickelt zwar, scheitert aber beim ``loads`` im Elternprozess."""
    try:
        pickle.loads(pickle.dumps(obj))
    except Exception:
        return False
    return True


def success_envelope(task_id: str, value: Any, **meta: Any) -> ResultEnvelope:
    """Rueckgabewert verpacken; nicht picklebare Werte werden zum Platzhalter."""
    if not _is_picklable(value):
        value = UnpicklableResult(f"return value of type {type(value).__name__} is not picklable")
    return ResultEnvelope(task_id=task_id, ok=True, value=value, **meta)


def error_envelope(task_id: str, exc: BaseException, **meta: Any) -> ResultEnvelope:
    """Exception verpacken: picklebar unveraendert, sonst als Beschreibung."""
    if _is_picklable(exc):
        return ResultEnvelope(task_id=task_id, ok=False, exception=exc, **meta)
    info = RemoteExceptionInfo(
        module=type(exc).__module__,
        qualname=type(exc).__qualname__,
        message=str(exc),
        traceback_text="".join(traceback.format_exception(exc)),
    )
    return ResultEnvelope(task_id=task_id, ok=False, remote_exception=info, **meta)


def reconstruct_exception(info: RemoteExceptionInfo) -> BaseException:
    """Exception-Typ per Import wiederherstellen, sonst ``RemoteTaskError``.

    Der Loop-Prozess importiert die Task-Module ohnehin, also sind auch
    projekteigene Exception-Klassen per Qualname erreichbar. Konstruktoren
    mit fremder Signatur fallen auf ``RemoteTaskError`` zurueck.
    """
    fallback = RemoteTaskError(info.message, remote_type=f"{info.module}.{info.qualname}")
    if "<locals>" in info.qualname:
        return fallback
    try:
        module = importlib.import_module(info.module)
        cls: Any = module
        for part in info.qualname.split("."):
            cls = getattr(cls, part)
    except Exception:
        return fallback
    if not (isinstance(cls, type) and issubclass(cls, BaseException)):
        return fallback
    exc_cls: type[BaseException] = cls
    rebuilt: BaseException | None = None
    for ctor_args in ((info.message,), ()):
        try:
            rebuilt = exc_cls(*ctor_args)
        except Exception:
            continue
        break
    if rebuilt is None:
        return fallback
    rebuilt.add_note(f"remote traceback:\n{info.traceback_text}")
    return rebuilt


__all__ = [
    "ComputeJob",
    "RemoteExceptionInfo",
    "RemoteTaskError",
    "ResultEnvelope",
    "UnpicklableResult",
    "error_envelope",
    "reconstruct_exception",
    "success_envelope",
]
