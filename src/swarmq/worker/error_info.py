"""Fehlerklasse eines Fehlerergebnisses (Plan 2026-10-12-1105, MR 3, P9).

``error_class_of`` liefert den voll qualifizierten Namen der Exception-Klasse
(``modul.qualname``, eingebaute als ``builtins.ValueError``; E5 A). Er steht
als ``error_class`` im Fehlerergebnis und an ``TaskError.error_class``.
"""

from __future__ import annotations

from swarmq.exceptions import LockWaitExceeded, RedeliveryLimitError, TaskNotFoundError
from swarmq.worker.compute.envelope import RemoteTaskError

#: ``remote_type`` eines Envelopes ohne Fehlerdetail (``envelope.py``).
_REMOTE_TYPE_UNKNOWN = "unknown"

#: Markierung vom Worker selbst erzeugter Fehler (harter Timeout, Drain,
#: Rechenkind-Pool). Im Instanz-``__dict__``, weil Exceptions keine
#: schwachen Referenzen erlauben.
_WORKER_ERROR_MARK = "_swarmq_worker_error"


def mark_worker_error[E: BaseException](exc: E) -> E:
    """Markiert *exc* als vom Worker erzeugt: ``error_class_of`` liefert
    dafuer ``None`` (T12). Die Klasse bleibt, ``error_type`` in den Logs
    aendert sich nicht."""
    exc.__dict__[_WORKER_ERROR_MARK] = True
    return exc


def _qualified(cls: type[BaseException]) -> str:
    return f"{cls.__module__}.{cls.__qualname__}"


#: Gate-Pfade ohne Task-Exception (T17, E7 A): nur oeffentliche
#: swarmq-Klassen. ``WorkerHungError`` (keine Klasse) und der ``ValueError``
#: der Lock-Render-Pfade fehlen bewusst.
_GATE_ERROR_CLASSES: dict[str, str] = {
    cls.__name__: _qualified(cls) for cls in (TaskNotFoundError, LockWaitExceeded, RedeliveryLimitError)
}


def gate_error_class(error_type: str) -> str | None:
    """``error_class`` eines Gate-Pfads aus seinem ``error_type``."""
    return _GATE_ERROR_CLASSES.get(error_type)


def error_class_of(exc: BaseException) -> str | None:
    """Voll qualifizierter Klassenname von *exc*; bei ``RemoteTaskError``
    der Name, den das Rechenkind meldete (T12), ohne Detail ``None``.
    Markierte Worker-Fehler (``mark_worker_error``) haben keine Klasse."""
    if exc.__dict__.get(_WORKER_ERROR_MARK, False):
        return None
    if isinstance(exc, RemoteTaskError):
        return None if exc.remote_type == _REMOTE_TYPE_UNKNOWN else exc.remote_type
    return _qualified(type(exc))


__all__ = ["error_class_of", "gate_error_class", "mark_worker_error"]
