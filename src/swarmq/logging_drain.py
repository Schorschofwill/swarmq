"""Begrenztes Leeren der Log-Queues (Plan 2026-10-06-1811, U4, KTD3).

Gemeinsame Bausteine fuer die eingebauten Listener (``swarmq.logging_queue``)
und die Datei-Modus-Listener (``swarmq.logging_file``): eine Gesamtfrist
ueber alle Listener. Importiert keines der beiden Module.
"""

from __future__ import annotations

import contextlib
import json as jsonlib
import logging
import os
import select
import signal
import sys
import threading
import time
import traceback
import weakref
from collections.abc import Iterable

from swarmq.logging_format import iso_utc


class Deadline:
    """Eine gemeinsame Frist ueber alle Listener eines Leerens (KTD3)."""

    def __init__(self, seconds: float) -> None:
        self._end = time.monotonic() + seconds

    def remaining(self) -> float:
        """Restzeit in Sekunden, nie negativ und hoechstens
        ``threading.TIMEOUT_MAX``: Mehr liesse ``Thread.join`` mit
        ``OverflowError`` scheitern. Ein Rueckhalt -- die Einstellungen
        lassen solche Fristen gar nicht erst zu."""
        return min(threading.TIMEOUT_MAX, max(0.0, self._end - time.monotonic()))


def write_drain_notice(count: int, timeout: float, *, json: bool) -> None:
    """Abschlusszeile ``log_drain_timeout_exceeded`` (KTD3), nur wenn das
    gefahrlos geht.

    Per ``os.write`` an ``sys.stderr`` und dessen Lock vorbei: Der
    aufgegebene Thread haelt ihn womoeglich. Hoechstens ``PIPE_BUF`` Bytes
    und nur, wenn ``poll`` fd 2 sofort als beschreibbar meldet -- dann
    blockiert der Schreibvorgang nicht. Sonst bleibt es beim Zaehler.
    O_NONBLOCK wird nicht gesetzt: Es gaelte fuer die gemeinsame File
    Description, also auch fuer die Schreibvorgaenge der Listener (KTD2).
    """
    if json:
        line = jsonlib.dumps({
            "event": "log_drain_timeout_exceeded",
            "count": count,
            "timeout_s": timeout,
            "level": "warning",
            "logger": "swarmq.logging",
            "timestamp": iso_utc(time.time()),
        })
    else:
        line = (
            f"swarmq: log_drain_timeout_exceeded — {count} log record(s) not written "
            f"within {timeout:g} s, counted as dropped"
        )
    _write_fd2_if_free((line + "\n").encode("utf-8", "replace"))


def _write_fd2_if_free(data: bytes) -> bool:
    """Schreibt ``data`` per ``os.write`` auf fd 2, wenn das nicht blockiert:
    hoechstens ``PIPE_BUF`` Bytes (ein Schreibvorgang) und nur, wenn ``poll``
    fd 2 sofort als beschreibbar meldet. ``False``, wenn nichts geschrieben
    wurde."""
    try:
        if _writable_now(2) and len(data) <= select.PIPE_BUF:
            os.write(2, data)
            return True
    except OSError:
        pass  # fd 2 geschlossen oder Leser weg
    return False


_ABANDONED: list[threading.Thread] = []
"""Aufgegebene Listener-Threads (KTD3); ein haengender bleibt eingetragen,
bis er endet."""

_STUCK_HANDLERS: weakref.WeakKeyDictionary[logging.Handler, threading.Thread] = (
    weakref.WeakKeyDictionary()
)
"""Ziel-Handler aufgegebener Threads und der Thread, der an ihnen haengt;
der Fork-Hook laesst sie aus (Plan 2026-10-06-1811, KTD10)."""

_STUCK_STREAMS: set[int] = set()
"""``id`` der Streams aufgegebener Ziel-Handler: Ihr Lock haelt womoeglich
der haengende Thread, ein ``flush`` blockierte."""

_STUCK_FILES: set[tuple[int, int]] = set()
"""``(st_dev, st_ino)`` hinter diesen Streams: Ein anderer Stream auf
derselben Datei -- etwa stdout auf derselben Pipe wie stderr (``2>&1``,
Journal) -- blockierte beim ``flush`` ebenso."""


def abandon(thread: threading.Thread, handlers: Iterable[logging.Handler]) -> None:
    """Gibt einen Listener-Thread auf, der seine Frist ueberschritten hat.

    Seine Ziel-Handler -- und jeder Handler auf demselben Stream, etwa
    ``logging.lastResort`` auf stderr -- werden aus ``logging._handlerList``
    ausgetragen: ``logging.shutdown`` (atexit, Kind-Ende) nimmt sonst ihr
    Lock oder leert ihren Stream und haengt am haengenden Thread.
    """
    _ABANDONED[:] = [t for t in _ABANDONED if t.is_alive()]
    _ABANDONED.append(thread)
    targets = set(handlers)
    for handler in targets:
        _STUCK_HANDLERS[handler] = thread
    _forget(targets)


def is_stuck(handler: logging.Handler) -> bool:
    """Haengt ein aufgegebener Thread noch an ``handler``? Sein Lock ist
    dann womoeglich auf Dauer belegt."""
    thread = _STUCK_HANDLERS.get(handler)
    return thread is not None and thread.is_alive()


def _stream_of(handler: logging.Handler | None) -> object:
    try:
        return getattr(handler, "stream", None)
    except Exception:  # eine Property der App darf hier nicht werfen
        return None


def _file_of(stream: object) -> tuple[int, int] | None:
    """``(st_dev, st_ino)`` der Datei hinter ``stream``, sonst ``None``."""
    try:
        info = os.fstat(stream.fileno())  # type: ignore[attr-defined]
    except Exception:  # kein fd (StringIO), geschlossen, fremde Klasse
        return None
    return info.st_dev, info.st_ino


def _forget(handlers: set[logging.Handler]) -> None:
    stuck = [s for h in handlers if (s := _stream_of(h)) is not None]
    streams = {id(s) for s in stuck}
    _STUCK_STREAMS.update(streams)
    _STUCK_FILES.update(f for s in stuck if (f := _file_of(s)) is not None)
    with logging._lock:  # type: ignore[attr-defined]
        refs = logging._handlerList  # type: ignore[attr-defined]
        refs[:] = [
            ref for ref in refs
            if (h := ref()) is None
            or (h not in handlers and id(_stream_of(h)) not in streams)
        ]


def exit_if_log_thread_stuck(code: int, exc: BaseException | None = None) -> None:
    """Beendet den Prozess hart mit ``code``, wenn nach dem Leeren noch ein
    aufgegebener Listener-Thread lebt (Plan 2026-10-06-1811, KTD3).

    ``exc`` ist die Ausnahme, an der der Prozess endet: Ihre Ausgabe -- der
    Traceback, beim ``SystemExit`` dessen Text -- stuende sonst erst in der
    Finalisierung an, die das harte Ende ueberspringt. Sie geht vorher wie
    :func:`write_drain_notice` auf fd 2, gekuerzt auf ``PIPE_BUF`` Bytes
    (Schluss-Review).

    Nur fuer Prozesse, die swarmq selbst startet (CLI und Supervisor,
    Worker-Subprozess, Compute-Kind), als letzter Schritt nach den eigenen
    Aufraeumarbeiten -- nie in eingebetteten Apps. Ein Thread, der im
    Schreiben festhaengt, haelt den Lock des ``BufferedWriter``; die
    Finalisierung bricht dann mit "Fatal Python error:
    _enter_buffered_busy" ab (Exit 134) oder haengt. Spaetere
    atexit-Funktionen laufen nicht; stdout und stderr werden vorher geleert,
    soweit das nicht blockiert (:func:`_flushable`).
    """
    if not any(thread.is_alive() for thread in _ABANDONED):
        return
    for stream in (sys.stdout, sys.stderr):
        if _flushable(stream):
            with contextlib.suppress(Exception):
                stream.flush()
    if exc is not None and (report := _exit_report(exc)):
        _write_fd2_if_free(report)
    _hard_exit(code)


_TRUNCATED = b"[...]\n"


def _exit_report(exc: BaseException) -> bytes:
    """Was der Interpreter beim Ende an ``exc`` ausgaebe, als Bytes fuer
    einen Schreibvorgang: der Text eines ``SystemExit`` (nichts bei einem
    Code), sonst der Traceback. Zu Langes verliert den Anfang -- die letzte
    Zeile nennt die Ausnahme."""
    if isinstance(exc, SystemExit):
        text = "" if exc.code is None or isinstance(exc.code, int) else f"{exc.code}\n"
    else:
        text = "".join(traceback.format_exception(exc))
    data = text.encode("utf-8", "replace")
    if len(data) > select.PIPE_BUF:
        data = _TRUNCATED + data[-(select.PIPE_BUF - len(_TRUNCATED)):]
    return data


def _flushable(stream: object) -> bool:
    """Blockiert ``flush`` nicht? Nicht, wenn ein haengender Thread an
    diesem Stream oder derselben Datei schreibt -- stdout teilt sich etwa
    mit stderr die Pipe --, und nur, wenn ``poll`` den fd sofort als
    beschreibbar meldet (wie :func:`write_drain_notice`)."""
    if stream is None or id(stream) in _STUCK_STREAMS:
        return False
    file = _file_of(stream)
    if file is None:
        return True  # ohne fd blockiert kein ``write(2)``
    if file in _STUCK_FILES:
        return False
    try:
        return _writable_now(stream.fileno())  # type: ignore[attr-defined]
    except Exception:
        return False


def _writable_now(fd: int) -> bool:
    """Meldet ``poll`` den fd sofort als beschreibbar? Fehler gibt es an den
    Aufrufer weiter."""
    poller = select.poll()
    poller.register(fd, select.POLLOUT)
    return any(events & select.POLLOUT for _, events in poller.poll(0))


_hard_exit = os._exit
"""Eigener Name, damit Tests das harte Ende ersetzen koennen, ohne
``os._exit`` prozessweit umzuhaengen (Fork-Tests brauchen es)."""


def exit_code_for(exc: BaseException | None) -> int:
    """Der Exit-Code, mit dem der Prozess ohne hartes Ende geendet haette:
    0 ohne Ausnahme, der Code eines ``SystemExit`` wie in
    ``multiprocessing``, 130 (128 + SIGINT) fuer ``KeyboardInterrupt`` wie
    beim Ende des Interpreters durch Strg-C, sonst 1."""
    if exc is None:
        return 0
    if isinstance(exc, KeyboardInterrupt):
        return 128 + signal.SIGINT
    if isinstance(exc, SystemExit):
        if exc.code is None:
            return 0
        return exc.code if isinstance(exc.code, int) else 1
    return 1
