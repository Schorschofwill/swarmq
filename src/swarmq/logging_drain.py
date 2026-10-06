"""Begrenztes Leeren der Log-Queues (Plan 2026-10-06-1811, U4, KTD3).

Gemeinsame Bausteine fuer die eingebauten Listener (``swarmq.logging_queue``)
und die Datei-Modus-Listener (``swarmq.logging_file``): eine Gesamtfrist
ueber alle Listener. Importiert keines der beiden Module.
"""

from __future__ import annotations

import contextlib
import datetime
import json as jsonlib
import logging
import os
import select
import sys
import threading
import time
from collections.abc import Iterable


class Deadline:
    """Eine gemeinsame Frist ueber alle Listener eines Leerens (KTD3)."""

    def __init__(self, seconds: float) -> None:
        self._end = time.monotonic() + seconds

    def remaining(self) -> float:
        """Restzeit in Sekunden, nie negativ."""
        return max(0.0, self._end - time.monotonic())


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
            "timestamp": datetime.datetime.now(datetime.UTC).isoformat().replace("+00:00", "Z"),
        })
    else:
        line = (
            f"swarmq: log_drain_timeout_exceeded — {count} log record(s) not written "
            f"within {timeout:g} s, counted as dropped"
        )
    data = (line + "\n").encode("utf-8", "replace")
    try:
        poller = select.poll()
        poller.register(2, select.POLLOUT)
        writable = any(events & select.POLLOUT for _, events in poller.poll(0))
        if writable and len(data) <= select.PIPE_BUF:
            os.write(2, data)
    except OSError:
        pass  # fd 2 geschlossen oder Leser weg: es bleibt beim Zaehler


_ABANDONED: list[threading.Thread] = []
"""Aufgegebene Listener-Threads (KTD3); ein haengender bleibt eingetragen,
bis er endet."""

_STUCK_STREAMS: set[int] = set()
"""``id`` der Streams aufgegebener Ziel-Handler: Ihr Lock haelt womoeglich
der haengende Thread, ein ``flush`` blockierte."""


def abandon(thread: threading.Thread, handlers: Iterable[logging.Handler]) -> None:
    """Gibt einen Listener-Thread auf, der seine Frist ueberschritten hat.

    Seine Ziel-Handler -- und jeder Handler auf demselben Stream, etwa
    ``logging.lastResort`` auf stderr -- werden aus ``logging._handlerList``
    ausgetragen: ``logging.shutdown`` (atexit, Kind-Ende) nimmt sonst ihr
    Lock oder leert ihren Stream und haengt am haengenden Thread.
    """
    _ABANDONED[:] = [t for t in _ABANDONED if t.is_alive()]
    _ABANDONED.append(thread)
    _forget(set(handlers))


def _stream_of(handler: logging.Handler | None) -> object:
    try:
        return getattr(handler, "stream", None)
    except Exception:  # eine Property der App darf hier nicht werfen
        return None


def _forget(handlers: set[logging.Handler]) -> None:
    streams = {id(s) for h in handlers if (s := _stream_of(h)) is not None}
    _STUCK_STREAMS.update(streams)
    with logging._lock:  # type: ignore[attr-defined]
        refs = logging._handlerList  # type: ignore[attr-defined]
        refs[:] = [
            ref for ref in refs
            if (h := ref()) is None
            or (h not in handlers and id(_stream_of(h)) not in streams)
        ]


def exit_if_log_thread_stuck(code: int) -> None:
    """Beendet den Prozess hart mit ``code``, wenn nach dem Leeren noch ein
    aufgegebener Listener-Thread lebt (Plan 2026-10-06-1811, KTD3).

    Nur fuer Prozesse, die swarmq selbst startet (CLI und Supervisor,
    Worker-Subprozess, Compute-Kind), als letzter Schritt nach den eigenen
    Aufraeumarbeiten -- nie in eingebetteten Apps. Ein Thread, der im
    Schreiben festhaengt, haelt den Lock des ``BufferedWriter``; die
    Finalisierung bricht dann mit "Fatal Python error:
    _enter_buffered_busy" ab (Exit 134) oder haengt. Spaetere
    atexit-Funktionen laufen nicht; stdout und stderr werden vorher geleert,
    soweit kein haengender Thread an ihnen schreibt.
    """
    if not any(thread.is_alive() for thread in _ABANDONED):
        return
    for stream in (sys.stdout, sys.stderr):
        if stream is not None and id(stream) not in _STUCK_STREAMS:
            with contextlib.suppress(Exception):
                stream.flush()
    _hard_exit(code)


_hard_exit = os._exit
"""Eigener Name, damit Tests das harte Ende ersetzen koennen, ohne
``os._exit`` prozessweit umzuhaengen (Fork-Tests brauchen es)."""


def exit_code_for(exc: BaseException | None) -> int:
    """Der Exit-Code, mit dem der Prozess ohne hartes Ende geendet haette:
    0 ohne Ausnahme, der Code eines ``SystemExit`` wie in
    ``multiprocessing``, sonst 1."""
    if exc is None:
        return 0
    if isinstance(exc, SystemExit):
        if exc.code is None:
            return 0
        return exc.code if isinstance(exc.code, int) else 1
    return 1
