"""SwarmQ logging -- Queue-Kern: Handler, Listener, Opt-in-Queue, Shutdown
und Fork-Hook (Plan 2026-10-06-1811, U1).

Aus ``swarmq.logging`` ausgelagert; dort bleiben die oeffentlichen Namen
importierbar. Haelt den Prozesszustand, den Einrichtung und Shutdown
teilen (``_CONFIGURED``, ``_APP_TAKEOVER_REPORTED``).
"""

from __future__ import annotations

import atexit
import contextlib
import contextvars
import json as jsonlib
import logging
import logging.handlers
import os
import queue
import sys
import threading
import time
import weakref
from typing import Any

from swarmq.logging_file import CONFIG_LISTENERS as _CONFIG_LISTENERS
from swarmq.logging_file import stop_queue_listeners
from swarmq.logging_format import _iso_utc

LISTENER_THREAD_NAME = "swarmq-log-listener"
"""Name des Listener-Threads; daran erkennbar in Thread-Dumps und Tests."""


_LogQueue = queue.Queue[logging.LogRecord]
_ListenerQueue = queue.Queue[Any]
"""Records plus der ``None``-Sentinel des Listeners."""

_LISTENER_LOCK = threading.Lock()
"""Schuetzt den Listener-Start: Der erste Eintrag kann mit einem expliziten
Start aus einem anderen Thread konkurrieren (KTD5)."""

_LISTENERS: weakref.WeakSet[_SwarmqQueueListener] = weakref.WeakSet()
"""Alle Listener des Prozesses. Ein laufender bleibt ueber seinen Thread am
Leben, auch wenn sein Handler schon weg ist; ``shutdown_logging`` leert ihn."""


class _SwarmqQueueListener(logging.handlers.QueueListener):
    """``QueueListener`` mit benanntem Thread (Plan 2026-10-06-0831, KTD5)."""

    # typeshed kennt nur die oeffentliche Schnittstelle; ``_thread``,
    # ``_monitor`` und ``_sentinel`` sind seit 3.2 unveraendert.
    queue: _ListenerQueue
    _thread: threading.Thread | None
    _sentinel: None
    _monitor: Any

    def __init__(self, queue_: _ListenerQueue, *handlers: logging.Handler) -> None:
        super().__init__(queue_, *handlers)
        _LISTENERS.add(self)

    @property
    def running(self) -> bool:
        return self._thread is not None

    def start(self) -> None:
        """Idempotent: unter 3.14 wirft der Basis-``start`` beim zweiten Mal."""
        with _LISTENER_LOCK:
            if self._thread is not None:
                return
            self._thread = thread = threading.Thread(
                target=self._monitor, name=LISTENER_THREAD_NAME, daemon=True
            )
            thread.start()

    def dequeue(self, block: bool) -> Any:
        """Eine per ``Queue.shutdown`` geschlossene, leere Queue beendet den
        Thread wie ``queue.Empty`` statt mit einer Ausnahme."""
        try:
            return self.queue.get(block)
        except queue.ShutDown:
            raise queue.Empty from None

    def enqueue_sentinel(self) -> None:
        """Blockierend: Bei voller Queue wuerde ``put_nowait`` werfen; der
        laufende Listener schafft Platz. Eine geschlossene Queue braucht
        keinen Sentinel, der Thread endet nach dem Leeren von selbst."""
        with contextlib.suppress(queue.ShutDown):
            self.queue.put(self._sentinel)


_DROPPED_TOTAL = 0
"""Verworfene Eintraege aller Queue-Handler dieses Prozesses; monoton bis
zum Fork (KTD5)."""


def dropped_log_records() -> int:
    """Anzahl der in diesem Prozess verworfenen Logeintraege (R7).

    Je Prozess: ``swarmq.metrics`` exportiert den Wert des Worker-Prozesses;
    Verluste in Compute-Kindern zeigt deren ``log_records_dropped``-Zeile.
    """
    return _DROPPED_TOTAL


_HANDLERS: weakref.WeakSet[_StructlogQueueHandler] = weakref.WeakSet()
"""Alle lebenden Queue-Handler des Prozesses; der Fork-Hook setzt sie zurueck."""


class _StructlogQueueHandler(logging.handlers.QueueHandler):
    """QueueHandler, der den Record unveraendert einreiht.

    ``prepare`` ueberspringt ``format()``, die Kopie und das Leeren von
    ``msg``, ``args`` und ``exc_info`` des Basis-``QueueHandler``: Der
    Formatter im Listener-Thread sieht den Original-Record samt aller
    Attribute (Felder, Traceback) und rendert erst dort.

    Drops are counted and surfaced. The stdlib path
    (queue.Full → handleError) silently discarded records under burst —
    a saturated log pipeline looked identical to a quiet one. ``dropped``
    is the cumulative counter; a rate-limited stderr notice (at most one
    per second) keeps the drop visible without amplifying the burst.

    ``handlers`` sind die Ziele des Listeners (Plan 2026-10-06-0831, KTD5).
    Der Listener startet lazy beim ersten Eintrag.
    """

    _DROP_NOTICE_INTERVAL_S = 1.0

    queue: _LogQueue
    listener: _SwarmqQueueListener | None

    def __init__(
        self,
        queue_: _LogQueue,
        *handlers: logging.Handler,
        json: bool = False,
    ) -> None:
        super().__init__(queue_)
        self.json = json
        self.dropped = 0
        self._last_drop_notice = 0.0
        self._reserve_at = self._reserve_threshold()
        self.listener = _SwarmqQueueListener(queue_, *handlers) if handlers else None
        _HANDLERS.add(self)

    def prepare(self, record: logging.LogRecord) -> logging.LogRecord:
        """Pass the record through without formatting."""
        return record

    def enqueue(self, record: logging.LogRecord) -> None:
        listener = self.listener
        if listener is not None and not listener.running:
            listener.start()
        if record.levelno < logging.WARNING and self._in_reserve():
            self._drop()
            return
        try:
            self.queue.put_nowait(record)
        except (queue.Full, queue.ShutDown):
            self._drop()

    def _in_reserve(self) -> bool:
        """Ab 90 % Fuellstand bleibt der Rest WARNING und hoeher vorbehalten."""
        reserve_at = self._reserve_at
        if reserve_at is None:
            return False
        q = self.queue
        # ``len`` der Deque ohne das Mutex von ``qsize``; Unterklassen koennen
        # ``_qsize`` ueberschreiben und behalten deshalb ``qsize``.
        level = len(q.queue) if type(q) is queue.Queue else q.qsize()
        return level >= reserve_at

    def _reserve_threshold(self) -> int | None:
        """Fuellstand, ab dem die Reserve gilt: ``ceil(maxsize * 0.9)``,
        ganzzahlig wie ``qsize * 10 >= maxsize * 9``; ``None`` ohne Grenze."""
        maxsize = self.queue.maxsize
        return -(-maxsize * 9 // 10) if maxsize > 0 else None

    def _drop(self) -> None:
        # ``dropped += 1`` is deliberately lock-free: under
        # multi-thread contention it may undercount — acceptable for
        # an observability counter; a lock here would serialize the
        # hot logging path exactly when it is saturated.
        global _DROPPED_TOTAL
        self.dropped += 1
        _DROPPED_TOTAL += 1
        now = time.monotonic()
        if now - self._last_drop_notice >= self._DROP_NOTICE_INTERVAL_S:
            self._last_drop_notice = now
            # stderr directly — going through logging would feed the
            # very queue that is full.
            print(self._drop_notice(), file=sys.stderr)

    def _reset_after_fork(self) -> None:
        """Frische Queue, Zaehler und Listener im Kind (KTD5)."""
        self.queue = fresh = _LogQueue(self.queue.maxsize)
        self._reserve_at = self._reserve_threshold()
        self.dropped = 0
        self._last_drop_notice = 0.0
        if self.listener is not None:
            # Der geerbte Listener verweist auf einen Thread, den es im Kind
            # nicht gibt.
            # Dieselbe Klasse: der Opt-in-Listener gibt an Root weiter (KTD10).
            self.listener = type(self.listener)(fresh, *self.listener.handlers)

    def _drop_notice(self) -> str:
        """Die Drop-Meldung; im JSON-Modus als vorgerenderte JSON-Zeile (R7)."""
        if not self.json:
            return (
                "swarmq: logging queue full — "
                f"{self.dropped} record(s) dropped so far"
            )
        return jsonlib.dumps(
            {
                "event": "log_records_dropped",
                "count": self.dropped,
                "level": "warning",
                # Der bisherige Logger-Name, unabhaengig vom Modul (U1).
                "logger": "swarmq.logging",
                "timestamp": _iso_utc(time.time()),
            }
        )


_CONTEXT_ATTR = "_swarmq_context"
"""Record-Attribut mit dem Kontext des Aufrufers (KTD10); nur bis zur
Weitergabe im Listener."""


class _OptinQueueListener(_SwarmqQueueListener):
    """Listener der Opt-in-Queue (Plan 2026-10-06-0831, KTD10).

    Gibt jeden Record zum Ausgabezeitpunkt an die Handler von Root weiter,
    also die Elternkette von ``swarmq`` -- ohne Schnappschuss.
    """

    def handle(self, record: logging.LogRecord) -> None:
        context = record.__dict__.pop(_CONTEXT_ATTR, None)
        if context is None:
            _dispatch_to_root(record)
        else:
            context.run(_dispatch_to_root, record)


def _dispatch_to_root(record: logging.LogRecord) -> None:
    """Wie ``Logger.callHandlers`` fuer Root, aber ohne es aufzurufen:
    Integrationen, die ``callHandlers`` umhaengen (Sentrys
    ``LoggingIntegration``), sahen den Record dort schon beim Aufrufer und
    saehen ihn sonst ein zweites Mal. Ohne Root-Handler gilt wie in der
    stdlib ``lastResort``."""
    handlers: list[logging.Handler | None] = [*logging.getLogger().handlers]
    for handler in handlers or [logging.lastResort]:
        if handler is not None and record.levelno >= handler.level:
            handler.handle(record)


class _OptinQueueHandler(_StructlogQueueHandler):
    """Queue-Handler am Logger ``swarmq`` fuer den Opt-in (KTD10)."""

    def __init__(self, queue_: _LogQueue, *, json: bool = False) -> None:
        super().__init__(queue_, json=json)
        self.listener = _OptinQueueListener(queue_)

    def prepare(self, record: logging.LogRecord) -> logging.LogRecord:
        """Haengt den Kontext des Aufrufers an (O(1)): Filter der App, die
        Kontextvariablen lesen, arbeiten im Listener wie im Aufrufer."""
        record.__dict__[_CONTEXT_ATTR] = contextvars.copy_context()
        return record


_OPTIN_HANDLER: _OptinQueueHandler | None = None
"""Der installierte Opt-in-Handler am Logger ``swarmq``, sonst ``None``."""

_OPTIN_SAVED: tuple[bool, int] = (True, logging.NOTSET)
"""``propagate`` und Level von ``swarmq`` vor dem Opt-in."""

_OPTIN_HOLDERS = 0
"""Referenzzaehler: Mehrere ``SwarmQ``-Instanzen teilen einen Handler."""

_OPTIN_TAKEOVER = False
"""Haelt die Regel "App gewinnt" (KTD4) den Opt-in in diesem Prozess?"""


def _enable_log_queue(level: str | int, *, json: bool = False, queue_size: int = 10_000) -> bool:
    """Leitet ``swarmq.*`` ueber eine eigene Hintergrund-Queue (R5, KTD10).

    Mit Referenzzaehlung: Die erste Anforderung installiert den Handler am
    Logger ``swarmq`` und setzt dessen Level; jede weitere zaehlt nur. Kein
    Thread -- der Listener startet beim ersten Eintrag (AE6).

    Hat ein Einstiegspunkt eingebaut oder per Datei konfiguriert, ist der
    Opt-in wirkungslos (``False``); unter "nicht konfigurieren" gilt er.
    """
    global _OPTIN_HOLDERS
    if _CONFIGURED:
        return False
    _OPTIN_HOLDERS += 1
    _install_log_queue(level, json=json, queue_size=queue_size)
    return True


def _disable_log_queue() -> None:
    """Gibt eine Anforderung frei. Die letzte schreibt die Queue und stellt
    Handler, ``propagate`` und Level von ``swarmq`` wieder her (KTD10)."""
    global _OPTIN_HOLDERS
    _OPTIN_HOLDERS = max(_OPTIN_HOLDERS - 1, 0)
    if _OPTIN_HOLDERS == 0 and not _OPTIN_TAKEOVER:
        _remove_log_queue()


def _reset_log_queue() -> None:
    """Hebt den Opt-in unabhaengig von der Zahl der Anforderungen auf."""
    global _OPTIN_HOLDERS, _OPTIN_TAKEOVER
    _OPTIN_HOLDERS = 0
    _OPTIN_TAKEOVER = False
    _remove_log_queue()


def _install_log_queue(level: str | int | None, *, json: bool, queue_size: int) -> None:
    """Installiert den Opt-in-Handler am Logger ``swarmq``, falls noch keiner
    da ist; ``level=None`` laesst das ``swarmq``-Level unangetastet."""
    global _OPTIN_HANDLER, _OPTIN_SAVED
    if _OPTIN_HANDLER is not None:
        return
    logger = logging.getLogger("swarmq")
    _OPTIN_SAVED = (logger.propagate, logger.level)
    _OPTIN_HANDLER = _OptinQueueHandler(_LogQueue(queue_size), json=json)
    logger.addHandler(_OPTIN_HANDLER)
    logger.propagate = False
    if level is not None:
        logger.setLevel(level)


def _remove_log_queue() -> None:
    """Entfernt den Opt-in-Handler, stellt ``swarmq`` wieder her und
    schreibt die Queue."""
    global _OPTIN_HANDLER
    handler, _OPTIN_HANDLER = _OPTIN_HANDLER, None
    if handler is None:
        return
    logger = logging.getLogger("swarmq")
    logger.removeHandler(handler)
    logger.propagate, level = _OPTIN_SAVED
    logger.setLevel(level)
    assert handler.listener is not None
    handler.listener.stop()


_CONFIGURED = False
"""Hat ``configure_logging`` in diesem Prozess eingerichtet? Je Prozess gibt
es genau einen Eigentuemer (KTD4); ``shutdown_logging`` gibt ihn frei."""


def shutdown_logging() -> None:
    """Schreibt alle Eintraege der Log-Queue und stoppt den Listener (KTD5).

    Der Queue-Handler bleibt installiert; der naechste Eintrag startet den
    Listener wieder. Ein folgendes ``configure_logging`` baut neu auf (KTD4).
    """
    global _CONFIGURED, _APP_TAKEOVER_REPORTED
    _CONFIGURED = _APP_TAKEOVER_REPORTED = False
    _stop_listeners()


def _stop_listeners() -> None:
    """Leert und stoppt alle Listener des Prozesses, ohne den Eigentuemer
    freizugeben."""
    for listener in list(_LISTENERS):
        listener.stop()
    stop_queue_listeners()


_APP_TAKEOVER_REPORTED = False
"""``logging_reconfigured_by_app`` erscheint einmal je Prozess (KTD4)."""


def _activate_swarmq_queue_after_app_takeover(*, json: bool, queue_size: int) -> None:
    """Nach dem Ersatz durch die App laufen die swarmq-Logs ueber die
    Opt-in-Queue am Logger ``swarmq`` an die Handler der App (KTD4, KTD10):
    Ein blockierender App-Handler haelt den Event-Loop nicht auf (R6). Das
    ``swarmq``-Level bleibt, wie es ist -- die App hat entschieden.
    """
    global _OPTIN_TAKEOVER
    _OPTIN_TAKEOVER = True
    _install_log_queue(None, json=json, queue_size=queue_size)


def _after_fork_in_child() -> None:
    """Setzt den Queue-Zustand im Kind zurueck (KTD5).

    Die geerbten Records schreibt der Elternprozess; im Kind erschienen sie
    sonst doppelt. Startet keinen Thread -- das uebernimmt der naechste
    Eintrag. Ein beim Fork gehaltenes Start-Lock hat im Kind keinen
    Besitzer mehr und wird ersetzt.

    Die geerbten Listener -- eigene wie die der dictConfig -- gehoeren dem
    Elternprozess: Ihr Thread fehlt im Kind, und ``shutdown_logging``
    haengte oder wuerfe sonst beim Sentinel auf einer beim Fork vollen
    Queue-Kopie.
    """
    global _LISTENER_LOCK, _DROPPED_TOTAL
    _LISTENER_LOCK = threading.Lock()
    _DROPPED_TOTAL = 0
    _LISTENERS.clear()
    _CONFIG_LISTENERS.clear()
    for handler in list(_HANDLERS):
        handler._reset_after_fork()


os.register_at_fork(after_in_child=_after_fork_in_child)

# Genau einmal, beim Import, ohne Thread (KTD5): dictConfig und
# ``logging.shutdown`` stoppen keine Listener.
atexit.register(shutdown_logging)
