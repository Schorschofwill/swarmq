"""SwarmQ logging -- Queue-Kern: Handler, Listener, Opt-in-Queue, Shutdown
und Fork-Hook (Plan 2026-10-06-1811, U1).

Aus ``swarmq.logging`` ausgelagert; dort bleiben die oeffentlichen Namen
importierbar. Haelt den Prozesszustand, den Einrichtung und Shutdown
teilen (``_CONFIGURED``, ``_APP_TAKEOVER_REPORTED``).
"""

from __future__ import annotations

import asyncio
import atexit
import contextlib
import contextvars
import logging
import logging.handlers
import os
import queue
import sys
import threading
import weakref
from typing import Any

from swarmq.logging_drain import Deadline, abandon, write_drain_notice
from swarmq.logging_file import CONFIG_LISTENERS as _CONFIG_LISTENERS
from swarmq.logging_file import (
    FileDraining,
    HandlerErrors,
    RateLimitedReport,
    close_queue_listeners,
)

LISTENER_THREAD_NAME = "swarmq-log-lsnr"
"""Name des Listener-Threads; daran erkennbar in Thread-Dumps und Tests.
Hoechstens 15 Zeichen: So steht er ungekuerzt in ``/proc/<pid>/task/*/comm``,
woran die Forkserver-Pruefung ihn erkennt (Plan 2026-10-06-1811, KTD7)."""


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
        self.errors = HandlerErrors(self._dispatch)
        # Verworfene Records des zugehoerigen Queue-Handlers; gemeldet aus
        # diesem Thread (Plan 2026-10-06-1811, KTD2).
        self.drops = RateLimitedReport("log_records_dropped", self.errors.handle)
        # Root-Handler, den der Opt-in-Listener gerade bedient; haengt er,
        # wird er beim Aufgeben ausgetragen (Plan 2026-10-06-1811, KTD3).
        self.current: logging.Handler | None = None
        _LISTENERS.add(self)

    def targets(self) -> tuple[logging.Handler, ...]:
        """Die Ziel-Handler, an denen ein aufgegebener Thread haengen kann."""
        current = self.current
        return (*self.handlers, *(() if current is None else (current,)))

    @property
    def running(self) -> bool:
        """Der Thread lebt (KTD1): Ein toter Listener gilt nicht als laufend,
        der naechste Eintrag startet ihn neu. Die Queue bleibt dieselbe, ihre
        Records gehen weder verloren noch zaehlen sie als verworfen."""
        thread = self._thread
        return thread is not None and thread.is_alive()

    def start(self) -> None:
        """Idempotent: unter 3.14 wirft der Basis-``start`` beim zweiten Mal."""
        with _LISTENER_LOCK:
            if self.running:
                return
            self._thread = thread = threading.Thread(
                target=self._serve, name=LISTENER_THREAD_NAME, daemon=True
            )
            thread.start()

    def _serve(self) -> None:
        """Thread-Ziel: Records ausgeben; beim Ende noch nicht gemeldete
        Drops melden, auch innerhalb der Frist (Plan 2026-10-06-1811, KTD2)."""
        self._monitor()
        self.drops.report_due(final=True)

    def stop(self) -> None:
        """Wie der Basis-``stop``, aber auf dem eigenen Thread: Zwischen dem
        Ende des Threads und dem Vergessen kann ein Eintrag (``running`` ist
        dann falsch, KTD1) schon einen neuen gestartet haben. Der bleibt
        eingetragen, sonst liefe er verwaist weiter und naehme dem naechsten
        ``stop`` den Sentinel weg."""
        thread = self._thread
        if thread is None:
            return
        self.enqueue_sentinel()
        thread.join()
        with _LISTENER_LOCK:
            if self._thread is thread:
                self._thread = None

    def handle(self, record: logging.LogRecord) -> None:
        """Faengt jede ``Exception`` pro Record (Plan 2026-10-06-1811, KTD1):
        Der stdlib-``_monitor`` faengt nichts, ein werfender Handler, Filter
        oder Formatter beendete sonst den Thread. Danach meldet er neue
        Drops (KTD2)."""
        self.errors.handle(record)
        self.drops.report_due()

    def _dispatch(self, record: logging.LogRecord) -> None:
        super().handle(record)

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


def count_dropped(count: int = 1) -> None:
    """Zaehlt verworfene Eintraege, auch aus den Datei-Modus-Queues
    (Plan 2026-10-06-1811, KTD1) und beim Aufgeben eines Leerens (KTD3)."""
    global _DROPPED_TOTAL
    _DROPPED_TOTAL += count


_IN_FORK_SOURCE = False
"""Ist dieser Prozess eine Fork-Quelle (Forkserver)? Dann startet kein
Listener-Thread (Plan 2026-10-06-1811, KTD6)."""


def mark_fork_source() -> None:
    """Markiert den Prozess als Fork-Quelle: Queue-Handler geben synchron
    ueber die Ziel-Handler aus, statt einen Listener-Thread zu starten, und
    der Forkserver bleibt einfaedig (Plan 2026-10-06-1811, KTD6). Gesetzt
    von ``swarmq.worker.compute.preload``; der Fork-Hook hebt es im Kind
    auf."""
    global _IN_FORK_SOURCE
    _IN_FORK_SOURCE = True


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
    is the cumulative counter. Der Aufrufer zaehlt nur; die Meldung
    ``log_records_dropped`` (hoechstens einmal pro Sekunde) gibt der
    Listener ueber seine eigenen Handler aus (Plan 2026-10-06-1811, KTD2).

    ``handlers`` sind die Ziele des Listeners (Plan 2026-10-06-0831, KTD5).
    Der Listener startet lazy beim ersten Eintrag.
    """

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
        self._reserve_at = self._reserve_threshold()
        self.listener = _SwarmqQueueListener(queue_, *handlers) if handlers else None
        # Aufgegebener Listener-Thread (Plan 2026-10-06-1811, KTD3).
        self._abandoned: threading.Thread | None = None
        _HANDLERS.add(self)

    def prepare(self, record: logging.LogRecord) -> logging.LogRecord:
        """Pass the record through without formatting."""
        return record

    def enqueue(self, record: logging.LogRecord) -> None:
        abandoned = self._abandoned
        if abandoned is not None:
            if abandoned.is_alive():
                # Ziel haengt noch (KTD3): zaehlen statt einreihen.
                self._drop()
                return
            self._abandoned = None
        listener = self.listener
        if listener is not None and (_IN_FORK_SOURCE or sys.is_finalizing()):
            # Fork-Quelle (KTD6): kein Thread, der Forkserver bleibt
            # einfaedig. Finalisierung (KTD5): Kein neuer Thread startet
            # mehr, und ein laufender kommt nicht mehr dran -- synchron
            # ausgeben.
            listener.handle(record)
            return
        if listener is not None and not listener.running:
            try:
                listener.start()
            except PythonFinalizationError:
                listener.handle(record)
                return
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
        self.dropped += 1
        count_dropped()
        # Gemeldet wird im Listener-Thread (Plan 2026-10-06-1811, KTD2): Hier
        # auf stderr zu schreiben, blockierte den Aufrufer -- die Queue ist
        # meist gerade deshalb voll, weil das Ziel blockiert.
        if self.listener is not None:
            self.listener.drops.count += 1

    def _reset_after_fork(self) -> None:
        """Frische Queue, Zaehler und Listener im Kind (KTD5)."""
        self._swap_queue()
        self.dropped = 0

    def _swap_queue(self) -> None:
        """Frische Queue und frischer, noch nicht gestarteter Listener: im
        Kind nach dem Fork (KTD5) und beim Leeren (Plan 2026-10-06-1811,
        KTD3)."""
        self.queue = fresh = _LogQueue(self.queue.maxsize)
        self._reserve_at = self._reserve_threshold()
        if self.listener is not None:
            # Der alte Listener verweist auf einen Thread, den es im Kind
            # nicht gibt oder der die alte Queue leert.
            # Dieselbe Klasse: der Opt-in-Listener gibt an Root weiter (KTD10).
            self.listener = type(self.listener)(fresh, *self.listener.handlers)


_CONTEXT_ATTR = "_swarmq_context"
"""Record-Attribut mit dem Kontext des Aufrufers (KTD10); nur bis zur
Weitergabe im Listener."""


class _OptinQueueListener(_SwarmqQueueListener):
    """Listener der Opt-in-Queue (Plan 2026-10-06-0831, KTD10).

    Gibt jeden Record zum Ausgabezeitpunkt an die Handler von Root weiter,
    also die Elternkette von ``swarmq`` -- ohne Schnappschuss.
    """

    def _dispatch(self, record: logging.LogRecord) -> None:
        context = record.__dict__.pop(_CONTEXT_ATTR, None)
        if context is None:
            _dispatch_to_root(record, self)
        else:
            context.run(_dispatch_to_root, record, self)


def _dispatch_to_root(record: logging.LogRecord, listener: _SwarmqQueueListener) -> None:
    """Wie ``Logger.callHandlers`` fuer Root, aber ohne es aufzurufen:
    Integrationen, die ``callHandlers`` umhaengen (Sentrys
    ``LoggingIntegration``), sahen den Record dort schon beim Aufrufer und
    saehen ihn sonst ein zweites Mal. Ohne Root-Handler gilt wie in der
    stdlib ``lastResort``."""
    handlers: list[logging.Handler | None] = [*logging.getLogger().handlers]
    for handler in handlers or [logging.lastResort]:
        if handler is not None and record.levelno >= handler.level:
            # Pro Handler (KTD1): Ein werfender App-Handler haelt den Record
            # nicht von den uebrigen fern.
            listener.current = handler
            try:
                handler.handle(record)
            except Exception:
                listener.errors.failed()
            listener.current = None


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


def _enable_log_queue(
    level: str | int, *, json: bool = False, queue_size: int = 10_000,
    drain_timeout: float | None = None,
) -> bool:
    """Leitet ``swarmq.*`` ueber eine eigene Hintergrund-Queue (R5, KTD10).

    Mit Referenzzaehlung: Die erste Anforderung installiert den Handler am
    Logger ``swarmq`` und setzt dessen Level; jede weitere zaehlt nur. Kein
    Thread -- der Listener startet beim ersten Eintrag (AE6).

    Hat ein Einstiegspunkt eingebaut oder per Datei konfiguriert, ist der
    Opt-in wirkungslos (``False``); unter "nicht konfigurieren" gilt er.

    ``drain_timeout`` ist die Frist der App (``log_drain_timeout``); sie gilt
    dann auch fuer ``shutdown_logging`` beim Prozessende (Plan
    2026-10-06-1811, KTD3/KTD4).
    """
    global _OPTIN_HOLDERS, _DRAIN_TIMEOUT
    if _CONFIGURED:
        return False
    _OPTIN_HOLDERS += 1
    if drain_timeout is not None:
        _DRAIN_TIMEOUT = drain_timeout
    _install_log_queue(level, json=json, queue_size=queue_size)
    return True


def _disable_log_queue() -> _Draining | None:
    """Gibt eine Anforderung frei. Die letzte stellt Handler, ``propagate``
    und Level von ``swarmq`` wieder her (KTD10) und schliesst die Queue, in
    O(1) und ohne zu warten (Plan 2026-10-06-1811, KTD4). Das Leeren
    erwartet danach ``_await_log_queue_drained``."""
    global _OPTIN_HOLDERS
    _OPTIN_HOLDERS = max(_OPTIN_HOLDERS - 1, 0)
    if _OPTIN_HOLDERS == 0 and not _OPTIN_TAKEOVER:
        return _detach_log_queue()
    return None


async def _await_log_queue_drained(draining: _Draining | None, timeout: float) -> None:
    """Wartet, ohne den Event-Loop anzuhalten, bis der Listener die
    geschlossene Opt-in-Queue geleert hat (Plan 2026-10-06-1811, KTD4)."""
    if draining is None:
        return
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(asyncio.to_thread(draining.thread.join, timeout), timeout)
    if not draining.settle():
        draining.notify(timeout)


def _reset_log_queue() -> None:
    """Hebt den Opt-in unabhaengig von der Zahl der Anforderungen auf."""
    global _OPTIN_HOLDERS, _OPTIN_TAKEOVER
    _OPTIN_HOLDERS = 0
    _OPTIN_TAKEOVER = False
    draining = _detach_log_queue()
    if draining is not None and not draining.wait(Deadline(_DRAIN_TIMEOUT)):
        draining.notify(_DRAIN_TIMEOUT)


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


def _detach_log_queue() -> _Draining | None:
    """Synchroner Teil des Abbaus (KTD4): Handler entfernen, ``swarmq``
    wiederherstellen, Queue schliessen. Ein neuer Opt-in bekommt danach
    einen frischen Handler mit frischer Queue."""
    global _OPTIN_HANDLER
    handler, _OPTIN_HANDLER = _OPTIN_HANDLER, None
    if handler is None:
        return None
    logger = logging.getLogger("swarmq")
    logger.removeHandler(handler)
    logger.propagate, level = _OPTIN_SAVED
    logger.setLevel(level)
    assert handler.listener is not None
    return _close(handler.listener, handler, swap=False)


_CONFIGURED = False
"""Hat ``configure_logging`` in diesem Prozess eingerichtet? Je Prozess gibt
es genau einen Eigentuemer (KTD4); ``shutdown_logging`` gibt ihn frei."""


_DRAIN_TIMEOUT = 5.0
"""Frist fuer ``shutdown_logging`` ohne ``timeout`` (KTD11)."""


def shutdown_logging(timeout: float | None = None) -> None:
    """Schreibt die Eintraege der Log-Queues und stoppt die Listener (KTD5),
    hoechstens ``timeout`` Sekunden lang (Plan 2026-10-06-1811, KTD3).

    Der Queue-Handler bleibt installiert; der naechste Eintrag startet den
    Listener wieder. Ein folgendes ``configure_logging`` baut neu auf (KTD4).
    Was bis zur Frist nicht geschrieben ist, gilt als verworfen.
    """
    global _CONFIGURED, _APP_TAKEOVER_REPORTED
    _CONFIGURED = _APP_TAKEOVER_REPORTED = False
    _stop_listeners(timeout)


def _stop_listeners(timeout: float | None = None) -> None:
    """Leert und stoppt alle Listener des Prozesses, ohne den Eigentuemer
    freizugeben: erst alle schliessen, dann gemeinsam bis zur Frist warten
    (KTD3), damit ein haengender die anderen nicht aufhaelt."""
    seconds = _DRAIN_TIMEOUT if timeout is None else timeout
    deadline = Deadline(seconds)
    owners = {id(h.listener): h for h in list(_HANDLERS) if h.listener is not None}
    closing = [_close(listener, owners.get(id(listener))) for listener in list(_LISTENERS)]
    file_closing = close_queue_listeners()
    given_up = [d for d in closing if d is not None and not d.wait(deadline)]
    file_given_up: list[FileDraining] = []
    while file_closing:
        file_given_up += [d for d in file_closing if not d.wait(deadline)]
        # Ein Eintrag im Fenster zwischen Ende und Vergessen startet einen
        # neuen Thread (KTD1); auch er wird geschlossen, nicht verwaist.
        file_closing = close_queue_listeners()
    file_lost = sum(d.lost for d in file_given_up)
    count_dropped(file_lost)
    if given_up or file_given_up:
        write_drain_notice(
            sum(d.lost for d in given_up) + file_lost, seconds,
            json=any(d.handler is not None and d.handler.json for d in given_up),
        )


class _Draining:
    """Ein geschlossener Listener, dessen Thread seine alte Queue leert."""

    def __init__(
        self, listener: _SwarmqQueueListener, thread: threading.Thread,
        handler: _StructlogQueueHandler | None,
    ) -> None:
        self.listener = listener
        self.thread = thread
        self.queue = listener.queue
        self.handler = handler
        self.lost = 0

    def wait(self, deadline: Deadline) -> bool:
        """Bis zur Frist warten; danach aufgeben und den Rest zaehlen.
        ``False``, wenn aufgegeben wurde."""
        self.thread.join(deadline.remaining())
        return self.settle()

    def settle(self) -> bool:
        """Nach der Frist: Lebt der Thread noch, aufgeben und den Rest
        zaehlen. ``False``, wenn aufgegeben wurde."""
        if not self.thread.is_alive():
            return True
        self.lost = lost = self.queue.qsize()
        # Sofort schliessen: Der Rest ist verworfen, auch wenn der Thread
        # spaeter doch noch frei kommt -- die Zaehlung bleibt genau.
        self.queue.shutdown(immediate=True)
        count_dropped(lost)
        if self.handler is not None:
            self.handler.dropped += lost
            # Solange der alte Thread lebt, startet auf diesem Handler kein
            # neuer Listener: Im wiederverwendeten Compute-Kind haeuften sich
            # sonst haengende Threads an.
            self.handler._abandoned = self.thread
        abandon(self.thread, self.listener.targets())
        # Das naechste Leeren wartet nicht noch einmal auf ihn.
        _LISTENERS.discard(self.listener)
        return False

    def notify(self, timeout: float) -> None:
        """Abschlusszeile fuer ein aufgegebenes Leeren (KTD3)."""
        handler = self.handler
        write_drain_notice(self.lost, timeout, json=handler is not None and handler.json)


def _close(
    listener: _SwarmqQueueListener, handler: _StructlogQueueHandler | None,
    *, swap: bool = True,
) -> _Draining | None:
    """Schliesst die Queue eines laufenden Listeners, ohne zu blockieren
    (``Queue.shutdown``). Zuvor bekommt sein Handler eine frische Queue und
    einen frischen Listener: Records anderer Threads gehen waehrend des
    Leerens nicht verloren (KTD3)."""
    thread = listener._thread
    if thread is None or not thread.is_alive():
        return None
    if handler is not None and swap:
        handler._swap_queue()
    listener.queue.shutdown()
    return _Draining(listener, thread, handler)


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
    global _LISTENER_LOCK, _DROPPED_TOTAL, _IN_FORK_SOURCE
    _LISTENER_LOCK = threading.Lock()
    _DROPPED_TOTAL = 0
    # Das Kind ist keine Fork-Quelle mehr (KTD6): Der naechste Eintrag
    # startet seinen Listener.
    _IN_FORK_SOURCE = False
    _LISTENERS.clear()
    _CONFIG_LISTENERS.clear()
    for handler in list(_HANDLERS):
        handler._reset_after_fork()


os.register_at_fork(after_in_child=_after_fork_in_child)

# Genau einmal, beim Import, ohne Thread (KTD5): dictConfig und
# ``logging.shutdown`` stoppen keine Listener.
atexit.register(shutdown_logging)
