"""SwarmQ logging — structlog setup with non-blocking I/O."""

from __future__ import annotations

import atexit
import contextlib
import contextvars
import json as jsonlib
import logging
import logging.config
import logging.handlers
import os
import queue
import sys
import threading
import time
import weakref
from typing import Any
from urllib.parse import urlparse, urlunparse

import structlog

from swarmq.exceptions import LoggingConfigError
from swarmq.logging_file import CONFIG_LISTENERS as _CONFIG_LISTENERS
from swarmq.logging_file import _iter_loggers, start_queue_listeners, stop_queue_listeners
from swarmq.logging_file import load_log_config as load_log_config
from swarmq.logging_format import _iso_utc
from swarmq.logging_format import build_formatter as build_formatter
from swarmq.logging_settings import LoggingSettings

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
                "logger": __name__,
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


def root_handler_snapshot() -> tuple[logging.Handler, ...]:
    """Die Root-Handler vor dem App-Import (Regel "App gewinnt", KTD4)."""
    return tuple(logging.getLogger().handlers)


def react_to_app_logging(before: tuple[logging.Handler, ...]) -> None:
    """Regel "App gewinnt" (Plan 2026-10-06-0831, U7, KTD4).

    Verglichen wird mit ``before`` aus :func:`root_handler_snapshot`, nach dem
    App-Import und noch einmal nach einem ``--configure``-Hook. Fehlt ein
    Handler von swarmq, hat die App ersetzt: swarmq stoppt seinen
    Root-Listener und meldet ``logging_reconfigured_by_app`` einmal. Hat eine
    dictConfig der App ``swarmq.*``-Logger stummgeschaltet, schaltet swarmq
    sie wieder ein und nennt die Zahl im selben Event.
    """
    global _APP_TAKEOVER_REPORTED
    if not _CONFIGURED:
        # Nichts eingerichtet (R12): das Logging gehoert ganz der App.
        return
    current = logging.getLogger().handlers
    replaced = any(handler not in current for handler in before)
    reenabled = _reenable_swarmq_loggers()
    if (replaced or reenabled) and not _APP_TAKEOVER_REPORTED:
        if replaced:
            _stop_listeners()
            _activate_swarmq_queue_after_app_takeover(**_queue_options(before))
        _APP_TAKEOVER_REPORTED = True
        get_logger(__name__).info("logging_reconfigured_by_app", reenabled_loggers=reenabled)
    added = [handler for handler in current if handler not in before]
    if added and not replaced:
        # Neben swarmqs Handler: beide schreiben -- swarmq entfernt nichts.
        get_logger(__name__).warning(
            "logging_handlers_added_by_app",
            handler_types=[type(handler).__name__ for handler in added],
        )


def _queue_options(handlers: tuple[logging.Handler, ...]) -> dict[str, Any]:
    """Format und Queue-Groesse aus swarmqs ersetztem Root-Handler."""
    for handler in handlers:
        if isinstance(handler, _StructlogQueueHandler):
            return {"json": handler.json, "queue_size": handler.queue.maxsize}
    return {"json": False, "queue_size": LoggingSettings().queue_size}


def configure_child_logging(settings: LoggingSettings) -> dict[str, Any] | None:
    """Einstiegspunkt Compute-Kind (Plan 2026-10-06-0831, U7, KTD4, KTD14).

    Wie ``configure_logging``, aber mit der Regel "App gewinnt": Root-Handler,
    die das Kind schon vorfindet, stammen aus dem Vorladen der App im
    Forkserver -- swarmq richtet dort nie etwas ein. Sie bleiben, und die
    swarmq-Logs laufen wie im Worker an die Handler der App.

    Liefert dann die Felder fuer ``logging_reconfigured_by_app``, sonst
    ``None``. Das Event schreibt nicht das Kind, sondern der Pool, einmal je
    Pool statt einmal je Kind.
    """
    global _CONFIGURED, _APP_TAKEOVER_REPORTED
    if not settings.configure or not root_handler_snapshot():
        configure_logging(settings)
        return None
    _CONFIGURED = _APP_TAKEOVER_REPORTED = True
    if settings.log_config is None:
        # Wie im Worker, der structlog vor dem App-Import einrichtet: Nutzer-
        # Tasks loggen per structlog an die Handler der App (R10, KTD6).
        _configure_structlog_for_user_code()
    # Ausdrueckliche Level je Logger gelten weiter; Root-Handler und
    # Root-Level gehoeren der App.
    _apply_named_levels(settings)
    _activate_swarmq_queue_after_app_takeover(
        json=settings.format == "json", queue_size=settings.queue_size
    )
    return {"reenabled_loggers": _reenable_swarmq_loggers()}


def _activate_swarmq_queue_after_app_takeover(*, json: bool, queue_size: int) -> None:
    """Nach dem Ersatz durch die App laufen die swarmq-Logs ueber die
    Opt-in-Queue am Logger ``swarmq`` an die Handler der App (KTD4, KTD10):
    Ein blockierender App-Handler haelt den Event-Loop nicht auf (R6). Das
    ``swarmq``-Level bleibt, wie es ist -- die App hat entschieden.
    """
    global _OPTIN_TAKEOVER
    _OPTIN_TAKEOVER = True
    _install_log_queue(None, json=json, queue_size=queue_size)


def _reenable_swarmq_loggers() -> int:
    """Schaltet von der App deaktivierte ``swarmq.*``-Logger wieder ein."""
    count = 0
    for name, lg in _iter_loggers():
        if lg.disabled and (name == "swarmq" or name.startswith("swarmq.")):
            lg.disabled = False
            count += 1
    return count


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


_RESERVED_RECORD_KEYS = frozenset(
    (
        *logging.LogRecord("", 0, "", 0, "", None, None).__dict__,
        "message",
        "asctime",
    )
) - frozenset(structlog.stdlib.LOG_KWARG_NAMES)
"""Felder, die ``Logger.makeRecord`` als ``extra`` ablehnt (``KeyError``).
``exc_info``/``stack_info``/``stacklevel`` sind ausgenommen: Als Aufruf-
kwargs gehen sie bestimmungsgemaess an die stdlib-Logmethode."""


def _guard_reserved_keys(
    _: Any, __: str, event_dict: structlog.typing.EventDict
) -> structlog.typing.EventDict:
    """Benennt LogRecord-Attributnamen in ``swarmq_<key>`` um.

    Ohne das wirft ``log.info("evt", name=...)`` im stdlib-``makeRecord``
    einen ``KeyError`` statt zu loggen.
    """
    for key in _RESERVED_RECORD_KEYS.intersection(event_dict):
        event_dict[f"swarmq_{key}"] = event_dict.pop(key)
    return event_dict


def _merge_contextvars_guarded(
    logger: Any, method_name: str, event_dict: structlog.typing.EventDict
) -> structlog.typing.EventDict:
    """``merge_contextvars`` mit Schutz der stdlib-Logkwargs.

    Eine Kontextvariable ``exc_info``/``stack_info``/``stacklevel`` ist ein
    Feld der App, kein Argument dieses Aufrufs: Sie wird als
    ``swarmq_<key>`` gemergt, sonst laege sie ``render_to_log_kwargs`` als
    kwarg an die stdlib-Methode. Felder des Aufrufs haben wie bei
    ``merge_contextvars`` Vorrang.
    """
    for key, value in structlog.contextvars.get_contextvars().items():
        if key in structlog.stdlib.LOG_KWARG_NAMES:
            key = f"swarmq_{key}"
        event_dict.setdefault(key, value)
    return event_dict


_CALLER_STACKLEVEL = 4
"""Abstand von ``Logger._log`` zum Aufrufer: ``BoundLogger.<methode>`` ->
``BoundLogger._proxy_to_logger`` -> ``BoundLoggerBase._proxy_to_logger``
-> stdlib-Logmethode (ermittelt fuer structlog 26.1)."""


def _set_caller_stacklevel(
    logger: Any, __: str, event_dict: structlog.typing.EventDict
) -> structlog.typing.EventDict:
    """Laesst ``funcName``/``lineno`` auf den Aufrufer zeigen statt auf
    structlogs Weiterleitung.

    Nur fuer Logger mit dem ``findCaller`` der stdlib: Hat die App per
    ``structlog.stdlib.LoggerFactory`` die Logger-Klasse umgestellt,
    ueberspringt deren ``findCaller`` die structlog-Frames schon selbst,
    und ein zusaetzlicher ``stacklevel`` schoesse ueber den Aufrufer hinaus.
    """
    if getattr(logger.findCaller, "__func__", None) is logging.Logger.findCaller:
        event_dict["stacklevel"] = _CALLER_STACKLEVEL
    return event_dict


LOGGER_PROCESSORS: list[Any] = [
    structlog.stdlib.filter_by_level,
    _merge_contextvars_guarded,
    _guard_reserved_keys,
    _set_caller_stacklevel,
    structlog.stdlib.render_to_log_kwargs,
]
"""Aufruferseitige Prozessorkette der Bibliotheks-Logger (Plan
2026-10-06-0831, KTD2). Nur billige Schritte; gerendert wird erst im
Handler der App bzw. im Listener-Thread."""


class _LazyStdlibLogger:
    """Holt den stdlib-Logger erst beim ersten Logaufruf (KTD1).

    Beim Modulimport angelegt, bliebe der Logger ein "existing logger", den
    eine dictConfig der App mit dem Default ``disable_existing_loggers=True``
    stumm schaltet. Attributzugriffe (``isEnabledFor``, ``info`` ...) gehen
    an den echten Logger; der liefert gebundene Methoden, also entsteht
    kein zusaetzlicher Stack-Frame.
    """

    __slots__ = ("_logger", "_name")

    def __init__(self, name: str) -> None:
        self._name = name
        self._logger: logging.Logger | None = None

    def __getattr__(self, attr: str) -> Any:
        logger = self._logger
        if logger is None:
            logger = self._logger = logging.getLogger(self._name)
        return getattr(logger, attr)


def get_logger(name: str) -> Any:
    """Liefert den Bibliotheks-Logger ``name`` (Plan 2026-10-06-0831, U1).

    KTD1: ``structlog.wrap_logger`` mit expliziten ``processors`` und
    ``wrapper_class`` -- ein ``structlog.configure`` der App greift damit
    nicht. Uebergeben wird ans Standard-``logging``: Der Event-Name wird
    zur Nachricht, alle Felder werden zu LogRecord-Attributen (R2, R3).
    """
    return structlog.wrap_logger(
        _LazyStdlibLogger(name),
        processors=LOGGER_PROCESSORS,
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


def _error_fields(exc: BaseException) -> dict[str, str]:
    """Einheitliches Fehlerschema fuer Logeintraege (R17, KTD11):
    ``error_type`` ist der Klassenname, ``error`` der Text -- bei leerer
    Nachricht (``TimeoutError()``) ebenfalls der Klassenname."""
    error_type = type(exc).__name__
    return {"error_type": error_type, "error": str(exc) or error_type}


def _task_log_context(**fields: Any) -> contextlib.AbstractContextManager[None]:
    """Bindet den Task-Kontext fuer einen Block (Plan 2026-10-06-0831, KTD13).

    Beim Verlassen werden die vorigen Werte wiederhergestellt; Variablen
    der App bleiben unberuehrt. ``None``-Werte entfallen: ein fehlendes
    ``workflow_id`` fehlt im Eintrag, statt als ``None`` zu erscheinen.
    """
    return structlog.contextvars.bound_contextvars(
        **{key: value for key, value in fields.items() if value is not None}
    )


def configure_logging(settings: LoggingSettings) -> None:
    """Richtet das Logging eines swarmq-Prozesses ein (Plan 2026-10-06-0831,
    U6, KTD4).

    Nur fuer Einstiegspunkte: Supervisor, Worker-Subprozess, Compute-Kind.
    Die Bibliothek selbst ruft das nie auf. Drei Modi:

    - eingebaut: Root schreibt ueber Queue und Listener nach stderr, und
      ``structlog.get_logger()`` im Nutzercode rendert genauso (R9, R10);
    - Datei (``log_config``): die dictConfig der App (R11, KTD9);
    - aus (``configure=False``): nichts (R12).

    Zuletzt gelten die Level je Logger-Name. Ein zweiter Aufruf im selben
    Prozess ist ein No-op mit DEBUG-Event; neu aufgebaut wird nur ueber
    ``shutdown_logging()`` und danach ``configure_logging()``.

    Raises:
        LoggingConfigError: Die Konfigurationsdatei ist nicht ladbar oder
            ungueltig; ``path`` nennt sie, ``__cause__`` die Ursache.
    """
    global _CONFIGURED
    if not settings.configure:
        # "Logging nicht konfigurieren" (R12): die App ist zustaendig.
        return
    if _CONFIGURED:
        get_logger(__name__).debug("logging_already_configured")
        return
    # KTD10: Eingebaut und per Datei ist der Opt-in wirkungslos. Ein bereits
    # aktiver -- etwa aus dem Vorladen im Forkserver geerbt -- wird
    # aufgehoben, ``swarmq`` wiederhergestellt.
    _reset_log_queue()
    if settings.log_config is not None:
        _configure_from_file(settings)
    else:
        _configure_builtin(settings)
    _apply_logger_levels(settings)
    _CONFIGURED = True


def _configure_from_file(settings: LoggingSettings) -> None:
    """Datei-Modus: die dictConfig der App statt der eingebauten Einrichtung
    (R11). structlog bleibt unkonfiguriert (KTD6)."""
    assert settings.log_config is not None
    # Vom Supervisor geprueft und weitergegeben (KTD9): Kinder lesen die
    # Datei nie.
    raw = settings.log_config_json or load_log_config(settings.log_config)
    try:
        logging.config.dictConfig(jsonlib.loads(raw))
    except Exception as exc:
        # dictConfig meldet Tippfehler als ValueError, TypeError,
        # AttributeError oder ImportError -- alle mit Pfad nach oben (AE4).
        raise LoggingConfigError(settings.log_config, exc) from exc
    start_queue_listeners()


def _apply_logger_levels(settings: LoggingSettings) -> None:
    """Level je Logger-Name (``--logger-level``) -- zuletzt, damit sie auch
    nach einer dictConfig gelten (KTD8). Ein ausdrueckliches Level gilt dort
    auch fuer Root; ein Default ueberschriebe das Level aus der Datei."""
    if settings.log_config is not None and settings.level_explicit:
        logging.getLogger().setLevel(settings.level)
    _apply_named_levels(settings)


def _apply_named_levels(settings: LoggingSettings) -> None:
    for name, level in settings.logger_levels:
        logging.getLogger(name).setLevel(level)


def _configure_builtin(settings: LoggingSettings) -> None:
    """Eingebauter Modus: Root schreibt ueber Queue und Listener nach stderr."""
    log_json = settings.format == "json"
    _configure_structlog_for_user_code()

    # Non-blocking I/O via QueueHandler + QueueListener
    log_queue = _LogQueue(maxsize=settings.queue_size)

    # Plan 2026-04-23-001 CLI-03: bind ``colors`` to the TTY state of
    # stderr at configure time. Keeps ANSI escapes out of piped /
    # captured stderr (log aggregators, CI output, pytest fixtures).
    # The decision is static — callers that redirect stderr AFTER
    # configuring must rebuild.
    formatter = build_formatter(log_json, sys.stderr.isatty())

    stream_handler = logging.StreamHandler(sys.stderr)
    stream_handler.setFormatter(formatter)

    # Der Listener startet lazy beim ersten Eintrag (KTD5).
    queue_handler = _StructlogQueueHandler(log_queue, stream_handler, json=log_json)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(queue_handler)
    root_logger.setLevel(settings.level)


def _configure_structlog_for_user_code() -> None:
    """``structlog.get_logger()`` in Nutzer-Tasks rendert ueber dieselbe
    Ausgabe wie swarmq (R10, KTD6).

    Dieselbe schlanke Kette wie die Bibliotheks-Logger: uebergeben wird ans
    Standard-``logging``, gerendert erst im Listener-Thread. Eine bereits
    bestehende Konfiguration -- etwa einer im Forkserver vorgeladenen App --
    bleibt unangetastet.
    """
    if structlog.is_configured():
        return
    structlog.configure(
        processors=LOGGER_PROCESSORS,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


def setup_logging(
    log_level: str = "INFO", log_json: bool = True
) -> logging.handlers.QueueListener:
    """Deprecated: Einstiegspunkte rufen ``configure_logging`` auf (U6).

    Baut bei jedem Aufruf neu auf -- ``shutdown_logging()`` und danach
    ``configure_logging()`` im eingebauten Modus -- und liefert den Listener.
    Ein zweiter Aufruf stoppt damit den Listener des ersten (B6: doppelte
    Logzeilen bei zwei Listenern). Wie bisher wird structlog dabei immer neu
    konfiguriert, auch ueber eine bestehende Konfiguration hinweg.
    """
    shutdown_logging()
    structlog.reset_defaults()
    configure_logging(
        LoggingSettings(level=log_level, format="json" if log_json else "human")
    )
    handler = logging.getLogger().handlers[0]
    assert isinstance(handler, _StructlogQueueHandler) and handler.listener is not None
    return handler.listener


flush_logging = shutdown_logging
"""Deprecated: Alias fuer ``shutdown_logging`` (Plan 2026-10-06-0831, U3).

Vor einem ``SystemExit`` noetig: die Logzeile liegt bis dahin nur in der
Queue, und der Listener-Thread kommt nicht mehr dazu, sie zu schreiben."""


def sanitize_url(url: str) -> str:
    """Replace passwords in URLs with '***'."""
    parsed = urlparse(url)
    if parsed.password:
        # Replace password in netloc
        replaced = parsed._replace(
            netloc=parsed.netloc.replace(f":{parsed.password}@", ":***@", 1)
        )
        return urlunparse(replaced)
    return url
