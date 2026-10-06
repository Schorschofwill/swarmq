"""SwarmQ logging — structlog setup with non-blocking I/O."""

from __future__ import annotations

import contextlib
import datetime
import logging
import logging.handlers
import queue
import sys
import time
from typing import Any
from urllib.parse import urlparse, urlunparse

import structlog


class _StructlogQueueHandler(logging.handlers.QueueHandler):
    """QueueHandler that preserves structlog's dict-based record.msg.

    The default QueueHandler.prepare() calls self.format(record) which
    stringifies record.msg. structlog's ProcessorFormatter needs
    record.msg to remain a dict (set by wrap_for_formatter).

    Drops are counted and surfaced. The stdlib path
    (queue.Full → handleError) silently discarded records under burst —
    a saturated log pipeline looked identical to a quiet one. ``dropped``
    is the cumulative counter; a rate-limited stderr notice (at most one
    per second) keeps the drop visible without amplifying the burst.
    """

    _DROP_NOTICE_INTERVAL_S = 1.0

    def __init__(self, queue_: queue.Queue[logging.LogRecord]) -> None:
        super().__init__(queue_)
        self.dropped = 0
        self._last_drop_notice = 0.0

    def prepare(self, record: logging.LogRecord) -> logging.LogRecord:
        """Pass the record through without formatting."""
        return record

    def enqueue(self, record: logging.LogRecord) -> None:
        try:
            self.queue.put_nowait(record)
        except queue.Full:
            # ``dropped += 1`` is deliberately lock-free: under
            # multi-thread contention it may undercount — acceptable for
            # an observability counter; a lock here would serialize the
            # hot logging path exactly when it is saturated.
            self.dropped += 1
            now = time.monotonic()
            if now - self._last_drop_notice >= self._DROP_NOTICE_INTERVAL_S:
                self._last_drop_notice = now
                # stderr directly — going through logging would feed the
                # very queue that is full.
                print(
                    "swarmq: logging queue full — "
                    f"{self.dropped} record(s) dropped so far",
                    file=sys.stderr,
                )


class _SafeProcessorFormatter(structlog.stdlib.ProcessorFormatter):
    """ProcessorFormatter, der an fremden Records nicht scheitert.

    Plan 2026-09-08-001, U7 (R27, KTD9). ``format`` ruft fuer fremde
    Records ``record.getMessage()`` — ``msg % args``. Wirft dabei ein
    ``__repr__`` (so ``aio_pika.tools.OneShotCallback`` in aio-pika 10),
    landete die Ausnahme in ``Handler.handleError`` und damit als
    Traceback auf stderr, mitten im Logstrom. Stattdessen rendert dieser
    Formatter ein Ersatz-Event ``log_record_unformattable`` mit
    ``logger``, ``level`` und ``error`` durch dieselbe Prozessorkette und
    denselben Renderer wie jede andere Zeile.
    """

    def format(self, record: logging.LogRecord) -> str:
        try:
            return super().format(record)
        except Exception as exc:
            return super().format(self._substitute_record(record, exc))

    def _substitute_record(
        self, record: logging.LogRecord, exc: Exception
    ) -> logging.LogRecord:
        """Baut einen structlog-nativen Record mit dem Ersatz-Event.

        Die ``foreign_pre_chain`` (Level, Zeitstempel) laeuft hier von
        Hand, weil der Ersatz-Record den structlog-Pfad von ``format``
        nimmt — dort laeuft sie nicht mehr.
        """
        logger = logging.getLogger(record.name)
        meth_name = record.levelname.lower()
        ed: Any = {
            "event": "log_record_unformattable",
            "logger": record.name,
            "error": f"{type(exc).__name__}: {exc}",
        }
        for proc in self.foreign_pre_chain or ():
            ed = proc(logger, meth_name, ed)
        args, kwargs = structlog.stdlib.ProcessorFormatter.wrap_for_formatter(
            logger, meth_name, ed
        )
        substitute = logging.LogRecord(
            record.name,
            record.levelno,
            record.pathname,
            record.lineno,
            args[0],
            None,
            None,
        )
        substitute.__dict__.update(kwargs["extra"])
        return substitute


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
    context = structlog.contextvars.merge_contextvars(logger, method_name, {})
    for key, value in context.items():
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


def _add_record_timestamp(
    _: Any, __: str, event_dict: structlog.typing.EventDict
) -> structlog.typing.EventDict:
    """Zeitstempel aus ``record.created`` statt aus dem Formatierungszeitpunkt.

    Gerendert wird im Listener-Thread; staut sich die Queue, laege die Zeit
    sonst um den Rueckstau daneben (KTD3). Format wie
    ``TimeStamper(fmt="iso")``: ISO 8601 in UTC mit Suffix ``Z``. Ohne
    ``_record`` (Ersatz-Event aus ``_SafeProcessorFormatter``) gilt die
    aktuelle Zeit.
    """
    record = event_dict.get("_record")
    created = record.created if record is not None else time.time()
    event_dict["timestamp"] = (
        datetime.datetime.fromtimestamp(created, tz=datetime.UTC)
        .isoformat()
        .replace("+00:00", "Z")
    )
    return event_dict


_EXTRA_ADDER = structlog.stdlib.ExtraAdder()

_FORMATTER_RECORD_ATTRS = ("message", "asctime")
"""Setzt ``logging.Formatter.format`` auf dem Record; ein frueherer Handler
hinterlaesst sie. Felder eines swarmq-Events sind sie nie: ``get_logger``
benennt gleichnamige Felder in ``swarmq_<key>`` um."""


def _add_record_extras(
    logger: Any, method_name: str, event_dict: structlog.typing.EventDict
) -> structlog.typing.EventDict:
    """``ExtraAdder`` ohne die Streufelder ``message``/``asctime`` (KTD3)."""
    event_dict = _EXTRA_ADDER(logger, method_name, event_dict)
    for key in _FORMATTER_RECORD_ATTRS:
        event_dict.pop(key, None)
    return event_dict


def build_formatter(json: bool, colors: bool) -> structlog.stdlib.ProcessorFormatter:
    """Liefert den Formatter fuer swarmq- und Fremd-Records (Plan
    2026-10-06-0831, U2, KTD3).

    ``json`` waehlt den ``JSONRenderer``, sonst rendert der
    ``ConsoleRenderer`` mit ANSI-Farben nur bei ``colors``. Records der
    Bibliotheks-Logger (``get_logger``) tragen ihre Felder als Attribute und
    laufen wie Fremd-Records durch die ``foreign_pre_chain``; structlog-native
    Records (``wrap_for_formatter``) bringen Level und Zeitstempel schon mit.
    Scheitert die Formatierung eines Records, erscheint
    ``log_record_unformattable`` (R8). In einer dictConfig nutzbar per
    ``{"()": "swarmq.logging.build_formatter", "json": true, "colors": false}``.
    """
    renderer: Any = (
        structlog.processors.JSONRenderer()
        if json
        else structlog.dev.ConsoleRenderer(colors=colors)
    )
    return _SafeProcessorFormatter(
        # Ohne ``remove_processors_meta`` rendert der Formatter seine
        # eigenen Arbeitsschluessel mit (B6): ``_record`` traegt den
        # LogRecord, die Nutzlast stand damit zweimal in jeder Zeile. Ueber
        # einen 24h-Lauf die doppelte Logmenge, und von Hand kaum lesbar
        # genau im Diagnosefall. Muss VOR dem Renderer stehen; beide
        # Renderer teilen diese Kette.
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
        foreign_pre_chain=[
            structlog.processors.add_log_level,
            structlog.stdlib.add_logger_name,
            _add_record_timestamp,
            _add_record_extras,
            structlog.processors.format_exc_info,
        ],
    )


_ACTIVE_LISTENER: logging.handlers.QueueListener | None = None


def setup_logging(
    log_level: str = "INFO", log_json: bool = True
) -> logging.handlers.QueueListener:
    """Configure structlog + stdlib logging with QueueHandler/QueueListener.

    Returns the QueueListener so it can be stopped on shutdown.

    Ein zweiter Aufruf im selben Prozess stoppt den Listener des ersten
    (U4/KTD8): Das Worker-Kind konfiguriert fruehzeitig, damit ein
    Konfigurationsfehler als strukturierte Zeile erscheint, und ``SwarmQ``
    konfiguriert danach erneut. Ohne das Stoppen blieben zwei Threads am
    Leben, von denen einer nie wieder etwas zu tun bekommt -- die Lehre aus
    B6 (doppelte Logzeilen bei zwei Listenern).
    """
    global _ACTIVE_LISTENER
    if _ACTIVE_LISTENER is not None:
        with contextlib.suppress(Exception):
            _ACTIVE_LISTENER.stop()
        _ACTIVE_LISTENER = None
    shared_processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.format_exc_info,
    ]

    # Reset any cached loggers from previous configure() calls
    structlog.reset_defaults()

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, log_level)
        ),
        cache_logger_on_first_use=True,
    )

    # Non-blocking I/O via QueueHandler + QueueListener
    log_queue: queue.Queue[logging.LogRecord] = queue.Queue(maxsize=10_000)

    # Plan 2026-04-23-001 CLI-03: bind ``colors`` to the TTY state of
    # stderr at configure time. Keeps ANSI escapes out of piped /
    # captured stderr (log aggregators, CI output, pytest fixtures).
    # The decision is static — callers that redirect stderr AFTER
    # ``setup_logging`` must re-invoke it.
    formatter = build_formatter(log_json, sys.stderr.isatty())

    stream_handler = logging.StreamHandler(sys.stderr)
    stream_handler.setFormatter(formatter)

    queue_handler = _StructlogQueueHandler(log_queue)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(queue_handler)
    root_logger.setLevel(getattr(logging, log_level))

    listener = logging.handlers.QueueListener(
        log_queue, stream_handler, respect_handler_level=False
    )
    listener.start()
    _ACTIVE_LISTENER = listener

    return listener


def flush_logging() -> None:
    """Stoppt den aktiven ``QueueListener`` und leert damit die Log-Queue.

    Vor einem ``SystemExit`` noetig: die Logzeile liegt bis dahin nur in der
    Queue, und der Listener-Thread kommt nicht mehr dazu, sie zu schreiben.
    Genau darum stand der Konfigurationsfehler frueher als synchrone
    stderr-Zeile da -- der strukturierte Logsatz (R21) braucht dieses Flush.
    """
    global _ACTIVE_LISTENER
    if _ACTIVE_LISTENER is not None:
        with contextlib.suppress(Exception):
            _ACTIVE_LISTENER.stop()
        _ACTIVE_LISTENER = None


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
