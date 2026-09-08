"""SwarmQ logging — structlog setup with non-blocking I/O."""

from __future__ import annotations

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


def setup_logging(
    log_level: str = "INFO", log_json: bool = True
) -> logging.handlers.QueueListener:
    """Configure structlog + stdlib logging with QueueHandler/QueueListener.

    Returns the QueueListener so it can be stopped on shutdown.
    """
    # Plan 2026-04-23-001 CLI-03: bind ``colors`` to the TTY state of
    # stderr at configure time. Keeps ANSI escapes out of piped /
    # captured stderr (log aggregators, CI output, pytest fixtures).
    # The decision is static — callers that redirect stderr AFTER
    # ``setup_logging`` must re-invoke it.
    renderer = (
        structlog.processors.JSONRenderer()
        if log_json
        else structlog.dev.ConsoleRenderer(colors=sys.stderr.isatty())
    )

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

    formatter = _SafeProcessorFormatter(
        processors=[
            # Ohne diesen Schritt rendert der Formatter seine eigenen
            # Arbeitsschluessel mit: ``_record`` traegt den LogRecord, dessen
            # ``msg`` das komplette Ereignis-Dict ist — die Nutzlast stand
            # damit zweimal in jeder Zeile. Ueber einen 24h-Lauf die doppelte
            # Logmenge, und von Hand kaum lesbar genau im Diagnosefall.
            # Muss VOR dem Renderer stehen; beide Renderer teilen diesen
            # Formatter, der Fix trifft JSON- und Textausgabe zusammen.
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
        foreign_pre_chain=shared_processors,
    )

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

    return listener


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
