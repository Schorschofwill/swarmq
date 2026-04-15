"""SwarmQ logging — structlog setup with non-blocking I/O."""

from __future__ import annotations

import logging
import logging.handlers
import queue
import sys
from urllib.parse import urlparse, urlunparse

import structlog


class _StructlogQueueHandler(logging.handlers.QueueHandler):
    """QueueHandler that preserves structlog's dict-based record.msg.

    The default QueueHandler.prepare() calls self.format(record) which
    stringifies record.msg. structlog's ProcessorFormatter needs
    record.msg to remain a dict (set by wrap_for_formatter).
    """

    def prepare(self, record: logging.LogRecord) -> logging.LogRecord:
        """Pass the record through without formatting."""
        return record


def setup_logging(
    log_level: str = "INFO", log_json: bool = True
) -> logging.handlers.QueueListener:
    """Configure structlog + stdlib logging with QueueHandler/QueueListener.

    Returns the QueueListener so it can be stopped on shutdown.
    """
    renderer = (
        structlog.processors.JSONRenderer()
        if log_json
        else structlog.dev.ConsoleRenderer()
    )

    shared_processors: list = [
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
    log_queue: queue.Queue = queue.Queue(maxsize=10_000)

    formatter = structlog.stdlib.ProcessorFormatter(
        processors=[renderer],
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
