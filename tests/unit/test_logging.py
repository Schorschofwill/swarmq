"""Tests for swarmq.logging — structlog setup and URL sanitization."""

from __future__ import annotations

# Test list:
# 1. setup_logging() returns a QueueListener
# 2. sanitize_url with password -> password replaced with ***
# 3. sanitize_url without password -> unchanged
# 4. Log output is JSON when log_json=True
# 5. Log level filtering works (DEBUG not output at INFO level)
# 6. contextvars binding: bind task_id, verify it appears in log output
# 7./8. Fehlerfelder (R17): ueber die emittierten Events der Module geprueft,
#    nicht am privaten Helfer -- error = Text etwa in
#    ``process_manager/test_supervisor.py``, error = Klassenname bei leerer
#    Nachricht in ``worker/test_lifecycle.py`` (``scheduler_stop_failed``).
import io
import json
import logging
import logging.handlers

import structlog

from swarmq.logging import sanitize_url, setup_logging


def test_setup_logging_returns_queue_listener():
    listener = setup_logging()
    try:
        assert isinstance(listener, logging.handlers.QueueListener)
    finally:
        listener.stop()


def test_sanitize_url_replaces_password():
    result = sanitize_url("amqp://user:pass@host:5672/")
    assert result == "amqp://user:***@host:5672/"


def test_sanitize_url_without_password_unchanged():
    url = "amqp://host:5672/"
    assert sanitize_url(url) == url


def test_log_output_is_json_when_log_json_true():
    listener = setup_logging(log_level="INFO", log_json=True)
    try:
        # Replace the listener's handler stream with a StringIO to capture output
        stream = io.StringIO()
        handler = listener.handlers[0]
        handler.stream = stream

        logger = structlog.get_logger()
        logger.info("test_event", key="value")

        # Stop flushes the queue
        listener.stop()

        output = stream.getvalue().strip()
        line = output.split("\n")[-1]
        data = json.loads(line)
        assert data["event"] == "test_event"
        assert data["key"] == "value"
    finally:
        if listener._thread is not None:
            listener.stop()


def test_log_level_filtering_debug_not_output_at_info():
    listener = setup_logging(log_level="INFO", log_json=True)
    try:
        stream = io.StringIO()
        handler = listener.handlers[0]
        handler.stream = stream

        logger = structlog.get_logger()
        logger.debug("should_be_filtered")
        logger.info("should_appear")

        listener.stop()

        output = stream.getvalue().strip()
        assert "should_be_filtered" not in output
        assert "should_appear" in output
    finally:
        if listener._thread is not None:
            listener.stop()


def test_contextvars_binding_appears_in_log_output():
    listener = setup_logging(log_level="INFO", log_json=True)
    try:
        stream = io.StringIO()
        handler = listener.handlers[0]
        handler.stream = stream

        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(task_id="abc-123")

        logger = structlog.get_logger()
        logger.info("task_started")

        listener.stop()

        output = stream.getvalue().strip()
        data = json.loads(output.split("\n")[-1])
        assert data["task_id"] == "abc-123"
        assert data["event"] == "task_started"
    finally:
        structlog.contextvars.clear_contextvars()
        if listener._thread is not None:
            listener.stop()


class TestQueueDropVisibility:
    """Härtung R2 P2: volle Log-Queue droppte Records still (stdlib
    handleError-Pfad) — ein gesättigtes Log-Pipeline sah aus wie ein
    ruhiges. Drops werden gezählt und rate-limitiert auf stderr
    gemeldet."""

    def test_full_queue_increments_drop_counter(self, capsys):
        import logging as _logging
        import queue as _queue

        from swarmq.logging_queue import _StructlogQueueHandler

        q: _queue.Queue = _queue.Queue(maxsize=1)
        handler = _StructlogQueueHandler(q)
        rec = _logging.LogRecord(
            "test",
            _logging.INFO,
            __file__,
            1,
            "msg",
            None,
            None,
        )

        handler.enqueue(rec)  # fills the queue
        assert handler.dropped == 0
        handler.enqueue(rec)  # queue full → dropped
        handler.enqueue(rec)  # dropped again
        assert handler.dropped == 2

        # Seit Plan 2026-10-06-1811, U3 meldet der Listener die Drops,
        # der Aufrufer schreibt nie selbst auf stderr.
        assert capsys.readouterr().err == ""

    def test_normal_enqueue_unaffected(self):
        import logging as _logging
        import queue as _queue

        from swarmq.logging_queue import _StructlogQueueHandler

        q: _queue.Queue = _queue.Queue(maxsize=10)
        handler = _StructlogQueueHandler(q)
        rec = _logging.LogRecord(
            "test",
            _logging.INFO,
            __file__,
            1,
            "msg",
            None,
            None,
        )
        handler.enqueue(rec)
        assert handler.dropped == 0
        assert q.qsize() == 1


class _RaisingRepr:
    """Stellvertreter fuer ``aio_pika.tools.OneShotCallback`` (aio-pika 10),
    dessen ``__repr__`` wirft — ein fremder Record mit diesem Objekt in
    ``args`` liess den Formatter scheitern."""

    def __repr__(self) -> str:
        raise RuntimeError("repr exploded")


class TestUnformattableRecord:
    """Plan 2026-09-08-001, U7 (R27, KTD9): Scheitert die Formatierung eines
    fremden Records, rendert der Formatter ein Ersatz-Event statt einen
    Traceback ueber ``Handler.handleError`` auf stderr zu kippen.

    Test-Liste:
    1. JSON-Renderer: Zeile ``log_record_unformattable`` mit logger, level,
       error; kein Traceback auf stderr (AE12)
    2. Console-Renderer: dieselbe Zeile im Textformat
    3. Normale fremde Records rendern unveraendert
    """

    @staticmethod
    def _emit_foreign(*, log_json: bool, args: tuple) -> str:
        listener = setup_logging(log_level="INFO", log_json=log_json)
        try:
            stream = io.StringIO()
            listener.handlers[0].stream = stream  # type: ignore[attr-defined]
            logging.getLogger("aio_pika.robust_connection").warning("callback %r failed", *args)
            listener.stop()
            return stream.getvalue().strip().split("\n")[-1]
        finally:
            if listener._thread is not None:
                listener.stop()

    def test_json_renders_substitute_event_without_traceback(self, capsys):
        """Covers AE12."""
        line = self._emit_foreign(log_json=True, args=(_RaisingRepr(),))

        err = capsys.readouterr().err
        assert "Traceback" not in err
        assert "Logging error" not in err

        data = json.loads(line)
        assert data["event"] == "log_record_unformattable"
        assert data["logger"] == "aio_pika.robust_connection"
        assert data["level"] == "warning"
        assert "repr exploded" in data["error"]

    def test_console_renders_substitute_event_without_traceback(self, capsys):
        """R27: beide Renderer haengen an demselben Formatter."""
        line = self._emit_foreign(log_json=False, args=(_RaisingRepr(),))

        err = capsys.readouterr().err
        assert "Traceback" not in err
        assert "Logging error" not in err

        assert "log_record_unformattable" in line
        assert "aio_pika.robust_connection" in line
        assert "warning" in line
        assert "repr exploded" in line

    def test_normal_foreign_record_renders_unchanged(self, capsys):
        """Regression: der Schutz greift nur im Fehlerfall."""
        line = self._emit_foreign(log_json=True, args=("fine",))

        assert capsys.readouterr().err == ""
        data = json.loads(line)
        assert data["event"] == "callback 'fine' failed"
        assert data["level"] == "warning"
        assert "log_record_unformattable" not in line


class TestSingleListenerPerProcess:
    """U4/KTD8 (R21): genau ein ``QueueListener`` je Prozess.

    Das Worker-Kind konfiguriert das Logging fruehzeitig, damit ein
    Konfigurationsfehler als strukturierte Zeile erscheint; ``SwarmQ``
    konfiguriert danach erneut. Zwei laufende Listener waeren zwei Threads,
    die dieselbe Ausgabe bedienen wollen -- die Lehre aus B6.
    """

    def test_second_setup_stops_the_previous_listener(self):
        import logging as stdlib_logging

        from swarmq.logging import setup_logging

        first = setup_logging("INFO", True)
        try:
            second = setup_logging("INFO", True)
            assert first is not second
            assert first._thread is None, "der erste Listener laeuft noch"
            assert len(stdlib_logging.getLogger().handlers) == 1
        finally:
            second.stop()
