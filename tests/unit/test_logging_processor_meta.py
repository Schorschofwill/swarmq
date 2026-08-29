"""Formatter-interne Schluessel gehoeren nicht in die Ausgabe.

Plan 2026-08-28-001, U5 (R8, R9).

Die Prozessorkette des ``ProcessorFormatter`` entfernte die Schluessel
``_record`` und ``_from_structlog`` nicht vor dem Renderer. ``_record``
traegt den vollstaendigen LogRecord — dessen ``msg`` wiederum das komplette
Ereignis-Dict. Jede Zeile enthielt die Nutzlast damit zweimal: ueber einen
24h-Lauf die doppelte Logmenge, und von Hand kaum lesbar genau dann, wenn
man wegen eines Haengers hineinschaut.

Beide Renderer haengen an demselben Formatter — der Fix trifft JSON- und
Textausgabe zwangslaeufig zusammen (R9).
"""

import io
import json

import structlog

from swarmq.logging import setup_logging

_FORMATTER_INTERNAL_KEYS = ("_record", "_from_structlog")


def _capture(*, log_json: bool) -> str:
    """Laesst ein Ereignis durch den echten Aufbau laufen und gibt die
    gerenderte Zeile zurueck."""
    listener = setup_logging(log_level="INFO", log_json=log_json)
    try:
        stream = io.StringIO()
        listener.handlers[0].stream = stream  # type: ignore[attr-defined]
        structlog.get_logger().info("hello_event", foo="bar", count=3)
        listener.stop()
        return stream.getvalue().strip().split("\n")[-1]
    finally:
        if listener._thread is not None:
            listener.stop()


class TestJsonOutput:
    def test_payload_appears_exactly_once(self):
        """Covers AE4."""
        line = _capture(log_json=True)
        data = json.loads(line)

        for key in _FORMATTER_INTERNAL_KEYS:
            assert key not in data, f"{key} steht noch in der JSON-Ausgabe"

    def test_event_fields_survive_unchanged(self):
        """Der Fix darf nur die formatter-internen Schluessel entfernen,
        nicht die Nutzlast."""
        data = json.loads(_capture(log_json=True))

        assert data["event"] == "hello_event"
        assert data["foo"] == "bar"
        assert data["count"] == 3
        assert data["level"] == "info"
        assert "timestamp" in data

    def test_context_binding_still_reaches_the_output(self):
        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(task_id="t-42")
        try:
            data = json.loads(_capture(log_json=True))
        finally:
            structlog.contextvars.clear_contextvars()

        assert data["task_id"] == "t-42"


class TestConsoleOutput:
    def test_console_loses_the_same_keys(self):
        """R9: Beide Renderer teilen den Formatter — die Textausgabe
        verliert dieselben Schluessel, ihr Aufbau bleibt sonst gleich."""
        line = _capture(log_json=False)

        for key in _FORMATTER_INTERNAL_KEYS:
            assert key not in line, f"{key} steht noch in der Textausgabe"

    def test_console_keeps_event_and_fields(self):
        line = _capture(log_json=False)

        assert "hello_event" in line
        assert "foo" in line
        assert "bar" in line
