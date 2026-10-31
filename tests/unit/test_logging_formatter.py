"""Tests fuer ``swarmq.logging.build_formatter`` -- den oeffentlichen Formatter.

Plan 2026-10-06-0831, U2 (R3, R8, R9; KTD3).
"""

from __future__ import annotations

# Test list:
# 1. Ein swarmq-Record ergibt eine JSON-Zeile mit ``event``, ``level``,
#    ``logger``, ``timestamp`` und allen Feldern.
# 2. Der Zeitstempel entspricht ``record.created``, nicht dem
#    Formatierungszeitpunkt (Record mit fixiertem ``created``), im Format
#    von ``TimeStamper(fmt="iso")`` (UTC, Suffix ``Z``).
# 3. Ein Record mit ``exc_info`` liefert in JSON einen Schluessel
#    ``exception`` mit Traceback.
# 4. Dasselbe im Text: ein Traceback-Block.
# 5. Ein Record, den ein anderer Formatter vorher formatiert hat (gesetztes
#    ``message``/``asctime``), erzeugt keine Felder ``message``/``asctime``.
# 6. Ein Fremd-Record mit werfendem ``__repr__`` ergibt
#    ``log_record_unformattable`` (Regression AE12).
# 7. ``_record`` und ``_from_structlog`` erscheinen weder in JSON noch im
#    Text (Regression B6).
# 8. Der Textmodus mit ``colors=False`` enthaelt kein ANSI, mit
#    ``colors=True`` schon.
# 9. Die Factory ist per dictConfig-``"()"`` nutzbar.
# 10. ``setup_logging`` rendert Records der Bibliotheks-Logger (U1) mit
#     allen Feldern (ueber ``build_formatter``).
# 12. Das Ersatz-Event traegt die Vorlage ``msg`` und je Argument ``str()``,
#     bei werfendem ``str`` den Typnamen in spitzen Klammern (E-090).
# 13. Dasselbe fuer Mapping-Argumente (``%(name)s``).
# 14. Eine nicht darstellbare Vorlage ohne Argumente: Typname, ``args`` leer.
# 15. ``args`` als Einzelwert statt Tupel (Filter, eigener LogRecord): ein
#     Element, das Ersatz-Event scheitert nicht selbst.
# 11. structlog-native Records aus der globalen Konfiguration von
#     ``setup_logging`` erscheinen weiter genau einmal, ohne doppelte
#     ``timestamp``/``level`` (B6).
import io
import json
import logging
import logging.config
from collections.abc import Callable, Iterator
from typing import override

import pytest
import structlog

from swarmq.logging import build_formatter, get_logger, setup_logging


@pytest.fixture
def logger_name(request: pytest.FixtureRequest) -> Iterator[str]:
    """Eindeutiger stdlib-Logger je Test, ohne Propagation an Root."""
    name = f"swarmq.tests.{request.node.name}"
    app_logger_class = logging.getLoggerClass()
    logging.setLoggerClass(logging.Logger)
    try:
        stdlib_logger = logging.getLogger(name)
    finally:
        logging.setLoggerClass(app_logger_class)
    stdlib_logger.setLevel(logging.DEBUG)
    stdlib_logger.propagate = False
    yield name
    stdlib_logger.handlers.clear()
    stdlib_logger.setLevel(logging.NOTSET)
    stdlib_logger.propagate = True


def _render(logger_name: str, formatter: logging.Formatter) -> io.StringIO:
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(formatter)
    logging.getLogger(logger_name).addHandler(handler)
    return stream


def test_swarmq_record_renders_as_json_with_all_fields(logger_name: str) -> None:
    stream = _render(logger_name, build_formatter(json=True, colors=False))

    get_logger(logger_name).info("hello_event", foo="bar", count=3)

    data = json.loads(stream.getvalue())
    assert data["event"] == "hello_event"
    assert data["level"] == "info"
    assert data["logger"] == logger_name
    assert data["foo"] == "bar"
    assert data["count"] == 3
    assert "timestamp" in data


def test_timestamp_comes_from_record_created() -> None:
    record = logging.makeLogRecord(
        {"name": "swarmq.x", "levelno": logging.INFO, "levelname": "INFO", "msg": "evt", "created": 0.25}
    )

    data = json.loads(build_formatter(json=True, colors=False).format(record))

    assert data["timestamp"] == "1970-01-01T00:00:00.250000Z"


def _raise_and_log(logger_name: str) -> None:
    try:
        raise ValueError("boom")
    except ValueError:
        get_logger(logger_name).exception("task_crashed", task_id="t-1")


def test_exc_info_renders_traceback_key_in_json(logger_name: str) -> None:
    stream = _render(logger_name, build_formatter(json=True, colors=False))

    _raise_and_log(logger_name)

    data = json.loads(stream.getvalue())
    assert data["event"] == "task_crashed"
    assert "Traceback" in data["exception"]
    assert "ValueError: boom" in data["exception"]


def test_exc_info_renders_traceback_block_in_text(logger_name: str) -> None:
    stream = _render(logger_name, build_formatter(json=False, colors=False))

    _raise_and_log(logger_name)

    output = stream.getvalue()
    first_line, _, block = output.partition("\n")
    assert "task_crashed" in first_line
    assert "Traceback" in block
    assert "ValueError: boom" in block


def test_record_formatted_before_has_no_message_or_asctime(
    logger_name: str,
) -> None:
    """Ein frueherer Handler mit ``%(asctime)s %(message)s`` setzt beide
    Attribute auf dem Record; sie sind keine Felder des Events."""
    earlier = logging.StreamHandler(io.StringIO())
    earlier.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
    logging.getLogger(logger_name).addHandler(earlier)
    stream = _render(logger_name, build_formatter(json=True, colors=False))

    get_logger(logger_name).info("hello_event", foo="bar")

    data = json.loads(stream.getvalue())
    assert "message" not in data
    assert "asctime" not in data
    assert data["foo"] == "bar"


class _RaisingRepr:
    """Stellvertreter fuer ``aio_pika.tools.OneShotCallback`` (aio-pika 10)."""

    @override
    def __repr__(self) -> str:
        raise RuntimeError("repr exploded")


def test_unformattable_foreign_record_renders_substitute_event() -> None:
    """Regression AE12 (R8). Direkt formatiert: Ueber einen Logger liefe der
    Record auch durch pytests Capture-Handler, der am selben ``__repr__``
    scheitert."""
    record = logging.makeLogRecord(
        {
            "name": "aio_pika.robust_connection",
            "levelno": logging.WARNING,
            "levelname": "WARNING",
            "msg": "callback %r failed",
            "args": (_RaisingRepr(),),
        }
    )

    line = build_formatter(json=True, colors=False).format(record)

    data = json.loads(line)
    assert data["event"] == "log_record_unformattable"
    assert data["logger"] == "aio_pika.robust_connection"
    assert data["level"] == "warning"
    assert "repr exploded" in data["error"]
    assert data["timestamp"].endswith("Z")


@pytest.mark.entscheidung("E-090")
def test_substitute_event_keeps_template_and_readable_args() -> None:
    """aio-pika loggt ``"Callback %r error: %s: %s"`` mit dem Callback, dem
    Ausnahmetyp und dem Text; nur der Callback ist nicht darstellbar."""
    record = logging.makeLogRecord(
        {
            "name": "aio_pika.tools",
            "levelno": logging.ERROR,
            "levelname": "ERROR",
            "msg": "Callback %r error: %s: %s",
            "args": (_RaisingRepr(), "ConnectionResetError", "peer reset"),
        }
    )

    data = json.loads(build_formatter(json=True, colors=False).format(record))

    assert data["event"] == "log_record_unformattable"
    assert data["msg"] == "Callback %r error: %s: %s"
    assert data["args"] == ["<_RaisingRepr>", "ConnectionResetError", "peer reset"]


@pytest.mark.entscheidung("E-090")
def test_substitute_event_keeps_mapping_args_by_name() -> None:
    record = logging.makeLogRecord(
        {
            "name": "aio_pika.tools",
            "levelno": logging.ERROR,
            "levelname": "ERROR",
            "msg": "%(cb)r failed with %(error)s",
            "args": {"cb": _RaisingRepr(), "error": "boom"},
        }
    )

    data = json.loads(build_formatter(json=True, colors=False).format(record))

    assert data["event"] == "log_record_unformattable"
    assert data["args"] == {"cb": "<_RaisingRepr>", "error": "boom"}


@pytest.mark.entscheidung("E-090")
def test_substitute_event_names_the_type_of_an_unprintable_template() -> None:
    record = logging.makeLogRecord(
        {"name": "thirdparty", "levelno": logging.WARNING, "levelname": "WARNING", "msg": _RaisingRepr(), "args": None}
    )

    data = json.loads(build_formatter(json=True, colors=False).format(record))

    assert data["event"] == "log_record_unformattable"
    assert data["msg"] == "<_RaisingRepr>"
    assert data["args"] == []


@pytest.mark.entscheidung("E-090")
@pytest.mark.parametrize(("args", "expected"), [(5, ["5"]), ("ab", ["ab"])])
def test_substitute_event_takes_a_single_non_tuple_argument_as_one(args: object, expected: list[str]) -> None:
    record = logging.makeLogRecord(
        {"name": "thirdparty", "levelno": logging.WARNING, "levelname": "WARNING", "msg": "%s %s %r", "args": args}
    )

    data = json.loads(build_formatter(json=True, colors=False).format(record))

    assert data["event"] == "log_record_unformattable"
    assert data["args"] == expected


@pytest.mark.parametrize("as_json", [True, False])
def test_formatter_internal_keys_stay_out_of_output(logger_name: str, as_json: bool) -> None:
    """Regression B6: ``_record`` traegt den ganzen LogRecord; mit ihm stand
    die Nutzlast zweimal in jeder Zeile."""
    stream = _render(logger_name, build_formatter(json=as_json, colors=False))

    get_logger(logger_name).info("hello_event", foo="bar")

    output = stream.getvalue()
    assert "hello_event" in output
    assert "_record" not in output
    assert "_from_structlog" not in output


@pytest.mark.parametrize(("colors", "has_ansi"), [(False, False), (True, True)])
def test_text_mode_emits_ansi_only_with_colors(logger_name: str, colors: bool, has_ansi: bool) -> None:
    """CLI-03: kein ANSI bei umgeleitetem stderr (``colors=False``)."""
    stream = _render(logger_name, build_formatter(json=False, colors=colors))

    get_logger(logger_name).info("hello_event", foo="bar")

    assert ("\x1b[" in stream.getvalue()) is has_ansi


def test_factory_is_usable_from_dictconfig(logger_name: str) -> None:
    """KTD3: Nutzer verwenden die Factory in ihrer dictConfig per ``"()"``.

    Gebaut wird der Formatter ueber den ``DictConfigurator`` der stdlib,
    denselben Pfad wie in ``dictConfig``, aber ohne dessen globale
    Nebenwirkungen (``dictConfig`` schliesst alle bestehenden Handler).
    """
    formatter = logging.config.DictConfigurator({}).configure_formatter(
        {"()": "swarmq.logging.build_formatter", "json": True, "colors": False}
    )
    stream = _render(logger_name, formatter)

    get_logger(logger_name).info("hello_event", foo="bar")

    data = json.loads(stream.getvalue())
    assert data["event"] == "hello_event"
    assert data["foo"] == "bar"


def _through_setup_logging(emit: Callable[[], None]) -> str:
    """Laesst ``emit`` durch den Aufbau von ``setup_logging`` laufen und gibt
    die gesamte Ausgabe des Listeners zurueck."""
    listener = setup_logging(log_level="INFO", log_json=True)
    try:
        stream = io.StringIO()
        listener.handlers[0].stream = stream  # type: ignore[attr-defined]
        emit()
        listener.stop()
        return stream.getvalue()
    finally:
        if listener._thread is not None:
            listener.stop()


def test_setup_logging_renders_library_logger_fields() -> None:
    output = _through_setup_logging(
        lambda: get_logger("swarmq.tests.setup_logging").info("hello_event", foo="bar", count=3)
    )

    lines = output.strip().split("\n")
    assert len(lines) == 1
    data = json.loads(lines[0])
    assert data["event"] == "hello_event"
    assert data["logger"] == "swarmq.tests.setup_logging"
    assert data["level"] == "info"
    assert data["foo"] == "bar"
    assert data["count"] == 3
    assert "timestamp" in data


def test_setup_logging_renders_structlog_native_record_once() -> None:
    output = _through_setup_logging(lambda: structlog.get_logger().info("native_event", foo="bar"))

    lines = output.strip().split("\n")
    assert len(lines) == 1
    assert lines[0].count('"timestamp"') == 1
    assert lines[0].count('"level"') == 1
    data = json.loads(lines[0])
    assert data["event"] == "native_event"
    assert data["foo"] == "bar"
