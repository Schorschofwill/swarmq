"""Tests fuer ``swarmq.logging.get_logger`` -- die Bibliotheks-Logger-Fabrik.

Plan 2026-10-06-0831, U1 (R2, R3, R6; KTD1, KTD2).
"""

from __future__ import annotations

# Test list:
# 1. Reine stdlib-App mit ``%(task_id)s``-Format rendert ein swarmq-Event
#    mit Feldwert (AE1).
# 2. App mit eigener ``structlog.configure`` und ProcessorFormatter+ExtraAdder
#    rendert Felder als Schluessel/Werte; ``structlog.get_config()`` bleibt
#    unveraendert (AE2).
# 3. ``logging.getLoggerClass()`` ist nach dem Erzeugen und Benutzen eines
#    swarmq-Loggers unveraendert.
# 4. Felder ``module=`` / ``name=`` kommen als ``swarmq_module`` /
#    ``swarmq_name`` an und werfen nicht.
# 5. Kontextvariablen landen als Felder auf dem Record.
# 6. Eine Kontextvariable ``exc_info`` wird umbenannt und nicht als
#    stdlib-kwarg interpretiert.
# 7. ``logger.exception(...)`` liefert einen Record mit gesetztem ``exc_info``.
# 8. ``funcName``/``lineno`` zeigen auf den Aufrufer fuer ``info``,
#    ``warning``, ``exception`` und ``log``.
# 8b. Dasselbe, wenn die App per ``structlog.stdlib.LoggerFactory`` die
#     Logger-Klasse umgestellt hat (deren ``findCaller`` ueberspringt
#     structlog-Frames schon selbst).
# 9. Ein deaktiviertes Level erzeugt keinen Record und laesst die
#    Prozessorkette nach dem Level-Check nicht laufen.
# 10. Nach einer dictConfig mit ``disable_existing_loggers=True`` ist ein
#     vorher erzeugter, aber erst danach benutzter swarmq-Logger aktiv.
# 11. Ein ``%`` im Event-Namen wird nicht als Format-String interpretiert.
import io
import json
import logging
from collections.abc import Iterator

import pytest
import structlog

from swarmq.logging import get_logger


class _ListHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


@pytest.fixture
def logger_name(request: pytest.FixtureRequest) -> Iterator[str]:
    """Eindeutiger stdlib-Logger je Test, ohne Propagation an Root.

    Der Logger wird als ``logging.Logger`` angelegt: Ein frueherer Test kann
    ueber ``structlog.stdlib.LoggerFactory`` die globale Logger-Klasse
    umgestellt haben, und die Testreihenfolge darf nicht zaehlen.
    """
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
    stdlib_logger.disabled = False
    stdlib_logger.setLevel(logging.NOTSET)
    stdlib_logger.propagate = True


def test_stdlib_app_renders_field_via_format_string(logger_name: str) -> None:
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(logging.Formatter("%(name)s %(message)s %(task_id)s"))
    logging.getLogger(logger_name).addHandler(handler)

    get_logger(logger_name).info("task_succeeded", task_id="t-1")

    assert stream.getvalue() == f"{logger_name} task_succeeded t-1\n"


@pytest.fixture
def app_structlog_config() -> Iterator[None]:
    """Stellt die globale structlog-Konfiguration nach dem Test wieder her."""
    saved = structlog.get_config()
    yield
    structlog.configure(**saved)


def test_structlog_app_renders_fields_as_json_keys(logger_name: str, app_structlog_config: None) -> None:
    structlog.configure(
        processors=[structlog.processors.KeyValueRenderer()],
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=False,
    )
    app_config = structlog.get_config()
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(
            foreign_pre_chain=[
                structlog.stdlib.add_log_level,
                structlog.stdlib.add_logger_name,
                structlog.stdlib.ExtraAdder(),
                structlog.processors.format_exc_info,
            ],
            processors=[
                structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                structlog.processors.JSONRenderer(),
            ],
        )
    )
    logging.getLogger(logger_name).addHandler(handler)

    try:
        raise ValueError("boom")
    except ValueError:
        get_logger(logger_name).exception("task_failed", task_id="t-1", error_type="ValueError", error="boom")

    line = json.loads(stream.getvalue())
    assert line["event"] == "task_failed"
    assert line["level"] == "error"
    assert line["logger"] == logger_name
    assert line["task_id"] == "t-1"
    assert line["error_type"] == "ValueError"
    assert line["error"] == "boom"
    assert "ValueError: boom" in line["exception"]
    assert structlog.get_config() == app_config


def test_creating_and_using_logger_keeps_app_logger_class(logger_name: str) -> None:
    before = logging.getLoggerClass()

    get_logger(logger_name).info("evt")

    assert logging.getLoggerClass() is before


@pytest.fixture
def records(logger_name: str) -> list[logging.LogRecord]:
    handler = _ListHandler()
    logging.getLogger(logger_name).addHandler(handler)
    return handler.records


def test_logrecord_attribute_fields_are_renamed(logger_name: str, records: list[logging.LogRecord]) -> None:
    get_logger(logger_name).info("evt", name="n", module="m", message="x", asctime="a")

    (record,) = records
    assert record.name == logger_name
    assert record.swarmq_name == "n"  # type: ignore[attr-defined]
    assert record.swarmq_module == "m"  # type: ignore[attr-defined]
    assert record.swarmq_message == "x"  # type: ignore[attr-defined]
    assert record.swarmq_asctime == "a"  # type: ignore[attr-defined]


def test_context_variables_become_record_fields(logger_name: str, records: list[logging.LogRecord]) -> None:
    with structlog.contextvars.bound_contextvars(task_id="t-1", name="ctx"):
        get_logger(logger_name).info("evt")

    (record,) = records
    assert record.task_id == "t-1"  # type: ignore[attr-defined]
    assert record.swarmq_name == "ctx"  # type: ignore[attr-defined]


def test_context_variable_exc_info_is_renamed_not_a_log_kwarg(
    logger_name: str, records: list[logging.LogRecord]
) -> None:
    with structlog.contextvars.bound_contextvars(exc_info="ctx-ei", stack_info="ctx-si", stacklevel="ctx-sl"):
        get_logger(logger_name).info("evt")

    (record,) = records
    assert record.exc_info is None
    assert record.stack_info is None
    assert record.swarmq_exc_info == "ctx-ei"  # type: ignore[attr-defined]
    assert record.swarmq_stack_info == "ctx-si"  # type: ignore[attr-defined]
    assert record.swarmq_stacklevel == "ctx-sl"  # type: ignore[attr-defined]


def test_exception_sets_exc_info_on_record(logger_name: str, records: list[logging.LogRecord]) -> None:
    try:
        raise ValueError("boom")
    except ValueError:
        get_logger(logger_name).exception("evt")

    (record,) = records
    assert record.levelno == logging.ERROR
    assert record.exc_info is not None
    assert record.exc_info[0] is ValueError


def test_record_points_to_calling_function(logger_name: str, records: list[logging.LogRecord]) -> None:
    log = get_logger(logger_name)
    lines = []
    try:
        raise ValueError("boom")
    except ValueError:
        lines.append(_current_line() + 1)
        log.info("evt")
        lines.append(_current_line() + 1)
        log.warning("evt")
        lines.append(_current_line() + 1)
        log.exception("evt")
        lines.append(_current_line() + 1)
        log.log(logging.INFO, "evt")

    assert [r.funcName for r in records] == ["test_record_points_to_calling_function"] * 4
    assert [r.lineno for r in records] == lines
    assert {r.filename for r in records} == {"test_logging_logger.py"}


def _current_line() -> int:
    import inspect

    frame = inspect.currentframe()
    assert frame is not None and frame.f_back is not None
    return frame.f_back.f_lineno


def test_record_points_to_caller_with_structlog_logger_factory_class(
    request: pytest.FixtureRequest,
) -> None:
    app_logger_class = logging.getLoggerClass()
    name = f"swarmq.tests.{request.node.name}"
    try:
        stdlib_logger = structlog.stdlib.LoggerFactory()(name)
    finally:
        logging.setLoggerClass(app_logger_class)
    handler = _ListHandler()
    stdlib_logger.addHandler(handler)
    stdlib_logger.setLevel(logging.DEBUG)
    stdlib_logger.propagate = False
    try:
        expected_line = _current_line() + 1
        get_logger(name).info("evt")
    finally:
        stdlib_logger.handlers.clear()
        stdlib_logger.setLevel(logging.NOTSET)
        stdlib_logger.propagate = True

    (record,) = handler.records
    assert record.funcName == request.node.name
    assert record.lineno == expected_line


def test_disabled_level_creates_no_record_and_skips_chain(
    logger_name: str,
    records: list[logging.LogRecord],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import swarmq.logging as swarmq_logging

    seen: list[str] = []

    def spy(_: object, method_name: str, event_dict: dict) -> dict:  # type: ignore[type-arg]
        seen.append(method_name)
        return event_dict

    chain = list(swarmq_logging.LOGGER_PROCESSORS)
    chain.insert(-1, spy)
    monkeypatch.setattr(swarmq_logging, "LOGGER_PROCESSORS", chain)
    logging.getLogger(logger_name).setLevel(logging.INFO)

    log = get_logger(logger_name)
    log.debug("evt", task_id="t-1")
    log.info("evt")

    assert [r.levelno for r in records] == [logging.INFO]
    assert seen == ["info"]


@pytest.fixture
def isolated_dict_config(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """dictConfig ohne Nebenwirkung auf den restlichen Testprozess.

    ``dictConfig`` schliesst alle bekannten Handler und deaktiviert alle
    vorhandenen Logger; mit ``root`` ersetzt sie Handler und Level von Root.
    Die Handler-Registry wird darum fuer den Test geleert, und Root sowie
    die ``disabled``-Flags werden danach zurueckgesetzt.
    """
    import weakref

    monkeypatch.setattr(logging, "_handlerList", [])
    monkeypatch.setattr(logging, "_handlers", weakref.WeakValueDictionary())
    root = logging.getLogger()
    root_handlers, root_level = root.handlers[:], root.level
    loggers = logging.Logger.manager.loggerDict
    disabled = {name: lg.disabled for name, lg in loggers.items() if isinstance(lg, logging.Logger)}
    yield
    for name, lg in list(loggers.items()):
        if isinstance(lg, logging.Logger):
            lg.disabled = disabled.get(name, False)
    root.handlers[:] = root_handlers
    root.setLevel(root_level)


def test_logger_used_after_app_dict_config_is_not_disabled(
    request: pytest.FixtureRequest, isolated_dict_config: None
) -> None:
    import logging.config

    name = f"swarmq.tests.{request.node.name}"
    log = get_logger(name)  # wie beim Modulimport
    handler = _ListHandler()
    logging.config.dictConfig(
        {
            "version": 1,
            "handlers": {"capture": {"()": lambda: handler}},
            "root": {"level": "INFO", "handlers": ["capture"]},
        }
    )
    log.info("evt")

    assert [r.name for r in handler.records] == [name]


def test_percent_in_event_is_not_a_format_string(logger_name: str, records: list[logging.LogRecord]) -> None:
    get_logger(logger_name).info("disk_100%_full %s %(x)s", x=1)

    (record,) = records
    assert record.getMessage() == "disk_100%_full %s %(x)s"
