"""Tests fuer ``tests.unit._log_capture`` -- das Erfassen von swarmq-Logs
ohne globale structlog-Konfiguration.

Plan 2026-10-06-0831, U4.
"""

from __future__ import annotations

# Test list:
# 1. ``capture_swarmq_logs`` erfasst ein swarmq-Event mit Feldern und
#    gebundenen Kontextvariablen in der Form von ``capture_logs``
#    (``event``, ``log_level`` klein, ``logger``).
# 2. Events von Loggern ausserhalb von ``swarmq`` werden nicht erfasst.
# 3. DEBUG-Events werden erfasst, auch wenn ``swarmq`` sonst auf WARNING
#    steht.
# 4. Nach dem Verlassen sind Level und Handler von ``swarmq`` wie vorher.
# 5. Das Fixture ``swarmq_logs`` liefert die erfassten Eintraege.
# 6. Ein Autouse-Fixture entfernt die Logging-Umgebungsvariablen.
# 7. Wirft der Block, wird der Zustand trotzdem wiederhergestellt.
# 8. ``preserved_root_logger`` (Basis des Autouse-Fixtures): nach
#    ``configure_logging`` sind Root-Handler und Root-Level wieder im
#    Ausgangszustand, und ein folgendes ``configure_logging`` baut neu auf.
import io
import logging
import os
import sys

import pytest
import structlog

from swarmq.logging import get_logger
from tests.unit._log_capture import capture_swarmq_logs, preserved_root_logger


@pytest.fixture(autouse=True)
def _clear_contextvars():
    structlog.contextvars.clear_contextvars()
    yield
    structlog.contextvars.clear_contextvars()


def test_captures_swarmq_event_with_fields_and_contextvars() -> None:
    log = get_logger("swarmq.capture_test")
    with capture_swarmq_logs() as entries:
        structlog.contextvars.bind_contextvars(task_id="t-1")
        log.info("task_started", queue="default")

    assert entries == [
        {
            "event": "task_started",
            "log_level": "info",
            "logger": "swarmq.capture_test",
            "queue": "default",
            "task_id": "t-1",
        }
    ]


def test_ignores_loggers_outside_swarmq() -> None:
    log = get_logger("thirdparty.capture_test")
    with capture_swarmq_logs() as entries:
        log.warning("foreign_event")

    assert entries == []


def test_captures_debug_even_when_swarmq_is_at_warning() -> None:
    swarmq_logger = logging.getLogger("swarmq")
    old_level = swarmq_logger.level
    swarmq_logger.setLevel(logging.WARNING)
    try:
        with capture_swarmq_logs() as entries:
            get_logger("swarmq.capture_test").debug("detail")
    finally:
        swarmq_logger.setLevel(old_level)

    assert [e["event"] for e in entries] == ["detail"]
    assert entries[0]["log_level"] == "debug"


def test_restores_swarmq_logger_level_handlers_and_disabled() -> None:
    swarmq_logger = logging.getLogger("swarmq")
    old = (swarmq_logger.level, list(swarmq_logger.handlers), swarmq_logger.disabled)
    swarmq_logger.setLevel(logging.ERROR)
    swarmq_logger.disabled = True
    try:
        with capture_swarmq_logs():
            assert swarmq_logger.level == logging.DEBUG
            assert not swarmq_logger.disabled
        assert swarmq_logger.level == logging.ERROR
        assert swarmq_logger.disabled is True
        assert swarmq_logger.handlers == old[1]
    finally:
        swarmq_logger.setLevel(old[0])
        swarmq_logger.disabled = old[2]


def test_restores_state_when_body_raises() -> None:
    swarmq_logger = logging.getLogger("swarmq")
    before = (swarmq_logger.level, list(swarmq_logger.handlers))
    with pytest.raises(RuntimeError), capture_swarmq_logs():
        raise RuntimeError("boom")

    assert (swarmq_logger.level, list(swarmq_logger.handlers)) == before


def test_swarmq_logs_fixture_yields_captured_entries(swarmq_logs) -> None:
    get_logger("swarmq.capture_test").info("via_fixture", n=1)

    assert swarmq_logs == [
        {
            "event": "via_fixture",
            "log_level": "info",
            "logger": "swarmq.capture_test",
            "n": 1,
        }
    ]


def test_logging_env_vars_are_removed_for_unit_tests() -> None:
    # ``SWARMQ_LOG`` deckt auch ``SWARMQ_LOGGER_LEVELS`` ab.
    leaked = [
        name
        for name in os.environ
        if name.startswith("SWARMQ_LOG") or name == "SWARMQ_CONFIGURE_LOGGING"
    ]

    assert leaked == []


def test_preserved_root_logger_undoes_configure_logging(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from swarmq.logging import configure_logging, shutdown_logging
    from swarmq.logging_settings import LoggingSettings

    root = logging.getLogger()

    def own_handlers() -> list[logging.Handler]:
        # pytest verwaltet seine Capture-Handler je Phase selbst.
        return [h for h in root.handlers if type(h).__module__ != "_pytest.logging"]

    before = (own_handlers(), root.level)
    structlog_config = structlog.get_config()
    structlog_configured = structlog.is_configured()
    monkeypatch.setattr(sys, "stderr", io.StringIO())
    try:
        with preserved_root_logger():
            configure_logging(LoggingSettings(level="DEBUG"))
            first = root.handlers[:]
        assert (own_handlers(), root.level) == before

        with preserved_root_logger():
            configure_logging(LoggingSettings())
            assert root.handlers != first  # neu aufgebaut, kein No-op
    finally:
        shutdown_logging()
        structlog.reset_defaults()
        if structlog_configured:
            structlog.configure(**structlog_config)
    assert (own_handlers(), root.level) == before
