"""Unit-test conftest — fixtures that keep unit tests fast and isolated."""

from __future__ import annotations

# Task registry reset is handled by root conftest.py (_reset_task_registry).
import pytest

from tests.unit._log_capture import capture_swarmq_logs, preserved_root_logger

# Logging-Umgebungsvariablen, die swarmq auswertet (Plan 2026-10-06-0831, U4).
_SWARMQ_LOGGING_ENV_VARS = (
    "SWARMQ_LOG_LEVEL",
    "SWARMQ_LOG_JSON",
    "SWARMQ_LOG_FORMAT",
    "SWARMQ_LOG_CONFIG",
    "SWARMQ_CONFIGURE_LOGGING",
    "SWARMQ_LOGGER_LEVELS",
    "SWARMQ_LOG_QUEUE_SIZE",
    "SWARMQ_LOG_LIFECYCLE_LEVEL",
    "SWARMQ_LOG_QUEUE",
)


@pytest.fixture(autouse=True)
def _clear_swarmq_logging_env(monkeypatch):
    """Plan 2026-10-06-0831, U4: Logging-Variablen aus der Shell des
    Entwicklers (oder aus CI) duerfen Unit-Tests nicht beeinflussen. Tests,
    die eine Variable brauchen, setzen sie selbst per ``monkeypatch.setenv``.
    """
    for name in _SWARMQ_LOGGING_ENV_VARS:
        monkeypatch.delenv(name, raising=False)


@pytest.fixture
def swarmq_logs():
    """Erfasste swarmq-Logs als Liste von Dicts (siehe ``_log_capture``).

    Ersatz fuer ``structlog.testing.capture_logs``, seit die
    Bibliotheks-Logger ans Standard-``logging`` uebergeben (Plan
    2026-10-06-0831, U4).
    """
    with capture_swarmq_logs() as entries:
        yield entries


@pytest.fixture(autouse=True)
def _restore_logging_state():
    """Gibt das Logging nach jedem Test frei und stellt Root wieder her
    (Plan 2026-10-06-0831, U4 Teil 2, U6).

    ``shutdown_logging()`` stoppt jeden Listener, den ein Test gestartet hat:
    Er schreibt aus einem eigenen Thread nach ``sys.stderr``, den ``capsys``
    beim Teardown schliesst -- die Folge waere ein ``ValueError: I/O
    operation on closed file`` als Rauschen in jedem Testlauf. Danach baut
    ein ``configure_logging`` im naechsten Test neu auf, und Root-Handler
    sowie Root-Level sind wieder im Ausgangszustand.
    """
    with preserved_root_logger():
        yield
