"""Unit-test conftest — fixtures that keep unit tests fast and isolated."""

from __future__ import annotations

# Task registry reset is handled by root conftest.py (_reset_task_registry).
import pytest

from tests.unit._log_capture import capture_swarmq_logs
from tests.unit._structlog_cache import reset_swarmq_logger_caches

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
def _uncache_structlog_module_loggers():
    """Härtung R2 (Test-Isolation): ``setup_logging`` konfiguriert
    structlog mit ``cache_logger_on_first_use=True``. Sobald irgendein
    früherer Test (z.B. via ``SwarmQ.__init__`` oder den Supervisor)
    diese Konfiguration aktiv hatte und ein Modul-Logger seinen ersten
    Log-Call machte, wurde der fertig gebundene stdlib-Logger auf dem
    Lazy-Proxy gecacht (Instanz-Attribut ``bind``) — und
    ``structlog.testing.capture_logs`` konnte ihn in späteren Tests
    nicht mehr abfangen. Der Cache wird vor jedem Unit-Test geleert;
    der Proxy fällt auf die jeweils aktive Konfiguration zurück.

    Plan U2: statt eines festen Modul-Tupels werden alle geladenen
    ``swarmq.*``-Module durchsucht — neue Submodule der Extraktionen
    (``swarmq.worker.pipeline`` usw.) sind damit automatisch erfasst.
    """
    reset_swarmq_logger_caches()
    yield


@pytest.fixture(autouse=True)
def _stop_leftover_log_listener():
    """Stoppt per ``shutdown_logging()`` jeden Listener, den ein Test
    gestartet hat (Plan 2026-10-06-0831, U4 Teil 2).

    Der Listener schreibt aus einem eigenen Thread nach ``sys.stderr``, den
    ``capsys`` beim Teardown schliesst — die Folge waere ein
    ``ValueError: I/O operation on closed file`` als Rauschen in jedem
    Testlauf, das echte Logging-Fehler verdeckt. Seit U4 konfiguriert der
    Worker-Zweig der CLI das Logging selbst, also trifft das mehr Tests als
    zuvor.
    """
    yield
    from swarmq.logging import shutdown_logging

    shutdown_logging()
