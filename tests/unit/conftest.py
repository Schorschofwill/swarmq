"""Unit-test conftest — fixtures that keep unit tests fast and isolated."""

from __future__ import annotations

# Task registry reset is handled by root conftest.py (_reset_task_registry).
import pytest

from tests.unit._structlog_cache import reset_swarmq_logger_caches


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
