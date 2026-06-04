"""Unit-test conftest — fixtures that keep unit tests fast and isolated."""

from __future__ import annotations

# Task registry reset is handled by root conftest.py (_reset_task_registry).

import pytest
import structlog


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
    """
    import swarmq.backend.valkey
    import swarmq.broker.rabbitmq
    import swarmq.client
    import swarmq.hot_reload
    import swarmq.middleware
    import swarmq.process_manager
    import swarmq.scheduler
    import swarmq.signals
    import swarmq.task
    import swarmq.worker

    modules = (
        swarmq.signals, swarmq.middleware, swarmq.worker, swarmq.client,
        swarmq.scheduler, swarmq.process_manager, swarmq.hot_reload,
        swarmq.backend.valkey, swarmq.broker.rabbitmq, swarmq.task,
    )
    # swarmq.task names its proxy ``_progress_logger`` — cover both names
    # (review R2 M1: missing modules made the fixture silently partial).
    for mod in modules:
        for attr in ("logger", "_progress_logger"):
            logger = getattr(mod, attr, None)
            if logger is not None:
                logger.__dict__.pop("bind", None)
    yield
