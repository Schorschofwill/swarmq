"""Integration-test conftest — fixtures for Docker-backed services."""

from __future__ import annotations

import pytest

from tests.support.services import (  # noqa: F401 — Fixtures in den Namensraum (Plan U4)
    check_docker,
    flush_valkey_db,
    rabbitmq_url,
    valkey_db_url,
    valkey_url,
)


@pytest.fixture(autouse=True)
async def flush_valkey():
    """Flush the Valkey test database after each test.

    ``flush_valkey_db`` refuses a local host on the default port 6379 (a
    foreign stack, not the swarmq test container) unless
    ``SWARMQ_TEST_ALLOW_DEFAULT_PORTS=1`` is set.
    """
    yield
    await flush_valkey_db(valkey_db_url())
