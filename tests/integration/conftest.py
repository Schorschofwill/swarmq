"""Integration-test conftest — fixtures for Docker-backed services."""

from __future__ import annotations

import pytest

from tests.support.services import (  # noqa: F401 — Fixtures in den Namensraum (Plan U4)
    check_docker,
    delete_session_queues,
    flush_valkey_db,
    rabbitmq_url,
    valkey_db_url,
    valkey_url,
)


@pytest.fixture(autouse=True)
async def flush_valkey():
    """Flush the Valkey test database before and after each test.

    Before as well: e2e tests share the database without flushing, and a
    worker they cancel keeps the scheduler leader lock until its TTL runs
    out (by design, an aborted leader does not hand over). An integration
    test that needs leadership right after one of them would otherwise
    depend on the run order (bz14 under pytest-randomly).

    ``flush_valkey_db`` refuses a local host on the default port 6379 (a
    foreign stack, not the swarmq test container) unless
    ``SWARMQ_TEST_ALLOW_DEFAULT_PORTS=1`` is set.
    """
    await flush_valkey_db(valkey_db_url())
    yield
    await flush_valkey_db(valkey_db_url())
