"""Integration-test conftest — fixtures for Docker-backed services."""

from __future__ import annotations

import pytest

from tests.support.services import (  # noqa: F401 — Fixtures in den Namensraum (Plan U4)
    VALKEY_HOST,
    VALKEY_PORT,
    check_docker,
    rabbitmq_url,
    valkey_url,
)


@pytest.fixture(autouse=True)
async def flush_valkey():
    """Flush the Valkey test database after each test."""
    yield
    # --- teardown ---
    import valkey.asyncio as aio_valkey

    client = aio_valkey.Valkey.from_url(f"valkey://{VALKEY_HOST}:{VALKEY_PORT}/0")
    try:
        await client.flushdb()
    finally:
        await client.aclose()
