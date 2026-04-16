"""Integration-test conftest — fixtures for Docker-backed services."""

from __future__ import annotations

import os
import socket

import pytest

_RABBITMQ_HOST = os.environ.get("SWARMQ_TEST_RABBITMQ_HOST", "localhost")
_RABBITMQ_PORT = int(os.environ.get("SWARMQ_TEST_RABBITMQ_PORT", "5672"))
_VALKEY_HOST = os.environ.get("SWARMQ_TEST_VALKEY_HOST", "localhost")
_VALKEY_PORT = int(os.environ.get("SWARMQ_TEST_VALKEY_PORT", "6379"))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _port_is_open(host: str, port: int, timeout: float = 1.0) -> bool:
    """Return True if a TCP connection to *host*:*port* succeeds."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def rabbitmq_url() -> str:
    return f"amqp://guest:guest@{_RABBITMQ_HOST}:{_RABBITMQ_PORT}/"


@pytest.fixture(scope="session")
def valkey_url() -> str:
    return f"valkey://{_VALKEY_HOST}:{_VALKEY_PORT}/0"


@pytest.fixture(autouse=True, scope="session")
def check_docker():
    """Skip all integration tests when Docker services are unreachable."""
    rabbitmq_ok = _port_is_open(_RABBITMQ_HOST, _RABBITMQ_PORT)
    valkey_ok = _port_is_open(_VALKEY_HOST, _VALKEY_PORT)

    if not (rabbitmq_ok and valkey_ok):
        missing = []
        if not rabbitmq_ok:
            missing.append(f"RabbitMQ ({_RABBITMQ_HOST}:{_RABBITMQ_PORT})")
        if not valkey_ok:
            missing.append(f"Valkey ({_VALKEY_HOST}:{_VALKEY_PORT})")
        pytest.skip(
            f"Docker services not available: {', '.join(missing)}"
        )


@pytest.fixture(autouse=True)
async def flush_valkey():
    """Flush the Valkey test database after each test."""
    yield
    # --- teardown ---
    import valkey.asyncio as aio_valkey

    client = aio_valkey.Valkey.from_url(f"valkey://{_VALKEY_HOST}:{_VALKEY_PORT}/0")
    try:
        await client.flushdb()
    finally:
        await client.aclose()
