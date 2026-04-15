"""Integration-test conftest — fixtures for Docker-backed services."""

from __future__ import annotations

import socket

import pytest

_RABBITMQ_HOST = "localhost"
_RABBITMQ_PORT = 5672
_VALKEY_HOST = "localhost"
_VALKEY_PORT = 6379


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
    return "amqp://guest:guest@localhost:5672/"


@pytest.fixture(scope="session")
def valkey_url() -> str:
    return "valkey://localhost:6379/0"


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
def flush_valkey():
    """Flush the Valkey test database after each test.

    TODO: Replace with a real ``valkey.asyncio`` flush once valkey-py is
    wired up.
    """
    yield
    # --- teardown ---
    # async with valkey.from_url(valkey_url) as client:
    #     await client.flushdb()
