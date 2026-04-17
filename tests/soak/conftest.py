"""Soak-test fixtures (E2E-Hardening Phase 3 U3.9)."""

from __future__ import annotations

import os
import socket

import pytest

# Soak tests default to the swarmq-dev Docker stack
# (docker-compose.swarmq-dev.yml): RabbitMQ 5673/15673, Valkey 6380.
# Override via env vars if running against a different stack.
_RABBITMQ_HOST = os.environ.get("SWARMQ_TEST_RABBITMQ_HOST", "localhost")
_RABBITMQ_PORT = int(os.environ.get("SWARMQ_TEST_RABBITMQ_PORT", "5673"))
_RABBITMQ_MGMT_PORT = int(os.environ.get("SWARMQ_TEST_RABBITMQ_MGMT_PORT", "15673"))
_VALKEY_HOST = os.environ.get("SWARMQ_TEST_VALKEY_HOST", "localhost")
_VALKEY_PORT = int(os.environ.get("SWARMQ_TEST_VALKEY_PORT", "6380"))


def _port_is_open(host: str, port: int, timeout: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def pytest_collection_modifyitems(config, items):
    for item in items:
        if "tests/soak/" in str(item.fspath):
            item.add_marker(pytest.mark.soak)


@pytest.fixture(autouse=True, scope="session")
def check_docker_soak():
    """Skip soak tests when Docker services are unreachable."""
    rabbitmq_ok = _port_is_open(_RABBITMQ_HOST, _RABBITMQ_PORT)
    valkey_ok = _port_is_open(_VALKEY_HOST, _VALKEY_PORT)

    if not (rabbitmq_ok and valkey_ok):
        missing = []
        if not rabbitmq_ok:
            missing.append(f"RabbitMQ ({_RABBITMQ_HOST}:{_RABBITMQ_PORT})")
        if not valkey_ok:
            missing.append(f"Valkey ({_VALKEY_HOST}:{_VALKEY_PORT})")
        pytest.skip(f"Docker services not available: {', '.join(missing)}")


@pytest.fixture(scope="session")
def rabbitmq_url() -> str:
    return f"amqp://guest:guest@{_RABBITMQ_HOST}:{_RABBITMQ_PORT}/"


@pytest.fixture(scope="session")
def valkey_url() -> str:
    return f"valkey://{_VALKEY_HOST}:{_VALKEY_PORT}/0"


@pytest.fixture(scope="session")
def rabbitmq_mgmt_url() -> str:
    return f"http://{_RABBITMQ_HOST}:{_RABBITMQ_MGMT_PORT}"
