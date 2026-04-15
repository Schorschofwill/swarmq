"""E2E conftest — fixtures for real RabbitMQ + Valkey roundtrip tests."""

from __future__ import annotations

import socket

import pytest

_RABBITMQ_HOST = "localhost"
_RABBITMQ_PORT = 5672
_VALKEY_HOST = "localhost"
_VALKEY_PORT = 6379


def _port_is_open(host: str, port: int, timeout: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


@pytest.fixture(autouse=True, scope="session")
def check_docker():
    """Skip all e2e tests when Docker services are unreachable."""
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


@pytest.fixture(scope="session")
def rabbitmq_url() -> str:
    return "amqp://guest:guest@localhost:5672/"


@pytest.fixture(scope="session")
def valkey_url() -> str:
    return "valkey://localhost:6379/0"

# Task registry reset is handled by root conftest.py (_reset_task_registry).
