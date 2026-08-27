"""Service-Fixtures für Integration und e2e (Plan U4): Ports, Erreichbarkeit,
URLs. Conftests importieren die Fixture-Funktionen in ihren Namensraum::

    from tests.support.services import check_docker, rabbitmq_url, valkey_url  # noqa: F401
"""

from __future__ import annotations

import os
import socket

import pytest

RABBITMQ_HOST = os.environ.get("SWARMQ_TEST_RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.environ.get("SWARMQ_TEST_RABBITMQ_PORT", "5672"))
VALKEY_HOST = os.environ.get("SWARMQ_TEST_VALKEY_HOST", "localhost")
VALKEY_PORT = int(os.environ.get("SWARMQ_TEST_VALKEY_PORT", "6379"))


def port_is_open(host: str, port: int, timeout: float = 1.0) -> bool:
    """True, wenn eine TCP-Verbindung zu ``host:port`` gelingt."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def rabbitmq_amqp_url() -> str:
    return f"amqp://guest:guest@{RABBITMQ_HOST}:{RABBITMQ_PORT}/"


def valkey_db_url() -> str:
    return f"valkey://{VALKEY_HOST}:{VALKEY_PORT}/0"


def missing_services() -> list[str]:
    missing: list[str] = []
    if not port_is_open(RABBITMQ_HOST, RABBITMQ_PORT):
        missing.append(f"RabbitMQ ({RABBITMQ_HOST}:{RABBITMQ_PORT})")
    if not port_is_open(VALKEY_HOST, VALKEY_PORT):
        missing.append(f"Valkey ({VALKEY_HOST}:{VALKEY_PORT})")
    return missing


@pytest.fixture(scope="session")
def rabbitmq_url() -> str:
    return rabbitmq_amqp_url()


@pytest.fixture(scope="session")
def valkey_url() -> str:
    return valkey_db_url()


@pytest.fixture(autouse=True, scope="session")
def check_docker() -> None:
    """Überspringt das ganze Tier, wenn die Docker-Dienste nicht erreichbar sind."""
    missing = missing_services()
    if missing:
        pytest.skip(f"Docker services not available: {', '.join(missing)}")
