"""Service-Fixtures für Integration und e2e (Plan U4): Ports, Erreichbarkeit,
URLs. Conftests importieren die Fixture-Funktionen in ihren Namensraum::

    from tests.support.services import check_docker, rabbitmq_url, valkey_url  # noqa: F401

Zwei RabbitMQ-Dienste (Plan 2026-10-08-1541, KTD9): der Hauptdienst
(``SWARMQ_TEST_RABBITMQ_*``, gepinnt auf 4.3.6) und ein zweiter auf 4.2.9
(``SWARMQ_TEST_RABBITMQ42_*``), gestartet mit ``channel_tick_interval``
1000 ms. Beide brauchen zusätzlich ihr Management-API für Policies
(``SWARMQ_TEST_RABBITMQ_MGMT_PORT``, ``SWARMQ_TEST_RABBITMQ42_MGMT_PORT``).
"""

from __future__ import annotations

import os
import re
import socket
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from urllib.parse import quote

import httpx
import pytest

RABBITMQ_HOST = os.environ.get("SWARMQ_TEST_RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.environ.get("SWARMQ_TEST_RABBITMQ_PORT", "5672"))
RABBITMQ_MGMT_PORT = int(os.environ.get("SWARMQ_TEST_RABBITMQ_MGMT_PORT", "15672"))
RABBITMQ42_HOST = os.environ.get("SWARMQ_TEST_RABBITMQ42_HOST", "localhost")
RABBITMQ42_PORT = int(os.environ.get("SWARMQ_TEST_RABBITMQ42_PORT", "5742"))
RABBITMQ42_MGMT_PORT = int(os.environ.get("SWARMQ_TEST_RABBITMQ42_MGMT_PORT", "15742"))
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


@dataclass(frozen=True)
class RabbitMQNode:
    """Ein RabbitMQ-Dienst mit AMQP- und Management-Zugang (KTD9)."""

    host: str
    port: int
    management_port: int

    @property
    def amqp_url(self) -> str:
        return f"amqp://guest:guest@{self.host}:{self.port}/"

    @property
    def management_url(self) -> str:
        return f"http://{self.host}:{self.management_port}/api"

    def missing(self) -> list[str]:
        return [
            f"RabbitMQ {kind} ({self.host}:{port})"
            for kind, port in (("AMQP", self.port), ("management", self.management_port))
            if not port_is_open(self.host, port)
        ]

    def version(self) -> str:
        response = httpx.get(f"{self.management_url}/overview", auth=("guest", "guest"), timeout=5.0)
        response.raise_for_status()
        return str(response.json()["rabbitmq_version"])

    @asynccontextmanager
    async def consumer_timeout_policy(self, queue: str, timeout_ms: int) -> AsyncIterator[None]:
        """Policy ``consumer-timeout`` nur für ``queue``; vor dem Declare setzen.

        Auf 4.2 prüft der Channel den Timeout nur je ``channel_tick_interval``,
        deshalb startet der 4.2-Dienst mit 1000 ms statt 60 s.
        """
        url = f"{self.management_url}/policies/%2F/{quote(f'swarmq-test-ct-{queue}', safe='')}"
        body = {
            "pattern": f"^{re.escape(queue)}$",
            "definition": {"consumer-timeout": timeout_ms},
            "apply-to": "queues",
            "priority": 1000,
        }
        async with httpx.AsyncClient(auth=("guest", "guest"), timeout=5.0) as client:
            (await client.put(url, json=body)).raise_for_status()
            try:
                yield
            finally:
                await client.delete(url)


RABBITMQ_MAIN = RabbitMQNode(RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_MGMT_PORT)
RABBITMQ_42 = RabbitMQNode(RABBITMQ42_HOST, RABBITMQ42_PORT, RABBITMQ42_MGMT_PORT)


def require_rabbitmq(node: RabbitMQNode, version_prefix: str) -> RabbitMQNode:
    """Wie ``check_docker``: überspringt, wenn der Dienst nicht erreichbar ist.

    Läuft dort eine andere Version als gepinnt, schlägt der Test fehl —
    eine Charakterisierung gegen die falsche Version wäre wertlos.
    """
    missing = node.missing()
    if missing:
        pytest.skip(f"Docker services not available: {', '.join(missing)}")
    version = node.version()
    if not version.startswith(version_prefix):
        pytest.fail(f"RabbitMQ at {node.host}:{node.port} runs {version}, expected {version_prefix}x")
    return node


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
