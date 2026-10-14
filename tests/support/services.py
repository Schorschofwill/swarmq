"""Service-Fixtures für Integration und e2e (Plan U4): Ports, Erreichbarkeit,
URLs. Conftests importieren die Fixture-Funktionen in ihren Namensraum::

    from tests.support.services import check_docker, rabbitmq_url, valkey_url  # noqa: F401

Zwei RabbitMQ-Dienste (Plan 2026-10-08-1541, KTD9): der Hauptdienst
(``SWARMQ_TEST_RABBITMQ_*``, gepinnt auf 4.3.6) und ein zweiter auf 4.2.9
(``SWARMQ_TEST_RABBITMQ42_*``), gestartet mit ``channel_tick_interval``
1000 ms. Beide brauchen zusätzlich ihr Management-API für Policies
(``SWARMQ_TEST_RABBITMQ_MGMT_PORT``, ``SWARMQ_TEST_RABBITMQ42_MGMT_PORT``).

Schutz vor fremden Stacks (2026-10-09): Standards sind die Testcontainer
5699/15699/6399. Ein lokaler Host auf 5672 bzw. 6379 wird hart verweigert
(``require_safe_services`` beim Sitzungsstart, ``flush_valkey_db`` vor jedem
Flush), außer ``SWARMQ_TEST_ALLOW_DEFAULT_PORTS=1`` ist gesetzt.
"""

from __future__ import annotations

import contextlib
import ipaddress
import os
import re
import socket
from collections.abc import AsyncIterator, Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from dataclasses import dataclass
from urllib.parse import quote, urlsplit

import httpx
import pytest

# Standards zeigen auf die swarmq-Testcontainer, nicht auf 5672/15672/6379:
# Dort laufen örtlich fremde Stacks, und die Fixtures flushen Valkey.
DEFAULT_RABBITMQ_PORT = 5699
DEFAULT_RABBITMQ_MGMT_PORT = 15699
DEFAULT_VALKEY_PORT = 6399

RABBITMQ_HOST = os.environ.get("SWARMQ_TEST_RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.environ.get("SWARMQ_TEST_RABBITMQ_PORT", str(DEFAULT_RABBITMQ_PORT)))
RABBITMQ_MGMT_PORT = int(os.environ.get("SWARMQ_TEST_RABBITMQ_MGMT_PORT", str(DEFAULT_RABBITMQ_MGMT_PORT)))
RABBITMQ42_HOST = os.environ.get("SWARMQ_TEST_RABBITMQ42_HOST", "localhost")
RABBITMQ42_PORT = int(os.environ.get("SWARMQ_TEST_RABBITMQ42_PORT", "5742"))
RABBITMQ42_MGMT_PORT = int(os.environ.get("SWARMQ_TEST_RABBITMQ42_MGMT_PORT", "15742"))
VALKEY_HOST = os.environ.get("SWARMQ_TEST_VALKEY_HOST", "localhost")
VALKEY_PORT = int(os.environ.get("SWARMQ_TEST_VALKEY_PORT", str(DEFAULT_VALKEY_PORT)))

# Namensraum je pytest-xdist-Prozess (Beschleunigung Stufe 3): ``gwN`` bekommt
# die vhost ``swarmq-test-gwN`` und die Valkey-Datenbank N+1, damit parallele
# Prozesse weder Queues noch Schlüssel (Leader-Lock, Crons) teilen und
# FLUSHDB nur die eigene Datenbank trifft. Ohne xdist: vhost ``/``, DB 0.
# Unterprozesse erben PYTEST_XDIST_WORKER und landen im selben Namensraum.
XDIST_WORKER = os.environ.get("PYTEST_XDIST_WORKER", "")
# Valkey hat 16 Datenbanken; DB 0 bleibt dem Lauf ohne xdist.
MAX_PARALLEL_WORKERS = 15


def worker_namespace(worker: str) -> tuple[str, int]:
    """vhost und Valkey-Datenbank für den xdist-Prozess ``worker`` (leer: keiner)."""
    if not worker:
        return "/", 0
    index = int(worker.removeprefix("gw"))
    if index >= MAX_PARALLEL_WORKERS:
        msg = f"xdist worker {worker}: at most {MAX_PARALLEL_WORKERS} parallel workers (Valkey has 16 databases)"
        raise RuntimeError(msg)
    return f"swarmq-test-{worker}", index + 1


RABBITMQ_VHOST, VALKEY_DB = worker_namespace(XDIST_WORKER)


ALLOW_DEFAULT_PORTS_VAR = "SWARMQ_TEST_ALLOW_DEFAULT_PORTS"
# Standardports, auf denen örtlich fremde Stacks laufen (etwa phoenix):
# Port -> (Dienst, Port-Variable).
_FOREIGN_DEFAULT_PORTS = {
    6379: ("Valkey", "SWARMQ_TEST_VALKEY_PORT"),
    5672: ("RabbitMQ", "SWARMQ_TEST_RABBITMQ_PORT"),
}


def _is_local_host(host: str) -> bool:
    name = host.strip().strip("[]").lower()
    if name == "localhost" or name.endswith(".localhost"):
        return True
    try:
        address = ipaddress.ip_address(name)
    except ValueError:
        return False
    return address.is_loopback or address.is_unspecified


def default_port_refusal(host: str, port: int, *, env: Mapping[str, str]) -> str | None:
    """Grund, warum ein flushendes Fixture ``host:port`` nicht anfassen darf."""
    if env.get(ALLOW_DEFAULT_PORTS_VAR) == "1":
        return None
    if port not in _FOREIGN_DEFAULT_PORTS or not _is_local_host(host):
        return None
    service, port_var = _FOREIGN_DEFAULT_PORTS[port]
    return (
        f"Refusing to run test fixtures that flush or clean up {service} at "
        f"{host}:{port}: a local host on the "
        f"{service} default port is most likely a foreign stack, not the swarmq "
        f"test containers (5699/15699/6399). Point {port_var} at the test "
        f"container, or set {ALLOW_DEFAULT_PORTS_VAR}=1 if this really is a "
        "disposable swarmq test service."
    )


def require_safe_target(host: str, port: int) -> None:
    """Bricht den Test hart ab, wenn ``host:port`` nach fremdem Stack aussieht.

    Pflicht vor jedem Flush und beim Sitzungsstart jeder Stufe mit Diensten.
    """
    reason = default_port_refusal(host, port, env=os.environ)
    if reason is not None:
        pytest.fail(reason, pytrace=False)


def require_safe_valkey_url(url: str) -> None:
    """``require_safe_target`` für eine Valkey-URL (Standardport 6379)."""
    parts = urlsplit(url)
    require_safe_target(parts.hostname or "localhost", parts.port or 6379)


async def flush_valkey_db(url: str) -> None:
    """FLUSHDB auf ``url`` — erst nach der Prüfung gegen fremde Stacks."""
    require_safe_valkey_url(url)
    import valkey.asyncio as aio_valkey

    client = aio_valkey.Valkey.from_url(url)
    try:
        await client.flushdb()
    finally:
        await client.aclose()


def require_safe_services() -> None:
    """``require_safe_target`` für RabbitMQ und Valkey dieser Sitzung."""
    require_safe_target(RABBITMQ_HOST, RABBITMQ_PORT)
    require_safe_target(VALKEY_HOST, VALKEY_PORT)


def port_is_open(host: str, port: int, timeout: float = 1.0) -> bool:
    """True, wenn eine TCP-Verbindung zu ``host:port`` gelingt."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _vhost_path(vhost: str) -> str:
    """URL-Pfad einer vhost; ``/`` bleibt ``/`` wie bisher."""
    return "" if vhost == "/" else quote(vhost, safe="")


def rabbitmq_amqp_url(host: str | None = None, port: int | None = None) -> str:
    """AMQP-URL in der vhost dieses Prozesses; ``host``/``port`` etwa für Toxiproxy."""
    return f"amqp://guest:guest@{host or RABBITMQ_HOST}:{port or RABBITMQ_PORT}/{_vhost_path(RABBITMQ_VHOST)}"


def valkey_db_url(host: str | None = None, port: int | None = None) -> str:
    """Valkey-URL auf der Datenbank dieses Prozesses; ``host``/``port`` etwa für Toxiproxy."""
    return f"valkey://{host or VALKEY_HOST}:{port or VALKEY_PORT}/{VALKEY_DB}"


@dataclass(frozen=True)
class RabbitMQNode:
    """Ein RabbitMQ-Dienst mit AMQP- und Management-Zugang (KTD9)."""

    host: str
    port: int
    management_port: int
    vhost: str = "/"

    @property
    def amqp_url(self) -> str:
        return f"amqp://guest:guest@{self.host}:{self.port}/{_vhost_path(self.vhost)}"

    @property
    def vhost_api(self) -> str:
        """vhost als Pfadsegment der Management-API (``/`` ist ``%2F``)."""
        return quote(self.vhost, safe="")

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
        url = f"{self.management_url}/policies/{self.vhost_api}/{quote(f'swarmq-test-ct-{queue}', safe='')}"
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


RABBITMQ_MAIN = RabbitMQNode(RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_MGMT_PORT, RABBITMQ_VHOST)
RABBITMQ_42 = RabbitMQNode(RABBITMQ42_HOST, RABBITMQ42_PORT, RABBITMQ42_MGMT_PORT, RABBITMQ_VHOST)


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
    """Überspringt das ganze Tier, wenn die Docker-Dienste nicht erreichbar sind.

    Vorher die harte Prüfung gegen fremde Stacks auf Standardports: Ein
    Treffer lässt jeden Test der Stufe fehlschlagen, bevor ein Fixture
    etwas flusht.
    """
    require_safe_services()
    missing = missing_services()
    if missing:
        pytest.skip(f"Docker services not available: {', '.join(missing)}")


def _queue_names(client: httpx.Client) -> set[str] | None:
    """Namen aller Queues der vhost dieses Prozesses; None ohne Management-API."""
    try:
        response = client.get(f"/queues/{RABBITMQ_MAIN.vhost_api}", params={"columns": "name"})
        response.raise_for_status()
    except httpx.HTTPError:
        return None
    return {entry["name"] for entry in response.json()}


@contextlib.contextmanager
def worker_vhost(node: RabbitMQNode) -> Iterator[None]:
    """Legt ``node.vhost`` mit Rechten für guest an und löscht sie danach."""
    with httpx.Client(base_url=node.management_url, auth=("guest", "guest"), timeout=10.0) as client:
        client.put(f"/vhosts/{node.vhost_api}").raise_for_status()
        permissions = {"configure": ".*", "write": ".*", "read": ".*"}
        client.put(f"/permissions/{node.vhost_api}/guest", json=permissions).raise_for_status()
        try:
            yield
        finally:
            with contextlib.suppress(httpx.HTTPError):  # Aufräumen ist best effort
                client.delete(f"/vhosts/{node.vhost_api}")


@pytest.fixture(autouse=True, scope="session")
def delete_session_queues() -> Iterator[None]:
    """Löscht am Sitzungsende die Queues, die in dieser Sitzung entstanden sind.

    Die Tests löschen ihre Queues nicht; ein voller Lauf über Integration und
    e2e hinterlässt rund 5700 Quorum-Queues. Am 2026-10-13 lief der
    Test-Broker so erst in den Kernel-OOM, mit Limit danach in den
    Speicheralarm, der jeden Verleger blockiert. Queues, die vor der Sitzung
    schon bestanden, bleiben stehen. Ohne erreichbares Management-API (etwa
    ein Broker ohne Plugin) entfällt das Aufräumen; die CI startet je Job
    einen frischen Broker.
    """
    if RABBITMQ_VHOST != "/":
        # xdist-Prozess: eigene vhost je erreichbarem Dienst, am Ende samt Queues weg.
        with contextlib.ExitStack() as stack:
            for node in (RABBITMQ_MAIN, RABBITMQ_42):
                if not node.missing():
                    stack.enter_context(worker_vhost(node))
            yield
        return
    with httpx.Client(base_url=RABBITMQ_MAIN.management_url, auth=("guest", "guest"), timeout=10.0) as client:
        before = _queue_names(client)
        yield
        after = _queue_names(client)
        if before is None or after is None:
            return

        def delete(name: str) -> None:
            with contextlib.suppress(httpx.HTTPError):  # Aufräumen ist best effort
                client.delete(f"/queues/{RABBITMQ_MAIN.vhost_api}/{quote(name, safe='')}")

        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(delete, sorted(after - before)))
