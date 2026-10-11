"""Chaos-test fixtures (E2E-Hardening Phase 2).

Provides:
- ``chaos_stack`` (scope=class): starts a dedicated docker compose stack
  with RabbitMQ + Valkey + Toxiproxy on randomised ports. Tests get
  the proxied URLs; a chaos toxic on toxiproxy disrupts the path
  between client/worker and the underlying service.
- ``toxiproxy``: function-scoped helper around the toxiproxy REST API
  (httpx, no extra package). Provides ``add_latency``, ``add_bandwidth``,
  ``down``, ``up``, ``reset`` and auto-resets on teardown.

Stack lifecycle is per-test-class to amortise the ~10s startup cost.
Auto-marker `chaos` ensures these tests stay opt-in.
"""

from __future__ import annotations

import os
import random
import re
import socket
import subprocess
import time
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from pathlib import Path

import httpx
import pytest

_COMPOSE_FILE = Path(__file__).parent / "compose.yml"


def _service_host() -> str:
    """Host where the compose-published ports are reachable.

    Locally the stack publishes onto the loopback interface, so 127.0.0.1
    is correct. Under GitLab docker-in-docker the compose containers run on
    the dind daemon's container, not the job container — their published
    ports are reachable via the ``DOCKER_HOST`` hostname (e.g. ``docker``),
    NOT the job container's loopback. Derive it from ``DOCKER_HOST`` so the
    same fixture works in both environments.
    """
    match = re.match(r"^[a-z0-9+.-]+://([^:/]+)", os.environ.get("DOCKER_HOST", ""))
    if match and match.group(1) not in ("127.0.0.1", "localhost"):
        return match.group(1)
    return "127.0.0.1"


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Auto-tag every test under tests/chaos/ as @pytest.mark.chaos."""
    for item in items:
        if "tests/chaos/" in str(item.fspath):
            item.add_marker(pytest.mark.chaos)


def _free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _wait_port(host: str, port: int, deadline: float) -> None:
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1.0):
                return
        except OSError:
            time.sleep(0.3)
    raise RuntimeError(f"port {host}:{port} never opened")


@dataclass
class ChaosStack:
    project: str
    rabbitmq_url: str  # via toxiproxy
    valkey_url: str  # via toxiproxy
    toxi_admin_url: str

    def stop(self) -> None:
        env = dict(os.environ)
        env["COMPOSE_PROJECT_NAME"] = self.project
        subprocess.run(
            ["docker", "compose", "-f", str(_COMPOSE_FILE), "down", "-v"],
            env=env,
            check=False,
            capture_output=True,
            timeout=30,
        )


#: When set, a missing chaos stack is a failure, not a skip. CI sets it for
#: the non-skippable chaos gate (plan U2 / KTD15).
#:
#: Without this, the gate was worthless and nobody could tell: both runs that
#: ever executed reported "1 passed, 40 skipped" and went green, because the
#: dind service had not come up and every test skipped itself. A gate that
#: cannot run must say so, not pass.
_REQUIRE_ENV = "SWARMQ_REQUIRE_CHAOS"


def _unavailable(reason: str) -> None:
    """Skip locally, fail where the stack is mandatory."""
    if os.environ.get(_REQUIRE_ENV, "").strip() not in ("", "0", "false"):
        pytest.fail(f"{reason} — {_REQUIRE_ENV} is set, so this is a failure, not a skip")
    pytest.skip(reason)


@pytest.fixture(scope="class")
def chaos_stack() -> Iterator[ChaosStack]:
    """Spin up a dedicated chaos stack on random ports for the test class."""
    if (
        subprocess.run(
            ["docker", "info"],
            capture_output=True,
            check=False,
        ).returncode
        != 0
    ):
        _unavailable("docker not available — chaos tests need a daemon")

    project = f"swarmq-chaos-{random.randint(0, 1_000_000):06d}"
    admin_port = _free_port()
    rmq_port = _free_port()
    vkey_port = _free_port()

    env = dict(os.environ)
    env["COMPOSE_PROJECT_NAME"] = project
    env["TOXI_ADMIN_PORT"] = str(admin_port)
    env["RABBITMQ_PROXY_PORT"] = str(rmq_port)
    env["VALKEY_PROXY_PORT"] = str(vkey_port)

    up = subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            str(_COMPOSE_FILE),
            "up",
            "-d",
            "--wait",
        ],
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    if up.returncode != 0:
        subprocess.run(
            ["docker", "compose", "-f", str(_COMPOSE_FILE), "down", "-v"],
            env=env,
            check=False,
            capture_output=True,
            timeout=30,
        )
        _unavailable(f"chaos compose stack failed to start: {up.stderr[-500:] if up.stderr else 'no stderr'}")

    host = _service_host()
    deadline = time.monotonic() + 30
    _wait_port(host, admin_port, deadline)

    # Register the two proxies so tests can manipulate them.
    with httpx.Client(base_url=f"http://{host}:{admin_port}") as c:
        for name, listen, upstream in (
            ("rmq", "0.0.0.0:5672", "rabbitmq:5672"),
            ("vkey", "0.0.0.0:6379", "valkey:6379"),
        ):
            r = c.post(
                "/proxies",
                json={
                    "name": name,
                    "listen": listen,
                    "upstream": upstream,
                    "enabled": True,
                },
            )
            assert r.status_code in (200, 201, 409), f"toxiproxy proxy create failed: {r.status_code} {r.text}"

    stack = ChaosStack(
        project=project,
        rabbitmq_url=f"amqp://guest:guest@{host}:{rmq_port}/",
        valkey_url=f"valkey://{host}:{vkey_port}/0",
        toxi_admin_url=f"http://{host}:{admin_port}",
    )
    try:
        yield stack
    finally:
        stack.stop()


class ToxiproxyClient:
    """Async helper for the Shopify toxiproxy REST API."""

    def __init__(self, admin_url: str) -> None:
        self._base = admin_url
        self._client = httpx.AsyncClient(base_url=admin_url, timeout=5.0)
        self._toxics_added: list[tuple[str, str]] = []

    async def aclose(self) -> None:
        await self._client.aclose()

    async def down(self, proxy: str) -> None:
        await self._client.post(f"/proxies/{proxy}", json={"enabled": False})

    async def up(self, proxy: str) -> None:
        await self._client.post(f"/proxies/{proxy}", json={"enabled": True})

    async def add_latency(
        self,
        proxy: str,
        *,
        latency_ms: int,
        jitter_ms: int = 0,
        upstream_only: bool = False,
    ) -> str:
        name = f"latency-{len(self._toxics_added)}"
        stream = "upstream" if upstream_only else "downstream"
        await self._client.post(
            f"/proxies/{proxy}/toxics",
            json={
                "name": name,
                "type": "latency",
                "stream": stream,
                "attributes": {"latency": latency_ms, "jitter": jitter_ms},
            },
        )
        self._toxics_added.append((proxy, name))
        return name

    async def add_blackhole(self, proxy: str) -> str:
        """Verschluckt Pakete, ohne die Verbindung zu beenden.

        Der Unterschied zu ``down()`` ist der ganze Punkt: Abschalten
        schliesst alle offenen Verbindungen, die Gegenseite sieht also ein
        Verbindungsende und der bestehende Wiederholpfad faengt es ab. Der
        ``timeout``-Toxic mit ``timeout: 0`` laesst die Verbindung dagegen
        offen und stoppt nur den Datenfluss — genau der stille Ausfall aus
        dem Referenzlauf vom 2026-08-28, bei dem ein Publish unbegrenzt auf
        sein Publisher-Confirm wartete (Plan 2026-08-28-001, U6).
        """
        name = f"blackhole-{len(self._toxics_added)}"
        await self._client.post(
            f"/proxies/{proxy}/toxics",
            json={
                "name": name,
                "type": "timeout",
                "stream": "upstream",
                "attributes": {"timeout": 0},
            },
        )
        self._toxics_added.append((proxy, name))
        return name

    async def add_bandwidth(
        self,
        proxy: str,
        *,
        kbps: int,
        upstream_only: bool = False,
    ) -> str:
        name = f"bandwidth-{len(self._toxics_added)}"
        stream = "upstream" if upstream_only else "downstream"
        await self._client.post(
            f"/proxies/{proxy}/toxics",
            json={
                "name": name,
                "type": "bandwidth",
                "stream": stream,
                "attributes": {"rate": kbps},
            },
        )
        self._toxics_added.append((proxy, name))
        return name

    async def add_slicer(
        self,
        proxy: str,
        *,
        average_size: int,
        size_variation: int = 0,
        delay_us: int = 0,
    ) -> str:
        name = f"slicer-{len(self._toxics_added)}"
        await self._client.post(
            f"/proxies/{proxy}/toxics",
            json={
                "name": name,
                "type": "slicer",
                "stream": "downstream",
                "attributes": {
                    "average_size": average_size,
                    "size_variation": size_variation,
                    "delay": delay_us,
                },
            },
        )
        self._toxics_added.append((proxy, name))
        return name

    async def reset(self) -> None:
        """Remove every toxic added via this instance and re-enable proxies."""
        for proxy, toxic in list(self._toxics_added):
            await self._client.delete(f"/proxies/{proxy}/toxics/{toxic}")
        self._toxics_added.clear()
        for name in ("rmq", "vkey"):
            await self._client.post(
                f"/proxies/{name}",
                json={"enabled": True},
            )


@pytest.fixture
async def toxiproxy(chaos_stack: ChaosStack) -> AsyncIterator[ToxiproxyClient]:
    """Per-test toxiproxy helper with auto-reset."""
    client = ToxiproxyClient(chaos_stack.toxi_admin_url)
    try:
        yield client
    finally:
        try:
            await client.reset()
        finally:
            await client.aclose()
