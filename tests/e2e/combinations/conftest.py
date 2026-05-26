"""Shared fixtures for the e2e feature-combinations test suite.

Builds on top of ``tests/e2e/conftest.py`` (which provides ``rabbitmq_url``,
``valkey_url`` and the ``check_docker`` skip-guard) and adds:

- Toxiproxy connectivity + Tier-2 proxied broker/backend URLs
- A function-scoped ``signal_recorder`` that captures Signal emit order
  for ``assert_signal_sequence`` (see ``_invariants``)
- An autouse cleanup that resets the Task registry + Toxiproxy state
  between tests so failures don't bleed across.
"""

from __future__ import annotations

import contextlib
import os
import uuid

import pytest

from swarmq.task import Task

from ._subprocess import port_open
from ._toxiproxy import (
    DEFAULT_ADMIN_HOST,
    DEFAULT_ADMIN_PORT,
    ProxyConfig,
    ToxiproxyClient,
)

_TOXIPROXY_HOST = os.environ.get(
    "SWARMQ_TEST_TOXIPROXY_HOST", DEFAULT_ADMIN_HOST,
)
_TOXIPROXY_PORT = int(
    os.environ.get("SWARMQ_TEST_TOXIPROXY_PORT", str(DEFAULT_ADMIN_PORT)),
)

# Proxy listen ports — must match docker-compose.chaos.yml.
_RMQ_PROXY_PORT = 5673
_VALKEY_PROXY_PORT = 6380

# Upstream service names — must match the docker network. The values default
# to docker-compose service names; env override lets a non-compose harness
# (CI service containers without DNS) override to e.g. ``localhost:5672``.
_RMQ_UPSTREAM = os.environ.get(
    "SWARMQ_TEST_TOXIPROXY_RMQ_UPSTREAM", "rabbitmq:5672",
)
_VALKEY_UPSTREAM = os.environ.get(
    "SWARMQ_TEST_TOXIPROXY_VALKEY_UPSTREAM", "valkey:6379",
)


# ---------------------------------------------------------------------------
# Toxiproxy fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def toxiproxy_available() -> bool:
    """Session-cached probe — is the Toxiproxy Admin API reachable?

    Tests that don't need Toxiproxy (Tier 1) can stay agnostic; tests that
    do (Tier 2/3) skip when False.
    """
    return port_open(_TOXIPROXY_HOST, _TOXIPROXY_PORT)


@pytest.fixture
async def toxiproxy(
    toxiproxy_available: bool,
) -> ToxiproxyClient:
    """Async ToxiproxyClient — context-managed so any leaked toxics are reset
    on test exit. Skips the test when Toxiproxy is not reachable."""
    if not toxiproxy_available:
        pytest.skip(
            f"Toxiproxy not reachable at {_TOXIPROXY_HOST}:{_TOXIPROXY_PORT}. "
            "Start it with `docker compose -f docker-compose.yml "
            "-f docker-compose.chaos.yml up -d`.",
        )
    async with ToxiproxyClient(
        host=_TOXIPROXY_HOST, port=_TOXIPROXY_PORT,
    ) as tp:
        yield tp


@pytest.fixture
async def rabbitmq_proxied_url(
    toxiproxy: ToxiproxyClient,
) -> str:
    """Ensure the RabbitMQ proxy exists and return a connection URL through it."""
    await toxiproxy.create_proxy(
        ProxyConfig(
            name="rabbitmq",
            listen=f"0.0.0.0:{_RMQ_PROXY_PORT}",
            upstream=_RMQ_UPSTREAM,
        ),
    )
    return f"amqp://guest:guest@localhost:{_RMQ_PROXY_PORT}/"


@pytest.fixture
async def valkey_proxied_url(
    toxiproxy: ToxiproxyClient,
) -> str:
    """Ensure the Valkey proxy exists and return a connection URL through it."""
    await toxiproxy.create_proxy(
        ProxyConfig(
            name="valkey",
            listen=f"0.0.0.0:{_VALKEY_PROXY_PORT}",
            upstream=_VALKEY_UPSTREAM,
        ),
    )
    return f"valkey://localhost:{_VALKEY_PROXY_PORT}/0"


# ---------------------------------------------------------------------------
# Test-isolation fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def unique_queue() -> str:
    """Per-test queue name so parallel/repeated runs don't collide on
    broker-side queue state."""
    return f"e2e-combo-{uuid.uuid4().hex[:10]}"


@pytest.fixture(autouse=True)
def _combinations_cleanup():
    """Autouse teardown for every combinations test:

    - Clear the global Task registry (`Task._registered_tasks`) so
      registration in one test doesn't leak to the next.
    """
    yield
    Task._clear_registered_tasks()
