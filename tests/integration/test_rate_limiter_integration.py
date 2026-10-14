"""Integration tests for the ``RateLimiter`` Python wrapper."""

from __future__ import annotations

import socket
import uuid

import pytest
import valkey.asyncio as valkey_asyncio

from swarmq.backend.valkey.rate_limit import RateLimiter
from swarmq.limits import parse_rate_limit
from tests.support.services import VALKEY_HOST, VALKEY_PORT, valkey_db_url

# Host/Port aus der gemeinsamen Testkonfiguration (Standard: Testcontainer 6399).
VALKEY_URL = valkey_db_url()


def _port_open(host: str, port: int, timeout: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not _port_open(VALKEY_HOST, VALKEY_PORT),
        reason=f"Valkey not reachable at {VALKEY_HOST}:{VALKEY_PORT}",
    ),
]


@pytest.fixture
async def valkey_client():
    client = valkey_asyncio.Valkey.from_url(VALKEY_URL)
    try:
        await client.ping()
        yield client
    finally:
        await client.aclose()


@pytest.fixture
def task_name() -> str:
    return f"test-{uuid.uuid4().hex[:12]}"


class TestCheck:
    async def test_five_per_second_allows_five_then_blocks(self, valkey_client, task_name):
        limiter = RateLimiter(valkey_client)
        config = parse_rate_limit("5/s")

        for _ in range(5):
            allowed, remaining, retry = await limiter.check(task_name, config)
            assert allowed is True
            assert retry == 0
            assert remaining >= 0

        allowed, remaining, retry = await limiter.check(task_name, config)
        assert allowed is False
        assert retry >= 1
        assert remaining == 0

    async def test_two_task_names_are_isolated(self, valkey_client):
        limiter = RateLimiter(valkey_client)
        # Minutenfenster: Geprüft wird die Trennung der Task-Namen, nicht die
        # Fensterlänge. Mit 1/s lief unter CI-Last mehr als eine Sekunde
        # zwischen den Aufrufen, und der zweite Aufruf von task_a ging durch.
        config = parse_rate_limit("1/m")
        task_a = f"a-{uuid.uuid4().hex[:8]}"
        task_b = f"b-{uuid.uuid4().hex[:8]}"

        allowed_a1, *_ = await limiter.check(task_a, config)
        assert allowed_a1 is True

        # task_a is now rate-limited.
        allowed_a2, *_ = await limiter.check(task_a, config)
        assert allowed_a2 is False

        # task_b has its own keys and should still be allowed.
        allowed_b1, *_ = await limiter.check(task_b, config)
        assert allowed_b1 is True

    async def test_returns_tuple_of_expected_python_types(self, valkey_client, task_name):
        """Guard against silently returning bytes or redis-isms."""
        limiter = RateLimiter(valkey_client)
        config = parse_rate_limit("10/m")

        result = await limiter.check(task_name, config)
        assert isinstance(result, tuple)
        assert len(result) == 3
        allowed, remaining, retry = result
        assert isinstance(allowed, bool)
        assert isinstance(remaining, int)
        assert isinstance(retry, int)
