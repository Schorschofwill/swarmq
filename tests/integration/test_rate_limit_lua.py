"""Integration tests for ``sliding_window_counter.lua``.

These exercise the raw Lua script against a real Valkey instance —
no Python wrapper yet. The ``RateLimiter`` Python class is tested
separately in ``test_rate_limiter_integration.py``.
"""

from __future__ import annotations

import asyncio
import socket
import uuid

import pytest
import valkey.asyncio as valkey_asyncio

from swarmq.lua import load_script
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
def script_source() -> str:
    return load_script("sliding_window_counter")


@pytest.fixture
def task_name() -> str:
    """Unique per test so parallel runs do not cross-contaminate."""
    return f"test-{uuid.uuid4().hex[:12]}"


async def _server_time_ms(client) -> int:
    """Pull the canonical now_ms the RateLimiter will use."""
    seconds, microseconds = await client.time()
    return int(seconds) * 1000 + int(microseconds) // 1000


def _keys_for(task_name: str, now_ms: int, window_size_ms: int) -> list[str]:
    window_ts = now_ms - (now_ms % window_size_ms)
    prev_ts = window_ts - window_size_ms
    return [
        f"sq:rl:{task_name}:{window_ts}",
        f"sq:rl:{task_name}:{prev_ts}",
    ]


class TestHappyPath:
    async def test_ten_requests_under_limit_all_allowed(self, valkey_client, script_source, task_name):
        script = valkey_client.register_script(script_source)
        max_requests = 100
        window_size_ms = 60_000

        for expected_remaining in range(max_requests - 1, max_requests - 11, -1):
            now_ms = await _server_time_ms(valkey_client)
            keys = _keys_for(task_name, now_ms, window_size_ms)
            allowed, remaining, retry = await script(
                keys=keys, args=[max_requests, window_size_ms, now_ms]
            )
            assert allowed == 1
            assert retry == 0
            assert remaining == expected_remaining


class TestBoundary:
    async def test_exactly_at_limit_the_last_call_is_still_allowed(
        self, valkey_client, script_source, task_name
    ):
        script = valkey_client.register_script(script_source)
        max_requests = 3
        window_size_ms = 60_000

        for _ in range(max_requests):
            now_ms = await _server_time_ms(valkey_client)
            keys = _keys_for(task_name, now_ms, window_size_ms)
            allowed, _remaining, retry = await script(
                keys=keys, args=[max_requests, window_size_ms, now_ms]
            )
            assert allowed == 1
            assert retry == 0

        now_ms = await _server_time_ms(valkey_client)
        keys = _keys_for(task_name, now_ms, window_size_ms)
        allowed, remaining, retry = await script(
            keys=keys, args=[max_requests, window_size_ms, now_ms]
        )
        assert allowed == 0
        assert remaining == 0
        assert retry >= 1

    async def test_max_zero_rejects_every_request(
        self, valkey_client, script_source, task_name
    ):
        script = valkey_client.register_script(script_source)
        window_size_ms = 60_000

        now_ms = await _server_time_ms(valkey_client)
        keys = _keys_for(task_name, now_ms, window_size_ms)
        allowed, _remaining, retry = await script(
            keys=keys, args=[0, window_size_ms, now_ms]
        )
        assert allowed == 0
        assert retry >= 1

    async def test_window_zero_returns_zero_tuple_division_guard(
        self, valkey_client, script_source, task_name
    ):
        script = valkey_client.register_script(script_source)

        now_ms = await _server_time_ms(valkey_client)
        keys = _keys_for(task_name, now_ms, 1)  # real window just for key derivation
        allowed, remaining, retry = await script(
            keys=keys, args=[100, 0, now_ms]
        )
        assert allowed == 0
        assert remaining == 0
        assert retry == 0


class TestAtomicity:
    async def test_parallel_bursts_respect_limit_exactly(
        self, valkey_client, script_source, task_name
    ):
        """150 concurrent EVALSHA against max=100 -> exactly 100 allowed.

        Single-call atomicity is the only way max_requests can be a hard
        cap across concurrent callers. Proves the script owns the whole
        read-check-incr sequence.
        """
        script = valkey_client.register_script(script_source)
        max_requests = 100
        window_size_ms = 60_000
        total_calls = 150

        async def one_call() -> int:
            now_ms = await _server_time_ms(valkey_client)
            keys = _keys_for(task_name, now_ms, window_size_ms)
            allowed, _remaining, _retry = await script(
                keys=keys, args=[max_requests, window_size_ms, now_ms]
            )
            return int(allowed)

        results = await asyncio.gather(*(one_call() for _ in range(total_calls)))
        allowed_count = sum(results)
        denied_count = total_calls - allowed_count
        assert allowed_count == max_requests, (
            f"expected {max_requests} allowed, got {allowed_count}"
        )
        assert denied_count == total_calls - max_requests


class TestSlidingBehaviour:
    async def test_ttl_is_set_on_first_increment(
        self, valkey_client, script_source, task_name
    ):
        """After the first allowed call the current-window key must have
        a TTL of 2× window size so the next window sees it as 'previous'."""
        script = valkey_client.register_script(script_source)
        window_size_ms = 2_000  # small so the test stays fast

        now_ms = await _server_time_ms(valkey_client)
        keys = _keys_for(task_name, now_ms, window_size_ms)
        await script(keys=keys, args=[10, window_size_ms, now_ms])

        pttl = await valkey_client.pttl(keys[0])
        # PTTL returns >0 when a TTL is set; we expect roughly 2× window.
        assert pttl > window_size_ms, f"expected pttl > {window_size_ms}, got {pttl}"
        assert pttl <= window_size_ms * 2 + 200  # some wiggle for clock
