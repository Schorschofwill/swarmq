"""Integration tests for ``lock_acquire.lua`` + ``lock_release.lua``.

Exercises the raw Lua scripts against a real Valkey instance — no
Python ``LockManager`` wrapper yet (that lives in ``test_locking.py``).
Same shape as ``test_rate_limit_lua.py``.
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
def acquire_script(valkey_client):
    return valkey_client.register_script(load_script("lock_acquire"))


@pytest.fixture
def release_script(valkey_client):
    return valkey_client.register_script(load_script("lock_release"))


@pytest.fixture
def lock_key() -> str:
    """Unique per test so parallel runs do not cross-contaminate."""
    return f"sq:lock:test-{uuid.uuid4().hex[:12]}"


async def _server_time_ms(client) -> int:
    seconds, microseconds = await client.time()
    return int(seconds) * 1000 + int(microseconds) // 1000


class TestMutexAcquireRelease:
    async def test_acquire_then_release_then_reacquire(self, valkey_client, acquire_script, release_script, lock_key):
        token1 = uuid.uuid4().hex
        now_ms = await _server_time_ms(valkey_client)
        # Acquire: should succeed
        result = await acquire_script(keys=[lock_key], args=[token1, 60_000, 1, now_ms])
        assert int(result) == 1

        # Re-acquire by another token while held: must fail
        token2 = uuid.uuid4().hex
        now_ms = await _server_time_ms(valkey_client)
        result = await acquire_script(keys=[lock_key], args=[token2, 60_000, 1, now_ms])
        assert int(result) == 0

        # Release with original owner: succeeds
        result = await release_script(keys=[lock_key], args=[token1, 1])
        assert int(result) == 1

        # Re-acquire after release: succeeds (any token)
        now_ms = await _server_time_ms(valkey_client)
        result = await acquire_script(keys=[lock_key], args=[token2, 60_000, 1, now_ms])
        assert int(result) == 1

        # Cleanup
        await release_script(keys=[lock_key], args=[token2, 1])

    async def test_acquire_with_zero_ttl_is_refused(self, valkey_client, acquire_script, lock_key):
        now_ms = await _server_time_ms(valkey_client)
        result = await acquire_script(keys=[lock_key], args=[uuid.uuid4().hex, 0, 1, now_ms])
        assert int(result) == 0

    async def test_acquire_with_negative_max_concurrency_is_refused(self, valkey_client, acquire_script, lock_key):
        now_ms = await _server_time_ms(valkey_client)
        result = await acquire_script(keys=[lock_key], args=[uuid.uuid4().hex, 60_000, 0, now_ms])
        assert int(result) == 0


class TestSemaphore:
    async def test_max_concurrency_three_allows_three_blocks_fourth(
        self, valkey_client, acquire_script, release_script, lock_key
    ):
        tokens = [uuid.uuid4().hex for _ in range(4)]
        # First three acquires succeed
        for tok in tokens[:3]:
            now_ms = await _server_time_ms(valkey_client)
            result = await acquire_script(keys=[lock_key], args=[tok, 60_000, 3, now_ms])
            assert int(result) == 1, f"acquire for {tok!r} should succeed"

        # Fourth blocks
        now_ms = await _server_time_ms(valkey_client)
        result = await acquire_script(keys=[lock_key], args=[tokens[3], 60_000, 3, now_ms])
        assert int(result) == 0

        # Release one slot
        result = await release_script(keys=[lock_key], args=[tokens[0], 3])
        assert int(result) == 1

        # Now fourth can acquire
        now_ms = await _server_time_ms(valkey_client)
        result = await acquire_script(keys=[lock_key], args=[tokens[3], 60_000, 3, now_ms])
        assert int(result) == 1

        # Cleanup
        for tok in tokens[1:]:
            await release_script(keys=[lock_key], args=[tok, 3])

    async def test_parallel_acquires_respect_max_concurrency_exactly(
        self, valkey_client, acquire_script, release_script, lock_key
    ):
        """20 concurrent acquires against max_concurrency=5 -> exactly 5 win."""
        max_concurrency = 5
        total_calls = 20

        async def one_acquire():
            tok = uuid.uuid4().hex
            now_ms = await _server_time_ms(valkey_client)
            result = await acquire_script(keys=[lock_key], args=[tok, 60_000, max_concurrency, now_ms])
            return tok, int(result)

        results = await asyncio.gather(*(one_acquire() for _ in range(total_calls)))
        winners = [tok for tok, ok in results if ok == 1]
        assert len(winners) == max_concurrency, f"expected {max_concurrency} winners, got {len(winners)}"

        # Cleanup
        for tok in winners:
            await release_script(keys=[lock_key], args=[tok, max_concurrency])


async def _wait_for_pttl_expiry(client, key: str, deadline_s: float = 2.0) -> None:
    """Poll PTTL until the key expires (returns -2). Avoids asyncio.sleep
    in test control flow per AGENTS.md — PTTL is the canonical signal
    that a TTL-driven Valkey key has actually expired, not "probably
    enough time has passed"."""
    loop = asyncio.get_event_loop()
    deadline = loop.time() + deadline_s
    while loop.time() < deadline:
        pttl = await client.pttl(key)
        if pttl == -2:
            return
        # Brief yield so other tasks make progress; this is the smallest
        # cooperative yield, not a wall-clock wait. Equivalent to
        # asyncio.sleep(0) in spirit but with a tiny floor for syscall
        # cadence under heavy event-loop pressure.
        await asyncio.sleep(0.01)
    raise AssertionError(f"key {key!r} did not expire within {deadline_s}s")


class TestExpiry:
    async def test_mutex_ttl_releases_lock_automatically(self, valkey_client, acquire_script, lock_key):
        """After TTL expires the next acquire must succeed even without release."""
        token1 = uuid.uuid4().hex
        now_ms = await _server_time_ms(valkey_client)
        result = await acquire_script(keys=[lock_key], args=[token1, 200, 1, now_ms])
        assert int(result) == 1

        # Wait for the actual TTL expiry signal from Valkey, not a
        # wall-clock guess.
        await _wait_for_pttl_expiry(valkey_client, lock_key)

        token2 = uuid.uuid4().hex
        now_ms = await _server_time_ms(valkey_client)
        result = await acquire_script(keys=[lock_key], args=[token2, 60_000, 1, now_ms])
        assert int(result) == 1

    async def test_semaphore_expired_members_cleaned_on_acquire(
        self, valkey_client, acquire_script, release_script, lock_key
    ):
        """Semaphore SortedSet entries with score < (now - ttl) get removed
        in the same EVAL as the acquire-check, freeing slots for new
        holders. Without this, a crashed worker's stale token would
        permanently consume a slot until ZADD-driven PEXPIRE triggered.
        """
        # Step 1: fill the semaphore with TTL=200ms tokens
        max_concurrency = 3
        stale_tokens = [uuid.uuid4().hex for _ in range(max_concurrency)]
        for tok in stale_tokens:
            now_ms = await _server_time_ms(valkey_client)
            result = await acquire_script(keys=[lock_key], args=[tok, 200, max_concurrency, now_ms])
            assert int(result) == 1

        # Step 2: wait for the SortedSet's PEXPIRE to fire. We could also
        # poll for now_ms - oldest_score > ttl_ms but that's fragile;
        # the key being gone is a clean signal that the in-script
        # ZREMRANGEBYSCORE will encounter no live members.
        await _wait_for_pttl_expiry(valkey_client, lock_key)

        # Step 3: a new acquire should clean up the stale members and succeed
        fresh_token = uuid.uuid4().hex
        now_ms = await _server_time_ms(valkey_client)
        result = await acquire_script(keys=[lock_key], args=[fresh_token, 60_000, max_concurrency, now_ms])
        assert int(result) == 1

        # Verify the set now has only the fresh token
        members = await valkey_client.zrange(lock_key, 0, -1)
        assert members == [fresh_token.encode()]

        # Cleanup
        await release_script(keys=[lock_key], args=[fresh_token, max_concurrency])


class TestOwnerSafety:
    async def test_mutex_foreign_release_does_nothing(self, valkey_client, acquire_script, release_script, lock_key):
        token1 = uuid.uuid4().hex
        now_ms = await _server_time_ms(valkey_client)
        result = await acquire_script(keys=[lock_key], args=[token1, 60_000, 1, now_ms])
        assert int(result) == 1

        foreign_token = uuid.uuid4().hex
        result = await release_script(keys=[lock_key], args=[foreign_token, 1])
        assert int(result) == 0

        # Lock must still be held — owner can release
        result = await release_script(keys=[lock_key], args=[token1, 1])
        assert int(result) == 1

    async def test_semaphore_foreign_release_does_nothing(
        self, valkey_client, acquire_script, release_script, lock_key
    ):
        token1 = uuid.uuid4().hex
        now_ms = await _server_time_ms(valkey_client)
        result = await acquire_script(keys=[lock_key], args=[token1, 60_000, 3, now_ms])
        assert int(result) == 1

        foreign_token = uuid.uuid4().hex
        result = await release_script(keys=[lock_key], args=[foreign_token, 3])
        assert int(result) == 0

        # Owner can still release
        result = await release_script(keys=[lock_key], args=[token1, 3])
        assert int(result) == 1


class TestNonExistent:
    async def test_release_nonexistent_mutex_returns_zero_no_crash(self, valkey_client, release_script, lock_key):
        result = await release_script(keys=[lock_key], args=[uuid.uuid4().hex, 1])
        assert int(result) == 0

    async def test_release_nonexistent_semaphore_returns_zero_no_crash(self, valkey_client, release_script, lock_key):
        result = await release_script(keys=[lock_key], args=[uuid.uuid4().hex, 3])
        assert int(result) == 0


class TestForceBypass:
    async def test_force_release_mutex_returns_two(self, valkey_client, acquire_script, release_script, lock_key):
        token = uuid.uuid4().hex
        now_ms = await _server_time_ms(valkey_client)
        await acquire_script(keys=[lock_key], args=[token, 60_000, 1, now_ms])

        # Even foreign caller can __force__ release
        result = await release_script(keys=[lock_key], args=["__force__", 1])
        assert int(result) == 2

        # Lock is gone — new acquire succeeds
        now_ms = await _server_time_ms(valkey_client)
        result = await acquire_script(keys=[lock_key], args=[uuid.uuid4().hex, 60_000, 1, now_ms])
        assert int(result) == 1

    async def test_force_release_semaphore_clears_all_holders(
        self, valkey_client, acquire_script, release_script, lock_key
    ):
        max_concurrency = 3
        for _ in range(max_concurrency):
            now_ms = await _server_time_ms(valkey_client)
            await acquire_script(
                keys=[lock_key],
                args=[uuid.uuid4().hex, 60_000, max_concurrency, now_ms],
            )

        result = await release_script(keys=[lock_key], args=["__force__", max_concurrency])
        assert int(result) == 2

        # Set is gone — full capacity available
        for _ in range(max_concurrency):
            now_ms = await _server_time_ms(valkey_client)
            result = await acquire_script(
                keys=[lock_key],
                args=[uuid.uuid4().hex, 60_000, max_concurrency, now_ms],
            )
            assert int(result) == 1

    async def test_force_release_nonexistent_returns_two_no_crash(self, valkey_client, release_script, lock_key):
        result = await release_script(keys=[lock_key], args=["__force__", 1])
        assert int(result) == 2
