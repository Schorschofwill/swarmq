"""Integration tests for ``LockManager``.

Exercises the Python wrapper around the Lua scripts against a real
Valkey instance. Mirrors ``tests/integration/test_rate_limiter_integration.py``.
"""

from __future__ import annotations

import asyncio
import socket
import uuid

import pytest
import valkey.asyncio as valkey_asyncio

from swarmq.backend.valkey.locks import LockManager
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
def lock_manager(valkey_client) -> LockManager:
    return LockManager(valkey_client)


@pytest.fixture
def lock_name() -> str:
    """Unique per test so parallel runs do not cross-contaminate."""
    return f"test-{uuid.uuid4().hex[:12]}"


class TestMutex:
    async def test_acquire_then_release_returns_true(self, lock_manager, lock_name):
        token = uuid.uuid4().hex
        assert await lock_manager.acquire(lock_name, token, 60_000, 1) is True
        assert await lock_manager.release(lock_name, token, 1) is True

    async def test_double_acquire_with_different_tokens_blocks_second(self, lock_manager, lock_name):
        token1 = uuid.uuid4().hex
        token2 = uuid.uuid4().hex
        assert await lock_manager.acquire(lock_name, token1, 60_000, 1) is True
        assert await lock_manager.acquire(lock_name, token2, 60_000, 1) is False
        await lock_manager.release(lock_name, token1, 1)

    async def test_release_with_foreign_token_returns_false_and_lock_held(self, lock_manager, lock_name):
        token = uuid.uuid4().hex
        await lock_manager.acquire(lock_name, token, 60_000, 1)
        foreign = uuid.uuid4().hex
        assert await lock_manager.release(lock_name, foreign, 1) is False
        # Owner can still release
        assert await lock_manager.release(lock_name, token, 1) is True


class TestSemaphore:
    async def test_max_concurrency_three_admits_exactly_three(self, lock_manager, lock_name):
        tokens = [uuid.uuid4().hex for _ in range(4)]
        for tok in tokens[:3]:
            assert await lock_manager.acquire(lock_name, tok, 60_000, 3) is True
        assert await lock_manager.acquire(lock_name, tokens[3], 60_000, 3) is False
        for tok in tokens[:3]:
            await lock_manager.release(lock_name, tok, 3)

    async def test_release_frees_a_slot(self, lock_manager, lock_name):
        max_concurrency = 2
        token1 = uuid.uuid4().hex
        token2 = uuid.uuid4().hex
        token3 = uuid.uuid4().hex
        await lock_manager.acquire(lock_name, token1, 60_000, max_concurrency)
        await lock_manager.acquire(lock_name, token2, 60_000, max_concurrency)
        # Full
        assert (await lock_manager.acquire(lock_name, token3, 60_000, max_concurrency)) is False
        # Free a slot
        await lock_manager.release(lock_name, token1, max_concurrency)
        assert (await lock_manager.acquire(lock_name, token3, 60_000, max_concurrency)) is True
        await lock_manager.release(lock_name, token2, max_concurrency)
        await lock_manager.release(lock_name, token3, max_concurrency)


class TestForceRelease:
    async def test_force_release_works_without_owner(self, lock_manager, lock_name):
        token = uuid.uuid4().hex
        await lock_manager.acquire(lock_name, token, 60_000, 1)
        # Anyone can __force__
        assert await lock_manager.release(lock_name, "__force__", 1) is True
        # Lock is gone — re-acquire works
        assert (await lock_manager.acquire(lock_name, uuid.uuid4().hex, 60_000, 1)) is True


class TestContention:
    async def test_parallel_acquires_atomically_observe_max_concurrency(self, lock_manager, lock_name):
        """20 parallel acquires against max_concurrency=4 → exactly 4 win."""
        max_concurrency = 4

        async def one():
            tok = uuid.uuid4().hex
            won = await lock_manager.acquire(lock_name, tok, 60_000, max_concurrency)
            return tok, won

        results = await asyncio.gather(*(one() for _ in range(20)))
        winners = [tok for tok, ok in results if ok]
        assert len(winners) == max_concurrency
        for tok in winners:
            await lock_manager.release(lock_name, tok, max_concurrency)


class TestRenew:
    """``renew`` extends TTL for the current mutex holder only.

    Covers the heartbeat path the embedded scheduler uses to keep its
    leader-lock alive (Unit 4 / Review-Finding P2-8).
    """

    async def test_renew_extends_ttl_for_owner(self, lock_manager, lock_name, valkey_client):
        token = uuid.uuid4().hex
        # Acquire with a short TTL, then renew to a much longer one.
        assert await lock_manager.acquire(lock_name, token, 2_000, 1) is True

        key = f"sq:lock:{lock_name}"
        pttl_before = await valkey_client.pttl(key)
        assert 0 < pttl_before <= 2_000

        assert await lock_manager.renew(lock_name, token, 60_000) is True
        pttl_after = await valkey_client.pttl(key)
        # Renew should push the TTL well past the original 2_000ms ceiling.
        assert pttl_after > pttl_before
        assert pttl_after > 30_000

        await lock_manager.release(lock_name, token, 1)

    async def test_renew_foreign_owner_returns_false_and_lock_unchanged(self, lock_manager, lock_name, valkey_client):
        owner = uuid.uuid4().hex
        intruder = uuid.uuid4().hex
        assert await lock_manager.acquire(lock_name, owner, 60_000, 1) is True

        key = f"sq:lock:{lock_name}"
        pttl_before = await valkey_client.pttl(key)

        assert await lock_manager.renew(lock_name, intruder, 5_000) is False
        pttl_after = await valkey_client.pttl(key)
        # Script must not have touched the key — TTL monotonically ticks
        # but the PEXPIRE path was not entered, so no sudden jump up.
        assert pttl_after <= pttl_before

        await lock_manager.release(lock_name, owner, 1)

    async def test_renew_absent_key_returns_false(self, lock_manager, lock_name):
        # No acquire — GET returns nil, script returns 0
        assert await lock_manager.renew(lock_name, "any-tok", 60_000) is False

    async def test_renew_after_expiry_returns_false(self, lock_manager, lock_name):
        token = uuid.uuid4().hex
        # Short TTL, wait for it to die naturally, then attempt renew.
        assert await lock_manager.acquire(lock_name, token, 300, 1) is True
        # Real-clock TTL wait — Valkey expiry is wall-clock based, no
        # observable event. Not a synchronization sleep.
        await asyncio.sleep(0.6)
        assert await lock_manager.renew(lock_name, token, 60_000) is False

    async def test_renew_rejects_semaphore_explicitly(self, lock_manager, lock_name):
        # P2-8 guard: semaphore-mode renew is intentionally NotImplemented
        with pytest.raises(NotImplementedError, match="mutex-only"):
            await lock_manager.renew(lock_name, "tok", 60_000, max_concurrency=2)
