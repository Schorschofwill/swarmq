"""Integration tests for ValkeyBackend against a real Valkey instance."""

from __future__ import annotations

import uuid

import pytest

from swarmq.backend.valkey import ValkeyBackend
from tests.support.services import require_safe_valkey_url


def _task_id() -> str:
    return f"test_{uuid.uuid4().hex[:12]}"


@pytest.fixture
async def backend(valkey_url: str):
    """Yield a connected ValkeyBackend; flush the test DB on teardown."""
    require_safe_valkey_url(valkey_url)
    b = ValkeyBackend(valkey_url)
    await b.connect()
    yield b
    # Flush test DB so tests don't leak state.
    if b._client is not None:
        await b._client.flushdb()
        await b.close()


class TestConnectDisconnect:
    async def test_connect_and_close(self, backend: ValkeyBackend) -> None:
        """connect() establishes a working client; close() tears it down."""
        assert backend._client is not None
        pong = await backend._client.ping()
        assert pong is True
        await backend.close()
        assert backend._client is None


class TestStoreAndGetResult:
    async def test_roundtrip(self, backend: ValkeyBackend) -> None:
        """store_result followed by get_result returns the same data."""
        tid = _task_id()
        data = b'{"result": 42}'
        await backend.store_result(tid, data, ttl_ms=60_000)

        retrieved = await backend.get_result(tid)
        assert retrieved == data


class TestTTL:
    async def test_pttl_is_set(self, backend: ValkeyBackend) -> None:
        """Stored key has a positive PTTL matching the requested TTL."""
        tid = _task_id()
        ttl_ms = 30_000
        await backend.store_result(tid, b"ttl-check", ttl_ms=ttl_ms)

        key = backend._key(tid)
        pttl = await backend._client.pttl(key)
        # PTTL should be positive and not exceed what we set.
        assert 0 < pttl <= ttl_ms


class TestGetNonExistent:
    async def test_returns_none(self, backend: ValkeyBackend) -> None:
        """get_result for a key that was never stored returns None."""
        result = await backend.get_result("nonexistent_task_id")
        assert result is None


class TestDeleteResult:
    async def test_delete_removes_key(self, backend: ValkeyBackend) -> None:
        """delete_result removes the key so get_result returns None."""
        tid = _task_id()
        await backend.store_result(tid, b"to-delete", ttl_ms=60_000)

        # Verify it exists first.
        assert await backend.get_result(tid) is not None

        await backend.delete_result(tid)
        assert await backend.get_result(tid) is None
