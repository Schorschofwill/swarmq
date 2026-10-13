"""Integration tests for ValkeyBackend against a real Valkey instance."""

from __future__ import annotations

import asyncio
import contextlib
import uuid

import pytest
import valkey.asyncio as aio_valkey

from swarmq.backend.valkey import ValkeyBackend
from swarmq.exceptions import BackendConnectionError
from tests.support.services import require_safe_valkey_url
from tests.support.wait import wait_until


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

    async def test_a_store_held_past_close_fails_instead_of_reconnecting(self, backend: ValkeyBackend) -> None:
        """Ein Worker hält ``backend.locks`` über ``close()`` hinaus; ein
        Handler gibt seinen Lock erst nach dem Abbruch frei. Statt über den
        geschlossenen Pool eine Verbindung zu öffnen, die niemand mehr
        schließt (ResourceWarning beim Abräumen), scheitert er wie jede
        Operation nach ``close()`` (Plan 2026-10-13-0902, U3)."""
        locks = backend.locks
        token = await locks.acquire("close-test", owner="o", ttl_ms=60_000, max_concurrency=1)
        assert token is not None

        await backend.close()

        with pytest.raises(BackendConnectionError, match="closed"):
            await locks.release(token)

    async def test_a_store_call_scheduled_before_close_fails_instead_of_reconnecting(
        self, backend: ValkeyBackend
    ) -> None:
        """Ein Handler startet seine Lock-Freigabe, bevor ``close()`` läuft,
        kommt aber erst danach zum Zug (bz20: Ergebnisse nach dem
        Worker-Abbruch). Er wartete auf die Pool-Sperre, die ``close()``
        hält, und verband danach die gerade getrennte Verbindung neu."""
        locks = backend.locks
        token = await locks.acquire("close-race", owner="o", ttl_ms=60_000, max_concurrency=1)
        assert token is not None

        release = asyncio.create_task(locks.release(token))
        await backend.close()

        with pytest.raises(BackendConnectionError, match="closed"):
            await release

    async def test_a_command_in_flight_during_close_leaves_no_connection_open(
        self, backend: ValkeyBackend, valkey_url: str
    ) -> None:
        """Ein Befehl hält beim ``close()`` eine Verbindung. ``close()`` trennt
        sie, die Retry-Politik verbindet sie für den nächsten Versuch neu, und
        der Befehl gibt sie danach an den getrennten Pool zurück (bz20). Nach
        seinem Ende darf keine Verbindung des Backends mehr offen sein."""
        name = f"close-inflight-{uuid.uuid4().hex[:8]}"
        await backend.client.client_setname(name)
        observer = aio_valkey.Valkey.from_url(valkey_url)
        try:

            async def clients_named() -> int:
                listed = await observer.client_list()
                return sum(1 for client in listed if client.get("name") == name)

            async def blocked() -> bool:
                listed = await observer.client_list()
                return any(c.get("name") == name and c.get("cmd") == "blpop" for c in listed)

            in_flight = asyncio.create_task(backend.client.blpop([f"{name}-key"], timeout=1))
            assert await wait_until(blocked, timeout=5.0)

            await backend.close()
            with contextlib.suppress(Exception):
                await in_flight

            assert await clients_named() == 0
        finally:
            await observer.aclose()


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
        assert backend._client is not None
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
