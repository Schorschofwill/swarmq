"""``ScriptPipeline`` des Ausfuehrungsschutzes gegen echtes Valkey (Plan
2026-10-08-1541, U8, Durchsatz): das selbst gepackte RESP, das Lesen
gebündelter Antworten und die NOSCRIPT-Erholung, die eine gefälschte
Verbindung nicht beweisen kann.

Test-Liste:
1. Viele nebenläufige Claims und Abschlüsse verschiedener Identitäten
   liefern je ihren eigenen Status (``claimed``, danach ``done``).
2. Nach ``SCRIPT FLUSH`` gelingt der nächste Claim (NOSCRIPT -> Nachladen).
3. Eine Fehlerantwort (WRONGTYPE) scheitert nur ihren eigenen Claim als
   ``BackendResponseError``; die Nachbarn im Stapel gelingen.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator, Awaitable
from typing import cast

import pytest

from swarmq.backend.valkey import ValkeyBackend
from swarmq.backend.valkey.execution_guard import run_key
from swarmq.exceptions import BackendResponseError

pytestmark = [pytest.mark.integration, pytest.mark.timeout(10)]

STATE_TTL = 600_000


@pytest.fixture
async def backend(valkey_url: str) -> AsyncIterator[ValkeyBackend]:
    backend = ValkeyBackend(valkey_url)
    await backend.connect()
    yield backend
    await backend.close()


async def _claim(backend: ValkeyBackend, identity: str) -> str:
    claim = await backend.execution_guard.claim(
        identity,
        owner=f"o-{identity}",
        retry_count=0,
        ttl_ms=60_000,
        state_ttl_ms=STATE_TTL,
        max_orphans=3,
    )
    return claim.status


async def test_concurrent_claims_and_finishes_get_their_own_status(backend: ValkeyBackend) -> None:
    identities = [f"pipe-{uuid.uuid4().hex[:10]}-{i}" for i in range(200)]

    assert await asyncio.gather(*(_claim(backend, i) for i in identities)) == ["claimed"] * 200
    await asyncio.gather(
        *(backend.execution_guard.mark_done(i, owner=f"o-{i}", state_ttl_ms=STATE_TTL) for i in identities)
    )
    assert await asyncio.gather(*(_claim(backend, i) for i in identities)) == ["done"] * 200


async def test_claim_succeeds_after_the_script_cache_was_flushed(backend: ValkeyBackend) -> None:
    assert await _claim(backend, f"pipe-{uuid.uuid4().hex}") == "claimed"
    await backend.client.script_flush()

    assert await _claim(backend, f"pipe-{uuid.uuid4().hex}") == "claimed"


async def test_error_reply_fails_only_its_own_claim(backend: ValkeyBackend) -> None:
    broken = f"pipe-{uuid.uuid4().hex}"
    await cast("Awaitable[int]", backend.client.hset(run_key(broken), "x", "1"))  # GET auf einen Hash: WRONGTYPE

    left, right = f"pipe-{uuid.uuid4().hex}", f"pipe-{uuid.uuid4().hex}"
    results = await asyncio.gather(
        _claim(backend, left),
        _claim(backend, broken),
        _claim(backend, right),
        return_exceptions=True,
    )

    assert results[0] == "claimed"
    assert isinstance(results[1], BackendResponseError)
    assert results[2] == "claimed"
