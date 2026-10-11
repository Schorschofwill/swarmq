"""``LockManager.extend``/``ValkeyLockStore.extend`` (Plan 0.12.0, MR 13,
BZ-25) gegen gemockte Skripte: Argumente, Rueckgabe, Fehler-Wrapping.

Test-Liste:
- [x] ``extend`` registriert ``lock_extend`` bei erster Nutzung und reicht
      Owner, TTL, max_concurrency und Server-TIME durch
- [x] Review MR 13 (Hinweis): Mutex ohne ``TIME`` (ein Roundtrip; das
      Skript liest ``now_ms`` nur fuer Semaphoren), ARGV bleibt 4
- [x] Fehlerantwort -> ``BackendResponseError``, Transportfehler ->
      ``BackendConnectionError``
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from valkey.exceptions import ResponseError

from swarmq.backend.valkey.locks import ValkeyLockStore
from swarmq.contracts import LockToken
from swarmq.exceptions import BackendConnectionError, BackendResponseError


def _store(extend_result: int = 1) -> tuple[ValkeyLockStore, AsyncMock]:
    client = MagicMock()
    client.time = AsyncMock(return_value=(1_700_000_000, 500_000))
    extend_script = AsyncMock(return_value=extend_result)
    scripts = [AsyncMock(), AsyncMock(), AsyncMock(), extend_script]
    client.register_script = MagicMock(side_effect=scripts)
    return ValkeyLockStore(client), extend_script


async def test_extend_passes_token_fields_and_server_time() -> None:
    store, script = _store()
    assert await store.extend(LockToken("sem", "owner-a", 4), ttl_ms=60_000) is True
    assert script.await_args.kwargs == {"keys": ["sq:lock:sem"], "args": ["owner-a", 60_000, 4, 1_700_000_000_500]}
    store2, _script2 = _store(extend_result=0)
    assert await store2.extend(LockToken("l", "owner-a", 1), ttl_ms=60_000) is False


async def test_extend_wraps_errors() -> None:
    store, script = _store()
    script.side_effect = ResponseError("WRONGTYPE")
    with pytest.raises(BackendResponseError, match="lock extend failed"):
        await store.extend(LockToken("l", "owner-a", 1), ttl_ms=1)
    script.side_effect = ConnectionError("gone")
    with pytest.raises(BackendConnectionError, match="lock extend failed"):
        await store.extend(LockToken("l", "owner-a", 1), ttl_ms=1)


async def test_mutex_extend_skips_server_time() -> None:
    store, script = _store()
    assert await store.extend(LockToken("l", "owner-a", 1), ttl_ms=60_000) is True
    store._client.time.assert_not_awaited()
    assert script.await_args.kwargs == {"keys": ["sq:lock:l"], "args": ["owner-a", 60_000, 1, 0]}
