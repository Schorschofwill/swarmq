"""``ValkeyExecutionGuardStore`` (Plan 2026-10-08-1541, U8) gegen eine
gefälschte Verbindung -- nur die Fehlerabbildung; die Semantik prüft die Contract-Suite
gegen echtes Valkey (``tests/integration/test_contracts_valkey.py``).

Test-Liste:
1. Ein Transportfehler wird zum einfachen ``BackendConnectionError``
   (Ausfall: das Gate stellt zurück).
2. Eine Fehlerantwort wird zu ``BackendResponseError`` (kein Ausfall).
3. Nebenläufige Aufrufe teilen sich einen Roundtrip (``ScriptPipeline``):
   drei Claims gehen in einem Schreibvorgang hinaus, jeder bekommt seinen
   Status.
"""

from __future__ import annotations

import asyncio
import hashlib
from typing import Any

import pytest
from valkey.exceptions import ResponseError

from swarmq.backend.valkey.execution_guard import ValkeyExecutionGuardStore
from swarmq.exceptions import BackendConnectionError, BackendResponseError
from tests.unit.backend._pipeline_fakes import FakeScript, fake_client


def _script(source: str) -> FakeScript:
    return FakeScript(sha=hashlib.sha1(source.encode()).hexdigest())


def _store(error: BaseException) -> ValkeyExecutionGuardStore:
    client, _, _ = fake_client(lambda argv: error)
    client.register_script = _script
    return ValkeyExecutionGuardStore(client)


async def _claim(store: ValkeyExecutionGuardStore) -> None:
    await store.claim("t-1", owner="o", retry_count=0, ttl_ms=1, state_ttl_ms=1, max_orphans=3)


async def test_transport_error_is_an_outage() -> None:
    with pytest.raises(BackendConnectionError) as info:
        await _claim(_store(ConnectionError("reset")))
    assert not isinstance(info.value, BackendResponseError)


async def test_error_reply_is_a_response_error() -> None:
    with pytest.raises(BackendResponseError):
        await _claim(_store(ResponseError("WRONGTYPE")))


async def test_concurrent_claims_share_one_round_trip() -> None:
    def reply(argv: list[bytes]) -> Any:
        return [b"claimed", 0]

    client, connection, _ = fake_client(reply)
    client.register_script = _script
    store = ValkeyExecutionGuardStore(client)

    claims = await asyncio.gather(*(
        store.claim(f"t-{i}", owner="o", retry_count=0, ttl_ms=1, state_ttl_ms=1, max_orphans=3) for i in range(3)
    ))

    assert [claim.status for claim in claims] == ["claimed"] * 3
    assert len(connection.sent) == 1
    assert [argv[3:5] for argv in connection.sent[0]] == [
        [f"sq:guard:{{t-{i}}}:run".encode(), f"sq:guard:{{t-{i}}}:state".encode()] for i in range(3)
    ]
