"""``ValkeyExecutionGuardStore`` (Plan 2026-10-08-1541, U8) gegen gemockte
Skripte -- nur die Fehlerabbildung; die Semantik prüft die Contract-Suite
gegen echtes Valkey (``tests/integration/test_contracts_valkey.py``).

Test-Liste:
1. Ein Transportfehler wird zum einfachen ``BackendConnectionError``
   (Ausfall: das Gate stellt zurück).
2. Eine Fehlerantwort wird zu ``BackendResponseError`` (kein Ausfall).
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from valkey.exceptions import ResponseError

from swarmq.backend.valkey.execution_guard import ValkeyExecutionGuardStore
from swarmq.exceptions import BackendConnectionError, BackendResponseError


def _store(error: BaseException) -> ValkeyExecutionGuardStore:
    client = MagicMock()
    client.register_script = MagicMock(return_value=AsyncMock(side_effect=error))
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
