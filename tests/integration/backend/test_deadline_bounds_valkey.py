"""Valkey ``PX``/``EX`` an der Fristgrenze (E-063, Nachtrag 2026-10-17; PF-11, U16).

Fristen und TTLs der Konfiguration gehen in ganzen Millisekunden (``PX``) oder
Sekunden (``EX``) an Valkey. Die Grenze ``MAX_DEADLINE_MS`` kommt von
RabbitMQ (``test_expiration_bounds_rabbitmq.py``) und liegt weit unter der von
Valkey: Valkey nimmt ``PX`` bis 2**63-1 minus der aktuellen Unix-Zeit in ms an
und lehnt darüber mit „invalid expire time“ ab. Auch die Ableitungen an der
Grenze (``scheduler_dedup_ttl``, ``execution_guard_done_ttl_s`` =
Grenze + 300 s) passen deshalb.

Testliste:
# [x] SET … PX MAX_DEADLINE_MS -> angenommen, PTTL in der Nähe der Grenze
# [x] SET … EX (MAX_DEADLINE_S + 300) -> angenommen (abgeleitete Felder)
# [x] SET … PX 2**63-1 -> abgelehnt (Valkeys eigene Grenze liegt darüber)
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
import valkey.asyncio as valkey_asyncio
from valkey.exceptions import ResponseError

from swarmq.config_validation import MAX_DEADLINE_MS, MAX_DEADLINE_S

pytestmark = [pytest.mark.integration, pytest.mark.timeout(10), pytest.mark.entscheidung("E-063")]

_KEY = "u16:deadline-bound"


@pytest.fixture
async def client(valkey_url: str) -> AsyncIterator[valkey_asyncio.Valkey]:
    connection = valkey_asyncio.Valkey.from_url(valkey_url)
    try:
        yield connection
    finally:
        await connection.aclose()


async def test_px_at_the_bound_is_accepted(client: valkey_asyncio.Valkey) -> None:
    assert await client.set(_KEY, "1", px=MAX_DEADLINE_MS)

    remaining = await client.pttl(_KEY)
    assert MAX_DEADLINE_MS - 60_000 < remaining <= MAX_DEADLINE_MS


async def test_ex_of_a_derived_field_at_the_bound_is_accepted(client: valkey_asyncio.Valkey) -> None:
    assert await client.set(_KEY, "1", ex=MAX_DEADLINE_S + 300)

    assert await client.ttl(_KEY) > MAX_DEADLINE_S


async def test_px_at_int64_is_rejected(client: valkey_asyncio.Valkey) -> None:
    with pytest.raises(ResponseError, match="invalid expire time"):
        await client.set(_KEY, "1", px=2**63 - 1)
