"""``sched_add.lua`` gegen echtes Valkey (Plan 2026-10-15-1138, U3, KTD4,
F3-01): HSETNX und bei Erfolg ZADD in einem EVAL.

Testliste:
# [x] neuer Eintrag mit Fire-Zeitpunkt -> 1, Definition und Score gesetzt
# [x] bestehender Eintrag -> 0, Definition und Score unverändert
# [x] ohne Fire-Zeitpunkt ("") -> 1, kein Eintrag im Fire-Index
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from typing import Any

import pytest
import valkey.asyncio as valkey_asyncio
from valkey.commands.core import AsyncScript

from swarmq.lua import load_script

pytestmark = [pytest.mark.integration, pytest.mark.timeout(5)]

ZSET_KEY = "sq:{sched}:zset"
DEFS_KEY = "sq:{sched}:defs"


# ``Any``: valkey-py typisiert die Befehle sync/async gemischt
# (``Awaitable[int] | int``); wie ``tests/integration/test_sched_lua_scripts.py``.
@pytest.fixture
async def valkey_client(valkey_url: str) -> AsyncIterator[Any]:
    client = valkey_asyncio.Valkey.from_url(valkey_url)
    try:
        yield client
    finally:
        await client.aclose()


@pytest.fixture
def add_script(valkey_client: Any) -> AsyncScript:
    script: AsyncScript = valkey_client.register_script(load_script("sched_add"))
    return script


@pytest.fixture
async def sched_name(valkey_client: Any) -> AsyncIterator[str]:
    name = f"sched-add-{uuid.uuid4().hex[:12]}"
    yield name
    await valkey_client.zrem(ZSET_KEY, name)
    await valkey_client.hdel(DEFS_KEY, name)


async def test_new_entry_is_stored_and_indexed(valkey_client: Any, add_script: AsyncScript, sched_name: str) -> None:
    assert await add_script(keys=[DEFS_KEY, ZSET_KEY], args=[sched_name, b"def", 1_000]) == 1
    assert await valkey_client.hget(DEFS_KEY, sched_name) == b"def"
    assert await valkey_client.zscore(ZSET_KEY, sched_name) == 1_000


async def test_existing_entry_is_left_unchanged(valkey_client: Any, add_script: AsyncScript, sched_name: str) -> None:
    await valkey_client.hset(DEFS_KEY, sched_name, b"old")
    await valkey_client.zadd(ZSET_KEY, {sched_name: 500})

    assert await add_script(keys=[DEFS_KEY, ZSET_KEY], args=[sched_name, b"new", 1_000]) == 0
    assert await valkey_client.hget(DEFS_KEY, sched_name) == b"old"
    assert await valkey_client.zscore(ZSET_KEY, sched_name) == 500


async def test_without_fire_time_no_index_entry(valkey_client: Any, add_script: AsyncScript, sched_name: str) -> None:
    assert await add_script(keys=[DEFS_KEY, ZSET_KEY], args=[sched_name, b"def", ""]) == 1
    assert await valkey_client.hget(DEFS_KEY, sched_name) == b"def"
    assert await valkey_client.zscore(ZSET_KEY, sched_name) is None
