"""Nachweis-Test BZ-28 gegen echtes Valkey (``sched_poll.lua``).

Test-Liste:
1. BZ-28: 100 verwaiste Eintraege im Fire-Index (Score ohne Definition,
   z. B. aus einem Wettlauf ``put`` gegen ``remove --permanent``) halten einen
   faelligen aktiven Cron nicht auf. ``sched_poll.lua`` liest per
   ``ZRANGEBYSCORE ... LIMIT 0 100`` und verwirft Waisen erst danach; ein
   Aufraeumen der Waisen, das der Skript-Kommentar verspricht, gibt es nicht.

Der In-Memory-Store filtert Waisen vor dem Limit und bildet diesen Fall nicht
nach; deshalb laeuft der Nachweis gegen Valkey (Testports ueber itest.sh).
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import msgspec
import pytest
import valkey.asyncio as valkey_asyncio

from swarmq.backend.valkey import ValkeyBackend
from swarmq.backend.valkey.schedules import ZSET_KEY
from swarmq.config import SwarmQConfig
from swarmq.locking import SCHEDULER_LOCK_NAME
from swarmq.scheduler import ScheduleDefinition, SchedulerLoop, ScheduleStore
from swarmq.signals import SignalDispatcher

pytestmark = [pytest.mark.integration, pytest.mark.timeout(10)]

_OWNER = "bz28-owner"


@pytest.fixture
async def backend(valkey_url):
    b = ValkeyBackend(valkey_url)
    await b.connect()
    try:
        yield b
    finally:
        await b.close()


@pytest.mark.befund("BZ-28")
async def test_bz28_orphans_in_fire_index_do_not_block_due_cron(backend, valkey_url):
    await backend.locks.acquire(
        SCHEDULER_LOCK_NAME,
        owner=_OWNER,
        ttl_ms=60_000,
        max_concurrency=1,
    )
    raw = valkey_asyncio.Valkey.from_url(valkey_url)
    try:
        await raw.zadd(ZSET_KEY, {f"orphan_{i}": 1_000 + i for i in range(100)})
    finally:
        await raw.aclose()
    active = ScheduleDefinition(
        schedule_name="active_cron",
        task_name="ping",
        args=(),
        kwargs={},
        queue="default",
        cron="*/5 * * * *",
        fire_ts_ms=5_000,
        source="static",
    )
    await backend.schedules.add("active_cron", msgspec.json.encode(active), 5_000)

    broker = MagicMock()
    broker.publish = AsyncMock()
    loop = SchedulerLoop(
        backend=backend,
        broker=broker,
        config=SwarmQConfig(broker_url="amqp://localhost", backend_url="valkey://localhost"),
        lock_manager=backend.locks,
        store=ScheduleStore(backend),
        signal_dispatcher=SignalDispatcher(),
        owner_token=_OWNER,
        worker_id="w-bz28",
        version="0.11.0",
    )
    loop._state = "leader"

    await loop._poll_tick()

    fired = [call.kwargs["headers"]["x-swarmq-schedfire"] for call in broker.publish.await_args_list]
    assert fired == ["active_cron:5000"]
