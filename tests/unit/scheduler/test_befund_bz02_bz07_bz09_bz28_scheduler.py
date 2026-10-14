"""Nachweis-Tests fuer Scheduler-Befunde aus ``docs/befunde/betrieb-ueber-zeit-2026-10.md``.

Jeder Test beschreibt das korrekte Verhalten und ist mit ``xfail(strict=True)``
markiert, solange der Befund offen ist. Wird ein Befund behoben, wird der Test
XPASS und damit rot: dann den ``xfail``-Marker entfernen.

Test-Liste:
1. BZ-02: Der Leader feuert einen ``schedule(eta=...)``-Eintrag unter der
   task_id, die ``schedule()`` zurueckgegeben hat.
2. BZ-07: Eine langsame Handover-Marker-Abfrage (SCAN ueber die ganze DB)
   direkt nach dem Lock-Erwerb kostet den neuen Leader nicht das Lock.
3. BZ-09: Ein Cron mit ``unique_for`` ueberlappt nicht, auch wenn der Leader
   den Task nicht in seiner Registry hat (andere App, anderes ``--module``).
4. BZ-28: 100 deaktivierte Alt-Eintraege mit vergangenem Score im Fire-Index
   halten einen faelligen aktiven Cron nicht auf.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import override
from unittest.mock import AsyncMock

import msgspec
import pytest

from swarmq.client import Client
from swarmq.locking import SCHEDULER_LOCK_NAME
from swarmq.scheduler import ScheduleStore
from swarmq.scheduler.reports import static_definitions
from swarmq.task import Task, TaskInfo
from tests.support.inmemory import InMemoryBackend
from tests.support.inmemory.clock import FakeClock
from tests.unit.scheduler._helpers import _def, _make_loop

pytestmark = [pytest.mark.timeout(5)]


async def _memory_with_leader_lock(owner: str = "test-owner") -> InMemoryBackend:
    memory = InMemoryBackend(FakeClock())
    await memory.connect()
    await memory.locks.acquire(
        SCHEDULER_LOCK_NAME,
        owner=owner,
        ttl_ms=60_000,
        max_concurrency=1,
    )
    return memory


def _published_task_ids(broker) -> list[str]:
    return [msgspec.json.decode(call.args[1])["task"]["id"] for call in broker.publish.await_args_list]


@pytest.mark.befund("BZ-02")
async def test_bz02_deferred_task_fires_under_returned_task_id(
    broker,
    config,
    lock_manager,
    signal_dispatcher,
):
    memory = await _memory_with_leader_lock()
    client = Client(broker=AsyncMock(), backend=memory)
    # eta knapp in der Vergangenheit (1 s Kulanz): sofort faellig, ohne zu warten.
    returned_id = await client.schedule(
        "ping",
        queue="default",
        eta=datetime.now(UTC) - timedelta(milliseconds=500),
    )
    loop = _make_loop(
        memory,
        broker,
        config,
        lock_manager,
        ScheduleStore(memory),
        signal_dispatcher,
    )
    loop._state = "leader"

    await loop._poll_tick()

    assert _published_task_ids(broker) == [returned_id]


@pytest.mark.befund("BZ-07")
async def test_bz07_slow_handover_scan_does_not_cost_the_new_leader_its_lock(
    backend,
    broker,
    config,
    store,
    signal_dispatcher,
):
    clock = FakeClock()
    memory = InMemoryBackend(clock)
    await memory.connect()
    locks = memory.locks
    heartbeat = config.scheduler_heartbeat_interval

    async def _scan_over_large_db() -> bool:
        # SCAN ueber eine grosse DB: dauert etwas laenger als Lock-TTL minus
        # Heartbeat (Fake-Uhr, kein echtes Warten).
        await clock.advance(config.scheduler_lock_ttl - heartbeat + 0.01)
        return False

    backend.schedules.take_handover_marker = AsyncMock(side_effect=_scan_over_large_db)
    loop = _make_loop(backend, broker, config, locks, store, signal_dispatcher)

    await loop._election_tick()  # Lock-Erwerb + Handover-Abfrage
    assert loop.state == "leader"
    await clock.advance(heartbeat)  # Wartezeit bis zum naechsten Election-Tick
    await loop._election_tick()  # erste Lock-Erneuerung

    assert loop.state == "leader"
    assert await locks.current_owner(SCHEDULER_LOCK_NAME) == loop.owner_token


@pytest.mark.entscheidung("E-011")
@pytest.mark.befund("BZ-09")
async def test_bz09_cron_unique_for_holds_on_leader_without_task_in_registry(
    broker,
    config,
    lock_manager,
    signal_dispatcher,
    monkeypatch,
):
    class LongRunningCron(Task):
        task_info = TaskInfo(
            name="long_running_cron",
            queues=["default"],
            cron="* * * * *",
            unique_for=3600,
        )

        @override
        async def run(self) -> None:
            return None

    # So meldet der Worker, der den Task kennt, seinen Cron (KTD10).
    [reported] = static_definitions([LongRunningCron], None)
    # Der Leader gehoert zu einer anderen App bzw. einem anderen --module.
    monkeypatch.setattr(Task, "_registered_tasks", [])

    memory = await _memory_with_leader_lock()
    name = reported.schedule_name
    await memory.schedules.add(
        name,
        msgspec.json.encode(msgspec.structs.replace(reported, fire_ts_ms=1_000)),
        1_000,
    )
    loop = _make_loop(
        memory,
        broker,
        config,
        lock_manager,
        ScheduleStore(memory),
        signal_dispatcher,
    )
    loop._state = "leader"

    await loop._poll_tick()  # 1. Fire; der Lauf dauert laenger als das Intervall
    # Der naechste Cron-Termin wird faellig, waehrend der erste Lauf noch laeuft.
    raw = await memory.schedules.get(name)
    assert raw is not None
    await memory.schedules.put(name, raw, 2_000)
    await loop._poll_tick()

    assert broker.publish.await_count == 1


@pytest.mark.befund("BZ-28")
async def test_bz28_disabled_backlog_in_fire_index_does_not_block_due_cron(
    broker,
    config,
    lock_manager,
    signal_dispatcher,
):
    memory = await _memory_with_leader_lock()
    # Altbestand aus der Zeit vor 0.10: deaktiviert, aber mit Score im Index.
    for i in range(100):
        old = _def(name=f"legacy_{i}", fire_ts_ms=1_000 + i, disabled=True)
        await memory.schedules.put(old.schedule_name, msgspec.json.encode(old), 1_000 + i)
    active = _def(name="active_cron", fire_ts_ms=5_000)
    await memory.schedules.add("active_cron", msgspec.json.encode(active), 5_000)
    loop = _make_loop(
        memory,
        broker,
        config,
        lock_manager,
        ScheduleStore(memory),
        signal_dispatcher,
    )
    loop._state = "leader"

    await loop._poll_tick()

    headers = [call.kwargs["headers"] for call in broker.publish.await_args_list]
    assert [h["x-swarmq-schedfire"] for h in headers] == ["active_cron:5000"]
