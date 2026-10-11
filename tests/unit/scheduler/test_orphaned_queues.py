"""Verwaiste Queues melden (Plan 0.12.0, MR 18, BZ-14).

Jeder Worker meldet die Queues, die er konsumiert, im Meldetakt mit
Valkey-Zeit (``sq:{sched}:report:queues``). Der Leader prueft die gemeldeten
Queues per passivem Declare: Eine Queue ist verwaist, wenn sie Nachrichten,
aber keinen Consumer hat und kein lebender Worker sie mehr meldet (letzte
Meldung aelter als drei Meldetakte). Log und Signal nur beim Uebergang, das
Signal speist eine Gauge. Eintraege fehlender oder leerer Queues, die laenger
als 7 d niemand gemeldet hat, entfernt der Leader. Grenze: InMemory-Backend
mit Fake-Uhr (Backend-Zeit), Broker gemockt.

Test-Liste:
- [x] Worker meldet seine konsumierten Queues beim Start, in jeder Rolle
- [x] nicht mehr gemeldete Queue mit Rueckstand ohne Consumer -> WARNING
      ``scheduler_queue_orphaned`` und Signal ``SCHEDULER_QUEUE_ORPHANED``
- [x] zweite Pruefung im selben Zustand: still (nur Uebergang)
- [x] lebend gemeldete Queue ohne Consumer (Neustart, Rolling Deploy): nicht verwaist
- [x] Queue mit Consumer (etwa ein Worker < 0.12): nicht verwaist
- [x] Rueckstand abgearbeitet: INFO ``scheduler_queue_orphan_resolved`` und Signal ``orphaned=False``
- [x] fehlende oder leere Queue, laenger als 7 d nicht gemeldet: Eintrag entfernt;
      juenger oder mit Rueckstand: bleibt
- [x] Broker ohne ``queue_statuses`` oder Store ohne Meldungen (Q15): keine Pruefung, kein Fehler
- [x] haengender Broker: Pruefung endet nach dem Zeitlimit (Q6)
- [x] neue Amtszeit meldet die verwaiste Queue erneut
- [x] Uebernahme-Takt direkt nach dem Erwerb prueft nicht, erst der periodische Takt
- [x] Gauge ``swarmq_scheduler_queues_orphaned`` aus dem Signal, 0 beim Standby
      (``tests/unit/test_metrics_queues_orphaned.py``)
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.config import SwarmQConfig
from swarmq.contracts.broker import QueueStatus
from swarmq.scheduler import SchedulerLoop, ScheduleStore
from swarmq.signals import Signal, SignalDispatcher
from tests.support.inmemory import InMemoryBackend
from tests.support.inmemory.clock import FakeClock

pytestmark = pytest.mark.timeout(5)


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
async def memory(clock: FakeClock) -> InMemoryBackend:
    backend = InMemoryBackend(clock)
    await backend.connect()
    return backend


class _Broker(MagicMock):
    """Broker-Grenze: ``queue_statuses`` antwortet wie RabbitMQ nur fuer die
    gefragten Queues; eine unbekannte Queue fehlt (``None``)."""

    statuses: dict[str, QueueStatus]


@pytest.fixture
def broker() -> MagicMock:
    broker = _Broker()
    broker.publish = AsyncMock()
    broker.statuses = {}

    async def _statuses(names: Any) -> dict[str, QueueStatus | None]:
        return {name: broker.statuses.get(name) for name in names}

    broker.queue_statuses = AsyncMock(side_effect=_statuses)
    return broker


def _loop(
    backend: Any, broker: Any, *, queues: tuple[str, ...] = (), dispatcher: SignalDispatcher | None = None
) -> SchedulerLoop:
    return SchedulerLoop(
        backend=backend,
        broker=broker,
        config=SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x"),
        lock_manager=backend.locks,
        store=ScheduleStore(backend),
        signal_dispatcher=dispatcher or SignalDispatcher(),
        owner_token="w-1",
        worker_id="w-1",
        consumed_queues=queues,
    )


async def test_worker_reports_its_consumed_queues_on_start(memory: InMemoryBackend, broker: MagicMock) -> None:
    loop = _loop(memory, broker, queues=("tasks", "tasks.high"))

    await loop.start()
    await loop.stop()

    assert set((await memory.schedules.queue_reports()).last_seen_ms) == {"tasks", "tasks.high"}


# Drei Meldetakte (Default 60 s) und eine Sekunde: die Meldung lebt nicht mehr.
_STALE_S = 181.0


def _recorded(dispatcher: SignalDispatcher) -> list[dict[str, Any]]:
    seen: list[dict[str, Any]] = []

    async def _handler(**kw: Any) -> None:
        seen.append(kw)

    dispatcher.connect(Signal.SCHEDULER_QUEUE_ORPHANED, _handler)
    return seen


async def _abandoned(memory: InMemoryBackend, clock: FakeClock, queue: str = "old") -> None:
    """Ein frueherer Worker hat ``queue`` gemeldet und meldet sie nicht mehr."""
    await memory.schedules.report_queues([queue], ttl_ms=7 * 24 * 3600 * 1000)
    await clock.advance(_STALE_S)


async def test_unreported_queue_with_backlog_and_no_consumer_is_reported(
    memory: InMemoryBackend, clock: FakeClock, broker: MagicMock, swarmq_logs: list[dict[str, Any]]
) -> None:
    await _abandoned(memory, clock)
    broker.statuses = {"old": QueueStatus(message_count=3, consumer_count=0)}
    dispatcher = SignalDispatcher()
    seen = _recorded(dispatcher)

    await _loop(memory, broker, dispatcher=dispatcher).check_orphaned_queues()

    [warning] = [e for e in swarmq_logs if e["event"] == "scheduler_queue_orphaned"]
    assert warning["log_level"] == "warning"
    assert warning["queue"] == "old" and warning["message_count"] == 3
    assert seen == [{"worker_id": "w-1", "queue": "old", "orphaned": True, "message_count": 3, "orphaned_count": 1}]


async def test_second_check_in_the_same_state_is_silent(
    memory: InMemoryBackend, clock: FakeClock, broker: MagicMock, swarmq_logs: list[dict[str, Any]]
) -> None:
    await _abandoned(memory, clock)
    broker.statuses = {"old": QueueStatus(message_count=3, consumer_count=0)}
    dispatcher = SignalDispatcher()
    seen = _recorded(dispatcher)
    loop = _loop(memory, broker, dispatcher=dispatcher)

    await loop.check_orphaned_queues()
    await loop.check_orphaned_queues()

    assert len([e for e in swarmq_logs if e["event"] == "scheduler_queue_orphaned"]) == 1
    assert len(seen) == 1


async def test_live_reported_queue_without_consumer_is_not_orphaned(
    memory: InMemoryBackend, broker: MagicMock, swarmq_logs: list[dict[str, Any]]
) -> None:
    # Ein lebender Worker meldet die Queue; sein Consumer fehlt nur kurz
    # (Neustart, Rolling Deploy, Start vor dem Consume).
    await memory.schedules.report_queues(["tasks"], ttl_ms=60_000)
    broker.statuses = {"tasks": QueueStatus(message_count=5, consumer_count=0)}

    await _loop(memory, broker).check_orphaned_queues()

    assert [e for e in swarmq_logs if e["event"] == "scheduler_queue_orphaned"] == []


async def test_queue_with_a_consumer_is_not_orphaned(
    memory: InMemoryBackend, clock: FakeClock, broker: MagicMock, swarmq_logs: list[dict[str, Any]]
) -> None:
    # Etwa ein Worker vor 0.12, der nichts meldet, aber konsumiert.
    await _abandoned(memory, clock)
    broker.statuses = {"old": QueueStatus(message_count=3, consumer_count=1)}

    await _loop(memory, broker).check_orphaned_queues()

    assert [e for e in swarmq_logs if e["event"] == "scheduler_queue_orphaned"] == []


async def test_drained_queue_is_no_longer_orphaned(
    memory: InMemoryBackend, clock: FakeClock, broker: MagicMock, swarmq_logs: list[dict[str, Any]]
) -> None:
    await _abandoned(memory, clock)
    broker.statuses = {"old": QueueStatus(message_count=3, consumer_count=0)}
    dispatcher = SignalDispatcher()
    seen = _recorded(dispatcher)
    loop = _loop(memory, broker, dispatcher=dispatcher)
    await loop.check_orphaned_queues()

    broker.statuses = {"old": QueueStatus(message_count=0, consumer_count=0)}
    await loop.check_orphaned_queues()

    [resolved] = [e for e in swarmq_logs if e["event"] == "scheduler_queue_orphan_resolved"]
    assert resolved["log_level"] == "info" and resolved["queue"] == "old"
    assert seen[-1] == {"worker_id": "w-1", "queue": "old", "orphaned": False, "message_count": 0, "orphaned_count": 0}


_DAY_S = 24 * 3600.0


async def test_missing_or_empty_queues_unreported_for_7_days_are_forgotten(
    memory: InMemoryBackend, clock: FakeClock, broker: MagicMock
) -> None:
    week_ms = int(8 * _DAY_S * 1000)
    await memory.schedules.report_queues(["gone", "empty", "full"], ttl_ms=week_ms)
    await clock.advance(6 * _DAY_S)
    await memory.schedules.report_queues(["young-gone"], ttl_ms=week_ms)
    await clock.advance(_DAY_S + 1)
    broker.statuses = {
        "empty": QueueStatus(message_count=0, consumer_count=0),
        "full": QueueStatus(message_count=3, consumer_count=0),
    }

    await _loop(memory, broker).check_orphaned_queues()

    assert set((await memory.schedules.queue_reports()).last_seen_ms) == {"full", "young-gone"}


@pytest.mark.parametrize("missing", ["broker", "store"])
async def test_without_capabilities_nothing_is_checked(
    memory: InMemoryBackend, clock: FakeClock, broker: MagicMock, swarmq_logs: list[dict[str, Any]], missing: str
) -> None:
    await _abandoned(memory, clock)
    broker.statuses = {"old": QueueStatus(message_count=3, consumer_count=0)}
    backend: Any = memory
    if missing == "broker":
        # Fremd-Broker ohne die optionale Faehigkeit (Q15).
        broker = MagicMock(spec=["publish"])
    else:
        # Store als ``MagicMock``: jede Methode existiert, keine ist awaitbar (Q15).
        backend = MagicMock(wraps=memory)
        backend.schedules = MagicMock()

    await _loop(backend, broker).check_orphaned_queues()

    assert [e for e in swarmq_logs if e["log_level"] == "warning"] == []


async def test_hanging_broker_ends_the_check_after_the_time_limit(
    memory: InMemoryBackend,
    clock: FakeClock,
    broker: MagicMock,
    swarmq_logs: list[dict[str, Any]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import asyncio

    monkeypatch.setattr("swarmq.scheduler.orphaned_queues.QUEUE_CHECK_TIMEOUT_S", 0.05)
    await _abandoned(memory, clock)
    never = asyncio.Event()

    async def _hang(_names: Any) -> dict[str, QueueStatus | None]:
        await never.wait()
        return {}

    broker.queue_statuses = AsyncMock(side_effect=_hang)

    await _loop(memory, broker).check_orphaned_queues()

    [failed] = [e for e in swarmq_logs if e["event"] == "scheduler_queue_check_failed"]
    assert failed["error_type"] == "TimeoutError"


async def test_new_term_reports_the_orphaned_queue_again(
    memory: InMemoryBackend, clock: FakeClock, broker: MagicMock
) -> None:
    # Beim Standby faellt die Gauge auf 0; die neue Amtszeit meldet neu.
    await _abandoned(memory, clock)
    broker.statuses = {"old": QueueStatus(message_count=3, consumer_count=0)}
    dispatcher = SignalDispatcher()
    seen = _recorded(dispatcher)
    loop = _loop(memory, broker, dispatcher=dispatcher)
    await loop.check_orphaned_queues()

    loop._begin_term(clock.now_ms())
    await loop.check_orphaned_queues()

    assert [e["orphaned"] for e in seen] == [True, True]


async def test_takeover_adopt_does_not_check_only_the_periodic_tick_does(
    memory: InMemoryBackend, clock: FakeClock, broker: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Direkt nach der Uebernahme startet meist der eigene Worker (Consumer,
    # zweite Verbindung); die Pruefung wartet bis zum ersten Meldetakt.
    import asyncio

    await _abandoned(memory, clock)
    broker.statuses = {"old": QueueStatus(message_count=3, consumer_count=0)}
    checked = asyncio.Event()
    probe = broker.queue_statuses.side_effect

    async def _probe(names: Any) -> dict[str, QueueStatus | None]:
        checked.set()
        return await probe(names)

    broker.queue_statuses = AsyncMock(side_effect=_probe)
    adopts: list[int] = []
    second_adopt = asyncio.Event()
    read = memory.schedules.cron_reports

    async def _cron_reports() -> Any:
        adopts.append(broker.queue_statuses.await_count)
        if len(adopts) == 2:
            second_adopt.set()
        return await read()

    monkeypatch.setattr(memory.schedules, "cron_reports", _cron_reports)
    loop = SchedulerLoop(
        backend=memory,
        broker=broker,
        config=SwarmQConfig(
            broker_url="amqp://x",
            backend_url="valkey://x",
            scheduler_heartbeat_interval=0.01,
            scheduler_lock_ttl=1.0,
            scheduler_seen_interval_s=0.02,
        ),
        lock_manager=memory.locks,
        store=ScheduleStore(memory),
        signal_dispatcher=SignalDispatcher(),
        owner_token="w-1",
        worker_id="w-1",
    )
    await loop.start()
    try:
        await asyncio.wait_for(second_adopt.wait(), timeout=2)
        await asyncio.wait_for(checked.wait(), timeout=2)
    finally:
        await loop.stop()

    # Zu Beginn des zweiten (periodischen) Takts hat noch keine Pruefung stattgefunden.
    assert adopts[:2] == [0, 0]
