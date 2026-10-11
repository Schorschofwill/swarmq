"""Stuendliche Pruefung der Valkey-Umgebung durch den Leader (Plan 0.12.0, MR 14, BZ-04).

Der Leader prueft im Uebernahme-Takt hoechstens einmal je Stunde erneut
(``backend.recheck_eviction``; die Warnung bei Befund gibt das Backend aus)
und meldet das Ergebnis als Signal ``SCHEDULER_VALKEY_CHECKED`` (Grundlage der
Gauge ``used_memory/maxmemory``). Grenze: InMemory-Backend, Uhr gepatcht.

Test-Liste:
- [x] erste Uebernahme: Pruefung + Signal mit Status, Policy, ``maxmemory``, ``used_memory``
- [x] innerhalb einer Stunde keine weitere Pruefung, danach wieder
- [x] Backend ohne ``recheck_eviction`` oder mit Ergebnis ``None`` -> kein Signal, kein Fehler (Q15)
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.config import SwarmQConfig
from swarmq.scheduler import SchedulerLoop, ScheduleStore
from swarmq.signals import Signal, SignalDispatcher
from swarmq.valkey_policy import EvictionCheck
from tests.support.inmemory import InMemoryBackend
from tests.support.inmemory.clock import FakeClock

pytestmark = pytest.mark.timeout(5)

_CHECK = EvictionCheck(status="unsafe", policy="volatile-lru", maxmemory=1000, used_memory=250)


@pytest.fixture
async def memory() -> InMemoryBackend:
    backend = InMemoryBackend(FakeClock())
    await backend.connect()
    return backend


def _loop(backend: Any, seen: list[dict[str, Any]]) -> SchedulerLoop:
    dispatcher = SignalDispatcher()

    @dispatcher.on_signal(Signal.SCHEDULER_VALKEY_CHECKED)
    async def _seen(**kw: Any) -> None:
        seen.append(kw)

    broker = MagicMock()
    broker.publish = AsyncMock()
    return SchedulerLoop(
        backend=backend,
        broker=broker,
        config=SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x"),
        lock_manager=backend.locks,
        store=ScheduleStore(backend),
        signal_dispatcher=dispatcher,
        owner_token="w-1",
        worker_id="w-1",
    )


async def test_first_adopt_rechecks_and_reports(memory: InMemoryBackend) -> None:
    memory.recheck_eviction = AsyncMock(return_value=_CHECK)  # type: ignore[attr-defined]
    seen: list[dict[str, Any]] = []

    await _loop(memory, seen).adopt_reported_crons()

    assert seen == [
        {
            "worker_id": "w-1",
            "status": "unsafe",
            "policy": "volatile-lru",
            "maxmemory": 1000,
            "used_memory": 250,
        }
    ]


async def test_recheck_at_most_once_per_hour(memory: InMemoryBackend, monkeypatch: pytest.MonkeyPatch) -> None:
    now = [1000.0]
    monkeypatch.setattr("swarmq.scheduler.valkey_watch.time", SimpleNamespace(monotonic=lambda: now[0]))
    recheck = AsyncMock(return_value=_CHECK)
    memory.recheck_eviction = recheck  # type: ignore[attr-defined]
    loop = _loop(memory, [])

    await loop.adopt_reported_crons()
    now[0] += 3599.0
    await loop.adopt_reported_crons()
    assert recheck.await_count == 1

    now[0] += 1.0
    await loop.adopt_reported_crons()
    assert recheck.await_count == 2


@pytest.mark.parametrize("recheck", [None, AsyncMock(return_value=None)], ids=["missing", "off"])
async def test_backend_without_recheck_result_reports_nothing(memory: InMemoryBackend, recheck: Any) -> None:
    if recheck is not None:
        memory.recheck_eviction = recheck  # type: ignore[attr-defined]
    seen: list[dict[str, Any]] = []

    await _loop(memory, seen).adopt_reported_crons()

    assert seen == []
