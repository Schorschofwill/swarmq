"""Epoch des Datenbestands (Plan 0.12.0, MR 14, BZ-04).

Der Nachweis (Leader, Zweig „kein Eintrag“) steht in
``tests/integration/test_befund_bz04_disabled_cron_data_loss.py``. Hier: jeder
Loop lernt den Epoch auch im Meldetakt, damit ein Standby, der nach einem
Valkey-Neustart Leader wird, den Verlust ebenso meldet. Grenze: InMemory-Store.

Test-Liste:
- [x] Meldetakt (auch als Standby): verschwundener Epoch -> ``valkey_data_loss_detected``
- [x] gleicher Epoch -> keine Warnung
- [x] Store ohne ``ensure_epoch`` (``MagicMock``) -> kein Fehler, keine Warnung (Q15)
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.config import SwarmQConfig
from swarmq.scheduler import SchedulerLoop, ScheduleStore
from swarmq.signals import SignalDispatcher
from tests.support.inmemory import InMemoryBackend
from tests.support.inmemory.clock import FakeClock

pytestmark = pytest.mark.timeout(5)


def _loop(backend: Any) -> SchedulerLoop:
    config = SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x")
    broker = MagicMock()
    broker.publish = AsyncMock()
    return SchedulerLoop(
        backend=backend,
        broker=broker,
        config=config,
        lock_manager=backend.locks,
        store=ScheduleStore(backend),
        signal_dispatcher=SignalDispatcher(),
        owner_token="w-1",
        worker_id="w-1",
    )


@pytest.fixture
async def memory() -> InMemoryBackend:
    backend = InMemoryBackend(FakeClock())
    await backend.connect()
    return backend


def _losses(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [e for e in entries if e["event"] == "valkey_data_loss_detected"]


@pytest.mark.entscheidung("E-077")
async def test_report_tick_notices_a_vanished_epoch(memory: InMemoryBackend, swarmq_logs: list[dict[str, Any]]) -> None:
    loop = _loop(memory)
    await loop.start()  # meldet sofort, lernt den Epoch
    await loop.stop()

    memory._schedules._epoch = None  # Valkey verliert seine Daten
    await loop.start()
    await loop.stop()

    [loss] = _losses(swarmq_logs)
    assert loss["log_level"] == "warning"


async def test_same_epoch_stays_silent(memory: InMemoryBackend, swarmq_logs: list[dict[str, Any]]) -> None:
    loop = _loop(memory)
    for _ in range(2):
        await loop.start()
        await loop.stop()

    assert _losses(swarmq_logs) == []


async def test_store_without_epoch_support_is_skipped(
    memory: InMemoryBackend, swarmq_logs: list[dict[str, Any]]
) -> None:
    """Q15: ein Store-Mock ohne ``ensure_epoch`` (``MagicMock``: nicht awaitbar)."""
    backend = MagicMock(wraps=memory)
    backend.locks = memory.locks
    backend.schedules = MagicMock()
    backend.schedules.report_cron = AsyncMock()
    backend.schedules.report_worker_version = AsyncMock()
    loop = _loop(backend)

    await loop.start()
    await loop.stop()

    assert _losses(swarmq_logs) == []
