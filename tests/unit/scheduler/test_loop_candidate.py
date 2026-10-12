"""Scheduler je Worker abschaltbar (Plan 2026-10-12-1105, MR 5, P1, T2).

Test-Liste:
4. ``candidate=False``: ``start()`` meldet sofort (Crons, Version) und startet
   keine Wahl: ``lock_manager.acquire`` wird nie aufgerufen, auch nach
   mehreren Melde-Takten.
5. ``candidate=False``: der Melde-Takt läuft weiter (weitere Meldungen im
   Abstand ``scheduler_seen_interval_s``) -- im Test zu 4 enthalten (drei
   Meldungen abgewartet).
6. ``candidate=False``: ``stop()`` sendet kein ``SCHEDULER_LEADER_HANDOVER``
   und gibt keinen Lock frei.
6a. ``candidate=False``: ein zweiter ``start()`` startet keinen zweiten
   Melde-Takt.
7. ``candidate=True`` (Default) unverändert: ``tests/unit/scheduler/test_loop.py``.

Die Takte laufen mit kurzem ``scheduler_seen_interval_s``; gewartet wird
event-gesteuert auf die n-te Meldung (kein ``asyncio.sleep``).
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock

import pytest

from swarmq.config import SwarmQConfig
from swarmq.scheduler import SchedulerLoop
from swarmq.signals import Signal, SignalDispatcher
from tests.unit.scheduler._helpers import _def

pytestmark = [pytest.mark.timeout(5)]


@pytest.fixture
def fast_config() -> SwarmQConfig:
    return SwarmQConfig(
        broker_url="amqp://localhost",
        backend_url="valkey://localhost",
        scheduler_poll_interval=0.01,
        scheduler_heartbeat_interval=0.01,
        scheduler_lock_ttl=1.0,
        scheduler_seen_interval_s=0.01,
    )


def _observer_loop(backend, broker, config, lock_manager, store, signal_dispatcher) -> SchedulerLoop:
    return SchedulerLoop(
        backend=backend,
        broker=broker,
        config=config,
        lock_manager=lock_manager,
        store=store,
        signal_dispatcher=signal_dispatcher,
        owner_token="observer",
        worker_id="w-off",
        static_crons=[_def()],
        version="0.12.1",
        candidate=False,
    )


def _count_reports(backend: Any, wanted: int) -> asyncio.Event:
    """Event, das nach der ``wanted``-ten Versionsmeldung gesetzt wird."""
    reached = asyncio.Event()
    calls = 0

    async def _report(*_a: Any, **_kw: Any) -> None:
        nonlocal calls
        calls += 1
        if calls >= wanted:
            reached.set()

    backend.schedules.report_cron = AsyncMock()
    backend.schedules.report_worker_version.side_effect = _report
    return reached


def _is_report_loop_of(task: asyncio.Task[Any], loop: SchedulerLoop) -> bool:
    coro = task.get_coro()
    frame = getattr(coro, "cr_frame", None)
    return (
        getattr(coro, "__qualname__", "").endswith("_report_loop")
        and frame is not None
        and frame.f_locals.get("self") is loop
    )


async def test_observer_reports_but_never_runs_for_leader(
    backend, broker, fast_config, lock_manager, store, signal_dispatcher
):
    reached = _count_reports(backend, 3)
    loop = _observer_loop(backend, broker, fast_config, lock_manager, store, signal_dispatcher)

    await loop.start()
    backend.schedules.report_cron.assert_awaited()
    await asyncio.wait_for(reached.wait(), timeout=2)
    await loop.stop()

    lock_manager.acquire.assert_not_awaited()
    assert loop.state == "stopped"


async def test_observer_stop_sends_no_handover_and_releases_no_lock(
    backend, broker, fast_config, lock_manager, store, signal_dispatcher: SignalDispatcher
):
    handovers: list[dict[str, Any]] = []

    @signal_dispatcher.on_signal(Signal.SCHEDULER_LEADER_HANDOVER)
    async def _on_handover(**kwargs: Any) -> None:
        handovers.append(kwargs)

    _count_reports(backend, 1)
    loop = _observer_loop(backend, broker, fast_config, lock_manager, store, signal_dispatcher)
    await loop.start()
    await loop.stop()

    assert handovers == []
    lock_manager.release.assert_not_awaited()
    backend.schedules.set_handover_marker.assert_not_awaited()


async def test_second_start_does_not_start_a_second_report_loop(
    backend, broker, fast_config, lock_manager, store, signal_dispatcher
):
    _count_reports(backend, 1)
    loop = _observer_loop(backend, broker, fast_config, lock_manager, store, signal_dispatcher)
    await loop.start()
    await loop.start()

    report_loops = [t for t in asyncio.all_tasks() if not t.done() and _is_report_loop_of(t, loop)]
    await loop.stop()

    assert len(report_loops) == 1
