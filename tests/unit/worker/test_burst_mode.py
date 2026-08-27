"""Unit tests for R4.5 Burst-Mode (worker --burst).

Validates the burst-monitor coroutine in isolation: idle clock,
shutdown trigger, polling cadence. Full E2E (CLI subprocess
``swarmq worker --burst`` against a populated queue) lives in
tests/e2e/test_burst_mode.py.
"""

from __future__ import annotations

import asyncio
import time
from unittest.mock import MagicMock

from swarmq.config import SwarmQConfig
from swarmq.worker import Worker, WorkerState


def _make_worker(*, burst_mode: bool, idle_timeout: float = 0.5) -> Worker:
    cfg = SwarmQConfig(
        broker_url="amqp://x",
        backend_url="valkey://x",
        burst_mode=burst_mode,
        burst_idle_timeout=idle_timeout,
        concurrency=1,
    )
    broker = MagicMock()
    backend = MagicMock()
    registry = MagicMock()
    registry.get_all = MagicMock(return_value=[])
    w = Worker(broker, backend, registry, cfg)
    # _request_shutdown gates on RUNNING. Tests skip start() so flip
    # state manually.
    w._state = WorkerState.RUNNING
    return w


class TestBurstConfig:
    def test_burst_disabled_by_default(self):
        cfg = SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x")
        assert cfg.burst_mode is False
        assert cfg.burst_idle_timeout == 5.0

    def test_burst_can_be_enabled(self):
        cfg = SwarmQConfig(
            broker_url="amqp://x",
            backend_url="valkey://x",
            burst_mode=True,
            burst_idle_timeout=2.5,
        )
        assert cfg.burst_mode is True
        assert cfg.burst_idle_timeout == 2.5


class TestBurstMonitor:
    async def test_monitor_triggers_shutdown_after_idle_timeout(self):
        w = _make_worker(burst_mode=True, idle_timeout=0.2)
        # Pretend the worker has been idle since startup (no message
        # ever received).
        w._last_message_received_at = time.monotonic()
        # Wait for the monitor to trip.
        await asyncio.wait_for(w._burst_monitor(), timeout=2.0)
        assert w._shutdown_event.is_set()

    async def test_monitor_does_not_trigger_when_active_tasks_present(self):
        """If a task is in-flight the monitor must NOT fire shutdown
        even if the queue has been quiet — the task itself is work."""
        w = _make_worker(burst_mode=True, idle_timeout=0.2)
        w._last_message_received_at = time.monotonic()
        # Park a fake active task.
        sentinel_done = asyncio.Event()

        async def _fake_task():
            await sentinel_done.wait()

        active = asyncio.create_task(_fake_task())
        w._active_tasks.add(active)

        monitor = asyncio.create_task(w._burst_monitor())
        # Idle window elapses, but active_tasks is non-empty → no exit.
        await asyncio.sleep(0.5)
        assert not w._shutdown_event.is_set()

        # Drain the fake task and verify shutdown fires.
        sentinel_done.set()
        await active
        w._active_tasks.discard(active)
        await asyncio.wait_for(monitor, timeout=2.0)
        assert w._shutdown_event.is_set()

    async def test_monitor_clock_resets_on_message(self):
        """Each delivery resets the idle clock; the monitor only fires
        after a fresh full idle window."""
        w = _make_worker(burst_mode=True, idle_timeout=0.4)
        w._last_message_received_at = time.monotonic()
        monitor = asyncio.create_task(w._burst_monitor())

        # 0.2s in, simulate a message — resets clock.
        await asyncio.sleep(0.2)
        assert not w._shutdown_event.is_set()
        w._last_message_received_at = time.monotonic()

        # 0.3s later (still within fresh window) — no shutdown.
        await asyncio.sleep(0.3)
        assert not w._shutdown_event.is_set()

        # Another 0.4s without message → must fire.
        await asyncio.wait_for(monitor, timeout=2.0)
        assert w._shutdown_event.is_set()

    async def test_monitor_returns_silently_on_cancel(self):
        w = _make_worker(burst_mode=True, idle_timeout=10.0)
        w._last_message_received_at = time.monotonic()
        monitor = asyncio.create_task(w._burst_monitor())
        await asyncio.sleep(0.05)
        monitor.cancel()
        # Monitor catches CancelledError internally and returns; the
        # task therefore awaits without re-raising.
        await monitor
        assert not w._shutdown_event.is_set()
