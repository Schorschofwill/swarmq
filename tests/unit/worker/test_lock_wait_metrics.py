"""Gauges der Lock-Wartezeit (Plan 0.12.0, MR 13, BZ-05, R11).

Test-Liste:
- [x] der Sampler setzt ``swarmq_lock_waits{worker_id}`` und
      ``swarmq_lock_wait_oldest_seconds{worker_id}`` aus dem Lock-Gate
"""

from __future__ import annotations

import time
from unittest.mock import AsyncMock

import pytest

from swarmq.metrics import Metrics

pytestmark = pytest.mark.timeout(5)


async def test_sampler_sets_the_lock_wait_gauges(worker, broker) -> None:
    m = Metrics()
    if not m.enabled:
        pytest.skip("prometheus_client missing")
    worker._metrics = m
    broker.queue_depth = AsyncMock(return_value=0)
    broker.dead_letter_depth = AsyncMock(return_value=0)
    now_ms = int(time.time() * 1000)
    worker._lock_gate.waits.record("t1", wait_since_ms=now_ms - 120_000, now_ms=now_ms)
    worker._lock_gate.waits.record("t2", wait_since_ms=now_ms - 1_000, now_ms=now_ms)

    await worker._sample_metrics_once()

    labels = {"worker_id": worker._config.worker_identity}
    assert m.registry is not None
    assert m.registry.get_sample_value("swarmq_lock_waits", labels) == 2.0
    oldest = m.registry.get_sample_value("swarmq_lock_wait_oldest_seconds", labels)
    assert oldest is not None
    assert oldest >= 120.0
