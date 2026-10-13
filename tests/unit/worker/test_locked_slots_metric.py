"""Gauge der gesperrten Worker-Slots (Plan 0.12.0, MR 17, BZ-27).

Test-Liste:
- [x] mit Supervisor-Kanal setzt der Sampler ``swarmq_supervisor_locked_slots{worker_id}``
- [x] ohne geteilten Wert (fremder Kanal) bleibt die Serie aus
"""

from __future__ import annotations

import multiprocessing
from unittest.mock import AsyncMock

import pytest

from swarmq.metrics import Metrics
from swarmq.supervision import SupervisorChannel, set_current_channel

pytestmark = pytest.mark.timeout(5)


@pytest.fixture
def metrics(worker, broker) -> Metrics:
    m = Metrics()
    if not m.enabled:
        pytest.skip("prometheus_client missing")
    worker._metrics = m
    broker.queue_depth = AsyncMock(return_value=0)
    broker.dead_letter_depth = AsyncMock(return_value=0)
    return m


@pytest.mark.entscheidung("E-024")
async def test_sampler_reports_the_locked_slots_of_the_supervisor(worker, metrics) -> None:
    ctx = multiprocessing.get_context("spawn")
    shared = ctx.Value("i", 2)
    set_current_channel(SupervisorChannel.create(ctx, locked_slots=shared))
    try:
        await worker._sample_metrics_once()
    finally:
        set_current_channel(None)

    labels = {"worker_id": worker._config.worker_identity}
    assert metrics.registry.get_sample_value("swarmq_supervisor_locked_slots", labels) == 2.0


async def test_without_a_shared_value_the_series_stays_absent(worker, metrics) -> None:
    set_current_channel(SupervisorChannel.create(multiprocessing.get_context("spawn")))
    try:
        await worker._sample_metrics_once()
    finally:
        set_current_channel(None)

    labels = {"worker_id": worker._config.worker_identity}
    assert metrics.registry.get_sample_value("swarmq_supervisor_locked_slots", labels) is None
