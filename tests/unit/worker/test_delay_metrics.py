"""Queue-Tiefen der Delay-Stufen in den Worker-Metriken (Plan 0.12.0, MR 10, BZ-01).

``swarmq_queue_depth{kind="pending"}`` behält seine Bedeutung (Haupt-Queue plus
alle Delay-Queues). Neu ist ``kind="delayed"``: nur die Delay-Queues, also
``pending`` minus ``ready_depth`` der Haupt-Queue. Fehlt ``ready_depth`` oder
liefert es keine Zahl, bleibt ``delayed`` aus (Q15).

Test-Liste:
- [x] ``delayed`` = ``pending`` - ``ready_depth``
- [x] Gauge ``swarmq_delay_tiers_unavailable{queue}`` je konsumierter Queue aus dem Broker
- [x] Broker ohne brauchbares ``ready_depth`` (MagicMock): ``pending`` und DLQ wie bisher, kein ``delayed``
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from swarmq.metrics import Metrics

pytestmark = pytest.mark.timeout(5)


@pytest.fixture
def metrics(worker) -> Metrics:
    m = Metrics()
    if not m.enabled:
        pytest.skip("prometheus_client missing")
    worker._metrics = m
    return m


async def test_delayed_depth_is_pending_minus_ready(worker, broker, metrics: Metrics) -> None:
    broker.queue_depth = AsyncMock(return_value=12)
    broker.ready_depth = AsyncMock(return_value=5)
    broker.dead_letter_depth = AsyncMock(return_value=0)

    await worker._sample_metrics_once()

    assert metrics.queue_depth.labels(queue="default", kind="pending")._value.get() == 12
    assert metrics.queue_depth.labels(queue="default", kind="delayed")._value.get() == 7


async def test_broker_without_usable_ready_depth_keeps_pending_and_dlq(worker, broker, metrics: Metrics) -> None:
    # ``broker`` ist ein MagicMock(spec=RabbitMQBroker): ``ready_depth`` liefert
    # dort keine Zahl (Q15).
    broker.queue_depth = AsyncMock(return_value=12)
    broker.dead_letter_depth = AsyncMock(return_value=3)

    await worker._sample_metrics_once()

    assert metrics.queue_depth.labels(queue="default", kind="pending")._value.get() == 12
    assert metrics.dlq_messages.labels(queue="default")._value.get() == 3
    sampled_kinds = {
        sample.labels["kind"]
        for family in metrics.registry.collect()
        for sample in family.samples
        if family.name.endswith("queue_depth")
    }
    assert sampled_kinds == {"pending"}


async def test_unavailable_delay_tiers_gauge_per_consumed_queue(worker, broker, metrics: Metrics) -> None:
    broker.queue_depth = AsyncMock(return_value=0)
    broker.dead_letter_depth = AsyncMock(return_value=0)
    broker.delay_tiers_unavailable = lambda queue: {"default": 2}.get(queue, 0)

    await worker._sample_metrics_once()

    assert metrics.delay_tiers_unavailable.labels(queue="default")._value.get() == 2
