"""Queue-Tiefen der Delay-Stufen in den Worker-Metriken (Plan 0.12.0, MR 10, BZ-01).

``swarmq_queue_depth{kind="pending"}`` behält seine Bedeutung (Haupt-Queue plus
alle Delay-Queues). Neu ist ``kind="delayed"``: nur die Delay-Queues, also
``pending`` minus ``ready_depth`` der Haupt-Queue. Fehlt ``ready_depth`` oder
liefert es keine Zahl, bleibt ``delayed`` aus (Q15).

Test-Liste:
- [x] ``delayed`` = ``pending`` - ``ready_depth``
- [x] Gauge ``swarmq_delay_tiers_unavailable{queue}`` je konsumierter Queue aus dem Broker
- [x] Broker ohne brauchbares ``ready_depth`` (MagicMock): ``pending`` und DLQ wie bisher, kein ``delayed``
- [x] mit Prioritäten tragen ``queue_depth`` und ``delay_tiers_unavailable`` dieselben Labels
      (die konsumierten Queues; Review MR 10, P2-6)
- [x] scheitert ``ready_depth`` in einem Takt, bleibt ``delayed`` nicht stehen (Review MR 10, Hinweis)
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from swarmq.config import SwarmQConfig
from swarmq.metrics import Metrics
from swarmq.worker import Worker
from tests.unit.worker._helpers import gauge_value

pytestmark = pytest.mark.timeout(5)


@pytest.fixture
def metrics(worker) -> Metrics:
    m = Metrics()
    if not m.enabled:
        pytest.skip("prometheus_client missing")
    worker._metrics = m
    return m


@pytest.mark.entscheidung("E-027")
async def test_delayed_depth_is_pending_minus_ready(worker, broker, metrics: Metrics) -> None:
    broker.queue_depth = AsyncMock(return_value=12)
    broker.ready_depth = AsyncMock(return_value=5)
    broker.dead_letter_depth = AsyncMock(return_value=0)

    await worker._sample_metrics_once()

    assert gauge_value(metrics.queue_depth.labels(queue="default", kind="pending")) == 12
    assert gauge_value(metrics.queue_depth.labels(queue="default", kind="delayed")) == 7


async def test_broker_without_usable_ready_depth_keeps_pending_and_dlq(worker, broker, metrics: Metrics) -> None:
    # ``broker`` ist ein MagicMock(spec=RabbitMQBroker): ``ready_depth`` liefert
    # dort keine Zahl (Q15).
    broker.queue_depth = AsyncMock(return_value=12)
    broker.dead_letter_depth = AsyncMock(return_value=3)

    await worker._sample_metrics_once()

    assert gauge_value(metrics.queue_depth.labels(queue="default", kind="pending")) == 12
    assert gauge_value(metrics.dlq_messages.labels(queue="default")) == 3
    assert metrics.registry is not None
    sampled_kinds = {
        sample.labels["kind"]
        for family in metrics.registry.collect()
        for sample in family.samples
        if family.name.endswith("queue_depth")
    }
    assert sampled_kinds == {"pending"}


@pytest.mark.entscheidung("E-026")
async def test_unavailable_delay_tiers_gauge_per_consumed_queue(worker, broker, metrics: Metrics) -> None:
    broker.queue_depth = AsyncMock(return_value=0)
    broker.dead_letter_depth = AsyncMock(return_value=0)
    broker.delay_tiers_unavailable = lambda queue: {"default": 2}.get(queue, 0)

    await worker._sample_metrics_once()

    assert gauge_value(metrics.delay_tiers_unavailable.labels(queue="default")) == 2


def _label_values(metrics: Metrics, suffix: str, **where: str) -> set[str]:
    assert metrics.registry is not None
    return {
        sample.labels["queue"]
        for family in metrics.registry.collect()
        for sample in family.samples
        if family.name.endswith(suffix) and all(sample.labels.get(k) == v for k, v in where.items())
    }


async def test_queue_depth_and_unavailable_tiers_share_the_consumed_queue_labels(broker, backend, registry) -> None:
    config = SwarmQConfig(
        broker_url="amqp://localhost",
        backend_url="valkey://localhost",
        concurrency=10,
        queues=["default"],
        priorities=True,
    )
    worker = Worker(broker=broker, backend=backend, registry=registry, config=config)
    metrics = Metrics()
    if not metrics.enabled:
        pytest.skip("prometheus_client missing")
    worker._metrics = metrics
    broker.queue_depth = AsyncMock(return_value=1)
    broker.dead_letter_depth = AsyncMock(return_value=0)
    broker.delay_tiers_unavailable = lambda queue: 0

    await worker._sample_metrics_once()

    consumed = {"default.high", "default", "default.low"}
    assert _label_values(metrics, "queue_depth", kind="pending") == consumed
    assert _label_values(metrics, "delay_tiers_unavailable") == consumed


async def test_delayed_series_does_not_go_stale_when_ready_depth_fails(worker, broker, metrics: Metrics) -> None:
    broker.queue_depth = AsyncMock(return_value=12)
    broker.ready_depth = AsyncMock(return_value=5)
    broker.dead_letter_depth = AsyncMock(return_value=0)
    await worker._sample_metrics_once()
    assert gauge_value(metrics.queue_depth.labels(queue="default", kind="delayed")) == 7
    broker.ready_depth = AsyncMock(side_effect=RuntimeError("channel gone"))

    await worker._sample_metrics_once()

    assert _label_values(metrics, "queue_depth", kind="pending") == {"default"}
    assert _label_values(metrics, "queue_depth", kind="delayed") == set()
