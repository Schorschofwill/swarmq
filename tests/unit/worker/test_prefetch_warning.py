"""Prefetch je Consumer gegen ``consumer_timeout`` (Plan 0.12.0, MR 18, BZ-20).

Der Worker meldet je konsumierter Queue einen Consumer mit
``prefetch=concurrency`` an; bei K Queues haelt er bis zu K x concurrency
unbestaetigte Nachrichten. Die letzte davon wartet bis zu K x ``hard_timeout``
auf ihr Ack, RabbitMQ schliesst den Channel nach ``consumer_timeout``. Globales
QoS geht mit Quorum-Queues nicht (Spike S3): dokumentierte Grenze mit
Startwarnung und Gauge.

Test-Liste:
- [x] K >= 2 Queues und ``hard_timeout`` x K > ``consumer_timeout_s`` -> WARNING
      ``prefetch_exceeds_consumer_timeout`` einmal je Start, mit Tasks, K und
      hoechstens gehaltenen Nachrichten
- [x] ``hard_timeout`` x K <= ``consumer_timeout_s``, eine Queue oder Task ohne
      ``hard_timeout``: keine Warnung
- [x] Gauge ``swarmq_worker_prefetch_limit{worker_id}`` = K x concurrency
"""

from __future__ import annotations

from typing import Any, override

import pytest

from swarmq.config import SwarmQConfig
from swarmq.registry import TaskRegistry
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker

pytestmark = [pytest.mark.timeout(5)]


class SlowTask(Task):
    task_info = TaskInfo(name="bz20-slow", queues=["a"], hard_timeout=1000)

    @override
    async def run(self) -> None:
        return None


def _config(queues: list[str], consumer_timeout_s: float) -> SwarmQConfig:
    return SwarmQConfig(
        broker_url="amqp://localhost",
        backend_url="valkey://localhost",
        concurrency=4,
        queues=queues,
        consumer_timeout_s=consumer_timeout_s,
    )


async def _start(broker: Any, backend: Any, config: SwarmQConfig) -> None:
    registry = TaskRegistry()
    registry.register(SlowTask)
    worker = Worker(broker=broker, backend=backend, registry=registry, config=config)
    worker._shutdown_event.set()
    await worker.start()


async def test_queues_times_hard_timeout_over_consumer_timeout_warns_once(
    broker: Any, backend: Any, swarmq_logs: list[dict[str, Any]]
) -> None:
    # 3 Queues x 1000 s > 1800 s, obwohl 1000 s allein darunter liegen.
    await _start(broker, backend, _config(["a", "b", "c"], consumer_timeout_s=1800))

    [entry] = [e for e in swarmq_logs if e["event"] == "prefetch_exceeds_consumer_timeout"]
    assert entry["log_level"] == "warning"
    assert entry["tasks"] == ["bz20-slow(1000s)"]
    assert entry["consumed_queues"] == 3
    assert entry["max_unacked"] == 12
    assert entry["consumer_timeout_s"] == 1800


@pytest.mark.parametrize(
    ("queues", "consumer_timeout_s"),
    [(["a", "b"], 2000), (["a"], 900)],
    ids=["within-timeout", "one-queue"],
)
async def test_no_warning_within_the_timeout_or_with_one_queue(
    broker: Any, backend: Any, swarmq_logs: list[dict[str, Any]], queues: list[str], consumer_timeout_s: float
) -> None:
    # 2 x 1000 s = 2000 s ist nicht ueber 2000 s; mit einer Queue warnt
    # ``hard_timeout_exceeds_consumer_timeout`` (R19), nicht diese Warnung.
    await _start(broker, backend, _config(queues, consumer_timeout_s=consumer_timeout_s))

    assert [e for e in swarmq_logs if e["event"] == "prefetch_exceeds_consumer_timeout"] == []


async def test_sampler_reports_the_prefetch_limit(broker: Any, backend: Any) -> None:
    from unittest.mock import AsyncMock

    from swarmq.metrics import Metrics

    metrics = Metrics()
    if not metrics.enabled:
        pytest.skip("prometheus_client missing")
    broker.queue_depth = AsyncMock(return_value=0)
    broker.dead_letter_depth = AsyncMock(return_value=0)
    config = _config(["a", "b", "c"], consumer_timeout_s=1800)
    worker = Worker(broker=broker, backend=backend, registry=TaskRegistry(), config=config)
    worker._metrics = metrics

    await worker._sample_metrics_once()

    labels = {"worker_id": config.worker_identity}
    assert metrics.registry.get_sample_value("swarmq_worker_prefetch_limit", labels) == 12.0
