"""Integration: the worker metrics sampler against real services.

Härtung R2 B4: the P1-3 sampler (queue_depth / dlq_messages /
worker_uptime_seconds + WORKER_HEARTBEAT) was unit-mock-only — nothing
proved the loop populates the gauges from a REAL broker. This drives a
real worker, seeds a real DLQ message, and reads the gauge values.
"""

from __future__ import annotations

import asyncio
import contextlib
import socket
import uuid

import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.signals import Signal
from swarmq.worker import Worker
from tests.support.queues import delete_queue_family

pytestmark = [pytest.mark.integration, pytest.mark.timeout(30)]


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def test_sampler_populates_gauges_from_real_broker(
    rabbitmq_url,
    valkey_url,
):
    pytest.importorskip("prometheus_client")

    queue = f"int-sampler-{uuid.uuid4().hex[:10]}"
    broker = RabbitMQBroker(rabbitmq_url)
    backend = ValkeyBackend(valkey_url)
    app = SwarmQ(broker, backend, queues=[queue], concurrency=1)

    heartbeats: list[dict] = []
    beat = asyncio.Event()

    @app.on_signal(Signal.WORKER_HEARTBEAT)
    async def _hb(**kw):
        heartbeats.append(kw)
        beat.set()

    async with app:
        await broker.declare_queue(queue)
        # Seed one REAL dead-lettered message so dlq_messages has a
        # non-trivial value to sample.
        await broker._publish_to_dead_letter_queue(queue, b"{}", headers={})

        app.config.metrics_port = _free_port()
        app.config.metrics_sample_interval = 0.2
        worker = Worker(
            app.broker,
            app.backend,
            app.registry,
            app.config,
            signal_dispatcher=app._signal_dispatcher,
        )
        task = asyncio.create_task(worker.start())
        try:
            await asyncio.wait_for(beat.wait(), timeout=20)
            # One full sample completed → gauges carry real values.
            m = worker._metrics
            assert m is not None
            assert m.enabled
            assert m.queue_depth.labels(queue=queue, kind="pending")._value.get() == 0
            assert m.dlq_messages.labels(queue=queue)._value.get() == 1
            worker_id = heartbeats[0]["worker_id"]
            assert m.worker_uptime_seconds.labels(worker_id=worker_id)._value.get() > 0
        finally:
            worker._request_shutdown()
            try:
                await asyncio.wait_for(task, timeout=20)
            except TimeoutError:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
            # Queue hygiene: drop the test queue family.
            await delete_queue_family(rabbitmq_url, queue)
