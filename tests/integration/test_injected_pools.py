"""Integration: producer-path pool/connection injection against real services.

Härtung R2 B3: the inject-existing-pools feature (2026-06-02) shipped
with unit-mock coverage only — nothing proved that a REAL injected
aio-pika connection / valkey client survives a swarmq producer
lifecycle and remains usable by the caller afterwards (the feature's
core promise: "swarmq borrows, never closes").
"""

from __future__ import annotations

import uuid

import aio_pika
import pytest
import valkey.asyncio as valkey_asyncio

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.task import Task, TaskInfo

pytestmark = [pytest.mark.integration, pytest.mark.timeout(30)]


def _q() -> str:
    return f"int-inject-{uuid.uuid4().hex[:10]}"


async def test_injected_connection_and_client_roundtrip_and_caller_reuse(
    rabbitmq_url,
    valkey_url,
):
    """Full producer roundtrip over injected resources, then caller reuse.

    1. Caller owns a robust RabbitMQ connection + a Valkey client.
    2. swarmq borrows both, schedules a task (publish path) and reads a
       pre-seeded result (backend read path).
    3. After the swarmq app teardown, BOTH resources must still work
       for the caller — swarmq closed only its own publish channel.
    """
    caller_conn = await aio_pika.connect_robust(rabbitmq_url)
    caller_client = valkey_asyncio.Valkey.from_url(valkey_url)
    queue = _q()

    class InjectRoundtrip(Task):
        task_info = TaskInfo(name=f"inject_rt_{queue}", queues=[queue])

        async def run(self) -> str:
            return "ok"

    try:
        broker = RabbitMQBroker(connection=caller_conn)
        backend = ValkeyBackend(client=caller_client)
        app = SwarmQ(broker, backend, queues=[queue])

        async with app:
            await broker.declare_queue(queue)
            task_id = await app.client.schedule(
                f"inject_rt_{queue}",
                queue=queue,
            )
            assert task_id
            # Publish really landed on the broker (visible via a
            # passive declare on the caller's own connection).
            depth = await broker.queue_depth(queue)
            assert depth == 1

            # Backend ops ride the borrowed client.
            await backend.store_result(task_id, b"\x01", ttl_ms=60_000)
            assert await backend.get_result(task_id) == b"\x01"

        # --- after teardown: borrowed resources belong to the caller ---
        assert not caller_conn.is_closed, "swarmq closed the borrowed RabbitMQ connection"
        # The caller can still open channels / run commands.
        ch = await caller_conn.channel()
        await ch.close()
        assert await caller_client.ping()
    finally:
        # Caller-side cleanup (and queue hygiene).
        try:
            ch = await caller_conn.channel()
            await ch.queue_delete(queue)
            await ch.close()
        finally:
            await caller_conn.close()
            await caller_client.aclose()


async def test_injected_pool_roundtrip_releases_to_pool(rabbitmq_url, valkey_url):
    """Pool variant: swarmq acquires from the caller's pool, publishes,
    and releases the checkout on close — the pool itself stays open."""
    caller_pool: aio_pika.pool.Pool = aio_pika.pool.Pool(
        lambda: aio_pika.connect_robust(rabbitmq_url),
        max_size=2,
    )
    caller_client = valkey_asyncio.Valkey.from_url(valkey_url)
    queue = _q()

    class InjectPoolTask(Task):
        task_info = TaskInfo(name=f"inject_pool_{queue}", queues=[queue])

        async def run(self) -> str:
            return "ok"

    try:
        broker = RabbitMQBroker(connection_pool=caller_pool)
        backend = ValkeyBackend(client=caller_client)
        app = SwarmQ(broker, backend, queues=[queue])

        async with app:
            await broker.declare_queue(queue)
            task_id = await app.client.schedule(
                f"inject_pool_{queue}",
                queue=queue,
            )
            assert task_id

        # Pool not closed; caller can still acquire.
        assert not caller_pool.is_closed
        async with caller_pool.acquire() as conn:
            ch = await conn.channel()
            await ch.queue_delete(queue)
            await ch.close()
    finally:
        await caller_pool.close()
        await caller_client.aclose()
