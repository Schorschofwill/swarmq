"""Delay-Stufen am echten Broker (Plan 0.12.0, MR 10, BZ-01).

Ergänzt den Nachweis ``test_befund_bz01_delay_queue_hol.py``: Ablehnung einer
Stufe ohne Abbruch, Selbstheilung einer gelöschten Stufe, Tiefen für Metriken
und Autoscaler.

Test-Liste:
- [x] Stufe abgelehnt (vorhandene Queue mit anderem Typ): Rückfall auf ``{queue}.DQ``, Nachricht kommt zurück
- [x] gelöschte Stufe: der nächste Retry legt sie neu an, die Nachricht kommt zurück
- [x] ``queue_depth`` zählt die Stufen mit, ``ready_depth`` nur die Haupt-Queue
- [x] ``queue_depth`` auf eine Familie ohne Stufen lässt keinen Channel offen (Review MR 10, P0-1)
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator

import aio_pika
import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.contracts import Delivery
from tests.integration.broker.test_declare_chain import _declare_raw, _legacy, _open_channel_numbers
from tests.support.queues import delete_queue_family

pytestmark = [pytest.mark.integration, pytest.mark.timeout(30)]


async def _delete(url: str, name: str) -> None:
    connection = await aio_pika.connect(url)
    try:
        channel = await connection.channel()
        await channel.queue_delete(name)
    finally:
        await connection.close()


async def _message_count(url: str, name: str) -> int:
    connection = await aio_pika.connect(url)
    try:
        channel = await connection.channel()
        declared = await channel.declare_queue(name, passive=True)
        return int(declared.declaration_result.message_count or 0)
    finally:
        await connection.close()


class Worker:
    """Broker mit deklarierter Familie und Consumer; erste Zustellungen und
    Rückkehrer (Header ``x-mr10-returned``) in getrennten Queues."""

    def __init__(self, broker: RabbitMQBroker, queue: str) -> None:
        self.broker = broker
        self.queue = queue
        self.first: asyncio.Queue[Delivery] = asyncio.Queue()
        self.returned: asyncio.Queue[Delivery] = asyncio.Queue()

    async def start(self) -> None:
        await self.broker.declare_queue(self.queue)
        await self.broker.consume(self.queue, self._on_delivery, concurrency=10)

    async def _on_delivery(self, delivery: Delivery) -> None:
        if delivery.headers.get("x-mr10-returned"):
            await self.broker.ack(delivery)
            await self.returned.put(delivery)
        else:
            await self.first.put(delivery)

    async def received(self, body: bytes) -> Delivery:
        await self.broker.publish(self.queue, body)
        return await asyncio.wait_for(self.first.get(), timeout=5)

    async def retry(self, delivery: Delivery, delay_ms: int) -> None:
        await self.broker.retry_later(delivery, delay_ms=delay_ms, headers={"x-mr10-returned": 1})


@pytest.fixture
async def worker(rabbitmq_url: str) -> AsyncIterator[Worker]:
    broker = RabbitMQBroker(rabbitmq_url)
    await broker.connect()
    queue = f"mr10-{uuid.uuid4().hex[:10]}"
    try:
        yield Worker(broker, queue)
    finally:
        try:
            await broker.cancel_consume()
        except Exception:
            pass
        await broker.close()
        await delete_queue_family(rabbitmq_url, queue)


@pytest.mark.entscheidung("E-026")
async def test_refused_tier_falls_back_to_the_delay_queue(worker: Worker, rabbitmq_url: str) -> None:
    tier = f"{worker.queue}.DQ.1s"
    connection = await aio_pika.connect(rabbitmq_url)
    try:
        channel = await connection.channel()
        await channel.declare_queue(tier, durable=True, arguments={"x-queue-type": "classic"})
    finally:
        await connection.close()
    await worker.start()

    await worker.retry(await worker.received(b"A"), delay_ms=200)

    back = await asyncio.wait_for(worker.returned.get(), timeout=5)
    assert back.body == b"A"
    assert worker.broker.delay_tiers_unavailable(worker.queue) == 1
    assert await _message_count(rabbitmq_url, tier) == 0


async def test_deleted_tier_is_declared_again_by_the_next_retry(worker: Worker, rabbitmq_url: str) -> None:
    await worker.start()
    await worker.retry(await worker.received(b"A"), delay_ms=200)
    await asyncio.wait_for(worker.returned.get(), timeout=5)
    await _delete(rabbitmq_url, f"{worker.queue}.DQ.1s")

    await worker.retry(await worker.received(b"B"), delay_ms=200)

    back = await asyncio.wait_for(worker.returned.get(), timeout=5)
    assert back.body == b"B"
    assert await _message_count(rabbitmq_url, f"{worker.queue}.DQ.1s") == 0


@pytest.mark.entscheidung("E-027")
async def test_queue_depth_counts_tiers_and_ready_depth_only_the_main_queue(worker: Worker) -> None:
    await worker.start()

    await worker.retry(await worker.received(b"A"), delay_ms=10_000)

    assert await worker.broker.queue_depth(worker.queue) == 1
    assert await worker.broker.ready_depth(worker.queue) == 0


async def test_queue_depth_on_a_family_without_tiers_leaves_no_channel_open(worker: Worker, rabbitmq_url: str) -> None:
    """Review MR 10, P0-1: jede fehlende Stufe ist ein NOT_FOUND, der Broker
    schliesst den Wegwerf-Channel; ohne eigenes ``close`` oeffnete ein
    RobustChannel ihn wieder, und die Verbindung erreichte in Stunden
    ``channel_max``."""
    for name, arguments in _legacy(worker.queue).items():
        await _declare_raw(rabbitmq_url, name, arguments)
    _ = await worker.broker._ensure_publish_channel()
    before = _open_channel_numbers(worker.broker)

    for published in range(3):
        assert await worker.broker.queue_depth(worker.queue) == published
        await worker.broker.publish(worker.queue, b"roundtrip")

    assert _open_channel_numbers(worker.broker) == before
