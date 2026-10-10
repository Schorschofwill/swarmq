"""Worker-Kopie an eine geloeschte eigene Queue (Plan 0.12.0, MR 6, BZ-03).

Am echten Broker: Die ``.DQ`` der Zustellungs-Queue wird geloescht, waehrend
der Worker laeuft. ``retry_later`` bekommt ein ``basic.return``, deklariert
die Familie auf einem Wegwerf-Channel neu und veroeffentlicht die Kopie ein
zweites Mal; das Original wird bestaetigt.

Test-Liste:
- [x] ``retry_later`` nach geloeschter ``.DQ``: Kopie liegt in der neu angelegten ``.DQ``
"""

from __future__ import annotations

import asyncio
import uuid

import aio_pika
import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.contracts import Delivery
from tests.support.queues import delete_queue_family

pytestmark = [pytest.mark.integration, pytest.mark.timeout(20)]


async def _ready_count(url: str, name: str) -> int:
    connection = await aio_pika.connect(url)
    try:
        channel = await connection.channel()
        declared = await channel.declare_queue(name, passive=True)
        return int(declared.declaration_result.message_count or 0)
    finally:
        await connection.close()


async def _delete(url: str, name: str) -> None:
    connection = await aio_pika.connect(url)
    try:
        channel = await connection.channel()
        await channel.queue_delete(name)
    finally:
        await connection.close()


async def test_retry_later_redeclares_deleted_delay_queue(rabbitmq_url: str) -> None:
    queue = f"mr6-redeclare-{uuid.uuid4().hex[:10]}"
    broker = RabbitMQBroker(rabbitmq_url)
    await broker.connect()
    try:
        await broker.declare_queue(queue)
        received: asyncio.Queue[Delivery] = asyncio.Queue()

        async def _on_delivery(delivery: Delivery) -> None:
            await received.put(delivery)

        await broker.consume(queue, _on_delivery, concurrency=1)
        await broker.publish(queue, b'{"probe": true}')
        delivery = await asyncio.wait_for(received.get(), timeout=5)
        await _delete(rabbitmq_url, f"{queue}.DQ")

        await broker.retry_later(delivery, delay_ms=7_200_000, headers={})

        assert await _ready_count(rabbitmq_url, f"{queue}.DQ") == 1
    finally:
        await broker.close()
        await delete_queue_family(rabbitmq_url, queue)
