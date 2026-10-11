"""``queue_statuses`` gegen den Test-Broker (Plan 0.12.0, MR 18, BZ-14).

Test-Liste:
- [x] Queue mit Rueckstand ohne Consumer, mit Consumer, fehlende Queue
"""

from __future__ import annotations

import uuid

import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.contracts import Delivery
from swarmq.contracts.broker import QueueStatus
from tests.support.queues import delete_queue_family
from tests.support.wait import wait_until

pytestmark = [pytest.mark.integration, pytest.mark.timeout(15)]


async def test_statuses_report_messages_consumers_and_missing_queues(rabbitmq_url: str) -> None:
    suffix = uuid.uuid4().hex[:10]
    idle, busy, missing = f"qs-idle-{suffix}", f"qs-busy-{suffix}", f"qs-missing-{suffix}"
    broker = RabbitMQBroker(rabbitmq_url)
    await broker.connect()
    try:
        await broker.declare_queue(idle)
        await broker.declare_queue(busy)
        await broker.publish(idle, b"{}")
        await broker.publish(idle, b"{}")

        async def _ignore(_delivery: Delivery) -> None:
            return None

        await broker.consume(busy, _ignore, concurrency=1)

        statuses: dict[str, QueueStatus | None] = {}

        async def consumer_registered() -> bool:
            statuses.update(await broker.queue_statuses([idle, missing, busy]))
            status = statuses.get(busy)
            return status is not None and status.consumer_count == 1

        assert await wait_until(consumer_registered, timeout=5)
        assert statuses[idle] == QueueStatus(message_count=2, consumer_count=0)
        assert statuses[missing] is None
        assert statuses[busy] == QueueStatus(message_count=0, consumer_count=1)
    finally:
        await broker.close()
        await delete_queue_family(rabbitmq_url, idle, busy)
