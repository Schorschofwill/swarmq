"""Übernahme vorhandener Queue-Argumente gegen echtes RabbitMQ (Patch 0.11.1).

Die Queues werden vorab so angelegt, wie 0.12 sie anlegt
(``x-dead-letter-strategy=at-least-once``, ``x-overflow=reject-publish``,
abweichendes ``x-delivery-limit``). 0.11.0 brach daran mit 406 ab; 0.11.1
übernimmt die vorhandenen Werte, ohne den Publish-Channel zu schließen.

Test-Liste:
1. 0.12-artige Queues (Haupt-Queue und ``.DQ``) → ``declare_queue`` gelingt,
   WARNING für das Delivery-Limit, Publish-Channel bleibt derselbe und offen.
2. Publishes, die während der Übernahme laufen, gelingen alle.
3. Nicht übernehmbarer Konflikt (classic statt quorum) → ``BrokerConnectionError``
   wie 0.11.0; der Publish-Channel bleibt offen.
4. Nach der Übernahme bleiben keine Wegwerf-Channels offen.
5. Review P2-2a: Queue ohne ``x-delivery-limit`` → ``BrokerConnectionError`` wie 0.11.0.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator
from typing import Any

import aio_pika
import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import BrokerConnectionError
from tests.unit._log_capture import capture_swarmq_logs

pytestmark = [pytest.mark.integration, pytest.mark.timeout(30)]

ALO = {"x-dead-letter-strategy": "at-least-once", "x-overflow": "reject-publish"}


def _unique(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


async def _declare_raw(url: str, name: str, arguments: dict[str, Any]) -> None:
    connection = await aio_pika.connect(url)
    try:
        channel = await connection.channel()
        await channel.declare_queue(name, durable=True, arguments=arguments)
    finally:
        await connection.close()


async def _delete_raw(url: str, *names: str) -> None:
    connection = await aio_pika.connect(url)
    try:
        for name in names:
            channel = await connection.channel()
            try:
                await channel.queue_delete(name)
            except Exception:  # Aufräumen ist best effort
                pass
            finally:
                if not channel.is_closed:
                    await channel.close()
    finally:
        await connection.close()


@pytest.fixture
async def queue(rabbitmq_url: str) -> AsyncIterator[str]:
    name = _unique("adopt")
    yield name
    await _delete_raw(rabbitmq_url, name, f"{name}.DQ", f"{name}.XQ")


@pytest.fixture
async def broker(rabbitmq_url: str) -> AsyncIterator[RabbitMQBroker]:
    b = RabbitMQBroker(rabbitmq_url)
    await b.connect()
    yield b
    await b.close()


async def _create_like_012(url: str, name: str, delivery_limit: int) -> None:
    await _declare_raw(
        url,
        name,
        {
            "x-queue-type": "quorum",
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": f"{name}.XQ",
            "x-delivery-limit": delivery_limit,
            **ALO,
        },
    )
    await _declare_raw(
        url,
        f"{name}.DQ",
        {"x-queue-type": "quorum", "x-dead-letter-exchange": "", "x-dead-letter-routing-key": name, **ALO},
    )


async def test_adopts_arguments_of_queues_created_by_012(rabbitmq_url: str, broker: RabbitMQBroker, queue: str) -> None:
    await _create_like_012(rabbitmq_url, queue, delivery_limit=40)
    publish_channel = await broker._ensure_publish_channel()

    with capture_swarmq_logs() as logs:
        await broker.declare_queue(queue)

    assert broker._publish_channel is publish_channel
    assert not publish_channel.is_closed
    [warning] = [e for e in logs if e["event"] == "queue_argument_adopted" and e["log_level"] == "warning"]
    assert (warning["queue"], warning["argument"], warning["existing"]) == (queue, "x-delivery-limit", 40)
    await broker.publish(queue, b"after adoption")


async def test_publishes_running_during_adoption_succeed(rabbitmq_url: str, broker: RabbitMQBroker, queue: str) -> None:
    target = _unique("adopt_target")
    try:
        await broker.declare_queue(target)
        await _create_like_012(rabbitmq_url, queue, delivery_limit=40)

        publishes = [asyncio.create_task(broker.publish(target, f"m{i}".encode())) for i in range(50)]
        await broker.declare_queue(queue)
        await asyncio.gather(*publishes)

        depth = await broker.queue_depth(target)
        assert depth == 50
    finally:
        await _delete_raw(rabbitmq_url, target, f"{target}.DQ", f"{target}.XQ")


async def test_other_conflict_fails_and_keeps_publish_channel(
    rabbitmq_url: str, broker: RabbitMQBroker, queue: str
) -> None:
    await _declare_raw(rabbitmq_url, queue, {"x-queue-type": "classic"})
    publish_channel = await broker._ensure_publish_channel()

    with pytest.raises(BrokerConnectionError, match="Existing queue"):
        await broker.declare_queue(queue)

    assert broker._publish_channel is publish_channel
    assert not publish_channel.is_closed


def _open_channel_numbers(broker: RabbitMQBroker) -> set[int]:
    assert broker._connection is not None
    underlay = broker._connection.transport.connection  # type: ignore[attr-defined]
    return {number for number, channel in underlay.channels.items() if channel is not None and not channel.is_closed}


async def test_no_probe_channel_stays_open(rabbitmq_url: str, broker: RabbitMQBroker, queue: str) -> None:
    await _create_like_012(rabbitmq_url, queue, delivery_limit=40)
    await broker._ensure_publish_channel()
    before = _open_channel_numbers(broker)

    await broker.declare_queue(queue)
    await broker.publish(queue, b"roundtrip")

    assert _open_channel_numbers(broker) == before


async def test_queue_without_delivery_limit_fails_as_in_0_11_0(
    rabbitmq_url: str, broker: RabbitMQBroker, queue: str
) -> None:
    """Review P2-2a: eine Quorum-Queue ohne ``x-delivery-limit`` wird nicht übernommen."""
    await _declare_raw(
        rabbitmq_url,
        queue,
        {"x-queue-type": "quorum", "x-dead-letter-exchange": "", "x-dead-letter-routing-key": f"{queue}.XQ"},
    )
    publish_channel = await broker._ensure_publish_channel()

    with pytest.raises(BrokerConnectionError, match="Existing queue"):
        await broker.declare_queue(queue)

    assert not publish_channel.is_closed
