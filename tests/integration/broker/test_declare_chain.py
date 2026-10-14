"""Queue-Deklaration gegen echtes RabbitMQ (Plan 0.12.0, MR 8, BZ-15/BZ-19).

Nur eigene, zufällig benannte Queues; sie werden am Ende gelöscht.

Test-Liste:
- [x] neue Queue: Haupt-Queue und ``.DQ`` at-least-once mit ``reject-publish``, ``.XQ`` ohne
- [x] Bestandsqueue mit 0.11-Argumenten: kein 406, Argumente unverändert, eine Warnung
- [x] Bestandsqueue mit Delivery-Limit 20, Konfiguration 50: effektives Limit 20
- [x] Queue gelöscht → ``UnroutableMessage`` → nächste Deklaration legt sie neu an (Rev. 3 F5)
- [x] Modus ``at-least-once`` auf einer 0.11-Queue → ``QueueArgumentsMismatch``
- [x] neue Queue (NOT_FOUND beim passiven Declare): kein Wegwerf-Channel bleibt offen (Review MR 8, H5b)
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from typing import Any

import aio_pika
import pytest
from aio_pika.exceptions import ChannelPreconditionFailed

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import QueueArgumentsMismatch, UnroutableMessage
from tests.support.queues import delete_queue_family
from tests.unit._log_capture import capture_swarmq_logs

pytestmark = [pytest.mark.integration, pytest.mark.timeout(30)]

ALO = {"x-dead-letter-strategy": "at-least-once", "x-overflow": "reject-publish"}


def _legacy(queue: str, limit: int = 20) -> dict[str, dict[str, Any]]:
    return {
        queue: {
            "x-queue-type": "quorum",
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": f"{queue}.XQ",
            "x-delivery-limit": limit,
        },
        f"{queue}.DQ": {"x-queue-type": "quorum", "x-dead-letter-exchange": "", "x-dead-letter-routing-key": queue},
        f"{queue}.XQ": {"x-queue-type": "quorum"},
    }


async def _declare_raw(url: str, name: str, arguments: dict[str, Any]) -> None:
    """Raw-Deklaration; gelingt nur, wenn die Queue genau diese Argumente trägt."""
    connection = await aio_pika.connect(url)
    try:
        channel = await connection.channel()
        await channel.declare_queue(name, durable=True, arguments=arguments)
    finally:
        await connection.close()


async def _delete_raw(url: str, name: str) -> None:
    connection = await aio_pika.connect(url)
    try:
        channel = await connection.channel()
        await channel.queue_delete(name)
    finally:
        await connection.close()


@pytest.fixture
async def queue(rabbitmq_url: str) -> AsyncIterator[str]:
    name = f"mr8-{uuid.uuid4().hex[:10]}"
    yield name
    await delete_queue_family(rabbitmq_url, name)


@pytest.fixture
async def broker(rabbitmq_url: str) -> AsyncIterator[RabbitMQBroker]:
    b = RabbitMQBroker(rabbitmq_url)
    await b.connect()
    yield b
    await b.close()


@pytest.mark.entscheidung("E-017")
async def test_new_queue_is_created_at_least_once(rabbitmq_url: str, broker: RabbitMQBroker, queue: str) -> None:
    await broker.declare_queue(queue)

    family = _legacy(queue)
    await _declare_raw(rabbitmq_url, queue, {**family[queue], **ALO})
    await _declare_raw(rabbitmq_url, f"{queue}.DQ", {**family[f"{queue}.DQ"], **ALO})
    await _declare_raw(rabbitmq_url, f"{queue}.XQ", family[f"{queue}.XQ"])
    with pytest.raises(ChannelPreconditionFailed, match="x-dead-letter-strategy"):
        await _declare_raw(rabbitmq_url, queue, family[queue])


@pytest.mark.entscheidung("E-017")
async def test_existing_0_11_queue_keeps_its_arguments_and_warns_once(
    rabbitmq_url: str, broker: RabbitMQBroker, queue: str
) -> None:
    for name, arguments in _legacy(queue).items():
        await _declare_raw(rabbitmq_url, name, arguments)

    with capture_swarmq_logs() as logs:
        await broker.declare_queue(queue)
        await broker.report_queue_declarations()

    assert not [e for e in logs if e["event"] == "queue_argument_adopted"], "no 406 for a 0.11 queue"
    [warning] = [e for e in logs if e["event"] == "queue_dead_letter_at_most_once"]
    assert warning["queues"] == [queue, f"{queue}.DQ"]
    for name, arguments in _legacy(queue).items():
        await _declare_raw(rabbitmq_url, name, arguments)


async def test_queue_limit_below_the_configuration_is_effective(
    rabbitmq_url: str, broker: RabbitMQBroker, queue: str
) -> None:
    for name, arguments in _legacy(queue, limit=20).items():
        await _declare_raw(rabbitmq_url, name, arguments)
    broker.delivery_limit = 50

    await broker.declare_queue(queue)

    assert broker.effective_delivery_limit(queue) == 20


async def test_deleted_queue_is_declared_again_after_an_unroutable_publish(
    rabbitmq_url: str, broker: RabbitMQBroker, queue: str
) -> None:
    await broker.declare_queue(queue)
    await _delete_raw(rabbitmq_url, queue)

    with pytest.raises(UnroutableMessage):
        await broker.publish(queue, b"lost")
    await broker.declare_queue(queue)
    await broker.publish(queue, b"delivered")


async def test_strict_mode_refuses_a_0_11_queue(rabbitmq_url: str, broker: RabbitMQBroker, queue: str) -> None:
    for name, arguments in _legacy(queue).items():
        await _declare_raw(rabbitmq_url, name, arguments)
    broker.dead_letter_strategy = "at-least-once"

    with pytest.raises(QueueArgumentsMismatch, match="x-dead-letter-strategy"):
        await broker.declare_queue(queue)

    for name, arguments in _legacy(queue).items():
        await _declare_raw(rabbitmq_url, name, arguments)


def _open_channel_numbers(broker: RabbitMQBroker) -> set[int]:
    assert broker._connection is not None
    assert broker._connection.transport is not None
    underlay = broker._connection.transport.connection
    return {number for number, channel in underlay.channels.items() if channel is not None and not channel.is_closed}


async def test_no_probe_channel_stays_open_after_not_found(broker: RabbitMQBroker, queue: str) -> None:
    """Review MR 8, H5b: der Broker schliesst den Channel nach NOT_FOUND; die
    Kette schliesst ihn auch auf ihrer Seite, sonst oeffnete ein
    RobustChannel ihn selbst wieder."""
    _ = await broker._ensure_publish_channel()
    before = _open_channel_numbers(broker)

    await broker.declare_queue(queue)
    await broker.publish(queue, b"roundtrip")

    assert _open_channel_numbers(broker) == before
