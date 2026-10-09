"""Spike S4 (MR 13, 16): Delivery-Count auf einer ``.XQ`` ohne Argumente (Default-Limit).

Die ``.XQ`` legt ``RabbitMQBroker.declare_queue`` an (heute nur
``x-queue-type=quorum``). Beobachtet:

- 4.2.9: jede Rückgabe zählt ``x-delivery-count`` um 1 hoch: ``basic.nack``
  und ``basic.reject`` mit requeue, Channel-Abbruch mit unbestätigter
  Nachricht (aus ``basic.get`` wie aus einem Consumer). Nach 20 Rückgaben
  ist die Nachricht weg -- die ``.XQ`` hat kein Dead-Letter-Ziel, sie geht
  verloren (21 Zustellungen).
- 4.3.6: ``basic.nack(requeue)`` zählt **nicht** (beliebig oft, Nachricht
  bleibt); ``basic.reject(requeue)`` und Channel-Abbruch zählen um 1. Nach
  20 gezählten Rückgaben ist die Nachricht ebenfalls weg. Zusätzlich trägt
  jede Zustellung ``x-acquired-count`` (zählt jede Zustellung).

Test-Liste:
- [x] Zählerfolge für close, close, nack, nack, reject, close
- [x] nack(requeue) im Consumer
- [x] .XQ verliert die Nachricht nach 20 gezählten Rückgaben
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import aio_pika
import pytest
from aio_pika.abc import AbstractConnection, AbstractIncomingMessage

from tests.integration.spikes._broker import (
    delete_queues,
    delivery_counts,
    depth,
    nack_requeue,
    publish,
    reject_requeue,
)
from tests.support.services import RabbitMQNode
from tests.support.wait import wait_until

pytestmark = [pytest.mark.integration, pytest.mark.spike]


@pytest.fixture
async def xq(node: RabbitMQNode, queue_name: Callable[[str], str]) -> Any:
    """Eine ``.XQ``, wie swarmq sie heute anlegt (über ``RabbitMQBroker``)."""
    from swarmq.broker.rabbitmq import RabbitMQBroker

    queue = queue_name("s4")
    broker = RabbitMQBroker(node.amqp_url)
    await broker.connect()
    try:
        await broker.declare_queue(queue)
    finally:
        await broker.close()
    yield f"{queue}.XQ"
    await delete_queues(node.amqp_url, f"{queue}.DQ", f"{queue}.XQ")


async def _step(connection: AbstractConnection, queue: str, how: str) -> tuple[Any, Any]:
    """Eine Zustellung per ``basic.get`` und Rückgabe auf die Art ``how``;
    liefert (``x-delivery-count``, ``x-acquired-count``) der Zustellung."""
    channel = await connection.channel()
    message = await (await channel.get_queue(queue, ensure=False)).get(no_ack=False)
    headers = message.headers or {}
    seen = (headers.get("x-delivery-count"), headers.get("x-acquired-count"))
    if how == "nack":
        await message.nack(requeue=True)
    elif how == "reject":
        await message.reject(requeue=True)
    await channel.close()

    async def back() -> bool:
        return await depth(connection, queue) == 1

    assert await wait_until(back, timeout=5.0), "message did not return to the queue"
    return seen


async def test_delivery_count_per_return_kind(version: str, amqp: AbstractConnection, xq: str) -> None:
    await publish(amqp, xq, aio_pika.Message(b"s4"))

    seen = [await _step(amqp, xq, how) for how in ("close", "close", "nack", "nack", "reject", "close", "close")]

    if version == "4.2":
        assert seen == [(None, None), (1, None), (2, None), (3, None), (4, None), (5, None), (6, None)]
    else:
        assert seen == [(None, None), (1, 1), (2, 2), (2, 3), (2, 4), (3, 5), (4, 6)]


async def test_consumer_nack_requeue(version: str, amqp: AbstractConnection, xq: str) -> None:
    await publish(amqp, xq, aio_pika.Message(b"s4"))
    channel = await amqp.channel()
    counts: list[Any] = []

    async def on_message(message: AbstractIncomingMessage) -> None:
        counts.append((message.headers or {}).get("x-delivery-count"))
        if len(counts) < 3:
            await message.nack(requeue=True)

    await (await channel.get_queue(xq, ensure=False)).consume(on_message)
    assert await wait_until(lambda: len(counts) >= 3, timeout=5.0)
    await channel.close()

    assert counts == ([None, 1, 2] if version == "4.2" else [None, None, None])


@pytest.mark.parametrize("give_back", ["nack", "reject"])
async def test_dead_letter_queue_drops_message_after_default_limit(
    version: str, amqp: AbstractConnection, xq: str, give_back: str
) -> None:
    await publish(amqp, xq, aio_pika.Message(b"s4"))

    counts = await delivery_counts(amqp, xq, nack_requeue if give_back == "nack" else reject_requeue, cap=30)

    if give_back == "nack" and version == "4.3":
        assert len(counts) == 30, "nack(requeue) does not count on 4.3: the message stays"
        assert await depth(amqp, xq) == 1
    else:
        assert len(counts) == 21, "default delivery limit 20 on the .XQ"
        assert await depth(amqp, xq) == 0, "the message is gone (no dead-letter target)"
