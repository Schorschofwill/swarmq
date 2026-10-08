"""B22: Abschluss-Verben auf einem echten, geschlossenen Kanal (0.9.1).

Unter Last (Toxiproxy: RabbitMQ 5 s weg) warf ``ack`` aiormqs
``ChannelInvalidStateError`` roh aus der Zustellungs-Task. Der Vertrag sagt
jetzt ``BrokerConnectionError``; der Broker stellt die unbestaetigte
Nachricht neu zu.

Test-Liste:
- [x] ``ack`` nach geschlossenem Consume-Kanal → ``BrokerConnectionError``,
      die Nachricht kommt bei einem anderen Consumer erneut an.
- [x] ``redeliver`` ebenso.
"""

from __future__ import annotations

import asyncio
import uuid

import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.contracts import Delivery
from swarmq.exceptions import BrokerConnectionError

pytestmark = [pytest.mark.timeout(20)]


async def _first_delivery(broker: RabbitMQBroker, queue: str) -> Delivery:
    got: asyncio.Future[Delivery] = asyncio.get_running_loop().create_future()

    async def _on_delivery(delivery: Delivery) -> None:
        if not got.done():
            got.set_result(delivery)

    await broker.consume(queue, _on_delivery, concurrency=1)
    return await got


@pytest.fixture
async def setup(rabbitmq_url: str):
    queue = f"settle_{uuid.uuid4().hex[:8]}"
    consumer = RabbitMQBroker(rabbitmq_url)
    other = RabbitMQBroker(rabbitmq_url)
    await consumer.connect()
    await other.connect()
    await other.declare_queue(queue)
    await other.publish(queue, b"payload")
    yield consumer, other, queue
    await consumer.close()
    ch = await other._connection.channel()
    for name in (queue, f"{queue}.DQ", f"{queue}.XQ"):
        await ch.queue_delete(name)
    await ch.close()
    await other.close()


@pytest.mark.parametrize("verb", ["ack", "redeliver"])
async def test_verb_on_closed_channel_raises_broker_connection_error_and_message_returns(setup, verb):
    consumer, other, queue = setup
    delivery = await _first_delivery(consumer, queue)

    assert consumer._consume_channel is not None
    await consumer._consume_channel.close()

    with pytest.raises(BrokerConnectionError):
        await getattr(consumer, verb)(delivery)

    again = await _first_delivery(other, queue)
    assert again.body == b"payload"
    await other.ack(again)
