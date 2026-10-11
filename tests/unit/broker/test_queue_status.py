"""Zustand gemeldeter Queues per passivem Declare (Plan 0.12.0, MR 18, BZ-14).

``queue_statuses`` liest ``message_count`` und ``consumer_count`` aus dem
declare-ok (AMQP), ohne Management-API. Eine fehlende Queue liefert ``None``;
der Broker schliesst dabei den Channel, die naechste Queue bekommt einen
neuen. Grenze: der aio-pika-Channel ist gemockt.

Test-Liste:
- [x] vorhandene Queue -> ``QueueStatus(message_count, consumer_count)``
- [x] fehlende Queue -> ``None``, die naechste Queue ueber einen neuen Channel
- [x] ohne Verbindung -> ``BrokerConnectionError``
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.contracts.broker import QueueStatus

pytestmark = pytest.mark.timeout(5)


def _declared(messages: int, consumers: int) -> SimpleNamespace:
    return SimpleNamespace(declaration_result=SimpleNamespace(message_count=messages, consumer_count=consumers))


def _broker(*channels: AsyncMock) -> RabbitMQBroker:
    broker = RabbitMQBroker("amqp://guest:guest@localhost/")
    broker._connection = AsyncMock(is_closed=False)
    broker._connection.channel = AsyncMock(side_effect=list(channels))
    return broker


async def test_existing_queue_reports_messages_and_consumers() -> None:
    channel = AsyncMock(is_closed=False)
    channel.declare_queue.return_value = _declared(messages=3, consumers=0)

    statuses = await _broker(channel).queue_statuses(["old"])

    assert statuses == {"old": QueueStatus(message_count=3, consumer_count=0)}
    channel.declare_queue.assert_awaited_once_with("old", passive=True)


async def test_missing_queue_is_none_and_the_next_queue_gets_a_fresh_channel() -> None:
    from aio_pika.exceptions import ChannelNotFoundEntity

    closed = AsyncMock(is_closed=False)
    closed.declare_queue.side_effect = ChannelNotFoundEntity("NOT_FOUND - no queue 'gone' in vhost '/'")
    fresh = AsyncMock(is_closed=False)
    fresh.declare_queue.return_value = _declared(messages=0, consumers=2)

    statuses = await _broker(closed, fresh).queue_statuses(["gone", "live"])

    assert statuses == {"gone": None, "live": QueueStatus(message_count=0, consumer_count=2)}
    fresh.declare_queue.assert_awaited_once_with("live", passive=True)


async def test_without_connection_raises_broker_connection_error() -> None:
    from swarmq.exceptions import BrokerConnectionError

    broker = RabbitMQBroker("amqp://guest:guest@localhost/")

    with pytest.raises(BrokerConnectionError):
        await broker.queue_statuses(["old"])
