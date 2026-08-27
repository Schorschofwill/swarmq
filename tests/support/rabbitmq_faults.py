"""Fehlerinjektion für den echten ``RabbitMQBroker`` (Plan U5).

``publish_failure(broker)`` ersetzt den Publish-Kanal über die injizierte
Channel-Factory durch eine Attrappe, deren ``publish`` den transienten
Kanalfehler wirft — ``_publish_with_retry`` erschöpft seine fünf Versuche
und wirft ``BrokerConnectionError``. Ein bloß geschlossener Kanal reicht
nicht, weil ``_ensure_publish_channel`` ihn transparent neu erzeugt.
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterator

import aio_pika
from aio_pika.exceptions import ChannelInvalidStateError

from swarmq.broker.rabbitmq import RabbitMQBroker


class _FailingExchange:
    async def publish(self, message: aio_pika.Message, routing_key: str) -> None:
        raise ChannelInvalidStateError("injected publish failure")


class FailingChannel:
    is_closed = False
    default_exchange = _FailingExchange()

    async def close(self) -> None:
        return None


@contextlib.contextmanager
def publish_failure(broker: RabbitMQBroker) -> Iterator[None]:
    async def _factory() -> aio_pika.abc.AbstractChannel:
        return FailingChannel()  # type: ignore[return-value]

    previous = broker._publish_channel
    broker._publish_channel_factory = _factory
    broker._publish_channel = None
    try:
        yield
    finally:
        broker._publish_channel_factory = None
        broker._publish_channel = previous
