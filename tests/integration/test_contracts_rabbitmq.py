"""Binding der Broker-Contract-Klasse an RabbitMQ (Plan U5, KTD9).

Läuft nur mit erreichbarem Dienst (``check_docker`` aus ``tests/support``).
Bindings liefern Fixtures, keine ``test_*``-Methoden (Struktur-Guard).
"""

from __future__ import annotations

import contextlib
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import aio_pika
import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from tests.contracts import publisher_consumer
from tests.support.inmemory import RealClock
from tests.support.rabbitmq_faults import publish_failure


class TestBrokerRabbitMQ(publisher_consumer.BrokerContract):
    @pytest.fixture
    def clock(self) -> RealClock:
        return RealClock()

    @pytest.fixture
    def queue(self) -> str:
        return f"ctr-{uuid.uuid4().hex[:8]}.high"

    @pytest.fixture
    async def impl(self, rabbitmq_url: str, queue: str) -> AsyncIterator[RabbitMQBroker]:
        broker = RabbitMQBroker(rabbitmq_url)
        await broker.connect()
        await broker.declare_queue(queue)
        yield broker
        with contextlib.suppress(Exception):
            await broker.cancel_consume()
        await _delete_triplet(rabbitmq_url, queue)
        await broker.close()

    @pytest.fixture
    def fresh_impl(self, rabbitmq_url: str) -> RabbitMQBroker:
        return RabbitMQBroker(rabbitmq_url)

    @pytest.fixture
    def unreachable_impl(self) -> RabbitMQBroker:
        return RabbitMQBroker("amqp://guest:guest@127.0.0.1:1/")

    @pytest.fixture
    def dead_letters(self, rabbitmq_url: str) -> Callable[[str], Awaitable[list[tuple[bytes, dict[str, Any]]]]]:
        async def _read(queue: str) -> list[tuple[bytes, dict[str, Any]]]:
            # Nicht-destruktiv: eigener Kanal, ``basic.get`` ohne Ack; das
            # Schließen des Kanals stellt die Nachrichten wieder in die Queue.
            connection = await aio_pika.connect_robust(rabbitmq_url)
            try:
                channel = await connection.channel()
                amqp_queue = await channel.get_queue(f"{queue}.XQ")
                letters: list[tuple[bytes, dict[str, Any]]] = []
                while True:
                    message = await amqp_queue.get(no_ack=False, fail=False)
                    if message is None:
                        break
                    letters.append((message.body, dict(message.headers or {})))
                await channel.close()
                return letters
            finally:
                await connection.close()

        return _read

    @pytest.fixture
    def publish_failure(self, impl: RabbitMQBroker) -> Callable[[], contextlib.AbstractContextManager[None]]:
        return lambda: publish_failure(impl)


async def _delete_triplet(url: str, queue: str) -> None:
    connection = await aio_pika.connect_robust(url)
    try:
        channel = await connection.channel()
        for suffix in ("", ".DQ", ".XQ"):
            with contextlib.suppress(Exception):
                await channel.queue_delete(f"{queue}{suffix}")
    finally:
        await connection.close()
