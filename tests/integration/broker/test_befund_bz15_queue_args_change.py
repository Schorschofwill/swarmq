"""Nachweis BZ-15: geänderte Queue-Argumente bei bestehender Queue.

Register: docs/befunde/betrieb-ueber-zeit-2026-10.md (BZ-15). ``declare_queue``
schickt ``x-delivery-limit`` (aus ``SWARMQ_DELIVERY_LIMIT``) und
``x-queue-type`` (aus ``SWARMQ_QUEUE_TYPE``) bei jeder Deklaration mit. Die
Doku legt nahe, das Delivery-Limit zu erhöhen.

Nur eigene, zufällig benannte Queues; sie werden am Ende gelöscht.

Test-Liste:
- [x] erhöhtes Delivery-Limit: erneute Deklaration gelingt (BZ-15)
- [x] geänderter Queue-Typ: Fehler ist als Konfigurationsfehler erkennbar (BZ-15)
"""

from __future__ import annotations

import uuid

import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import ConfigurationError
from tests.support.queues import delete_queue_family

pytestmark = [pytest.mark.integration, pytest.mark.timeout(20)]


@pytest.fixture
async def queue_name(rabbitmq_url: str):
    name = f"bz15-{uuid.uuid4().hex[:10]}"
    yield name
    await delete_queue_family(rabbitmq_url, name)


async def _declare(rabbitmq_url: str, queue: str, *, queue_type: str = "quorum", delivery_limit: int = 20) -> None:
    broker = RabbitMQBroker(rabbitmq_url, queue_type=queue_type)
    broker.delivery_limit = delivery_limit
    await broker.connect()
    try:
        await broker.declare_queue(queue)
    finally:
        await broker.close()


@pytest.mark.befund("BZ-15")
async def test_raising_delivery_limit_keeps_existing_queue_declarable(rabbitmq_url: str, queue_name: str) -> None:
    await _declare(rabbitmq_url, queue_name, delivery_limit=20)

    # Deploy mit SWARMQ_DELIVERY_LIMIT=40 gegen die bestehende Queue.
    await _declare(rabbitmq_url, queue_name, delivery_limit=40)


@pytest.mark.befund("BZ-15")
@pytest.mark.xfail(
    strict=True,
    reason=(
        "BZ-15: ein geänderter SWARMQ_QUEUE_TYPE endet als BrokerConnectionError "
        "(Exit 1, Neustart-Schleife) statt als Konfigurationsfehler"
    ),
)
async def test_changed_queue_type_is_reported_as_configuration_error(rabbitmq_url: str, queue_name: str) -> None:
    await _declare(rabbitmq_url, queue_name, queue_type="quorum")

    with pytest.raises(ConfigurationError):
        await _declare(rabbitmq_url, queue_name, queue_type="classic")
