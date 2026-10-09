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

import aio_pika
import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import ConfigurationError

pytestmark = [pytest.mark.integration, pytest.mark.timeout(20)]


@pytest.fixture
async def queue_name(rabbitmq_url: str):
    name = f"bz15-{uuid.uuid4().hex[:10]}"
    yield name
    conn = await aio_pika.connect(rabbitmq_url)
    try:
        for qname in (name, f"{name}.DQ", f"{name}.XQ"):
            ch = await conn.channel()
            try:
                await ch.queue_delete(qname)
            except Exception:
                pass
            finally:
                if not ch.is_closed:
                    await ch.close()
    finally:
        await conn.close()


async def _declare(rabbitmq_url: str, queue: str, *, queue_type: str = "quorum", delivery_limit: int = 20) -> None:
    broker = RabbitMQBroker(rabbitmq_url, queue_type=queue_type)
    broker.delivery_limit = delivery_limit
    await broker.connect()
    try:
        await broker.declare_queue(queue)
    finally:
        await broker.close()


@pytest.mark.befund("BZ-15")
@pytest.mark.xfail(
    strict=True,
    reason=(
        "BZ-15: ein erhöhtes SWARMQ_DELIVERY_LIMIT lässt die Deklaration einer "
        "bestehenden Queue mit PRECONDITION_FAILED scheitern, der Worker startet nicht"
    ),
)
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
