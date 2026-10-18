"""Per-Message-``expiration`` an der Fristgrenze (E-063, Nachtrag 2026-10-17;
PF-11, U16).

``retry_max_delay``, ``lock_wait_max_s`` und ``dead_letter_ttl_s`` enden als
Per-Message-``expiration`` an der Delay- oder Dead-Letter-Queue. Kodiert wird
wie in ``broker/rabbitmq/delivery.py``: Sekunden als ``float``, aio-pika macht
daraus ganze Millisekunden. RabbitMQ 4.3 nimmt höchstens ``MAX_DEADLINE_MS``
(zehn Jahre) an; eine Millisekunde mehr schließt den Kanal mit
``PRECONDITION_FAILED`` (``value_too_large``). Die Konfiguration lehnt alles
darüber vorher ab (``tests/unit/test_config_bounds.py``).

Testliste:
# [x] expiration = MAX_DEADLINE_S -> angenommen, Nachricht liegt in der Queue
# [x] expiration = MAX_DEADLINE_S + 1 ms -> PRECONDITION_FAILED value_too_large
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator

import aio_pika
import pytest
from aio_pika.exceptions import ChannelPreconditionFailed

from swarmq.config_validation import MAX_DEADLINE_MS, MAX_DEADLINE_S
from tests.support.queues import delete_queue_family

pytestmark = [pytest.mark.integration, pytest.mark.timeout(15), pytest.mark.entscheidung("E-063")]


@pytest.fixture
async def bounds_queue(rabbitmq_url: str) -> AsyncIterator[str]:
    name = f"u16-bounds-{uuid.uuid4().hex[:10]}"
    yield name
    await delete_queue_family(rabbitmq_url, name)


async def _publish_with_expiration(url: str, queue: str, expiration_s: float) -> int:
    """Veröffentlicht mit Bestätigung und liefert die Länge der Queue danach."""
    connection = await aio_pika.connect(url)
    try:
        channel = await connection.channel()
        declared = await channel.declare_queue(queue, durable=True)
        message = aio_pika.Message(b"x", delivery_mode=aio_pika.DeliveryMode.PERSISTENT, expiration=expiration_s)
        await channel.default_exchange.publish(message, routing_key=queue)
        result = await declared.declare()
        assert result.message_count is not None
        return result.message_count
    finally:
        await connection.close()


async def test_expiration_at_the_bound_is_accepted(rabbitmq_url: str, bounds_queue: str) -> None:
    count = await _publish_with_expiration(rabbitmq_url, bounds_queue, MAX_DEADLINE_S)

    assert count == 1


async def test_expiration_one_millisecond_above_the_bound_is_rejected(rabbitmq_url: str, bounds_queue: str) -> None:
    above_s = (MAX_DEADLINE_MS + 1) / 1000

    with pytest.raises(
        ChannelPreconditionFailed, match=rf"invalid expiration '{MAX_DEADLINE_MS + 1}'.*value_too_large"
    ):
        await _publish_with_expiration(rabbitmq_url, bounds_queue, above_s)
