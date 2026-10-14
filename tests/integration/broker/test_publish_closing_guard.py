"""FS-08-Waechter gegen echtes RabbitMQ mit aio-pikas ``RobustChannel``.

Der Publish laeuft gegen ``closing`` des aiormq-Kanals unter dem
Publish-Kanal. Liefert die installierte aio-pika/aiormq-Kombination dieses
Future nicht, faellt der Publish still in den Pfad vor FS-08 zurueck und
meldet ``publish_channel_closing_unreadable``. Die Unit-Doubles setzen
``closing`` selbst; nur hier spricht der echte Kanal.

Test-Liste:
- [x] Publish ueber den echten RobustChannel -> zugestellt, keine Meldung
      ``publish_channel_closing_unreadable``
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import AsyncIterator

import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from tests.support.queues import delete_queue_family

pytestmark = [pytest.mark.timeout(15)]


@pytest.fixture
async def broker(rabbitmq_url: str) -> AsyncIterator[tuple[RabbitMQBroker, str]]:
    queue = f"closingguard-{uuid.uuid4().hex[:8]}"
    impl = RabbitMQBroker(rabbitmq_url)
    await impl.connect()
    await impl.declare_queue(queue)
    yield impl, queue
    await delete_queue_family(rabbitmq_url, queue)
    await impl.close()


async def test_a_publish_over_the_real_robust_channel_uses_the_closing_guard(
    broker: tuple[RabbitMQBroker, str], caplog: pytest.LogCaptureFixture
) -> None:
    impl, queue = broker

    with caplog.at_level(logging.WARNING, logger="swarmq"):
        receipt = await impl.publish(queue, b"{}")

    assert receipt.queue == queue
    assert [r.msg for r in caplog.records if r.msg == "publish_channel_closing_unreadable"] == []
