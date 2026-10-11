"""Zustands-Header ``x-swarmq-unknown-since`` an der DLQ (Plan 0.12.0, MR 11, BZ-06, Q6).

Test-Liste:
- [x] der DLQ-Stempel (``_publish_to_dead_letter_queue``) entfernt den Header
- [x] ``dlq retry`` entfernt ihn aus einer DLX-Kopie (``nack``, Header blieben)
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock, Mock

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.headers import HEADER_UNKNOWN_SINCE

_NOW = 1_760_000_000.0


def _broker() -> Any:
    broker = RabbitMQBroker("amqp://guest:guest@localhost/")
    broker._publish_channel = AsyncMock(is_closed=False)
    broker._connection = AsyncMock()
    broker.clock = lambda: _NOW
    return broker


async def test_dead_letter_stamp_drops_unknown_since() -> None:
    broker = _broker()
    await broker._publish_to_dead_letter_queue("tasks", b"body", headers={HEADER_UNKNOWN_SINCE: 1, "x-keep": 1})
    headers = broker._publish_channel.default_exchange.publish.call_args.args[0].headers
    assert HEADER_UNKNOWN_SINCE not in headers and headers["x-keep"] == 1


def _dlq_broker(*messages: Any) -> Any:
    broker = _broker()
    queue = Mock()
    queue.get = AsyncMock(side_effect=[*messages, None])
    broker._dlq_queue = AsyncMock(return_value=queue)
    broker.published = []

    async def publish(routing_key: str, msg: Any) -> None:
        broker.published.append((routing_key, msg))

    broker._publish_with_retry = publish
    return broker


async def test_dlq_retry_drops_unknown_since_from_a_dlx_copy() -> None:
    body = json.dumps({"task": {"id": "t-1", "name": "n"}}).encode()
    broker = _dlq_broker(Mock(body=body, headers={HEADER_UNKNOWN_SINCE: 1, "x-death": []}))
    found, _ = await broker.retry_dead_letter("tasks", "t-1")
    [(routing_key, msg)] = broker.published
    assert found is True and routing_key == "tasks"
    assert HEADER_UNKNOWN_SINCE not in msg.headers
