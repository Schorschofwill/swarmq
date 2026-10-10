"""Q6 (Plan 0.12.0, MR 13, BZ-05): die Lock-Zustands-Header enden am
DLQ-Stempel und bei ``dlq retry``.

Test-Liste:
- [x] der DLQ-Stempel (``_publish_to_dead_letter_queue``) entfernt sie
- [x] ``dlq retry`` entfernt sie aus einer DLX-Kopie (``nack``, Header blieben)
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock, Mock

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.headers import (
    HEADER_LOCK_CONFLICTS,
    HEADER_LOCK_HOLDER,
    HEADER_LOCK_HOLDER_SINCE,
    HEADER_LOCK_HOLDER_TTL_MS,
    HEADER_LOCK_WAIT_SINCE,
)

_STATE = {
    HEADER_LOCK_CONFLICTS: 3,
    HEADER_LOCK_WAIT_SINCE: 1,
    HEADER_LOCK_HOLDER: "abc",
    HEADER_LOCK_HOLDER_SINCE: 1,
    HEADER_LOCK_HOLDER_TTL_MS: 90_000,
}


def _broker() -> Any:
    broker = RabbitMQBroker("amqp://guest:guest@localhost/")
    broker._publish_channel = AsyncMock(is_closed=False)
    broker._connection = AsyncMock()
    broker.clock = lambda: 1_760_000_000.0
    return broker


async def test_dead_letter_stamp_drops_the_lock_wait_state() -> None:
    broker = _broker()
    await broker._publish_to_dead_letter_queue("tasks", b"body", headers={**_STATE, "x-swarmq-lock-name": "r"})
    headers = broker._publish_channel.default_exchange.publish.call_args.args[0].headers
    assert not set(_STATE) & set(headers) and headers["x-swarmq-lock-name"] == "r"


async def test_dlq_retry_drops_the_lock_wait_state() -> None:
    broker = _broker()
    body = json.dumps({"task": {"id": "t-1", "name": "n"}}).encode()
    queue = Mock()
    queue.get = AsyncMock(side_effect=[Mock(body=body, headers={**_STATE, "x-death": []}), None])
    broker._dlq_queue = AsyncMock(return_value=queue)
    published: list[Any] = []

    async def publish(routing_key: str, msg: Any) -> None:
        published.append(msg)

    broker._publish_with_retry = publish
    found, _ = await broker.retry_dead_letter("tasks", "t-1")
    assert found is True
    assert not set(_STATE) & set(published[0].headers)
