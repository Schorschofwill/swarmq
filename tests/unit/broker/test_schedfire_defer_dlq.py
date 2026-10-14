"""``dlq retry`` setzt den Schedfire-Defer-Header zurueck (FS-03, Plan U5, KTD5, F9).

Eine zurueckgestellte Schedfire-Kopie, die spaeter in die DLQ geht (Guard-
Orphans, Zustelllimit), traegt ``x-swarmq-schedfire-defer-count`` weiter. Die
Wiederholung beginnt -- wie beim Retry-Zaehler -- ohne ihn, sonst wuchse ihr
Backoff weiter und der Deckel griffe zu frueh.

Test-Liste:
- [x] ``dlq retry`` entfernt den Header, andere Header bleiben
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock, Mock

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.headers import HEADER_SCHEDFIRE_DEFER_COUNT

_NOW = 1_760_000_000.0


def _dlq_broker(*messages: Any) -> Any:
    broker: Any = RabbitMQBroker("amqp://guest:guest@localhost/")
    broker._publish_channel = AsyncMock(is_closed=False)
    broker._connection = AsyncMock()
    broker.clock = lambda: _NOW
    queue = Mock()
    queue.get = AsyncMock(side_effect=[*messages, None])
    broker._dlq_queue = AsyncMock(return_value=queue)
    broker.published = []

    async def publish(routing_key: str, msg: Any) -> None:
        broker.published.append((routing_key, msg))

    broker._publish_with_retry = publish
    return broker


async def test_dlq_retry_drops_the_schedfire_defer_count() -> None:
    body = json.dumps({"task": {"id": "t-1", "name": "n"}}).encode()
    headers = {HEADER_SCHEDFIRE_DEFER_COUNT: 9, "x-swarmq-schedfire": "cron:a:1", "x-death": []}
    broker = _dlq_broker(Mock(body=body, headers=headers))

    found, _ = await broker.retry_dead_letter("tasks", "t-1")

    [(_, msg)] = broker.published
    assert found is True
    assert HEADER_SCHEDFIRE_DEFER_COUNT not in msg.headers
    assert msg.headers["x-swarmq-schedfire"] == "cron:a:1"
