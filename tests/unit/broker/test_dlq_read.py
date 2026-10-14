"""DLQ nicht-zerstoerend lesen (Plan 0.12.0, MR 16, BZ-21).

``dlq list``/``inspect``/``retry`` lesen bestaetigungspflichtig ueber einen
eigenen Channel. Spike S4: jede Rueckgabe zaehlt den Delivery-Count der XQ
hoch (nach 20 waere die Nachricht weg), deshalb Kopie ans Ende + Ack statt
Halten und Zurueckgeben. Nachweis:
``tests/integration/broker/test_befund_bz21_dlq_destructive_read.py``.

Test-Liste:
- [x] ``get(no_ack=False)``; das Original wird erst nach dem Publish seiner Kopie bestaetigt
- [x] ``dlq retry``: Kopie in die Haupt-Queue, dann Ack
- [x] Kopie scheitert: Original unbestaetigt, ``BrokerConnectionError``, die anderen laufen weiter
- [x] abgelaufene Nachricht: Ack ohne Kopie
- [x] der eigene Channel wird geschlossen, auch nach einem Fehler beim Lesen
- [x] eigener Channel auf der Consume-Verbindung, falls offen; nie der Publish-Kanal
- [x] gescheitertes Ack: WARNING, kein Fehler
- [x] Kopien tragen keine Zustellzaehler des Brokers (``x-delivery-count``, ``x-acquired-count``)
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import BrokerConnectionError

pytestmark = [pytest.mark.timeout(5)]

_NOW = 1_760_000_000.0


def _message(task_id: str, headers: dict[str, Any] | None = None, events: list[str] | None = None) -> Mock:
    body = json.dumps({"task": {"id": task_id, "name": "n"}}).encode()
    msg = Mock(body=body, headers={"x-swarmq-dead-lettered-at": int(_NOW * 1000), **(headers or {})})

    async def ack() -> None:
        if events is not None:
            events.append(f"ack {task_id}")

    msg.ack = AsyncMock(side_effect=ack)
    return msg


def _reader(*messages: Mock, events: list[str] | None = None, fail_for: str | None = None) -> RabbitMQBroker:
    broker = RabbitMQBroker("amqp://guest:guest@localhost/")
    broker.clock = lambda: _NOW
    queue = Mock()
    queue.get = AsyncMock(side_effect=[*messages, None])
    queue.channel = AsyncMock()
    broker._dlq_queue = AsyncMock(return_value=queue)  # type: ignore[method-assign]
    broker.published = []  # type: ignore[attr-defined]
    broker.queue = queue  # type: ignore[attr-defined]

    async def publish(routing_key: str, msg: Any) -> None:
        task_id = json.loads(msg.body)["task"]["id"]
        if task_id == fail_for:
            raise BrokerConnectionError("publish failed")
        if events is not None:
            events.append(f"publish {task_id} -> {routing_key}")
        broker.published.append((routing_key, msg))  # type: ignore[attr-defined]

    broker._publish_with_retry = publish  # type: ignore[method-assign]
    return broker


@pytest.mark.entscheidung("E-028")
async def test_list_reads_with_ack_and_acks_after_the_copy() -> None:
    events: list[str] = []
    broker = _reader(_message("t-1", events=events), _message("t-2", events=events), events=events)

    await broker.list_dead_letters("tasks")

    for call in broker.queue.get.await_args_list:  # type: ignore[attr-defined]
        assert call.kwargs["no_ack"] is False
    assert events == ["publish t-1 -> tasks.XQ", "ack t-1", "publish t-2 -> tasks.XQ", "ack t-2"]


async def test_retry_acks_the_original_after_the_copy_to_the_main_queue() -> None:
    events: list[str] = []
    broker = _reader(_message("t-1", events=events), events=events)

    assert (await broker.retry_dead_letter("tasks", "t-1"))[0] is True

    assert events == ["publish t-1 -> tasks", "ack t-1"]


@pytest.mark.entscheidung("E-028")
async def test_failed_copy_leaves_the_original_unacknowledged() -> None:
    events: list[str] = []
    first, second = _message("t-1", events=events), _message("t-2", events=events)
    broker = _reader(first, second, events=events, fail_for="t-1")

    with pytest.raises(BrokerConnectionError, match="1 of 2"):
        await broker.list_dead_letters("tasks")

    first.ack.assert_not_awaited()
    assert events == ["publish t-2 -> tasks.XQ", "ack t-2"]


async def test_expired_message_is_acked_without_copy() -> None:
    expired = _message(
        "t-1", {"x-swarmq-dead-lettered-at": int((_NOW - 7200) * 1000), "x-swarmq-dead-letter-ttl-ms": 3_600_000}
    )
    broker = _reader(expired)

    entries, scanned = await broker.list_dead_letters("tasks")

    assert (entries, scanned) == ([], 0)
    expired.ack.assert_awaited_once()
    assert broker.published == []  # type: ignore[attr-defined]


async def test_own_channel_is_closed_even_when_reading_fails() -> None:
    broker = _reader()
    broker.queue.get = AsyncMock(side_effect=ConnectionError("gone"))  # type: ignore[attr-defined]

    with pytest.raises(ConnectionError):
        await broker.list_dead_letters("tasks")

    broker.queue.channel.close.assert_awaited_once()  # type: ignore[attr-defined]


async def test_failed_ack_is_a_warning_not_an_error(swarmq_logs) -> None:
    msg = _message("t-1")
    msg.ack = AsyncMock(side_effect=ConnectionError("channel gone"))
    broker = _reader(msg)

    entries, _ = await broker.list_dead_letters("tasks")

    assert len(entries) == 1
    assert [e["event"] for e in swarmq_logs if e["log_level"] == "warning"] == ["dlq_read_ack_failed"]


def _connection() -> AsyncMock:
    conn = AsyncMock(is_closed=False)
    conn.channel.return_value = AsyncMock(is_closed=False)
    return conn


async def test_dlq_handle_lives_on_an_own_channel_of_the_consume_connection() -> None:
    broker = RabbitMQBroker("amqp://guest:guest@localhost/")
    broker._connection = _connection()
    broker._consume_connection = _connection()
    broker._publish_channel = AsyncMock(is_closed=False)

    await broker._dlq_queue("tasks")

    broker._consume_connection.channel.return_value.get_queue.assert_awaited_once_with("tasks.XQ")
    broker._connection.channel.assert_not_awaited()
    broker._publish_channel.get_queue.assert_not_awaited()


async def test_dlq_handle_without_consume_connection_uses_an_own_channel_of_the_connection() -> None:
    broker = RabbitMQBroker("amqp://guest:guest@localhost/")
    broker._connection = _connection()
    broker._publish_channel = AsyncMock(is_closed=False)

    await broker._dlq_queue("tasks")

    broker._connection.channel.return_value.get_queue.assert_awaited_once_with("tasks.XQ")
    broker._publish_channel.get_queue.assert_not_awaited()


@pytest.mark.parametrize("call", ["list", "retry"])
async def test_copies_carry_no_broker_delivery_counters(call: str) -> None:
    # Ein abgebrochenes Lesen gibt die Nachricht zurueck; der Broker zaehlt
    # dann. Die Kopie ist eine neue Nachricht mit eigenem Zustellbudget.
    counted = _message("t-1", {"x-delivery-count": 1, "x-acquired-count": 1, "x-keep": 1})
    broker = _reader(counted)

    if call == "list":
        await broker.list_dead_letters("tasks")
    else:
        await broker.retry_dead_letter("tasks", "t-1")

    [(_, msg)] = broker.published  # type: ignore[attr-defined]
    assert "x-delivery-count" not in msg.headers
    assert "x-acquired-count" not in msg.headers
    assert msg.headers["x-keep"] == 1
