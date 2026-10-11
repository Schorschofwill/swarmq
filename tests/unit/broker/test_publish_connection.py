"""Getrennte Publish- und Consume-Verbindung (Plan 0.12.0, MR 16, BZ-16).

Bei einem Broker-Alarm blockiert RabbitMQ nur Verbindungen, die publizieren.
Laufen Acks ueber dieselbe Verbindung, haengen sie hinter dem blockierten
Publish (Nachweis ``tests/integration/broker/test_befund_bz16_connection_blocked.py``).

Test-Liste:
- [x] ``separate`` (Default): ``connect()`` oeffnet nur den Publish-Kanal (test_rabbitmq.py)
- [x] ``shared``: ``connect()`` oeffnet beide Kanaele auf einer Verbindung (test_rabbitmq.py)
- [x] ``separate``: der erste ``consume()`` oeffnet eine zweite Verbindung mit derselben URL,
      der Consume-Kanal liegt auf ihr; ein zweiter ``consume()`` oeffnet keine weitere
- [x] ``shared``: ``consume()`` bleibt auf der einen Verbindung
- [x] injizierte Verbindung: immer ``shared`` (keine URL)
- [x] ``close()`` schliesst beide Verbindungen
- [x] Consume-Verbindung scheitert -> ``BrokerConnectionError``
- [x] Admin-Lesen (``queue_depth``, ``dead_letter_depth``) ueber die Consume-Verbindung, falls offen
- [x] ... sonst wie bisher ueber die Publish-Verbindung (auch wenn die Consume-Verbindung zu ist)
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import BrokerConnectionError

pytestmark = [pytest.mark.timeout(5)]

_URL = "amqp://guest:guest@localhost/"
_CONNECT = "swarmq.broker.rabbitmq.connection.aio_pika.connect_robust"


def _connection() -> AsyncMock:
    conn = AsyncMock(is_closed=False)
    channel = AsyncMock(is_closed=False)
    channel.get_queue.return_value = AsyncMock()
    conn.channel.return_value = channel
    return conn


async def _connected(mode: str = "separate") -> tuple[RabbitMQBroker, AsyncMock, AsyncMock]:
    broker = RabbitMQBroker(_URL)
    broker.publish_connection = mode
    publish_conn, consume_conn = _connection(), _connection()
    with patch(_CONNECT, AsyncMock(side_effect=[publish_conn, consume_conn])) as connect:
        await broker.connect()
        await broker.consume("q1", AsyncMock(), concurrency=4)
        await broker.consume("q2", AsyncMock(), concurrency=4)
    broker.connect_calls = connect.await_args_list  # type: ignore[attr-defined]
    return broker, publish_conn, consume_conn


async def test_first_consume_opens_a_second_connection_for_the_consume_channel() -> None:
    broker, publish_conn, consume_conn = await _connected()

    assert [c.args for c in broker.connect_calls] == [(_URL,), (_URL,)]  # type: ignore[attr-defined]
    assert broker._consume_channel is consume_conn.channel.return_value
    publish_conn.channel.assert_awaited_once_with(on_return_raises=True)
    consume_conn.channel.assert_awaited_once_with()


async def test_shared_mode_consumes_on_the_one_connection() -> None:
    broker, publish_conn, consume_conn = await _connected("shared")

    assert len(broker.connect_calls) == 1  # type: ignore[attr-defined]
    assert publish_conn.channel.await_count == 2
    consume_conn.channel.assert_not_awaited()


async def test_injected_connection_is_always_shared() -> None:
    conn = _connection()
    broker = RabbitMQBroker(connection=conn)
    broker.publish_connection = "separate"
    await broker.connect()

    with patch(_CONNECT, AsyncMock()) as connect:
        await broker.consume("q1", AsyncMock(), concurrency=4)

    connect.assert_not_awaited()
    assert conn.channel.await_count == 2


async def test_close_closes_both_connections() -> None:
    broker, publish_conn, consume_conn = await _connected()

    await broker.close()

    consume_conn.close.assert_awaited_once()
    publish_conn.close.assert_awaited_once()
    assert broker._consume_connection is None
    assert broker._connection is None


async def test_failing_consume_connection_raises_broker_connection_error() -> None:
    broker = RabbitMQBroker(_URL)
    with patch(_CONNECT, AsyncMock(side_effect=[_connection(), ConnectionError("refused")])):
        await broker.connect()
        with pytest.raises(BrokerConnectionError, match="consume connection"):
            await broker.consume("q1", AsyncMock(), concurrency=4)


def _counting_connection(count: int) -> Mock:
    conn = _connection()
    queue = SimpleNamespace(declaration_result=SimpleNamespace(message_count=count))
    conn.channel.return_value.declare_queue = AsyncMock(return_value=queue)
    return conn


async def test_admin_reads_go_over_the_open_consume_connection() -> None:
    # Im Alarm blockiert die Publish-Verbindung jeden Frame, auch ein
    # passives Declare; die Consume-Verbindung bleibt lesbar.
    broker = RabbitMQBroker(_URL)
    broker._connection = _counting_connection(1)
    broker._consume_connection = _counting_connection(5)

    assert await broker.dead_letter_depth("tasks") == 5
    assert await broker.ready_depth("tasks") == 5
    broker._connection.channel.assert_not_awaited()


async def test_admin_reads_fall_back_to_the_publish_connection() -> None:
    broker = RabbitMQBroker(_URL)
    broker._connection = _counting_connection(1)
    assert await broker.dead_letter_depth("tasks") == 1

    broker._consume_connection = _counting_connection(5)
    broker._consume_connection.is_closed = True
    assert await broker.ready_depth("tasks") == 1
