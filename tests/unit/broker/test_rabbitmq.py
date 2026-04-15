"""Unit tests for RabbitMQBroker."""

from unittest.mock import AsyncMock, call, patch

import aio_pika
import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import BrokerConnectionError


class TestRabbitMQBrokerInit:
    def test_stores_url_and_has_no_connection(self):
        broker = RabbitMQBroker("amqp://guest:guest@localhost/")

        assert broker._url == "amqp://guest:guest@localhost/"
        assert broker._connection is None

    def test_default_queue_type_is_quorum(self):
        broker = RabbitMQBroker("amqp://guest:guest@localhost/")
        assert broker._queue_type == "quorum"

    def test_custom_queue_type(self):
        broker = RabbitMQBroker("amqp://guest:guest@localhost/", queue_type="classic")
        assert broker._queue_type == "classic"

    def test_explicit_quorum_queue_type(self):
        broker = RabbitMQBroker("amqp://guest:guest@localhost/", queue_type="quorum")
        assert broker._queue_type == "quorum"


class TestRabbitMQBrokerConnect:
    async def test_connect_creates_connection_and_channels(self):
        broker = RabbitMQBroker("amqp://guest:guest@localhost/")
        mock_connection = AsyncMock()
        mock_publish_channel = AsyncMock()
        mock_consume_channel = AsyncMock()
        mock_connection.channel.side_effect = [mock_publish_channel, mock_consume_channel]

        with patch("swarmq.broker.rabbitmq.aio_pika.connect_robust", return_value=mock_connection) as mock_connect:
            await broker.connect()

        mock_connect.assert_awaited_once_with("amqp://guest:guest@localhost/")
        assert mock_connection.channel.await_count == 2
        assert broker._publish_channel is mock_publish_channel
        assert broker._consume_channel is mock_consume_channel


class TestRabbitMQBrokerDeclareQueue:
    @pytest.fixture()
    def connected_broker(self):
        broker = RabbitMQBroker("amqp://guest:guest@localhost/")
        broker._publish_channel = AsyncMock(is_closed=False)
        broker._connection = AsyncMock()
        broker._consume_channel = AsyncMock(is_closed=False)
        broker._connection = AsyncMock()
        broker._connection = AsyncMock()
        return broker

    async def test_declare_queue_creates_three_queues_with_correct_args(self, connected_broker):
        broker = connected_broker

        await broker.declare_queue("tasks")

        calls = broker._publish_channel.declare_queue.call_args_list
        assert len(calls) == 3

        # Main queue: tasks
        main_call = calls[0]
        assert main_call.args[0] == "tasks"
        assert main_call.kwargs["durable"] is True
        main_args = main_call.kwargs["arguments"]
        assert main_args["x-queue-type"] == "quorum"
        assert main_args["x-dead-letter-exchange"] == ""
        assert main_args["x-dead-letter-routing-key"] == "tasks.XQ"

        # Delay queue: tasks.DQ
        delay_call = calls[1]
        assert delay_call.args[0] == "tasks.DQ"
        assert delay_call.kwargs["durable"] is True
        delay_args = delay_call.kwargs["arguments"]
        assert delay_args["x-queue-type"] == "quorum"
        assert delay_args["x-dead-letter-exchange"] == ""
        assert delay_args["x-dead-letter-routing-key"] == "tasks"

        # Dead-letter queue: tasks.XQ
        dlq_call = calls[2]
        assert dlq_call.args[0] == "tasks.XQ"
        assert dlq_call.kwargs["durable"] is True
        dlq_args = dlq_call.kwargs["arguments"]
        assert dlq_args["x-queue-type"] == "quorum"

    async def test_declare_queue_wraps_amqp_channel_error(self, connected_broker):
        """ce-review REL-05 / T-02: pre-existing queue with mismatched
        args raises aio_pika AMQPChannelError → we wrap it in
        BrokerConnectionError with a fix hint."""
        import aio_pika.exceptions

        from swarmq.exceptions import BrokerConnectionError

        broker = connected_broker
        broker._publish_channel.declare_queue = AsyncMock(
            side_effect=aio_pika.exceptions.ChannelPreconditionFailed(
                "arg-mismatch",
            ),
        )
        with pytest.raises(BrokerConnectionError, match="Existing queue"):
            await broker.declare_queue("tasks")

    async def test_declare_queue_uses_classic_when_configured(self):
        broker = RabbitMQBroker("amqp://guest:guest@localhost/", queue_type="classic")
        broker._publish_channel = AsyncMock()
        broker._connection = AsyncMock()

        await broker.declare_queue("tasks")

        calls = broker._publish_channel.declare_queue.call_args_list
        assert len(calls) == 3
        for c in calls:
            assert c.kwargs["arguments"]["x-queue-type"] == "classic"


class TestRabbitMQBrokerGetQueueDepth:
    """R4.7 Autoscaling — passive-declare for queue depth."""

    async def test_sums_main_and_delay_counts(self):
        broker = RabbitMQBroker("amqp://guest:guest@localhost/")
        conn = AsyncMock(is_closed=False)
        broker._connection = conn

        # Two throwaway channels, one per queue name.
        chan_main = AsyncMock(is_closed=False)
        chan_delay = AsyncMock(is_closed=False)
        conn.channel = AsyncMock(side_effect=[chan_main, chan_delay])

        q_main = AsyncMock()
        q_main.declaration_result = type("R", (), {"message_count": 5})()
        q_delay = AsyncMock()
        q_delay.declaration_result = type("R", (), {"message_count": 3})()
        chan_main.declare_queue = AsyncMock(return_value=q_main)
        chan_delay.declare_queue = AsyncMock(return_value=q_delay)

        depth = await broker.get_queue_depth("tasks")
        assert depth == 8
        chan_main.declare_queue.assert_awaited_once_with("tasks", passive=True)
        chan_delay.declare_queue.assert_awaited_once_with(
            "tasks.DQ", passive=True,
        )
        chan_main.close.assert_awaited()
        chan_delay.close.assert_awaited()

    async def test_missing_queue_returns_zero(self):
        import aio_pika.exceptions

        broker = RabbitMQBroker("amqp://guest:guest@localhost/")
        conn = AsyncMock(is_closed=False)
        broker._connection = conn
        chan_main = AsyncMock(is_closed=True)
        chan_delay = AsyncMock(is_closed=True)
        conn.channel = AsyncMock(side_effect=[chan_main, chan_delay])
        chan_main.declare_queue = AsyncMock(
            side_effect=aio_pika.exceptions.ChannelNotFoundEntity("NOT_FOUND"),
        )
        chan_delay.declare_queue = AsyncMock(
            side_effect=aio_pika.exceptions.ChannelNotFoundEntity("NOT_FOUND"),
        )

        depth = await broker.get_queue_depth("tasks")
        assert depth == 0

    async def test_raises_when_not_connected(self):
        broker = RabbitMQBroker("amqp://guest:guest@localhost/")
        broker._connection = None
        with pytest.raises(BrokerConnectionError, match="not connected"):
            await broker.get_queue_depth("tasks")

    async def test_wraps_transport_errors(self):
        broker = RabbitMQBroker("amqp://guest:guest@localhost/")
        conn = AsyncMock(is_closed=False)
        broker._connection = conn
        chan = AsyncMock(is_closed=False)
        conn.channel = AsyncMock(return_value=chan)
        chan.declare_queue = AsyncMock(side_effect=ConnectionError("gone"))

        with pytest.raises(BrokerConnectionError):
            await broker.get_queue_depth("tasks")


class TestRabbitMQBrokerPublish:
    async def test_publish_sends_message_with_persistent_delivery(self):
        broker = RabbitMQBroker("amqp://guest:guest@localhost/")
        broker._publish_channel = AsyncMock(is_closed=False)
        broker._connection = AsyncMock()
        mock_exchange = broker._publish_channel.default_exchange

        await broker.publish("tasks", b'{"task": "data"}')

        mock_exchange.publish.assert_awaited_once()
        publish_call = mock_exchange.publish.call_args
        message = publish_call.args[0]
        assert isinstance(message, aio_pika.Message)
        assert message.body == b'{"task": "data"}'
        assert message.delivery_mode == aio_pika.DeliveryMode.PERSISTENT
        assert publish_call.kwargs["routing_key"] == "tasks"


class TestRabbitMQBrokerConsume:
    async def test_consume_sets_qos_and_starts_consuming(self):
        broker = RabbitMQBroker("amqp://guest:guest@localhost/")
        broker._consume_channel = AsyncMock(is_closed=False)
        broker._connection = AsyncMock()
        mock_queue = AsyncMock()
        broker._consume_channel.get_queue.return_value = mock_queue
        callback = AsyncMock()

        await broker.consume("tasks", callback, prefetch_count=10)

        broker._consume_channel.set_qos.assert_awaited_once_with(prefetch_count=10)
        broker._consume_channel.get_queue.assert_awaited_once_with("tasks")
        mock_queue.consume.assert_awaited_once_with(callback)


class TestRabbitMQBrokerAckNack:
    async def test_ack_delegates_to_message(self):
        broker = RabbitMQBroker("amqp://guest:guest@localhost/")
        mock_message = AsyncMock()

        await broker.ack(mock_message)

        mock_message.ack.assert_awaited_once()

    async def test_nack_delegates_to_message_with_requeue_false(self):
        broker = RabbitMQBroker("amqp://guest:guest@localhost/")
        mock_message = AsyncMock()

        await broker.nack(mock_message)

        mock_message.nack.assert_awaited_once_with(requeue=False)

    async def test_nack_delegates_to_message_with_requeue_true(self):
        broker = RabbitMQBroker("amqp://guest:guest@localhost/")
        mock_message = AsyncMock()

        await broker.nack(mock_message, requeue=True)

        mock_message.nack.assert_awaited_once_with(requeue=True)


class TestRabbitMQBrokerConnectGuard:
    async def test_connect_twice_only_calls_connect_robust_once(self):
        broker = RabbitMQBroker("amqp://guest:guest@localhost/")
        mock_connection = AsyncMock()
        mock_connection.channel.side_effect = [AsyncMock(), AsyncMock()]

        with patch("swarmq.broker.rabbitmq.aio_pika.connect_robust", return_value=mock_connection) as mock_connect:
            await broker.connect()
            await broker.connect()

        mock_connect.assert_awaited_once()


class TestRabbitMQBrokerConnectError:
    async def test_connect_wraps_connection_error_in_broker_error(self):
        broker = RabbitMQBroker("amqp://guest:guest@localhost/")

        with patch(
            "swarmq.broker.rabbitmq.aio_pika.connect_robust",
            side_effect=ConnectionError("refused"),
        ):
            with pytest.raises(BrokerConnectionError):
                await broker.connect()


class TestRabbitMQBrokerClose:
    async def test_close_closes_connection(self):
        broker = RabbitMQBroker("amqp://guest:guest@localhost/")
        mock_connection = AsyncMock()
        broker._connection = mock_connection

        await broker.close()

        mock_connection.close.assert_awaited_once()
        assert broker._connection is None

    async def test_close_is_idempotent(self):
        broker = RabbitMQBroker("amqp://guest:guest@localhost/")
        broker._connection = None

        # Should not raise when called without a connection
        await broker.close()
        await broker.close()


class TestRabbitMQBrokerPublishToDelayQueue:
    async def test_publish_to_delay_queue_sends_to_dq_with_ttl(self):
        broker = RabbitMQBroker("amqp://guest:guest@localhost/")
        broker._publish_channel = AsyncMock(is_closed=False)
        broker._connection = AsyncMock()
        mock_exchange = broker._publish_channel.default_exchange

        await broker.publish_to_delay_queue(
            "tasks", b'{"task": "data"}', delay_ms=5000, headers={"x-swarmq-retry-count": 1}
        )

        mock_exchange.publish.assert_awaited_once()
        publish_call = mock_exchange.publish.call_args
        message = publish_call.args[0]
        assert isinstance(message, aio_pika.Message)
        assert message.body == b'{"task": "data"}'
        assert message.delivery_mode == aio_pika.DeliveryMode.PERSISTENT
        assert message.expiration == 5.0
        assert message.headers["x-swarmq-retry-count"] == 1
        assert publish_call.kwargs["routing_key"] == "tasks.DQ"

    async def test_publish_to_delay_queue_without_headers(self):
        broker = RabbitMQBroker("amqp://guest:guest@localhost/")
        broker._publish_channel = AsyncMock(is_closed=False)
        broker._connection = AsyncMock()
        mock_exchange = broker._publish_channel.default_exchange

        await broker.publish_to_delay_queue("tasks", b"body", delay_ms=1000)

        publish_call = mock_exchange.publish.call_args
        message = publish_call.args[0]
        assert message.expiration == 1.0
        assert publish_call.kwargs["routing_key"] == "tasks.DQ"


class TestRabbitMQBrokerPublishToDeadLetterQueue:
    async def test_publish_to_dead_letter_queue_sends_to_xq(self):
        broker = RabbitMQBroker("amqp://guest:guest@localhost/")
        broker._publish_channel = AsyncMock(is_closed=False)
        broker._connection = AsyncMock()
        mock_exchange = broker._publish_channel.default_exchange

        await broker.publish_to_dead_letter_queue(
            "tasks", b'{"error": "data"}', headers={"x-swarmq-traceback": "boom"}
        )

        mock_exchange.publish.assert_awaited_once()
        publish_call = mock_exchange.publish.call_args
        message = publish_call.args[0]
        assert isinstance(message, aio_pika.Message)
        assert message.body == b'{"error": "data"}'
        assert message.delivery_mode == aio_pika.DeliveryMode.PERSISTENT
        assert message.headers["x-swarmq-traceback"] == "boom"
        assert publish_call.kwargs["routing_key"] == "tasks.XQ"

    async def test_publish_to_dead_letter_queue_without_headers(self):
        broker = RabbitMQBroker("amqp://guest:guest@localhost/")
        broker._publish_channel = AsyncMock(is_closed=False)
        broker._connection = AsyncMock()
        mock_exchange = broker._publish_channel.default_exchange

        await broker.publish_to_dead_letter_queue("tasks", b"body")

        publish_call = mock_exchange.publish.call_args
        message = publish_call.args[0]
        assert publish_call.kwargs["routing_key"] == "tasks.XQ"
