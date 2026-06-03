"""Unit tests for RabbitMQBroker."""

from unittest.mock import AsyncMock, Mock, call, patch

import aio_pika
import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import BrokerConnectionError, ConfigurationError


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


class TestRabbitMQBrokerInjection:
    """Producer-path injection: borrow an existing connection or pool.
    swarmq opens its own publish channel but never closes the borrowed
    connection — only the channels it created itself."""

    def test_url_path_owns_connection(self):
        broker = RabbitMQBroker("amqp://guest:guest@localhost/")
        assert broker._owns_connection is True

    def test_injected_connection_stored_and_not_owned(self):
        mock_conn = AsyncMock()
        broker = RabbitMQBroker(connection=mock_conn)

        assert broker._connection is mock_conn
        assert broker._owns_connection is False
        # Credential-free placeholder, never None and never the AMQP URL.
        assert broker._url == "<injected>"

    def test_injected_pool_not_owned(self):
        mock_pool = Mock()
        broker = RabbitMQBroker(connection_pool=mock_pool)

        assert broker._connection_pool is mock_pool
        assert broker._owns_connection is False
        assert broker._connection is None
        assert broker._url == "<injected>"

    def test_no_source_raises_configuration_error(self):
        with pytest.raises(ConfigurationError, match="exactly one"):
            RabbitMQBroker()

    def test_url_and_connection_together_raises(self):
        with pytest.raises(ConfigurationError, match="exactly one"):
            RabbitMQBroker("amqp://localhost/", connection=AsyncMock())

    def test_queue_type_preserved_with_injection(self):
        broker = RabbitMQBroker(connection=AsyncMock(), queue_type="classic")
        assert broker._queue_type == "classic"

    async def test_connect_opens_only_publish_channel_on_injected_connection(self):
        """Producer path needs no consume channel (AE2)."""
        mock_conn = AsyncMock()
        mock_channel = AsyncMock()
        mock_conn.channel.return_value = mock_channel
        broker = RabbitMQBroker(connection=mock_conn)

        await broker.connect()

        mock_conn.channel.assert_awaited_once()
        assert broker._publish_channel is mock_channel
        assert broker._consume_channel is None

    async def test_connect_is_idempotent_on_injected_connection(self):
        mock_conn = AsyncMock()
        mock_conn.channel.return_value = AsyncMock()
        broker = RabbitMQBroker(connection=mock_conn)

        await broker.connect()
        await broker.connect()

        mock_conn.channel.assert_awaited_once()

    async def test_close_does_not_close_injected_connection(self):
        """AE2: borrowed connection survives teardown; own channel closes."""
        mock_conn = AsyncMock()
        mock_channel = AsyncMock(is_closed=False)
        broker = RabbitMQBroker(connection=mock_conn)
        broker._publish_channel = mock_channel

        await broker.close()

        mock_channel.close.assert_awaited_once()
        mock_conn.close.assert_not_awaited()

    async def test_url_path_still_closes_owned_connection(self):
        broker = RabbitMQBroker("amqp://guest:guest@localhost/")
        mock_conn = AsyncMock()
        broker._connection = mock_conn

        await broker.close()

        mock_conn.close.assert_awaited_once()
        assert broker._connection is None

    async def test_pool_connect_acquires_and_close_releases(self):
        """A borrowed pool: connect() acquires-and-holds, close() releases
        back to the pool (never closes the connection)."""
        mock_conn = AsyncMock()
        mock_channel = AsyncMock(is_closed=False)
        mock_conn.channel.return_value = mock_channel
        acquire_cm = AsyncMock()
        acquire_cm.__aenter__.return_value = mock_conn
        mock_pool = Mock()
        mock_pool.acquire = Mock(return_value=acquire_cm)
        broker = RabbitMQBroker(connection_pool=mock_pool)

        await broker.connect()

        mock_pool.acquire.assert_called_once()
        acquire_cm.__aenter__.assert_awaited_once()
        assert broker._connection is mock_conn
        assert broker._publish_channel is mock_channel

        await broker.close()

        # Released back to the pool, connection not closed.
        acquire_cm.__aexit__.assert_awaited_once()
        mock_conn.close.assert_not_awaited()
        mock_channel.close.assert_awaited_once()

    async def test_pool_connect_failure_releases_checkout(self):
        """If channel() fails after the pool acquire, the borrowed connection
        must be released — connect() runs outside SwarmQ.__aenter__'s
        try/except, so a leak here is never cleaned up by close()."""
        mock_conn = AsyncMock()
        mock_conn.channel.side_effect = RuntimeError("boom")
        acquire_cm = AsyncMock()
        acquire_cm.__aenter__.return_value = mock_conn
        mock_pool = Mock()
        mock_pool.acquire = Mock(return_value=acquire_cm)
        broker = RabbitMQBroker(connection_pool=mock_pool)

        with pytest.raises(BrokerConnectionError):
            await broker.connect()

        acquire_cm.__aexit__.assert_awaited_once()
        assert broker._pool_ctx is None
        assert broker._connection is None

    async def test_injected_connection_connect_failure_keeps_borrowed_ref(self):
        """A bare injected connection is the caller's: on connect() failure
        wrap the error but never release/close it."""
        mock_conn = AsyncMock()
        mock_conn.channel.side_effect = RuntimeError("boom")
        broker = RabbitMQBroker(connection=mock_conn)

        with pytest.raises(BrokerConnectionError):
            await broker.connect()

        assert broker._connection is mock_conn
        mock_conn.close.assert_not_awaited()

    async def test_injected_connection_reusable_after_close(self):
        """connect() -> close() -> connect() works on a bare injected
        connection: close() keeps the borrowed connection and reopens a
        publish channel on the next connect() (mirrors the owned path)."""
        mock_conn = AsyncMock()
        ch1 = AsyncMock(is_closed=False)
        ch2 = AsyncMock()
        mock_conn.channel.side_effect = [ch1, ch2]
        broker = RabbitMQBroker(connection=mock_conn)

        await broker.connect()
        assert broker._publish_channel is ch1

        await broker.close()
        ch1.close.assert_awaited_once()
        mock_conn.close.assert_not_awaited()
        assert broker._connection is mock_conn

        await broker.connect()
        assert broker._publish_channel is ch2

    async def test_pool_close_releases_even_if_channel_close_raises(self):
        """A raised channel.close() during injected close() must still
        release the borrowed pool connection (try/finally)."""
        mock_conn = AsyncMock()
        bad_channel = AsyncMock(is_closed=False)
        bad_channel.close.side_effect = RuntimeError("channel boom")
        mock_conn.channel.return_value = bad_channel
        acquire_cm = AsyncMock()
        acquire_cm.__aenter__.return_value = mock_conn
        mock_pool = Mock()
        mock_pool.acquire = Mock(return_value=acquire_cm)
        broker = RabbitMQBroker(connection_pool=mock_pool)
        await broker.connect()

        with pytest.raises(RuntimeError, match="channel boom"):
            await broker.close()

        acquire_cm.__aexit__.assert_awaited_once()
        assert broker._pool_ctx is None


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
