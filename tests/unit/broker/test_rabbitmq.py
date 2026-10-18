"""Unit tests for RabbitMQBroker."""

import asyncio
import json
import logging
from types import SimpleNamespace
from typing import override
from unittest.mock import AsyncMock, Mock, patch

import aio_pika
import aiormq.exceptions
import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import BrokerConnectionError, ConfigurationError
from tests.support.rabbitmq_faults import hanging_publish


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

        with patch(
            "swarmq.broker.rabbitmq.connection.aio_pika.connect_robust", return_value=mock_connection
        ) as mock_connect:
            await broker.connect()

        mock_connect.assert_awaited_once()
        assert mock_connect.await_args is not None
        assert mock_connect.await_args.args == ("amqp://guest:guest@localhost/",)
        # BZ-16 (MR 16): der Consume-Kanal entsteht erst mit ``consume()`` auf
        # einer eigenen Verbindung.
        assert mock_connection.channel.await_count == 1
        assert broker._publish_channel is mock_publish_channel
        assert broker._consume_channel is None

    async def test_shared_mode_opens_both_channels_on_one_connection(self):
        broker = RabbitMQBroker("amqp://guest:guest@localhost/")
        broker.publish_connection = "shared"
        mock_connection = AsyncMock()
        mock_publish_channel = AsyncMock()
        mock_consume_channel = AsyncMock()
        mock_connection.channel.side_effect = [mock_publish_channel, mock_consume_channel]

        with patch(
            "swarmq.broker.rabbitmq.connection.aio_pika.connect_robust", return_value=mock_connection
        ) as mock_connect:
            await broker.connect()

        mock_connect.assert_awaited_once()
        assert mock_connect.await_args is not None
        assert mock_connect.await_args.args == ("amqp://guest:guest@localhost/",)
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

    async def test_pool_acquire_exhausted_raises_instead_of_hanging(
        self,
        monkeypatch,
    ):
        """Härtung R2 P1-3: ``pool.acquire().__aenter__()`` blockiert
        bei erschöpftem (nicht-overflow) Pool unbegrenzt auf
        ``Queue.get()`` — ``async with SwarmQ(...)`` hing dann still.
        connect() muss den Acquire mit Timeout binden und als
        BrokerConnectionError surfacen."""
        import asyncio

        monkeypatch.setattr(
            "swarmq.broker.rabbitmq.connection._POOL_ACQUIRE_TIMEOUT_S",
            0.1,
        )

        async def _never_returns():
            await asyncio.sleep(60)

        acquire_cm = AsyncMock()
        acquire_cm.__aenter__ = Mock(side_effect=_never_returns)
        mock_pool = Mock()
        mock_pool.acquire = Mock(return_value=acquire_cm)
        broker = RabbitMQBroker(connection_pool=mock_pool)

        with pytest.raises(BrokerConnectionError, match="pool"):
            await asyncio.wait_for(broker.connect(), timeout=5)

        # Nothing was acquired — no checkout to leak, state reset.
        assert broker._pool_ctx is None
        assert broker._connection is None

    async def test_injected_plain_connection_logs_warning(self):
        """Härtung R2 P2: eine plain (nicht-robuste) injizierte
        Connection hat kein Self-Healing — nach einem Drop läuft jeder
        Publish in die volle 10s-``ready()``-Timeout-Wand. Der Docstring-
        Caveat allein ist zu leise; connect() warnt explizit."""
        mock_conn = AsyncMock(spec=aio_pika.abc.AbstractConnection)
        mock_conn.channel = AsyncMock(return_value=AsyncMock())
        broker = RabbitMQBroker(connection=mock_conn)

        with patch("swarmq.broker.rabbitmq.connection.logger") as mock_logger:
            await broker.connect()

        assert mock_logger.warning.called
        event = mock_logger.warning.call_args.args[0]
        assert "robust" in event

    async def test_injected_robust_connection_no_warning(self):
        mock_conn = AsyncMock(spec=aio_pika.abc.AbstractRobustConnection)
        mock_conn.channel = AsyncMock(return_value=AsyncMock())
        broker = RabbitMQBroker(connection=mock_conn)

        with patch("swarmq.broker.rabbitmq.connection.logger") as mock_logger:
            await broker.connect()

        mock_logger.warning.assert_not_called()

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
    @pytest.fixture
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
        assert len(calls) == 10  # Familie plus sieben Delay-Stufen (Review MR 10, P1-2)

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

        # Zustellbudget (B5): ab Werk 20, nur auf der Hauptqueue.
        assert main_args["x-delivery-limit"] == 20

    async def test_declare_queue_uses_configured_delivery_limit(self, connected_broker):
        # B5: der konfigurierte Wert landet im x-delivery-limit der
        # Hauptqueue. Delay- und Dead-Letter-Queue bekommen ihn nicht --
        # dort waere ein Zustellbudget sinnlos bis schaedlich.
        broker = connected_broker
        broker.delivery_limit = 7

        await broker.declare_queue("tasks")

        calls = broker._publish_channel.declare_queue.call_args_list
        assert calls[0].kwargs["arguments"]["x-delivery-limit"] == 7
        assert "x-delivery-limit" not in calls[1].kwargs["arguments"]
        assert "x-delivery-limit" not in calls[2].kwargs["arguments"]

    async def test_classic_queues_get_no_delivery_limit(self, connected_broker):
        # Klassische Queues kennen das Argument nicht -- es zu setzen waere
        # ein Deklarationsfehler beim Broker.
        broker = connected_broker
        broker._queue_type = "classic"
        broker.delivery_limit = 7

        await broker.declare_queue("tasks")

        for call in broker._publish_channel.declare_queue.call_args_list:
            assert "x-delivery-limit" not in call.kwargs["arguments"]

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
        assert len(calls) == 10  # Familie plus sieben Delay-Stufen (Review MR 10, P1-2)
        for c in calls:
            assert c.kwargs["arguments"]["x-queue-type"] == "classic"


class TestRabbitMQBrokerGetQueueDepth:
    """R4.7 Autoscaling — passive-declare for queue depth."""

    async def test_sums_main_delay_queue_and_delay_tiers_over_one_channel(self):
        # Plan 0.12.0, MR 10 (BZ-01): "pending" bleibt Haupt-Queue plus alle
        # Delay-Queues, jetzt mit den Delay-Stufen; abgefragt ueber einen
        # Channel, der nur nach NOT_FOUND neu geoeffnet wird.
        from swarmq.queue_names import delay_tiers

        broker = RabbitMQBroker("amqp://guest:guest@localhost/")
        conn = AsyncMock(is_closed=False)
        broker._connection = conn
        chan = AsyncMock(is_closed=False)
        conn.channel = AsyncMock(return_value=chan)
        counts = {"tasks": 5, "tasks.DQ": 3, "tasks.DQ.1s": 2, "tasks.DQ.1h": 1}

        async def declare_queue(name, *, passive):
            q = AsyncMock()
            q.declaration_result = type("R", (), {"message_count": counts.get(name, 0)})()
            return q

        chan.declare_queue = AsyncMock(side_effect=declare_queue)

        depth = await broker.queue_depth("tasks")
        assert depth == 11
        assert conn.channel.await_count == 1
        assert [c.args[0] for c in chan.declare_queue.await_args_list] == [
            "tasks",
            "tasks.DQ",
            *delay_tiers("tasks"),
        ]
        assert all(c.kwargs == {"passive": True} for c in chan.declare_queue.await_args_list)
        chan.close.assert_awaited()

    async def test_missing_queue_returns_zero(self):
        # Fehlende Queues (auch nie benutzte Delay-Stufen) zaehlen 0; nach
        # NOT_FOUND hat der Broker den Channel geschlossen, der naechste
        # Name bekommt einen neuen.
        import aio_pika.exceptions

        broker = RabbitMQBroker("amqp://guest:guest@localhost/")
        conn = AsyncMock(is_closed=False)
        broker._connection = conn
        opened = []

        async def open_channel():
            chan = AsyncMock(is_closed=False)
            chan.declare_queue = AsyncMock(
                side_effect=aio_pika.exceptions.ChannelNotFoundEntity("NOT_FOUND"),
            )
            opened.append(chan)
            return chan

        conn.channel = AsyncMock(side_effect=open_channel)

        depth = await broker.queue_depth("tasks")
        assert depth == 0
        assert len(opened) == 9

    async def test_every_channel_is_closed_after_not_found_even_when_the_broker_closed_it(self):
        # Review MR 10, P0-1: nach NOT_FOUND ist ``is_closed`` schon True (der
        # Broker hat den Channel geschlossen). Ohne eigenes ``close`` oeffnet
        # ein RobustChannel sich wieder und bleibt offen: ein Channel-Leck je
        # fehlender Stufe und Metrik-Takt.
        import aio_pika.exceptions

        broker = RabbitMQBroker("amqp://guest:guest@localhost/")
        conn = AsyncMock(is_closed=False)
        broker._connection = conn
        opened = []

        async def open_channel():
            chan = AsyncMock(is_closed=False)

            async def declare_queue(name, *, passive):
                chan.is_closed = True
                raise aio_pika.exceptions.ChannelNotFoundEntity("NOT_FOUND")

            chan.declare_queue = AsyncMock(side_effect=declare_queue)
            opened.append(chan)
            return chan

        conn.channel = AsyncMock(side_effect=open_channel)

        assert await broker.queue_depth("tasks") == 0

        assert len(opened) == 9
        for chan in opened:
            chan.close.assert_awaited_once()

    async def test_raises_when_not_connected(self):
        broker = RabbitMQBroker("amqp://guest:guest@localhost/")
        broker._connection = None
        with pytest.raises(BrokerConnectionError, match="not connected"):
            await broker.queue_depth("tasks")

    async def test_wraps_transport_errors(self):
        broker = RabbitMQBroker("amqp://guest:guest@localhost/")
        conn = AsyncMock(is_closed=False)
        broker._connection = conn
        chan = AsyncMock(is_closed=False)
        conn.channel = AsyncMock(return_value=chan)
        chan.declare_queue = AsyncMock(side_effect=ConnectionError("gone"))

        with pytest.raises(BrokerConnectionError):
            await broker.queue_depth("tasks")


class TestRabbitMQBrokerGetDlqDepth:
    """2026-06-02 audit P1-3 — passive-declare of the .XQ dead-letter queue."""

    async def test_returns_xq_message_count(self):
        broker = RabbitMQBroker("amqp://guest:guest@localhost/")
        conn = AsyncMock(is_closed=False)
        broker._connection = conn
        chan = AsyncMock(is_closed=False)
        conn.channel = AsyncMock(return_value=chan)
        q_xq = AsyncMock()
        q_xq.declaration_result = type("R", (), {"message_count": 7})()
        chan.declare_queue = AsyncMock(return_value=q_xq)

        depth = await broker.dead_letter_depth("tasks")
        assert depth == 7
        chan.declare_queue.assert_awaited_once_with("tasks.XQ", passive=True)
        chan.close.assert_awaited()

    async def test_missing_dlq_returns_zero(self):
        import aio_pika.exceptions

        broker = RabbitMQBroker("amqp://guest:guest@localhost/")
        conn = AsyncMock(is_closed=False)
        broker._connection = conn
        chan = AsyncMock(is_closed=True)
        conn.channel = AsyncMock(return_value=chan)
        chan.declare_queue = AsyncMock(
            side_effect=aio_pika.exceptions.ChannelNotFoundEntity("NOT_FOUND"),
        )

        assert await broker.dead_letter_depth("tasks") == 0

    async def test_missing_dlq_closes_the_channel_the_broker_closed(self):
        # Review MR 10, P0-1 (gleiche Fehlerklasse wie ``queue_depth``): der
        # Broker schliesst den Channel nach NOT_FOUND; ohne eigenes ``close``
        # oeffnet ein RobustChannel sich wieder.
        import aio_pika.exceptions

        broker = RabbitMQBroker("amqp://guest:guest@localhost/")
        conn = AsyncMock(is_closed=False)
        broker._connection = conn
        chan = AsyncMock(is_closed=False)
        conn.channel = AsyncMock(return_value=chan)

        async def declare_queue(name, *, passive):
            chan.is_closed = True
            raise aio_pika.exceptions.ChannelNotFoundEntity("NOT_FOUND")

        chan.declare_queue = AsyncMock(side_effect=declare_queue)

        assert await broker.dead_letter_depth("tasks") == 0

        chan.close.assert_awaited_once()


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

        await broker.consume("tasks", callback, concurrency=10)

        broker._consume_channel.set_qos.assert_awaited_once_with(prefetch_count=10)
        broker._consume_channel.get_queue.assert_awaited_once_with("tasks")
        # U5: der Broker registriert einen Wrapper, der aio_pika-Nachrichten
        # in ``Delivery`` übersetzt — nicht mehr den rohen Callback.
        mock_queue.consume.assert_awaited_once()
        assert mock_queue.consume.await_args.args[0] is not callback


class TestRabbitMQBrokerAckNack:
    async def test_ack_delegates_to_message(self):
        broker = RabbitMQBroker("amqp://guest:guest@localhost/")
        mock_message = AsyncMock()

        await broker.ack(mock_message)

        mock_message.ack.assert_awaited_once()

    async def test_route_via_dlx_delegates_to_message_nack_no_requeue(self):
        broker = RabbitMQBroker("amqp://guest:guest@localhost/")
        mock_message = AsyncMock()

        await broker.route_via_dlx(mock_message)

        mock_message.nack.assert_awaited_once_with(requeue=False)

    async def test_redeliver_delegates_to_message_nack_requeue(self):
        broker = RabbitMQBroker("amqp://guest:guest@localhost/")
        mock_message = AsyncMock()

        await broker.redeliver(mock_message)

        mock_message.nack.assert_awaited_once_with(requeue=True)


class TestSettleOnLostChannel:
    """B22: Abschluss-Verben auf einem verlorenen Kanal.

    Test-Liste:
    - [x] ``ack`` auf geschlossenem Kanal (``ChannelInvalidStateError``) →
          ``BrokerConnectionError``, Ursache als ``__cause__``.
    - [x] ``redeliver`` ebenso (``ConnectionClosed``).
    - [x] ``route_via_dlx`` ebenso (``ChannelClosed``).
    - [x] Abbruch der eigenen Task: ``CancelledError`` geht unveraendert durch.
    - [x] aio_pikas interne ``CancelledError`` (eigene Task nicht abgebrochen)
          → ``BrokerConnectionError``.
    """

    async def test_ack_on_closed_channel_raises_broker_connection_error(self):
        broker = RabbitMQBroker("amqp://guest:guest@localhost/")
        cause = aiormq.exceptions.ChannelInvalidStateError("channel closed")
        mock_message = AsyncMock()
        mock_message.ack.side_effect = cause

        with pytest.raises(BrokerConnectionError) as info:
            await broker.ack(mock_message)

        assert info.value.__cause__ is cause

    async def test_redeliver_on_lost_connection_raises_broker_connection_error(self):
        broker = RabbitMQBroker("amqp://guest:guest@localhost/")
        cause = aiormq.exceptions.ConnectionClosed(320, "CONNECTION_FORCED")
        mock_message = AsyncMock()
        mock_message.nack.side_effect = cause

        with pytest.raises(BrokerConnectionError) as info:
            await broker.redeliver(mock_message)

        assert info.value.__cause__ is cause

    async def test_route_via_dlx_on_closed_channel_raises_broker_connection_error(self):
        broker = RabbitMQBroker("amqp://guest:guest@localhost/")
        cause = aiormq.exceptions.ChannelClosed(406, "PRECONDITION_FAILED")
        mock_message = AsyncMock()
        mock_message.nack.side_effect = cause

        with pytest.raises(BrokerConnectionError) as info:
            await broker.route_via_dlx(mock_message)

        assert info.value.__cause__ is cause

    @pytest.mark.timeout(5)
    async def test_cancelling_the_own_task_passes_cancelled_error_through(self):
        broker = RabbitMQBroker("amqp://guest:guest@localhost/")
        started = asyncio.Event()

        async def _hanging_ack() -> None:
            started.set()
            await asyncio.Event().wait()

        mock_message = AsyncMock()
        mock_message.ack.side_effect = _hanging_ack
        task = asyncio.create_task(broker.ack(mock_message))
        await started.wait()

        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

    async def test_internal_cancelled_error_of_aio_pika_counts_as_lost_channel(self):
        """aio_pikas ``drain_future`` endet bei geschlossenem Transport als
        ``CancelledError``, ohne dass die eigene Task abgebrochen wurde."""
        broker = RabbitMQBroker("amqp://guest:guest@localhost/")
        mock_message = AsyncMock()
        mock_message.ack.side_effect = asyncio.CancelledError()

        with pytest.raises(BrokerConnectionError):
            await broker.ack(mock_message)


class TestRabbitMQBrokerConnectGuard:
    async def test_connect_twice_only_calls_connect_robust_once(self):
        broker = RabbitMQBroker("amqp://guest:guest@localhost/")
        mock_connection = AsyncMock()
        mock_connection.channel.side_effect = [AsyncMock(), AsyncMock()]

        with patch(
            "swarmq.broker.rabbitmq.connection.aio_pika.connect_robust", return_value=mock_connection
        ) as mock_connect:
            await broker.connect()
            await broker.connect()

        mock_connect.assert_awaited_once()


class TestRabbitMQBrokerConnectError:
    async def test_connect_wraps_connection_error_in_broker_error(self):
        broker = RabbitMQBroker("amqp://guest:guest@localhost/")

        with (
            patch(
                "swarmq.broker.rabbitmq.connection.aio_pika.connect_robust",
                side_effect=ConnectionError("refused"),
            ),
            pytest.raises(BrokerConnectionError),
        ):
            await broker.connect()


class TestRabbitMQBrokerErrorSanitization:
    """The broker URL carries a password; channel-reopen errors must not
    leak it into BrokerConnectionError messages / logs (2026-06-02 audit,
    P1-6). The project already sanitizes URLs elsewhere via sanitize_url."""

    async def test_channel_reopen_error_redacts_password(self):
        broker = RabbitMQBroker("amqp://user:supersecret@rabbit.host:5672/vh")
        broker._publish_channel = None
        conn = AsyncMock()
        conn.ready = AsyncMock(return_value=None)
        conn.channel = AsyncMock(side_effect=RuntimeError("channel boom"))
        broker._connection = conn

        with pytest.raises(BrokerConnectionError) as exc_info:
            _ = await broker._ensure_publish_channel()

        message = str(exc_info.value)
        assert "supersecret" not in message
        assert "***" in message

    async def test_connection_not_ready_error_redacts_password(self):
        broker = RabbitMQBroker("amqp://user:supersecret@rabbit.host:5672/vh")
        broker._publish_channel = None
        conn = AsyncMock()
        # ready() never resolves → wait_for raises TimeoutError.
        conn.ready = AsyncMock(side_effect=TimeoutError())
        broker._connection = conn

        with pytest.raises(BrokerConnectionError) as exc_info:
            _ = await broker._ensure_publish_channel()

        message = str(exc_info.value)
        assert "supersecret" not in message
        assert "***" in message


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

        await broker._publish_to_delay_queue(
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

        await broker._publish_to_delay_queue("tasks", b"body", delay_ms=1000)

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

        await broker._publish_to_dead_letter_queue(
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

        await broker._publish_to_dead_letter_queue("tasks", b"body")

        publish_call = mock_exchange.publish.call_args
        publish_call.args[0]
        assert publish_call.kwargs["routing_key"] == "tasks.XQ"


class TestDeadLetterTtl:
    """Plan 2026-10-08-1541, U9 (KTD6): Rest-TTL pro Nachricht in der DLQ.

    Test-Liste:
    1. DLQ-Publish traegt standardmaessig die kodierte Property
       ``expiration == "604800000"`` (7 Tage).
    2. DLQ-Publish traegt ``x-swarmq-dead-lettered-at`` (Epoch-Millisekunden).
    3. ``dead_letter_ttl_s=None`` -> keine ``expiration``.
    4. ``dlq list``/``inspect`` legen mit der Rest-TTL zurueck (Frist aus
       ``x-swarmq-dead-letter-ttl-ms``, siehe ``test_dead_letter_ttl_header.py``).
    5. Abgelaufene Nachricht beim Lesen -> verworfen.
    6. Altbestand ohne Header -> volle TTL ab jetzt, Header wird gesetzt.
    7. ``dlq retry`` -> Hauptqueue ohne ``expiration``.
    8. ``SwarmQ`` reicht ``dead_letter_ttl_s`` an den Broker durch.
    """

    @staticmethod
    def _broker():
        broker = RabbitMQBroker("amqp://guest:guest@localhost/")
        broker._publish_channel = AsyncMock(is_closed=False)
        broker._connection = AsyncMock()
        return broker

    async def test_dead_letter_publish_carries_seven_day_expiration(self):
        broker = self._broker()
        await broker._publish_to_dead_letter_queue("tasks", b"body")
        message = broker._publish_channel.default_exchange.publish.call_args.args[0]
        assert message.properties.expiration == "604800000"

    async def test_dead_letter_publish_stamps_dead_lettered_at_in_epoch_ms(self):
        broker = self._broker()
        broker.clock = lambda: 1_760_000_000.123
        await broker._publish_to_dead_letter_queue("tasks", b"body", headers={"x-swarmq-traceback": "boom"})
        message = broker._publish_channel.default_exchange.publish.call_args.args[0]
        assert message.headers["x-swarmq-dead-lettered-at"] == 1_760_000_000_123
        assert message.headers["x-swarmq-traceback"] == "boom"

    async def test_dead_letter_publish_without_ttl_has_no_expiration(self):
        broker = self._broker()
        broker.dead_letter_ttl_s = None
        await broker._publish_to_dead_letter_queue("tasks", b"body")
        message = broker._publish_channel.default_exchange.publish.call_args.args[0]
        assert message.properties.expiration is None
        assert "x-swarmq-dead-lettered-at" in message.headers

    _NOW = 1_760_000_000.0

    def _dlq_broker(self, *messages, ttl=7200.0):
        """Broker mit einer DLQ aus ``messages`` (Kopf zuerst); die
        Republishes landen in ``broker.published`` als (routing_key, msg)."""
        broker = self._broker()
        broker.dead_letter_ttl_s = ttl
        broker.clock = lambda: self._NOW
        queue = Mock()
        queue.get = AsyncMock(side_effect=[*messages, None])
        broker._dlq_queue = AsyncMock(return_value=queue)
        broker.published = []

        async def publish(routing_key, msg):
            broker.published.append((routing_key, msg))

        broker._publish_with_retry = publish
        return broker

    @staticmethod
    def _incoming(task_id="t-1", headers=None):
        body = json.dumps({"task": {"id": task_id, "name": "n"}}).encode()
        return Mock(body=body, headers=dict(headers or {}))

    async def test_list_puts_message_back_with_remaining_ttl(self):
        dead_lettered_at = int((self._NOW - 3600) * 1000)
        broker = self._dlq_broker(
            self._incoming(
                headers={
                    "x-swarmq-dead-lettered-at": dead_lettered_at,
                    "x-swarmq-dead-letter-ttl-ms": 7_200_000,
                }
            )
        )
        await broker.list_dead_letters("tasks")
        [(routing_key, msg)] = broker.published
        assert routing_key == "tasks.XQ"
        assert msg.properties.expiration == "3600000"
        assert msg.headers["x-swarmq-dead-lettered-at"] == dead_lettered_at

    async def test_legacy_message_without_header_gets_full_ttl_from_now(self):
        """Altbestand und DLX-Wege tragen keinen Zeitstempel: volle Frist ab
        jetzt, und der gesetzte Header haelt sie beim naechsten Lesen fest."""
        broker = self._dlq_broker(self._incoming(headers={"x-death": []}))
        await broker.inspect_dead_letter("tasks", "t-1")
        [(_, msg)] = broker.published
        assert msg.properties.expiration == "7200000"
        assert msg.headers["x-swarmq-dead-lettered-at"] == int(self._NOW * 1000)
        assert msg.headers["x-death"] == []

    async def test_expired_message_is_dropped_when_read(self):
        """Eine abgelaufene Nachricht mitten in der Queue verwirft RabbitMQ
        erst am Kopf; beim Lesen faellt sie weg, statt neu zu leben."""
        ttl = {"x-swarmq-dead-letter-ttl-ms": 7_200_000}
        expired = self._incoming("t-old", {"x-swarmq-dead-lettered-at": int((self._NOW - 7201) * 1000), **ttl})
        fresh = self._incoming("t-new", {"x-swarmq-dead-lettered-at": int((self._NOW - 1) * 1000), **ttl})
        broker = self._dlq_broker(expired, fresh)
        entries, _scanned = await broker.list_dead_letters("tasks")
        assert [e["task_id"] for e in entries] == ["t-new"]
        assert [json.loads(msg.body)["task"]["id"] for _, msg in broker.published] == ["t-new"]

    async def test_retry_forwards_to_main_queue_without_expiration(self):
        """Die wiederholte Nachricht ist keine DLQ-Nachricht mehr: keine Frist,
        kein Zeitstempel (ein erneutes Dead-Lettering stempelt neu)."""
        stamped = self._incoming(headers={"x-swarmq-dead-lettered-at": int((self._NOW - 60) * 1000)})
        broker = self._dlq_broker(stamped)
        found, _ = await broker.retry_dead_letter("tasks", "t-1")
        [(routing_key, msg)] = broker.published
        assert found is True
        assert routing_key == "tasks"
        assert msg.properties.expiration is None
        assert "x-swarmq-dead-lettered-at" not in msg.headers

    @pytest.mark.parametrize("bad", ["garbage", b"1760000000000", None, True])
    async def test_invalid_timestamp_counts_as_missing(self, bad):
        """Ein unlesbarer Zeitstempel darf die Nachricht nicht verlieren: sie
        ist per ``no_ack`` schon aus der Queue und muss zurueck."""
        broker = self._dlq_broker(self._incoming(headers={"x-swarmq-dead-lettered-at": bad}))
        await broker.list_dead_letters("tasks")
        [(_, msg)] = broker.published
        assert msg.properties.expiration == "7200000"
        assert msg.headers["x-swarmq-dead-lettered-at"] == int(self._NOW * 1000)


# ---------------------------------------------------------------------------
# Logging (Plan 2026-10-06-0831, U9: R2, R17)
# ---------------------------------------------------------------------------
# Test list:
# 1. Scheitert das Abbestellen eines Consumers, loggt ``consumer_cancel_failed``
#    auf WARNING ueber ``swarmq.broker.rabbitmq.delivery`` mit ``consumer_tag``,
#    ``error_type`` und ``error``.
# 2. Scheitert das entkoppelte Schliessen eines Publish-Kanals, loggt
#    ``publish_channel_close_incomplete`` auf WARNING ueber
#    ``swarmq.broker.rabbitmq.connection`` mit ``error_type`` und ``error``.
# 3. Läuft das Schließen im Ressourcenalarm (Unblocked-Event nicht gesetzt) in
#    die Frist, loggt ``publish_channel_close_incomplete`` WARNING ohne
#    Traceback und mit ``blocked=True`` (PF-19 b, Nachtrag E-025); außerhalb
#    des Alarms (Event gesetzt) bleibt der Traceback, ohne ``blocked``.


class TestBrokerLogging:
    @pytest.mark.timeout(5)
    async def test_failed_consumer_cancel_logs_error_fields(self, swarmq_logs):
        broker = RabbitMQBroker("amqp://guest:guest@localhost/")
        broker._consume_channel = AsyncMock(is_closed=False)
        broker._connection = AsyncMock()
        queue = AsyncMock()
        queue.consume.return_value = "tag-1"
        queue.cancel.side_effect = RuntimeError("channel gone")
        broker._consume_channel.get_queue.return_value = queue
        await broker.consume("q1", AsyncMock(), concurrency=1)

        with pytest.raises(BrokerConnectionError):
            await broker.cancel_consume()

        [entry] = [
            e
            for e in swarmq_logs
            if e["event"] == "consumer_cancel_failed" and e["logger"] == "swarmq.broker.rabbitmq.delivery"
        ]
        assert entry["log_level"] == "warning"
        assert entry["consumer_tag"] == "tag-1"
        assert entry["error_type"] == "RuntimeError"
        assert entry["error"] == "channel gone"

    @pytest.mark.timeout(5)
    async def test_failed_detached_channel_close_logs_error_fields(self, swarmq_logs):
        broker = RabbitMQBroker("amqp://guest:guest@localhost/")
        broker._connection = AsyncMock()
        broker.publish_timeout_s = 0.05
        with hanging_publish(broker) as channel:
            channel.close = AsyncMock(side_effect=RuntimeError("close failed"))  # type: ignore[method-assign]
            with pytest.raises(BrokerConnectionError):
                await broker.publish("tasks", b"{}")
        await broker.close()  # sammelt den nebenlaeufigen Schliess-Task ein

        [entry] = [
            e
            for e in swarmq_logs
            if e["event"] == "publish_channel_close_incomplete" and e["logger"] == "swarmq.broker.rabbitmq.connection"
        ]
        assert entry["log_level"] == "warning"
        assert entry["error_type"] == "RuntimeError"
        assert entry["error"] == "close failed"


_UNBLOCKED_ATTR = "_Connection__connection_unblocked"


class _RecordCollector(logging.Handler):
    """Sammelt die rohen LogRecords, um ``exc_info`` prüfen zu können."""

    def __init__(self) -> None:
        super().__init__(logging.DEBUG)
        self.records: list[logging.LogRecord] = []

    @override
    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


async def _close_record_after_timeout(*, alarm: bool) -> logging.LogRecord:
    """Publish läuft in die Frist, das Schließen des verworfenen Kanals
    ebenfalls; ``alarm`` sagt, ob die Verbindung dabei blockiert ist."""
    broker = RabbitMQBroker("amqp://guest:guest@localhost/")
    connection = AsyncMock()
    broker._connection = connection
    broker.publish_timeout_s = 0.05
    collector = _RecordCollector()
    logger = logging.getLogger("swarmq.broker.rabbitmq.connection")
    logger.addHandler(collector)
    try:
        with hanging_publish(broker) as channel:

            async def close_into_the_deadline() -> None:
                # Der Zustand wechselt erst, nachdem der Publish auf dem Socket
                # lag; blockiert liest der Broker die Verbindung nicht mehr.
                unblocked = asyncio.Event()
                if not alarm:
                    unblocked.set()
                connection.transport.connection = SimpleNamespace(**{_UNBLOCKED_ATTR: unblocked})
                raise TimeoutError

            channel.close = close_into_the_deadline  # type: ignore[method-assign]  # Test ersetzt close durch einen Fake
            with pytest.raises(BrokerConnectionError):
                await broker.publish("tasks", b"{}")
        await broker.close()  # sammelt den nebenläufigen Schließ-Task ein
    finally:
        logger.removeHandler(collector)
    [record] = [r for r in collector.records if r.msg == "publish_channel_close_incomplete"]
    return record


class TestChannelCloseDuringAlarm:
    @pytest.mark.timeout(5)
    @pytest.mark.entscheidung("E-025")
    async def test_close_into_the_deadline_during_an_alarm_logs_without_traceback(self, swarmq_logs):
        record = await _close_record_after_timeout(alarm=True)

        assert record.levelno == logging.WARNING
        assert record.exc_info is None
        assert record.blocked is True  # type: ignore[attr-defined]  # Feld als LogRecord-Attribut
        assert record.error_type == "TimeoutError"  # type: ignore[attr-defined]  # Feld als LogRecord-Attribut

    @pytest.mark.timeout(5)
    @pytest.mark.entscheidung("E-025")
    async def test_close_into_the_deadline_outside_an_alarm_keeps_the_traceback(self, swarmq_logs):
        record = await _close_record_after_timeout(alarm=False)

        assert record.levelno == logging.WARNING
        assert record.exc_info is not None
        assert not hasattr(record, "blocked")
