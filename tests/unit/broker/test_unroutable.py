"""Unroutbarer Publish im RabbitMQ-Broker (Plan 0.12.0, MR 6, BZ-03).

Test-Liste:
- [x] eigene Verbindung: Publish-Channel mit ``on_return_raises`` (Nachweis BZ-03, Integration)
- [x] injizierte Verbindung: Publish-Channel mit ``on_return_raises``
- [x] neu geoeffneter Publish-Channel (nach Verlust) mit ``on_return_raises``
- [x] ``PublishError`` wird sofort ``UnroutableMessage``, ohne Wiederholung, Kanal bleibt
- [x] ``retry_later`` an die fehlende ``.DQ``: eigene Queue auf Wegwerf-Channel neu deklariert, zweiter Versuch
- [x] ``dead_letter`` an die fehlende ``.XQ``: ebenso
- [x] Neu-Deklarieren scheitert (beliebige Ausnahme) -> ``BrokerConnectionError``, Original unbestaetigt
- [x] zweiter Versuch wieder unroutbar -> ``UnroutableMessage``, nur ein Neu-Deklarieren
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from aio_pika.exceptions import PublishError
from pamqp.commands import Basic

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.contracts import Delivery
from swarmq.exceptions import BrokerConnectionError, UnroutableMessage

pytestmark = [pytest.mark.timeout(5)]


async def test_injected_connection_opens_publish_channel_raising_on_return() -> None:
    conn = AsyncMock()
    conn.channel.return_value = AsyncMock()
    broker = RabbitMQBroker(connection=conn)

    await broker.connect()

    conn.channel.assert_awaited_once_with(on_return_raises=True)


async def test_reopened_publish_channel_raises_on_return() -> None:
    broker = RabbitMQBroker("amqp://guest:guest@localhost/")
    broker._publish_channel = None
    conn = AsyncMock()
    conn.channel.return_value = AsyncMock(is_closed=False)
    broker._connection = conn

    await broker._ensure_publish_channel()

    conn.channel.assert_awaited_once_with(on_return_raises=True)


def _returned(routing_key: str) -> PublishError:
    """Ein ``basic.return`` NO_ROUTE, wie aiormq es mit ``on_return_raises`` wirft."""
    frame = Basic.Return(reply_code=312, reply_text="NO_ROUTE", exchange="", routing_key=routing_key)
    return PublishError(SimpleNamespace(delivery=frame), frame)


async def test_returned_publish_raises_unroutable_without_retry_and_keeps_channel() -> None:
    broker = RabbitMQBroker("amqp://guest:guest@localhost/")
    channel = AsyncMock(is_closed=False)
    channel.default_exchange.publish.side_effect = _returned("missing")
    broker._publish_channel = channel
    broker._connection = AsyncMock()

    with pytest.raises(UnroutableMessage, match="missing"):
        await broker.publish("missing", b"{}")

    assert channel.default_exchange.publish.await_count == 1
    assert broker._publish_channel is channel


def _worker_broker(*publish_effects: object) -> tuple[RabbitMQBroker, AsyncMock, AsyncMock]:
    """Broker mit Publish-Kanal (Ergebnisse je Versuch) und Wegwerf-Kanal."""
    broker = RabbitMQBroker("amqp://guest:guest@localhost/")
    channel = AsyncMock(is_closed=False)
    channel.default_exchange.publish.side_effect = list(publish_effects)
    broker._publish_channel = channel
    throwaway = AsyncMock(is_closed=False)
    broker._connection = AsyncMock(is_closed=False)
    broker._connection.channel.return_value = throwaway
    return broker, channel, throwaway


def _delivery(queue: str = "tasks") -> Delivery:
    handle = AsyncMock(consumer_tag=None)
    return Delivery(body=b"{}", headers={}, queue=queue, handle=handle)


def _declared_names(channel: AsyncMock) -> list[str]:
    return [call.args[0] for call in channel.declare_queue.await_args_list]


async def test_retry_later_redeclares_own_queue_on_throwaway_channel_and_retries() -> None:
    broker, channel, throwaway = _worker_broker(_returned("tasks.DQ"), None)
    delivery = _delivery()

    await broker.retry_later(delivery, delay_ms=1000, headers={})

    assert _declared_names(throwaway) == ["tasks", "tasks.DQ", "tasks.XQ"]
    channel.declare_queue.assert_not_awaited()
    throwaway.close.assert_awaited_once()
    assert channel.default_exchange.publish.await_count == 2
    delivery.handle.ack.assert_awaited_once()


async def test_dead_letter_redeclares_own_queue_and_retries() -> None:
    broker, channel, throwaway = _worker_broker(_returned("tasks.p5.XQ"), None)
    delivery = _delivery("tasks.p5")

    await broker.dead_letter(delivery, headers={})

    assert _declared_names(throwaway) == ["tasks.p5", "tasks.p5.DQ", "tasks.p5.XQ"]
    assert channel.default_exchange.publish.await_count == 2
    delivery.handle.ack.assert_awaited_once()


async def test_failed_redeclare_becomes_broker_connection_error_and_leaves_original_unacked() -> None:
    broker, channel, throwaway = _worker_broker(_returned("tasks.DQ"), None)
    throwaway.declare_queue.side_effect = RuntimeError("406 PRECONDITION_FAILED")
    delivery = _delivery()

    with pytest.raises(BrokerConnectionError) as exc_info:
        await broker.retry_later(delivery, delay_ms=1000, headers={})

    assert not isinstance(exc_info.value, UnroutableMessage)
    assert channel.default_exchange.publish.await_count == 1
    delivery.handle.ack.assert_not_awaited()


async def test_second_unroutable_publish_raises_after_a_single_redeclare() -> None:
    broker, _channel, throwaway = _worker_broker(_returned("tasks.DQ"), _returned("tasks.DQ"))
    delivery = _delivery()

    with pytest.raises(UnroutableMessage):
        await broker.retry_later(delivery, delay_ms=1000, headers={})

    assert _declared_names(throwaway) == ["tasks", "tasks.DQ", "tasks.XQ"]
    delivery.handle.ack.assert_not_awaited()
