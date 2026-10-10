"""Delay-Stufen im RabbitMQ-Broker (Plan 0.12.0, MR 10, BZ-01).

``retry_later`` stellt eine Nachricht in die kleinste Stufe ``{queue}.DQ.<stufe>``
zurück, deren Obergrenze die Wartezeit erreicht. Eine Stufe entsteht beim
ersten Gebrauch über die Deklarations-Kette aus MR 8 (eigene Wegwerf-Channels).
Der Broker ist die Grenze: ``FakeRabbit`` aus ``test_declare_chain`` antwortet
wie RabbitMQ 4.2/4.3 (Spike S1).

Test-Liste:
- [x] unroutbare Stufe (gelöscht): einmal neu deklariert, zweiter Versuch an die Stufe
- [x] Stufe nicht deklarierbar (406/403, erkannter Konflikt): Rückfall auf ``{queue}.DQ``,
      WARNING ``delay_tier_unavailable``, kein neuer Versuch je Rückstellung
- [x] Verbindungsabriss beim Deklarieren der Stufe → ``BrokerConnectionError``, Original unbestätigt
- [x] Deklaration der Stufe hängt: gedeckelt durch die Publish-Frist (Q6) → ``BrokerConnectionError``
- [x] Stufe nur für eine selbst deklarierte Familie; sonst ``{queue}.DQ`` wie bis 0.11
- [x] über 1 h: ``{queue}.DQ``, keine Stufe
- [x] Argumente der Stufe wie ``{queue}.DQ``: ``auto`` at-least-once, ``legacy`` wie 0.11, classic ohne
- [x] eine Stufe wird je Prozess nur einmal deklariert
- [x] ``ready_depth``: nur die Haupt-Queue (Autoscaling, Abschnitt 1)
- [x] ``delay_tiers_unavailable(queue)``: Zahl der abgelehnten Stufen einer Familie (Gauge)
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from aio_pika.exceptions import PublishError
from aiormq.exceptions import ConnectionClosed
from pamqp.commands import Basic

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.contracts import Delivery
from swarmq.exceptions import BrokerConnectionError
from tests.unit.broker.test_declare_chain import FakeRabbit, make_broker

pytestmark = pytest.mark.timeout(5)


def _returned(routing_key: str) -> PublishError:
    """Ein ``basic.return`` NO_ROUTE, wie aiormq es mit ``on_return_raises`` wirft."""
    frame = Basic.Return(reply_code=312, reply_text="NO_ROUTE", exchange="", routing_key=routing_key)
    return PublishError(SimpleNamespace(delivery=frame), frame)


def _delivery(queue: str = "tasks") -> Delivery:
    return Delivery(body=b"{}", headers={}, queue=queue, handle=AsyncMock(consumer_tag=None))


def _routing_keys(broker: RabbitMQBroker) -> list[str]:
    publish = broker._publish_channel.default_exchange.publish  # type: ignore[union-attr]
    return [call.kwargs["routing_key"] for call in publish.await_args_list]


def _chains(rabbit: FakeRabbit) -> list[str]:
    """Je Deklaration über die Kette ein passives Declare."""
    return [
        call.args[0]
        for channel in rabbit.channels
        for call in channel.declare_queue.await_args_list
        if call.kwargs.get("passive")
    ]


async def test_unroutable_tier_is_declared_again_and_published_to_once_more() -> None:
    rabbit = FakeRabbit()
    broker = make_broker(rabbit)
    await broker.declare_queue("tasks")
    broker._publish_channel.default_exchange.publish.side_effect = [_returned("tasks.DQ.1s"), None]  # type: ignore[union-attr]
    delivery = _delivery()

    await broker.retry_later(delivery, delay_ms=500, headers={})

    assert _routing_keys(broker) == ["tasks.DQ.1s", "tasks.DQ.1s"]
    assert _chains(rabbit).count("tasks.DQ.1s") == 2
    delivery.handle.ack.assert_awaited_once()


async def test_tier_that_cannot_be_declared_falls_back_to_the_delay_queue_with_one_warning(swarmq_logs) -> None:
    # Eine fremde Queue gleichen Namens mit anderem Typ: erkannter Konflikt (406).
    rabbit = FakeRabbit({"tasks.DQ.4s": {"x-queue-type": "classic"}})
    broker = make_broker(rabbit)
    await broker.declare_queue("tasks")
    first, second = _delivery(), _delivery()

    await broker.retry_later(first, delay_ms=2_000, headers={})
    await broker.retry_later(second, delay_ms=3_000, headers={})

    assert _routing_keys(broker) == ["tasks.DQ", "tasks.DQ"]
    assert _chains(rabbit).count("tasks.DQ.4s") == 1
    [entry] = [e for e in swarmq_logs if e["event"] == "delay_tier_unavailable"]
    assert entry["log_level"] == "warning"
    assert entry["queue"] == "tasks" and entry["tier"] == "tasks.DQ.4s" and entry["fallback"] == "tasks.DQ"
    assert entry["error_type"] == "QueueArgumentsMismatch"
    first.handle.ack.assert_awaited_once()
    second.handle.ack.assert_awaited_once()


async def test_connection_loss_while_declaring_a_tier_leaves_the_original_unacked() -> None:
    rabbit = FakeRabbit()
    broker = make_broker(rabbit)
    await broker.declare_queue("tasks")
    broker._connection.channel = AsyncMock(side_effect=ConnectionClosed(320, "CONNECTION_FORCED"))  # type: ignore[union-attr]
    delivery = _delivery()

    with pytest.raises(BrokerConnectionError, match="tasks.DQ.1s"):
        await broker.retry_later(delivery, delay_ms=500, headers={})

    assert _routing_keys(broker) == []
    delivery.handle.ack.assert_not_awaited()


async def test_hanging_tier_declaration_is_bounded_by_the_publish_deadline() -> None:
    rabbit = FakeRabbit()
    broker = make_broker(rabbit)
    await broker.declare_queue("tasks")
    broker.publish_timeout_s = 0.05
    never = asyncio.Event()

    async def hanging_channel(**_: object) -> AsyncMock:
        channel = AsyncMock(is_closed=False)

        async def declare_queue(*_: object, **__: object) -> None:
            await never.wait()

        channel.declare_queue = AsyncMock(side_effect=declare_queue)
        return channel

    broker._connection.channel = AsyncMock(side_effect=hanging_channel)  # type: ignore[union-attr]
    delivery = _delivery()

    with pytest.raises(BrokerConnectionError, match="tasks.DQ.1s"):
        await broker.retry_later(delivery, delay_ms=500, headers={})

    delivery.handle.ack.assert_not_awaited()


async def test_family_not_declared_by_this_process_uses_the_delay_queue() -> None:
    rabbit = FakeRabbit()
    broker = make_broker(rabbit)

    await broker.retry_later(_delivery(), delay_ms=500, headers={})

    assert _routing_keys(broker) == ["tasks.DQ"]
    assert rabbit.channels == []


async def test_delay_over_one_hour_stays_in_the_delay_queue() -> None:
    rabbit = FakeRabbit()
    broker = make_broker(rabbit)
    await broker.declare_queue("tasks")

    await broker.retry_later(_delivery(), delay_ms=3_600_001, headers={})

    assert _routing_keys(broker) == ["tasks.DQ"]
    assert not [name for name in rabbit.queues if name.startswith("tasks.DQ.")]


@pytest.mark.parametrize(
    ("queue_type", "strategy", "extra"),
    [
        ("quorum", "auto", {"x-dead-letter-strategy": "at-least-once", "x-overflow": "reject-publish"}),
        ("quorum", "legacy", {}),
        ("classic", "auto", {}),
    ],
)
async def test_tier_gets_the_arguments_of_the_delay_queue(queue_type: str, strategy: str, extra: dict) -> None:
    rabbit = FakeRabbit()
    broker = make_broker(rabbit, _queue_type=queue_type, dead_letter_strategy=strategy)
    await broker.declare_queue("tasks")

    await broker.retry_later(_delivery(), delay_ms=30_000, headers={})

    assert _routing_keys(broker) == ["tasks.DQ.1m"]
    assert rabbit.queues["tasks.DQ.1m"] == rabbit.queues["tasks.DQ"]
    assert rabbit.queues["tasks.DQ.1m"] == {
        "x-queue-type": queue_type,
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": "tasks",
        **extra,
    }


async def test_a_tier_is_declared_once_per_process() -> None:
    rabbit = FakeRabbit()
    broker = make_broker(rabbit)
    await broker.declare_queue("tasks")

    for _ in range(3):
        await broker.retry_later(_delivery(), delay_ms=10_000, headers={})

    assert _routing_keys(broker) == ["tasks.DQ.15s"] * 3
    assert _chains(rabbit).count("tasks.DQ.15s") == 1


async def test_ready_depth_counts_only_the_main_queue() -> None:
    broker = RabbitMQBroker("amqp://guest:guest@localhost/")
    broker._connection = AsyncMock(is_closed=False)
    channel = AsyncMock(is_closed=False)
    broker._connection.channel = AsyncMock(return_value=channel)
    channel.declare_queue.return_value = SimpleNamespace(declaration_result=SimpleNamespace(message_count=4))

    assert await broker.ready_depth("tasks") == 4
    channel.declare_queue.assert_awaited_once_with("tasks", passive=True)


async def test_unavailable_tiers_are_counted_per_family() -> None:
    rabbit = FakeRabbit({"tasks.DQ.4s": {"x-queue-type": "classic"}})
    broker = make_broker(rabbit)
    await broker.declare_queue("tasks")
    await broker.declare_queue("other")

    await broker.retry_later(_delivery(), delay_ms=2_000, headers={})
    await broker.retry_later(_delivery(), delay_ms=500, headers={})

    assert broker.delay_tiers_unavailable("tasks") == 1
    assert broker.delay_tiers_unavailable("other") == 0
