"""``declare_queue`` übernimmt vorhandene Argumente (Patch 0.11.1).

Ein 406 schließt den Channel, auf dem deklariert wurde. Auf dem
Publish-Channel risse das parallele Publishes mit offenen Confirms ab. 0.11.1
prüft deshalb jede Deklaration zuerst auf einem Wegwerf-Channel, übernimmt dort
``x-dead-letter-strategy``/``x-overflow`` (INFO) und ``x-delivery-limit``
(WARNING) von der vorhandenen Queue und deklariert erst dann mit den
übernommenen Argumenten auf dem Publish-Channel. Dort kommt so kein 406 an.

Test-Liste:
1. Queue hat ``at-least-once`` → Publish-Channel deklariert mit übernommener Strategy.
2. Abweichendes Delivery-Limit → übernommen, WARNING ``queue_argument_adopted``.
3. Strategy/Overflow-Übernahme → INFO, nicht WARNING.
4. Nicht übernehmbarer 406 (``x-queue-type``) → ``BrokerConnectionError`` wie 0.11.0,
   Publish-Channel unberührt.
5. Dasselbe Argument widerspricht erneut → ``BrokerConnectionError`` (Obergrenze).
6. Strategy, Overflow und Limit nacheinander → 4 Versuche, dann Erfolg.
7. Wegwerf-Channels werden immer geschlossen, auch nach einem 406.
8. ``.DQ`` übernimmt ebenso.
9. Ohne eigene Verbindung (kein Wegwerf-Channel möglich) → direkt auf dem
   Publish-Channel wie 0.11.0.
10. Ein hängender Versuch endet nach ``declare_probe_timeout_s`` mit
    ``BrokerConnectionError`` (Q6), der Wegwerf-Channel wird geschlossen.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock

import pytest
from aio_pika.exceptions import ChannelPreconditionFailed

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import BrokerConnectionError

pytestmark = pytest.mark.timeout(5)


def _conflict(queue: str, arg: str, rest: str) -> ChannelPreconditionFailed:
    return ChannelPreconditionFailed(
        f"PRECONDITION_FAILED - inequivalent arg '{arg}' for queue '{queue}' in vhost '/': {rest}"
    )


def _strategy(queue: str) -> ChannelPreconditionFailed:
    return _conflict(
        queue, "x-dead-letter-strategy", "received none but current is the value 'at-least-once' of type 'longstr'"
    )


class Probes:
    """Wegwerf-Channels: je Öffnung ein neuer Mock; ``outcomes[name]`` liefert
    die Ausgänge der Deklarationen dieser Queue der Reihe nach (``None`` = ok)."""

    def __init__(self, outcomes: dict[str, list[BaseException | None]]) -> None:
        self.outcomes = {name: list(seq) for name, seq in outcomes.items()}
        self.channels: list[AsyncMock] = []
        self.declared: list[tuple[str, dict[str, Any]]] = []

    async def open(self) -> AsyncMock:
        channel = AsyncMock(is_closed=False)

        async def declare_queue(name: str, *, durable: bool, arguments: dict[str, Any]) -> None:
            self.declared.append((name, dict(arguments)))
            pending = self.outcomes.get(name) or [None]
            outcome = pending.pop(0) if self.outcomes.get(name) else None
            if outcome is not None:
                channel.is_closed = True
                raise outcome

        channel.declare_queue = AsyncMock(side_effect=declare_queue)
        self.channels.append(channel)
        return channel


def _broker(probes: Probes) -> RabbitMQBroker:
    broker = RabbitMQBroker("amqp://guest:guest@localhost/")
    broker._publish_channel = AsyncMock(is_closed=False)
    connection = AsyncMock(is_closed=False)
    connection.channel = AsyncMock(side_effect=probes.open)
    broker._connection = connection
    return broker


def _published(broker: RabbitMQBroker) -> dict[str, dict[str, Any]]:
    calls = broker._publish_channel.declare_queue.call_args_list  # type: ignore[union-attr]
    return {call.args[0]: call.kwargs["arguments"] for call in calls}


async def test_publish_channel_declares_with_adopted_strategy() -> None:
    probes = Probes({"tasks": [_strategy("tasks")]})
    broker = _broker(probes)

    await broker.declare_queue("tasks")

    assert _published(broker)["tasks"]["x-dead-letter-strategy"] == "at-least-once"


async def test_differing_delivery_limit_is_adopted_with_warning(swarmq_logs: list[dict[str, Any]]) -> None:
    limit = _conflict("tasks", "x-delivery-limit", "received '20' but current is '40'")
    broker = _broker(Probes({"tasks": [limit]}))

    await broker.declare_queue("tasks")

    assert _published(broker)["tasks"]["x-delivery-limit"] == 40
    [warning] = [e for e in swarmq_logs if e["event"] == "queue_argument_adopted"]
    assert warning["log_level"] == "warning"
    assert (warning["queue"], warning["argument"], warning["declared"], warning["existing"]) == (
        "tasks",
        "x-delivery-limit",
        20,
        40,
    )


async def test_adopted_strategy_is_logged_as_info(swarmq_logs: list[dict[str, Any]]) -> None:
    broker = _broker(Probes({"tasks": [_strategy("tasks")]}))

    await broker.declare_queue("tasks")

    [info] = [e for e in swarmq_logs if e["event"] == "queue_argument_adopted"]
    assert info["log_level"] == "info"
    assert (info["argument"], info["declared"], info["existing"]) == ("x-dead-letter-strategy", None, "at-least-once")


async def test_other_conflict_fails_as_before_without_touching_publish_channel() -> None:
    queue_type = _conflict("tasks", "x-queue-type", "received 'quorum' but current is 'classic'")
    broker = _broker(Probes({"tasks": [queue_type]}))

    with pytest.raises(BrokerConnectionError, match="Existing queue"):
        await broker.declare_queue("tasks")

    broker._publish_channel.declare_queue.assert_not_called()  # type: ignore[union-attr]


async def test_same_argument_conflicting_again_fails() -> None:
    probes = Probes({"tasks": [_strategy("tasks"), _strategy("tasks")]})
    broker = _broker(probes)

    with pytest.raises(BrokerConnectionError, match="Existing queue"):
        await broker.declare_queue("tasks")

    assert len(probes.declared) == 2


async def test_all_three_arguments_take_four_attempts() -> None:
    overflow = _conflict(
        "tasks", "x-overflow", "received none but current is the value 'reject-publish' of type 'longstr'"
    )
    limit = _conflict("tasks", "x-delivery-limit", "received none but current is the value '1000' of type 'short'")
    probes = Probes({"tasks": [_strategy("tasks"), overflow, limit]})
    broker = _broker(probes)

    await broker.declare_queue("tasks")

    assert [name for name, _ in probes.declared].count("tasks") == 4
    main = _published(broker)["tasks"]
    assert (main["x-dead-letter-strategy"], main["x-overflow"], main["x-delivery-limit"]) == (
        "at-least-once",
        "reject-publish",
        1000,
    )


async def test_throwaway_channels_are_always_closed() -> None:
    probes = Probes({"tasks": [_strategy("tasks")]})
    broker = _broker(probes)

    await broker.declare_queue("tasks")

    assert len(probes.channels) == 4  # tasks zweimal, .DQ, .XQ
    for channel in probes.channels:
        channel.close.assert_awaited_once()


async def test_delay_queue_adopts_too() -> None:
    broker = _broker(Probes({"tasks.DQ": [_strategy("tasks.DQ")]}))

    await broker.declare_queue("tasks")

    published = _published(broker)
    assert published["tasks.DQ"]["x-dead-letter-strategy"] == "at-least-once"
    assert "x-dead-letter-strategy" not in published["tasks"]


async def test_without_own_connection_declares_on_publish_channel_like_0_11_0() -> None:
    broker = _broker(Probes({}))
    broker._connection = None

    await broker.declare_queue("tasks")

    assert list(_published(broker)) == ["tasks", "tasks.DQ", "tasks.XQ"]


async def test_hanging_probe_fails_with_broker_connection_error() -> None:
    probes = Probes({})
    broker = _broker(probes)
    never = asyncio.Event()

    async def hang() -> AsyncMock:
        channel = AsyncMock(is_closed=False)

        async def declare_queue(*args: Any, **kwargs: Any) -> None:
            await never.wait()

        channel.declare_queue = AsyncMock(side_effect=declare_queue)
        probes.channels.append(channel)
        return channel

    broker._connection.channel = AsyncMock(side_effect=hang)  # type: ignore[union-attr]
    broker.declare_probe_timeout_s = 0.05

    with pytest.raises(BrokerConnectionError, match="timed out"):
        await broker.declare_queue("tasks")

    probes.channels[0].close.assert_awaited_once()
    broker._publish_channel.declare_queue.assert_not_called()  # type: ignore[union-attr]
