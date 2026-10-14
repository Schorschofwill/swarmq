"""``declare_queue`` übernimmt vorhandene Argumente (Patch 0.11.1).

Ab 0.12 (Plan MR 8) deklariert swarmq nur noch auf den Wegwerf-Channels, nach
einem passiven Declare; ``_published`` liest deshalb die dort gelungenen
Deklarationen.

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
4. Nicht übernehmbarer 406 (``x-queue-type``) → ab 0.12 ``QueueArgumentsMismatch``
   (Exit 78), Publish-Channel unberührt.
5. Dasselbe Argument widerspricht erneut → ``BrokerConnectionError`` (Obergrenze).
6. Strategy, Overflow und Limit nacheinander → 4 Versuche, dann Erfolg.
7. Wegwerf-Channels werden immer geschlossen, auch nach einem 406.
8. ``.DQ`` übernimmt ebenso.
9. Ohne eigene Verbindung (kein Wegwerf-Channel möglich) → direkt auf dem
   Publish-Channel wie 0.11.0.
10. Ein hängendes Öffnen des Wegwerf-Channels endet nach
    ``declare_probe_timeout_s`` mit ``BrokerConnectionError`` (Q6).
11. Review P2-1: Queue.Declare selbst hat keine Client-Frist (wie 0.11.0).
"""

from __future__ import annotations

import asyncio
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import pytest
from aio_pika.exceptions import ChannelInvalidStateError, ChannelPreconditionFailed

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import BrokerConnectionError, QueueArgumentsMismatch
from swarmq.queue_names import delay_tiers

FAMILY_AND_TIERS = ["tasks", "tasks.DQ", "tasks.XQ", *delay_tiers("tasks")]

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
        #: Die zuletzt gelungene Deklaration je Queue (0.12: die wirksamen Argumente).
        self.succeeded: dict[str, dict[str, Any]] = {}

    async def open(self) -> AsyncMock:
        channel = AsyncMock(is_closed=False)

        async def declare_queue(
            name: str, *, durable: bool = True, arguments: dict[str, Any] | None = None, passive: bool = False
        ) -> None:
            if passive:  # 0.12 (MR 8): passives Declare vorab; die Queue existiert hier
                return
            assert arguments is not None
            self.declared.append((name, dict(arguments)))
            pending = self.outcomes.get(name) or [None]
            outcome = pending.pop(0) if self.outcomes.get(name) else None
            if outcome is not None:
                channel.is_closed = True
                raise outcome
            self.succeeded[name] = dict(arguments)

        channel.declare_queue = AsyncMock(side_effect=declare_queue)
        self.channels.append(channel)
        return channel


def _broker(probes: Probes) -> RabbitMQBroker:
    broker = RabbitMQBroker("amqp://guest:guest@localhost/")
    broker._publish_channel = AsyncMock(is_closed=False)
    connection = AsyncMock(is_closed=False)
    connection.channel = AsyncMock(side_effect=probes.open)
    connection.probes = probes
    broker._connection = connection
    return broker


def _published(broker: RabbitMQBroker) -> dict[str, dict[str, Any]]:
    """Die wirksam deklarierten Argumente je Queue. Ab 0.12 (MR 8) deklariert
    swarmq nur noch auf den Wegwerf-Channels; ohne eigene Verbindung wie
    0.11.0 auf dem Publish-Channel."""
    if broker._connection is not None:
        return dict(cast("AsyncMock", broker._connection).probes.succeeded)
    calls = cast("AsyncMock", broker._publish_channel).declare_queue.call_args_list
    return {call.args[0]: call.kwargs["arguments"] for call in calls}


async def test_publish_channel_declares_with_adopted_strategy() -> None:
    probes = Probes({"tasks": [_strategy("tasks")]})
    broker = _broker(probes)

    await broker.declare_queue("tasks")

    assert _published(broker)["tasks"]["x-dead-letter-strategy"] == "at-least-once"


@pytest.mark.entscheidung("E-016")
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
    """0.12 (Plan MR 8, Punkt 3): ein anderer erkannter Konflikt ist ein
    ``QueueArgumentsMismatch`` (Exit 78) statt ``BrokerConnectionError``."""
    queue_type = _conflict("tasks", "x-queue-type", "received 'quorum' but current is 'classic'")
    broker = _broker(Probes({"tasks": [queue_type]}))

    with pytest.raises(QueueArgumentsMismatch, match="x-queue-type"):
        await broker.declare_queue("tasks")

    cast("AsyncMock", broker._publish_channel).declare_queue.assert_not_called()


async def test_same_argument_conflicting_again_fails() -> None:
    probes = Probes({"tasks": [_strategy("tasks"), _strategy("tasks")]})
    broker = _broker(probes)

    with pytest.raises(BrokerConnectionError, match="Existing queue"):
        await broker.declare_queue("tasks")

    assert len(probes.declared) == 2


@pytest.mark.entscheidung("E-016")
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

    assert len(probes.channels) == 11  # tasks zweimal, .DQ, .XQ, sieben Stufen (Review MR 10, P1-2)
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

    assert list(_published(broker)) == FAMILY_AND_TIERS


async def test_hanging_channel_open_fails_with_broker_connection_error() -> None:
    probes = Probes({})
    broker = _broker(probes)
    never = asyncio.Event()

    async def hang() -> AsyncMock:
        await never.wait()
        raise AssertionError("unreachable")

    cast("AsyncMock", broker._connection).channel = AsyncMock(side_effect=hang)
    broker.declare_probe_timeout_s = 0.05

    with pytest.raises(BrokerConnectionError, match="timed out"):
        await broker.declare_queue("tasks")

    cast("AsyncMock", broker._publish_channel).declare_queue.assert_not_called()


async def test_slow_declare_is_not_cut_off_by_the_open_timeout() -> None:
    """Review P2-1: 0.11.0 hatte für Queue.Declare keine Client-Frist; das
    Anlegen einer Quorum-Queue (Raft) kann unter Last lange dauern. Die Frist
    gilt nur für das Öffnen des Wegwerf-Channels."""
    probes = Probes({})
    broker = _broker(probes)
    released = asyncio.Event()

    async def open_slow() -> AsyncMock:
        channel = await probes.open()
        fast = channel.declare_queue.side_effect

        async def declare_queue(*args: Any, **kwargs: Any) -> None:
            await released.wait()
            await fast(*args, **kwargs)

        channel.declare_queue = AsyncMock(side_effect=declare_queue)
        return channel

    cast("AsyncMock", broker._connection).channel = AsyncMock(side_effect=open_slow)
    broker.declare_probe_timeout_s = 0.01
    asyncio.get_running_loop().call_later(0.05, released.set)

    await broker.declare_queue("tasks")

    assert list(_published(broker)) == FAMILY_AND_TIERS


@pytest.mark.parametrize(
    "error",
    [RuntimeError("Connection was not opened"), ChannelInvalidStateError("No active transport in channel")],
)
async def test_connection_lost_while_opening_probe_is_broker_connection_error(error: Exception) -> None:
    """Review P2-4: aio_pika wirft beim Öffnen auf einer abgerissenen
    Verbindung synchron ``RuntimeError``; der Aufrufer erwartet
    ``BrokerConnectionError`` wie bei jedem anderen Verbindungsfehler."""
    broker = _broker(Probes({}))
    cast("AsyncMock", broker._connection).channel = Mock(side_effect=error)

    with pytest.raises(BrokerConnectionError, match="probe channel"):
        await broker.declare_queue("tasks")

    cast("AsyncMock", broker._connection).ready.assert_awaited()


class _HalfOpenChannel:
    """Wie ``aio_pika.Channel``: ``connection.channel()`` liefert das Objekt
    sofort, erst ``await`` öffnet es. Hier hängt das Öffnen."""

    def __init__(self) -> None:
        self.close = AsyncMock()
        self._never = asyncio.Event()

    def __await__(self) -> Any:
        yield from self._never.wait().__await__()
        return self


async def test_half_open_probe_channel_is_closed_after_open_timeout() -> None:
    """Review-Hinweis 5: läuft das Öffnen in das Zeitlimit, wird der halb
    geöffnete Channel geschlossen statt liegen gelassen."""
    broker = _broker(Probes({}))
    half_open = _HalfOpenChannel()
    cast("AsyncMock", broker._connection).channel = Mock(return_value=half_open)
    broker.declare_probe_timeout_s = 0.05

    with pytest.raises(BrokerConnectionError, match="timed out"):
        await broker.declare_queue("tasks")

    half_open.close.assert_awaited_once()


@pytest.mark.parametrize(
    ("existing", "expected"),
    [
        pytest.param(10, "dead-letters at the existing limit - 1", id="below-config"),
        pytest.param(40, "SWARMQ_DELIVERY_LIMIT - 1", id="above-config"),
    ],
)
async def test_delivery_limit_warning_explains_the_effect(
    swarmq_logs: list[dict[str, Any]], existing: int, expected: str
) -> None:
    """Review P2-2b: darüber greift weiter das Gate bei config - 1. Darunter
    dead-lettert ab 0.12 (Plan MR 8, ``min(config, effektiv)``) ebenfalls das
    Gate, bei vorhandenem Limit - 1 und mit Fehlerergebnis."""
    limit = _conflict("tasks", "x-delivery-limit", f"received '20' but current is '{existing}'")
    broker = _broker(Probes({"tasks": [limit]}))

    await broker.declare_queue("tasks")

    [warning] = [e for e in swarmq_logs if e["event"] == "queue_argument_adopted"]
    assert expected in warning["hint"]


async def test_queue_without_delivery_limit_is_adopted_with_warning(swarmq_logs: list[dict[str, Any]]) -> None:
    """0.12 (Plan MR 8, BZ-15): eine Queue ohne Limit wird übernommen (Broker-
    Default 20), mit WARNING; 0.11.1 brach hier noch ab (Review P2-2a)."""
    none = _conflict("tasks", "x-delivery-limit", "received the value '20' of type 'byte' but current is none")
    broker = _broker(Probes({"tasks": [none]}))

    await broker.declare_queue("tasks")

    assert "x-delivery-limit" not in _published(broker)["tasks"]
    [warning] = [e for e in swarmq_logs if e["event"] == "queue_argument_adopted"]
    assert (warning["log_level"], warning["argument"], warning["existing"]) == ("warning", "x-delivery-limit", None)
