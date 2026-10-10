"""Queue-Deklaration auf eigenem Channel: Kette, Cache, effektives Limit (Plan 0.12.0, MR 8).

BZ-15/BZ-19. Ein kleiner Fake-Broker hält die Argumente je Queue und antwortet
wie RabbitMQ 4.2/4.3 (Spike S1): passives Declare auf eine fehlende Queue →
``ChannelNotFoundEntity``; abweichende Argumente → ``ChannelPreconditionFailed``
mit dem 406-Text für das erste abweichende Argument.

Test-Liste:
- [x] Bestandsqueue einer 0.12-Anlage (at-least-once): ein 406 je Queue, dann der volle 0.12-Satz
- [x] Wettlauf: nach dem passiven Declare legt ein 0.11-Pod die Queue an → "current is none" übernehmen
- [x] Modus ``legacy``: neue Queues bekommen die Argumente aus 0.11
- [x] anderer erkannter Konflikt (``x-queue-type``) → ``QueueArgumentsMismatch`` (Exit 78) mit alt und neu
- [x] Modus ``at-least-once``: Bestandsqueue ohne at-least-once → ``QueueArgumentsMismatch``, Queue unverändert
- [x] Modus ``at-least-once``: von 0.12 angelegte Queue → in Ordnung
- [x] Deklarationen laufen nie auf dem Publish-Channel (ein 406 risse dort Publishes ab)
- [x] Cache: eine zweite Deklaration derselben Queue öffnet keinen Channel
- [x] Cache: ``UnroutableMessage`` an eine Queue der Familie verwirft den Eintrag (F5)
- [x] Cache: NOT_FOUND beim Consume verwirft den Eintrag
- [x] effektives Delivery-Limit je Queue: Wert der Queue, -1 unbegrenzt (None), 0 bleibt 0,
      fehlend 20 (S1), classic und unbekannte Queue None
- [x] Queue ohne Delivery-Limit: WARNING mit Hinweis auf den Broker-Default 20
- [x] Bestandsqueues at-most-once: eine Warnung ``queue_dead_letter_at_most_once`` mit Liste, nur einmal
- [x] Modus ``legacy``: keine Warnung
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest
from aio_pika.exceptions import ChannelNotFoundEntity, ChannelPreconditionFailed, PublishError
from pamqp.commands import Basic

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import ConfigurationError, QueueArgumentsMismatch, UnroutableMessage

pytestmark = pytest.mark.timeout(5)

ALO = {"x-dead-letter-strategy": "at-least-once", "x-overflow": "reject-publish"}


def legacy(queue: str, limit: int = 20) -> dict[str, dict[str, Any]]:
    """Die Argumente aus 0.11 für die Queue-Familie."""
    return {
        queue: {
            "x-queue-type": "quorum",
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": f"{queue}.XQ",
            "x-delivery-limit": limit,
        },
        f"{queue}.DQ": {"x-queue-type": "quorum", "x-dead-letter-exchange": "", "x-dead-letter-routing-key": queue},
        f"{queue}.XQ": {"x-queue-type": "quorum"},
    }


def created_by_012(queue: str, limit: int = 20) -> dict[str, dict[str, Any]]:
    family = legacy(queue, limit)
    family[queue] = {**family[queue], **ALO}
    family[f"{queue}.DQ"] = {**family[f"{queue}.DQ"], **ALO}
    return family


def _type(value: object) -> str:
    return "byte" if isinstance(value, int) else "longstr"


class FakeRabbit:
    """Queues mit Argumenten; zählt geöffnete Channels und 406-Antworten."""

    def __init__(self, queues: dict[str, dict[str, Any]] | None = None) -> None:
        self.queues: dict[str, dict[str, Any]] = {name: dict(args) for name, args in (queues or {}).items()}
        self.channels: list[AsyncMock] = []
        self.refusals: list[str] = []
        #: Wird vor der ersten aktiven Deklaration der Queue angelegt (Wettlauf nach dem passiven Declare).
        self.race: dict[str, dict[str, Any]] = {}

    def conflict(self, queue: str, declared: dict[str, Any]) -> str | None:
        current = self.queues[queue]
        for arg in sorted(set(declared) | set(current)):
            have, want = current.get(arg), declared.get(arg)
            if have == want:
                continue
            if have is None:
                rest = f"received the value '{want}' of type '{_type(want)}' but current is none"
            elif want is None:
                rest = f"received none but current is the value '{have}' of type '{_type(have)}'"
            else:
                rest = f"received '{want}' but current is '{have}'"
            return f"PRECONDITION_FAILED - inequivalent arg '{arg}' for queue '{queue}' in vhost '/': {rest}"
        return None

    async def open(self, **_: Any) -> AsyncMock:
        channel = AsyncMock(is_closed=False)

        async def declare_queue(
            name: str, *, durable: bool = True, arguments: dict[str, Any] | None = None, passive: bool = False
        ) -> None:
            if passive:
                if name not in self.queues:
                    channel.is_closed = True
                    raise ChannelNotFoundEntity(f"NOT_FOUND - no queue '{name}' in vhost '/'")
                return
            if name in self.race:
                self.queues[name] = self.race.pop(name)
            declared = dict(arguments or {})
            if name not in self.queues:
                self.queues[name] = declared
                return
            text = self.conflict(name, declared)
            if text is not None:
                self.refusals.append(name)
                channel.is_closed = True
                raise ChannelPreconditionFailed(text)

        channel.declare_queue = AsyncMock(side_effect=declare_queue)
        self.channels.append(channel)
        return channel


def make_broker(rabbit: FakeRabbit, **attributes: Any) -> RabbitMQBroker:
    broker = RabbitMQBroker("amqp://guest:guest@localhost/")
    broker._publish_channel = AsyncMock(is_closed=False)
    broker._connection = AsyncMock(is_closed=False)
    broker._connection.channel = AsyncMock(side_effect=rabbit.open)
    for name, value in attributes.items():
        setattr(broker, name, value)
    return broker


async def test_queue_created_by_012_costs_one_refusal_per_queue() -> None:
    rabbit = FakeRabbit(created_by_012("tasks"))
    broker = make_broker(rabbit)

    await broker.declare_queue("tasks")

    assert rabbit.refusals == ["tasks", "tasks.DQ"]
    assert rabbit.queues == created_by_012("tasks")


async def test_queue_created_by_0_11_after_the_passive_declare_is_adopted() -> None:
    rabbit = FakeRabbit()
    family = legacy("tasks")
    rabbit.race = {"tasks": family["tasks"], "tasks.DQ": family["tasks.DQ"]}
    broker = make_broker(rabbit)

    await broker.declare_queue("tasks")

    assert rabbit.queues == legacy("tasks")


async def test_legacy_mode_creates_queues_with_0_11_arguments() -> None:
    rabbit = FakeRabbit()
    broker = make_broker(rabbit, dead_letter_strategy="legacy")

    await broker.declare_queue("tasks")

    assert rabbit.queues == legacy("tasks")


async def test_changed_queue_type_raises_queue_arguments_mismatch() -> None:
    family = legacy("tasks")
    rabbit = FakeRabbit({"tasks": {**family["tasks"], "x-queue-type": "classic"}})
    broker = make_broker(rabbit)

    with pytest.raises(QueueArgumentsMismatch) as exc_info:
        await broker.declare_queue("tasks")

    assert isinstance(exc_info.value, ConfigurationError)
    message = str(exc_info.value)
    assert "'tasks'" in message and "x-queue-type" in message
    assert "'quorum'" in message and "'classic'" in message


async def test_strict_mode_refuses_an_existing_at_most_once_queue() -> None:
    rabbit = FakeRabbit(legacy("tasks"))
    broker = make_broker(rabbit, dead_letter_strategy="at-least-once")

    with pytest.raises(QueueArgumentsMismatch) as exc_info:
        await broker.declare_queue("tasks")

    assert (exc_info.value.argument, exc_info.value.declared, exc_info.value.existing) == (
        "x-dead-letter-strategy",
        "at-least-once",
        None,
    )
    assert "SWARMQ_QUEUE_DEAD_LETTER_STRATEGY" in str(exc_info.value)
    assert rabbit.queues == legacy("tasks")


@pytest.mark.parametrize("existing", [{}, created_by_012("tasks")], ids=["new", "created-by-012"])
async def test_strict_mode_accepts_at_least_once_queues(existing: dict[str, dict[str, Any]]) -> None:
    rabbit = FakeRabbit(existing)
    broker = make_broker(rabbit, dead_letter_strategy="at-least-once")

    await broker.declare_queue("tasks")

    assert rabbit.queues == created_by_012("tasks")


async def test_declarations_never_touch_the_publish_channel() -> None:
    broker = make_broker(FakeRabbit(created_by_012("tasks")))

    await broker.declare_queue("tasks")

    broker._publish_channel.declare_queue.assert_not_called()  # type: ignore[union-attr]


async def test_second_declaration_of_the_same_queue_is_cached() -> None:
    rabbit = FakeRabbit()
    broker = make_broker(rabbit)
    await broker.declare_queue("tasks")
    opened = len(rabbit.channels)

    await broker.declare_queue("tasks")

    assert len(rabbit.channels) == opened


@pytest.mark.parametrize("routing_key", ["tasks", "tasks.DQ", "tasks.XQ"])
async def test_unroutable_publish_drops_the_cached_declaration(routing_key: str) -> None:
    rabbit = FakeRabbit()
    broker = make_broker(rabbit)
    await broker.declare_queue("tasks")
    frame = Basic.Return(reply_code=312, reply_text="NO_ROUTE", exchange="", routing_key=routing_key)
    broker._publish_channel.default_exchange.publish.side_effect = PublishError(  # type: ignore[union-attr]
        SimpleNamespace(delivery=frame), frame
    )
    with pytest.raises(UnroutableMessage):
        await broker.publish(routing_key, b"{}")
    del rabbit.queues[routing_key]  # die Queue wurde geloescht
    opened = len(rabbit.channels)

    await broker.declare_queue("tasks")

    assert len(rabbit.channels) > opened
    assert routing_key in rabbit.queues


async def test_not_found_on_consume_drops_the_cached_declaration() -> None:
    rabbit = FakeRabbit()
    broker = make_broker(rabbit)
    await broker.declare_queue("tasks")
    consume_channel = AsyncMock(is_closed=False)
    consume_channel.get_queue.side_effect = ChannelNotFoundEntity("NOT_FOUND - no queue 'tasks' in vhost '/'")
    broker._consume_channel = consume_channel
    del rabbit.queues["tasks"]

    with pytest.raises(ChannelNotFoundEntity):
        await broker.consume("tasks", AsyncMock(), concurrency=1)
    await broker.declare_queue("tasks")

    assert "tasks" in rabbit.queues


def _with_limit(limit: object) -> dict[str, dict[str, Any]]:
    family = legacy("tasks")
    main = {key: value for key, value in family["tasks"].items() if key != "x-delivery-limit"}
    if limit is not None:
        main["x-delivery-limit"] = limit
    family["tasks"] = main
    return family


@pytest.mark.parametrize(
    ("existing", "expected"),
    [
        pytest.param(_with_limit(20), 20, id="queue-20"),
        pytest.param(_with_limit(-1), None, id="unbounded"),
        pytest.param(_with_limit(0), 0, id="no-redelivery"),
        pytest.param(_with_limit(None), 20, id="none-is-20"),
        pytest.param({}, 50, id="new-queue"),
    ],
)
async def test_effective_delivery_limit_per_queue(existing: dict[str, dict[str, Any]], expected: int | None) -> None:
    broker = make_broker(FakeRabbit(existing), delivery_limit=50)
    assert broker.effective_delivery_limit("tasks") is None

    await broker.declare_queue("tasks")

    assert broker.effective_delivery_limit("tasks") == expected
    assert broker.effective_delivery_limit("tasks.DQ") is None


async def test_classic_queues_have_no_effective_delivery_limit() -> None:
    broker = make_broker(FakeRabbit(), _queue_type="classic")

    await broker.declare_queue("tasks")

    assert broker.effective_delivery_limit("tasks") is None


async def test_queue_without_delivery_limit_warns_about_the_broker_default(swarmq_logs: list[dict[str, Any]]) -> None:
    broker = make_broker(FakeRabbit(_with_limit(None)))

    await broker.declare_queue("tasks")

    [warning] = [e for e in swarmq_logs if e["event"] == "queue_argument_adopted"]
    assert warning["log_level"] == "warning"
    assert "default of 20" in warning["hint"]


async def test_at_most_once_queues_are_reported_once_with_a_list(swarmq_logs: list[dict[str, Any]]) -> None:
    broker = make_broker(FakeRabbit({**legacy("tasks"), **legacy("other")}))
    await broker.declare_queue("tasks")
    await broker.declare_queue("other")

    await broker.report_queue_declarations()
    await broker.report_queue_declarations()

    [warning] = [e for e in swarmq_logs if e["event"] == "queue_dead_letter_at_most_once"]
    assert warning["log_level"] == "warning"
    assert warning["queues"] == ["other", "other.DQ", "tasks", "tasks.DQ"]
    assert "dead-letter-strategy" in warning["hint"]
    assert "SWARMQ_QUEUE_DEAD_LETTER_STRATEGY=legacy" in warning["hint"]


async def test_legacy_mode_does_not_warn_about_at_most_once_queues(swarmq_logs: list[dict[str, Any]]) -> None:
    broker = make_broker(FakeRabbit(legacy("tasks")), dead_letter_strategy="legacy")
    await broker.declare_queue("tasks")

    await broker.report_queue_declarations()

    assert not [e for e in swarmq_logs if e["event"] == "queue_dead_letter_at_most_once"]
