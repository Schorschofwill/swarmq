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
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest
from aio_pika.exceptions import ChannelNotFoundEntity, ChannelPreconditionFailed, PublishError
from aiormq.abc import DeliveredMessage
from aiormq.exceptions import ChannelInvalidStateError, ConnectionClosed
from pamqp.commands import Basic

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.contracts import Delivery
from swarmq.exceptions import BrokerConnectionError, ConfigurationError, QueueArgumentsMismatch, UnroutableMessage
from swarmq.queue_names import delay_tiers

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


def with_tiers(
    family: dict[str, dict[str, Any]], queue: str = "tasks", *, at_least_once: bool = True
) -> dict[str, dict[str, Any]]:
    """Die Familie plus die sieben Delay-Stufen, die der Start seit Review
    MR 10 (P1-2) mit den Argumenten von ``{queue}.DQ`` anlegt."""
    tier_args = {**legacy(queue)[f"{queue}.DQ"], **(ALO if at_least_once else {})}
    return {**family, **{tier: dict(tier_args) for tier in delay_tiers(queue)}}


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
        #: Diese Argumente prüft der Fake zuerst (406-Reihenfolge), sonst alphabetisch.
        self.first: tuple[str, ...] = ()

    def conflict(self, queue: str, declared: dict[str, Any]) -> str | None:
        current = self.queues[queue]
        for arg in sorted(set(declared) | set(current), key=lambda arg: (arg not in self.first, arg)):
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
    assert rabbit.queues == with_tiers(created_by_012("tasks"))


async def test_queue_created_by_0_11_after_the_passive_declare_is_adopted() -> None:
    rabbit = FakeRabbit()
    family = legacy("tasks")
    rabbit.race = {"tasks": family["tasks"], "tasks.DQ": family["tasks.DQ"]}
    broker = make_broker(rabbit)

    await broker.declare_queue("tasks")

    assert rabbit.queues == with_tiers(legacy("tasks"))


@pytest.mark.entscheidung("E-017")
async def test_legacy_mode_creates_queues_with_0_11_arguments() -> None:
    rabbit = FakeRabbit()
    broker = make_broker(rabbit, dead_letter_strategy="legacy")

    await broker.declare_queue("tasks")

    assert rabbit.queues == with_tiers(legacy("tasks"), at_least_once=False)


@pytest.mark.entscheidung("E-016")
async def test_changed_queue_type_raises_queue_arguments_mismatch() -> None:
    family = legacy("tasks")
    rabbit = FakeRabbit({"tasks": {**family["tasks"], "x-queue-type": "classic"}})
    broker = make_broker(rabbit)

    with pytest.raises(QueueArgumentsMismatch) as exc_info:
        await broker.declare_queue("tasks")

    assert isinstance(exc_info.value, ConfigurationError)
    message = str(exc_info.value)
    assert "'tasks'" in message
    assert "x-queue-type" in message
    assert "'quorum'" in message
    assert "'classic'" in message


@pytest.mark.entscheidung("E-017")
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

    assert rabbit.queues == with_tiers(created_by_012("tasks"))


async def test_declarations_never_touch_the_publish_channel() -> None:
    broker = make_broker(FakeRabbit(created_by_012("tasks")))

    await broker.declare_queue("tasks")

    cast("AsyncMock", broker._publish_channel).declare_queue.assert_not_called()


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
    cast("AsyncMock", broker._publish_channel).default_exchange.publish.side_effect = PublishError(
        cast("DeliveredMessage", SimpleNamespace(delivery=frame)), frame
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


@pytest.mark.entscheidung("E-017")
async def test_legacy_mode_does_not_warn_about_at_most_once_queues(swarmq_logs: list[dict[str, Any]]) -> None:
    broker = make_broker(FakeRabbit(legacy("tasks")), dead_letter_strategy="legacy")
    await broker.declare_queue("tasks")

    await broker.report_queue_declarations()

    assert not [e for e in swarmq_logs if e["event"] == "queue_dead_letter_at_most_once"]


# --- Review MR 8 ---------------------------------------------------------------


def _unroutable(broker: RabbitMQBroker, routing_key: str) -> None:
    frame = Basic.Return(reply_code=312, reply_text="NO_ROUTE", exchange="", routing_key=routing_key)
    cast("AsyncMock", broker._publish_channel).default_exchange.publish.side_effect = PublishError(
        cast("DeliveredMessage", SimpleNamespace(delivery=frame)), frame
    )


async def test_effective_delivery_limit_survives_an_unroutable_publish() -> None:
    """Review MR 8, P2-1: das Verwerfen des Caches (F5) darf das wirksame
    Limit nicht vergessen; sonst gaelte bis zum naechsten Declare die
    Konfiguration (50) statt der Queue (20)."""
    broker = make_broker(FakeRabbit(_with_limit(None)), delivery_limit=50)
    await broker.declare_queue("tasks")
    _unroutable(broker, "tasks.DQ")

    with pytest.raises(UnroutableMessage):
        await broker.publish("tasks.DQ", b"{}")

    assert broker.effective_delivery_limit("tasks") == 20


def _at_least_once_with_overflow(overflow: str) -> dict[str, dict[str, Any]]:
    """Von 0.12 angelegt, aber die Haupt-Queue mit einem anderen ``x-overflow``:
    RabbitMQ dead-lettert dann trotz ``at-least-once`` nur at-most-once."""
    family = created_by_012("tasks")
    family["tasks"] = {**family["tasks"], "x-overflow": overflow}
    return family


async def test_at_least_once_without_reject_publish_counts_as_at_most_once(
    swarmq_logs: list[dict[str, Any]],
) -> None:
    """Review MR 8, P2-2: at-least-once wirkt nur zusammen mit reject-publish."""
    rabbit = FakeRabbit(_at_least_once_with_overflow("drop-head"))
    broker = make_broker(rabbit)
    await broker.declare_queue("tasks")

    await broker.report_queue_declarations()

    assert rabbit.queues == with_tiers(_at_least_once_with_overflow("drop-head"))
    [warning] = [e for e in swarmq_logs if e["event"] == "queue_dead_letter_at_most_once"]
    assert warning["queues"] == ["tasks"]


async def test_strict_mode_refuses_at_least_once_without_reject_publish() -> None:
    """Review MR 8, P2-2: der strenge Modus prueft auch ``x-overflow``."""
    rabbit = FakeRabbit(_at_least_once_with_overflow("drop-head"))
    broker = make_broker(rabbit, dead_letter_strategy="at-least-once")

    with pytest.raises(QueueArgumentsMismatch) as exc_info:
        await broker.declare_queue("tasks")

    assert (exc_info.value.argument, exc_info.value.declared, exc_info.value.existing) == (
        "x-overflow",
        "reject-publish",
        "drop-head",
    )
    assert rabbit.queues == _at_least_once_with_overflow("drop-head")


@pytest.mark.parametrize("first", ["x-dead-letter-strategy", "x-overflow"])
async def test_adopted_overflow_is_kept_in_either_refusal_order(first: str, swarmq_logs: list[dict[str, Any]]) -> None:
    """Review MR 8, P2-2: der volle 0.12-Satz ueberschreibt ein schon
    uebernommenes ``x-overflow`` nicht, egal welchen 406 der Broker zuerst meldet."""
    rabbit = FakeRabbit(_at_least_once_with_overflow("drop-head"))
    rabbit.first = (first,)
    broker = make_broker(rabbit)

    await broker.declare_queue("tasks")
    await broker.report_queue_declarations()

    assert rabbit.queues == with_tiers(_at_least_once_with_overflow("drop-head"))
    [warning] = [e for e in swarmq_logs if e["event"] == "queue_dead_letter_at_most_once"]
    assert warning["queues"] == ["tasks"]


@pytest.mark.entscheidung("E-016")
async def test_unrecognized_refusal_reports_the_broker_message_without_advising_deletion() -> None:
    """Review MR 8, P2-5: ein unerkannter 406 (S8, z. B. Queue-Limit) wird
    ``BrokerConnectionError`` mit der Broker-Meldung; kein Rat zum Loeschen,
    die Queue kann Retries und Termine enthalten."""
    refusal = "PRECONDITION_FAILED - cannot declare queue 'tasks': queue limit in vhost '/' (1) is reached"
    broker = make_broker(FakeRabbit())
    cast("AsyncMock", broker._connection).channel = AsyncMock(
        return_value=AsyncMock(is_closed=False, declare_queue=AsyncMock(side_effect=ChannelPreconditionFailed(refusal)))
    )

    with pytest.raises(BrokerConnectionError) as exc_info:
        await broker.declare_queue("tasks")

    message = str(exc_info.value)
    assert refusal in message
    assert "delete" not in message.lower()


async def test_redeclare_after_unroutable_runs_the_chain_and_refills_the_cache() -> None:
    """Review MR 8, H1: das Neu-Deklarieren aus MR 6 laeuft ueber die Kette
    mit Uebernahme wie beim Start. Hat ein 0.11-Pod die geloeschte ``.DQ``
    inzwischen mit seinen Argumenten angelegt, gelingt es trotzdem; danach
    ist der Cache wieder gesetzt."""
    rabbit = FakeRabbit(created_by_012("tasks"))
    broker = make_broker(rabbit)
    await broker.declare_queue("tasks")
    rabbit.queues["tasks.DQ"] = legacy("tasks")["tasks.DQ"]  # geloescht, von 0.11 neu angelegt
    frame = Basic.Return(reply_code=312, reply_text="NO_ROUTE", exchange="", routing_key="tasks.DQ")
    cast("AsyncMock", broker._publish_channel).default_exchange.publish.side_effect = [
        PublishError(cast("DeliveredMessage", SimpleNamespace(delivery=frame)), frame),
        None,
    ]
    delivery = Delivery(body=b"{}", headers={}, queue="tasks", handle=AsyncMock(consumer_tag=None))

    await broker.retry_later(delivery, delay_ms=1000, headers={})

    cast("AsyncMock", delivery.handle).ack.assert_awaited_once()
    assert rabbit.queues["tasks.DQ"] == legacy("tasks")["tasks.DQ"]
    opened = len(rabbit.channels)
    await broker.declare_queue("tasks")
    assert len(rabbit.channels) == opened


@pytest.mark.parametrize(
    ("step", "error"),
    [
        pytest.param("passive", ConnectionClosed(320, "CONNECTION_FORCED"), id="passive-connection-closed"),
        pytest.param("active", ConnectionClosed(320, "CONNECTION_FORCED"), id="active-connection-closed"),
        pytest.param("active", ChannelInvalidStateError("channel closed"), id="active-channel-invalid"),
        pytest.param("active", RuntimeError("connection closed"), id="active-runtime-error"),
    ],
)
async def test_connection_loss_inside_the_chain_becomes_broker_connection_error(step: str, error: Exception) -> None:
    """Review MR 8, H5a: ein Verbindungsabriss mitten in der Kette ist ein
    ``BrokerConnectionError`` (Neustart, Wiederverbinden), kein roher Fehler."""
    rabbit = FakeRabbit(legacy("tasks"))
    broker = make_broker(rabbit)
    real_open = rabbit.open

    async def open_failing(**kwargs: Any) -> AsyncMock:
        channel = await real_open(**kwargs)
        real_declare = channel.declare_queue.side_effect

        async def declare_queue(name: str, **declare_kwargs: Any) -> None:
            if bool(declare_kwargs.get("passive")) == (step == "passive"):
                raise error
            await real_declare(name, **declare_kwargs)

        channel.declare_queue = AsyncMock(side_effect=declare_queue)
        return channel

    cast("AsyncMock", broker._connection).channel = AsyncMock(side_effect=open_failing)

    with pytest.raises(BrokerConnectionError) as exc_info:
        await broker.declare_queue("tasks")

    assert exc_info.value.__cause__ is error
    assert all(channel.close.await_count for channel in rabbit.channels)


def _advises_safe_migration(message: str) -> bool:
    """Review MR 8, P2-3: erst Producer stoppen, Worker leeren lassen, dann
    Worker stoppen; ``.DQ`` enthaelt Retries und Termine -> verschieben oder
    abwarten, nie einfach loeschen."""
    lowered = message.lower()
    order = ["stop its producers", "let its workers drain", "stop the workers"]
    positions = [lowered.find(step) for step in order]
    return (
        all(position >= 0 for position in positions)
        and positions == sorted(positions)
        and "shovel" in lowered
        and "drain and delete" not in lowered
    )


async def test_queue_arguments_mismatch_advises_a_safe_migration() -> None:
    family = legacy("tasks")
    broker = make_broker(FakeRabbit({"tasks": {**family["tasks"], "x-queue-type": "classic"}}))

    with pytest.raises(QueueArgumentsMismatch) as exc_info:
        await broker.declare_queue("tasks")

    assert _advises_safe_migration(str(exc_info.value))


async def test_strict_mode_advises_a_safe_migration() -> None:
    broker = make_broker(FakeRabbit(legacy("tasks")), dead_letter_strategy="at-least-once")

    with pytest.raises(QueueArgumentsMismatch) as exc_info:
        await broker.declare_queue("tasks")

    assert _advises_safe_migration(str(exc_info.value))


async def test_migration_hint_names_the_delay_tiers() -> None:
    """Review MR 10, P2-5: seit BZ-01 halten auch die Stufen ``{queue}.DQ.*``
    Retries und Termine; der Rat zum Leeren muss sie nennen."""
    family = legacy("tasks")
    broker = make_broker(FakeRabbit({"tasks": {**family["tasks"], "x-queue-type": "classic"}}))

    with pytest.raises(QueueArgumentsMismatch) as exc_info:
        await broker.declare_queue("tasks")

    message = str(exc_info.value)
    assert ".DQ.* tiers" in message
    assert _advises_safe_migration(message)
