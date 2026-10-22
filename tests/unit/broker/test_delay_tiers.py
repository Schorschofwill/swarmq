"""Delay-Stufen im RabbitMQ-Broker (Plan 0.12.0, MR 10, BZ-01).

``retry_later`` stellt eine Nachricht in die kleinste Stufe ``{queue}.DQ.<stufe>``
zurück, deren Obergrenze die Wartezeit erreicht. Die Stufen entstehen beim Start
mit der Familie über die Deklarations-Kette aus MR 8 (eigene Wegwerf-Channels;
Review MR 10, P1-2); nur das Neu-Deklarieren nach einem unroutbaren Publish
deklariert im Betrieb.
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
- [x] Stufen werden beim Start mit der Familie deklariert; ``retry_later`` oeffnet keinen Channel
      (Review MR 10, P1-2)
- [x] strenger Modus ``at-least-once``: eine vorhandene at-most-once-Stufe ist fatal (Exit 78),
      wie bei Bestandsqueues (Review MR 10, P2-4)
- [x] Modus ``auto``: eine uebernommene at-most-once-Stufe steht in ``queue_dead_letter_at_most_once``
      (Review MR 10, P2-4; durch die Start-Deklaration vor ``report_queue_declarations``)
- [x] unroutbare Stufe: nur die Stufe wird neu deklariert, nicht die Familie; die WARNING nennt die Stufe
      (Review MR 10, P2-3)

Testlücken Runde 4 (Plan 2026-10-20-1500, U3):
- [x] E-16: unroutbare Stufe, Neu-Deklarieren scheitert an einem Kanalfehler (403, nicht
      am Verbindungsabriss, V-B): diese und die nächste Rückstellung gehen an ``{queue}.DQ``
      (Mutant ``declarations.py`` ``self._ready_delay_tiers.discard(name)`` -> ``discard(None)``)
- [x] E-18: erneutes Deklarieren der Familie deklariert bereite und abgelehnte Stufen nicht neu
      (Mutant ``declarations.py`` ``tier not in … and tier not in …`` -> ``or``)

PF-21 (Entscheidungsvorlage 2026-10-21, Variante D; E-026 Nachtrag): ein vorübergehend
gescheitertes Neu-Deklarieren einer Stufe im Betrieb
- [x] Verbindungsabriss: diese Rückstellung geht an ``{queue}.DQ``, Original bestätigt,
      WARNING ``delay_tier_redeclare_failed`` (ersetzt den ``BrokerConnectionError`` von früher)
- [x] hängendes Neu-Deklarieren: nach ``declare_probe_timeout_s`` abgebrochen, nicht erst nach der
      Publish-Frist (ersetzt den Test mit der Publish-Frist)
- [x] in der Pause von 30 s: Rückstellungen an die Stufe gehen ohne Versuch an ``{queue}.DQ``
- [x] nach der Pause: erst Neu-Deklarieren, dann Publish an die Stufe; INFO ``delay_tier_restored``
- [x] scheitert der Versuch nach der Pause erneut: ``{queue}.DQ`` und neue Pause
- [x] höchstens ein Versuch je Stufe gleichzeitig: eine parallele Rückstellung geht sofort an ``{queue}.DQ``
- [x] eine kürzere Shutdown-Restfrist begrenzt den Versuch
- [x] Review P3-2: auch eine Rückstellung, deren Publish erst in der Pause zurückkommt, versucht es nicht
- [x] Review P3-7: ein abgebrochener Versuch gibt die Stufe für den nächsten Versuch frei
- [x] ist die Verbindung zu, gilt das als vorübergehend, und der Publish-Channel deklariert nichts
      (wie früher ``redeclare_on_throwaway_channel``: ein 406 risse parallele Publishes ab)
- [x] strenger Mismatch im Betrieb: Ablehnung wie E-026 (WARNING ``delay_tier_unavailable``, Gauge),
      nicht fatal, danach kein Versuch mehr
- [x] Ablehnung (403) beim Versuch nach der Pause: Stufe aus, kein weiterer Versuch
- [x] die Familien-Deklaration heilt eine ausstehende Stufe mit (INFO ``delay_tier_restored``)
- [x] scheitert auch der Publish an ``{queue}.DQ``: ``BrokerConnectionError``, Original unbestätigt
      (mit der Shutdown-Restfrist)
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest
from aio_pika.exceptions import PublishError
from aiormq.abc import DeliveredMessage
from aiormq.exceptions import ChannelAccessRefused, ConnectionClosed
from pamqp.commands import Basic

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.contracts import Delivery
from swarmq.exceptions import BrokerConnectionError, QueueArgumentsMismatch
from swarmq.queue_names import delay_tiers
from tests.unit.broker.test_declare_chain import FakeRabbit, legacy, make_broker

pytestmark = pytest.mark.timeout(5)


def _returned(routing_key: str) -> PublishError:
    """Ein ``basic.return`` NO_ROUTE, wie aiormq es mit ``on_return_raises`` wirft."""
    frame = Basic.Return(reply_code=312, reply_text="NO_ROUTE", exchange="", routing_key=routing_key)
    return PublishError(cast("DeliveredMessage", SimpleNamespace(delivery=frame)), frame)


def _delivery(queue: str = "tasks") -> Delivery:
    return Delivery(body=b"{}", headers={}, queue=queue, handle=AsyncMock(consumer_tag=None))


def _routing_keys(broker: RabbitMQBroker) -> list[str]:
    publish = cast("AsyncMock", broker._publish_channel).default_exchange.publish
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
    cast("AsyncMock", broker._publish_channel).default_exchange.publish.side_effect = [_returned("tasks.DQ.1s"), None]
    delivery = _delivery()

    await broker.retry_later(delivery, delay_ms=500, headers={})

    assert _routing_keys(broker) == ["tasks.DQ.1s", "tasks.DQ.1s"]
    assert _chains(rabbit).count("tasks.DQ.1s") == 2
    cast("AsyncMock", delivery.handle).ack.assert_awaited_once()


@pytest.mark.entscheidung("E-026")
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
    assert entry["queue"] == "tasks"
    assert entry["tier"] == "tasks.DQ.4s"
    assert entry["fallback"] == "tasks.DQ"
    assert entry["error_type"] == "QueueArgumentsMismatch"
    cast("AsyncMock", first.handle).ack.assert_awaited_once()
    cast("AsyncMock", second.handle).ack.assert_awaited_once()


@pytest.mark.entscheidung("E-026")
@pytest.mark.befund("PF-21")
async def test_connection_loss_while_redeclaring_a_tier_sends_this_deferral_to_the_delay_queue(swarmq_logs) -> None:
    # Seit Review MR 10 (P1-2) deklariert nur noch das Neu-Deklarieren nach
    # einem unroutbaren Publish eine Stufe im Hot-Path. PF-21: kein
    # BrokerConnectionError mehr, also keine Neuzustellung und keine Schleife
    # bis ``{queue}.XQ``; die zurückgegebene Kopie war die einzige.
    rabbit = FakeRabbit()
    broker = make_broker(rabbit)
    await broker.declare_queue("tasks")
    cast("AsyncMock", broker._publish_channel).default_exchange.publish.side_effect = [_returned("tasks.DQ.1s"), None]
    cast("AsyncMock", broker._connection).channel = AsyncMock(side_effect=ConnectionClosed(320, "CONNECTION_FORCED"))
    delivery = _delivery()

    await broker.retry_later(delivery, delay_ms=500, headers={})

    assert _routing_keys(broker) == ["tasks.DQ.1s", "tasks.DQ"]
    cast("AsyncMock", delivery.handle).ack.assert_awaited_once()
    [entry] = [e for e in swarmq_logs if e["event"] == "delay_tier_redeclare_failed"]
    assert entry["log_level"] == "warning"
    assert (entry["queue"], entry["tier"], entry["fallback"]) == ("tasks", "tasks.DQ.1s", "tasks.DQ")
    assert entry["error_type"] == "ConnectionClosed"
    assert entry["retry_in_s"] == 30.0
    assert broker.delay_tiers_unavailable("tasks") == 0


def _hanging_channels(broker: RabbitMQBroker) -> asyncio.Event:
    """Jede Deklaration auf einem neuen Wegwerf-Channel hängt, bis das
    zurückgegebene Ereignis gesetzt ist."""
    release = asyncio.Event()

    async def hanging_channel(**_: object) -> AsyncMock:
        channel = AsyncMock(is_closed=False)

        async def declare_queue(*_: object, **__: object) -> None:
            await release.wait()

        channel.declare_queue = AsyncMock(side_effect=declare_queue)
        return channel

    cast("AsyncMock", broker._connection).channel = AsyncMock(side_effect=hanging_channel)
    return release


@pytest.mark.befund("PF-21")
async def test_hanging_tier_redeclaration_is_bounded_by_the_probe_timeout_not_the_publish_deadline(
    swarmq_logs,
) -> None:
    """Review-Punkt P2: Ein hängendes Anlegen belegte bis zur Publish-Frist
    (Default 120 s) einen Slot. Ein abgebrochenes Anlegen läuft beim Broker
    weiter; der nächste Versuch findet die Stufe."""
    rabbit = FakeRabbit()
    broker = make_broker(rabbit)
    await broker.declare_queue("tasks")
    cast("AsyncMock", broker._publish_channel).default_exchange.publish.side_effect = [_returned("tasks.DQ.1s"), None]
    broker.publish_timeout_s = 60.0
    broker.declare_probe_timeout_s = 0.05
    _hanging_channels(broker)
    delivery = _delivery()

    await broker.retry_later(delivery, delay_ms=500, headers={})

    assert _routing_keys(broker) == ["tasks.DQ.1s", "tasks.DQ"]
    cast("AsyncMock", delivery.handle).ack.assert_awaited_once()
    [entry] = [e for e in swarmq_logs if e["event"] == "delay_tier_redeclare_failed"]
    assert entry["error_type"] == "TimeoutError"


async def _tier_redeclaration_failed_once(rabbit: FakeRabbit, now: list[float], **attributes: Any) -> RabbitMQBroker:
    """Ein Broker, dessen Neu-Deklarieren von ``tasks.DQ.1s`` einmal an einem
    Verbindungsabriss gescheitert ist (Uhr ``now``); danach antwortet der
    Broker wieder normal. Gesendet: ``tasks.DQ.1s`` (zurückgegeben), ``tasks.DQ``."""
    broker = make_broker(rabbit, monotonic=lambda: now[0], **attributes)
    await broker.declare_queue("tasks")
    del rabbit.queues["tasks.DQ.1s"]
    connection = cast("AsyncMock", broker._connection)
    connection.channel = AsyncMock(side_effect=ConnectionClosed(320, "CONNECTION_FORCED"))
    publish = cast("AsyncMock", broker._publish_channel).default_exchange.publish
    publish.side_effect = [_returned("tasks.DQ.1s"), None]
    await broker.retry_later(_delivery(), delay_ms=500, headers={})
    connection.channel = AsyncMock(side_effect=rabbit.open)
    publish.side_effect = None
    return broker


@pytest.mark.entscheidung("E-026")
@pytest.mark.befund("PF-21")
async def test_after_the_pause_the_next_deferral_declares_the_tier_again_and_uses_it(swarmq_logs) -> None:
    rabbit = FakeRabbit()
    now = [1_000.0]
    broker = await _tier_redeclaration_failed_once(rabbit, now)
    chains_before = len(_chains(rabbit))
    now[0] += 30.0

    await broker.retry_later(_delivery(), delay_ms=500, headers={})

    assert _chains(rabbit)[chains_before:] == ["tasks.DQ.1s"]
    assert "tasks.DQ.1s" in rabbit.queues
    assert _routing_keys(broker) == ["tasks.DQ.1s", "tasks.DQ", "tasks.DQ.1s"]
    [entry] = [e for e in swarmq_logs if e["event"] == "delay_tier_restored"]
    assert entry["log_level"] == "info"
    assert (entry["queue"], entry["tier"]) == ("tasks", "tasks.DQ.1s")


@pytest.mark.entscheidung("E-026")
@pytest.mark.befund("PF-21")
async def test_during_the_pause_deferrals_go_to_the_delay_queue_without_an_attempt() -> None:
    """Review-Punkt P2: ohne Pause kostete jede Rückstellung an die Stufe
    einen Versuch. Gegenprobe ``monotonic() < retry_at`` -> ``False``: rot."""
    rabbit = FakeRabbit()
    now = [1_000.0]
    broker = await _tier_redeclaration_failed_once(rabbit, now)
    chains_before = len(_chains(rabbit))
    now[0] += 29.9

    await broker.retry_later(_delivery(), delay_ms=500, headers={})

    assert _chains(rabbit)[chains_before:] == []
    assert _routing_keys(broker) == ["tasks.DQ.1s", "tasks.DQ", "tasks.DQ"]


@pytest.mark.befund("PF-21")
async def test_a_failed_attempt_after_the_pause_starts_a_new_pause(swarmq_logs) -> None:
    rabbit = FakeRabbit()
    now = [1_000.0]
    broker = await _tier_redeclaration_failed_once(rabbit, now)
    cast("AsyncMock", broker._connection).channel = AsyncMock(side_effect=ConnectionClosed(320, "CONNECTION_FORCED"))
    now[0] += 30.0
    await broker.retry_later(_delivery(), delay_ms=500, headers={})
    cast("AsyncMock", broker._connection).channel = AsyncMock(side_effect=rabbit.open)
    chains_before = len(_chains(rabbit))
    now[0] += 29.9

    await broker.retry_later(_delivery(), delay_ms=500, headers={})

    assert _chains(rabbit)[chains_before:] == []
    assert _routing_keys(broker) == ["tasks.DQ.1s", "tasks.DQ", "tasks.DQ", "tasks.DQ"]
    assert len([e for e in swarmq_logs if e["event"] == "delay_tier_redeclare_failed"]) == 2
    assert not [e for e in swarmq_logs if e["event"] == "delay_tier_restored"]


@pytest.mark.entscheidung("E-026")
@pytest.mark.befund("PF-21")
async def test_while_an_attempt_runs_a_parallel_deferral_goes_to_the_delay_queue_at_once() -> None:
    """Höchstens ein Versuch je Stufe gleichzeitig (single-flight): sonst
    belegte ein hängendes Anlegen bis zu ``concurrency`` Slots."""
    rabbit = FakeRabbit()
    now = [1_000.0]
    broker = await _tier_redeclaration_failed_once(rabbit, now)
    now[0] += 30.0
    release = _hanging_channels(broker)
    opened = asyncio.Event()
    hanging = cast("AsyncMock", broker._connection).channel.side_effect

    async def channel_opened(**kwargs: object) -> AsyncMock:
        opened.set()
        return cast("AsyncMock", await hanging(**kwargs))

    cast("AsyncMock", broker._connection).channel = AsyncMock(side_effect=channel_opened)
    first = asyncio.create_task(broker.retry_later(_delivery(), delay_ms=500, headers={}))
    await opened.wait()

    await broker.retry_later(_delivery(), delay_ms=500, headers={})

    assert _routing_keys(broker) == ["tasks.DQ.1s", "tasks.DQ", "tasks.DQ"]
    assert cast("AsyncMock", broker._connection).channel.await_count == 1
    release.set()
    await first


@pytest.mark.befund("PF-21")
async def test_a_shorter_shutdown_deadline_bounds_the_attempt() -> None:
    """Beim Herunterfahren gilt die kürzere Restfrist; nach ihr geht auch
    der Rückfall an ``{queue}.DQ`` nicht mehr auf die Leitung (FS-02), das
    Original bleibt unbestätigt."""
    rabbit = FakeRabbit()
    broker = make_broker(rabbit)
    await broker.declare_queue("tasks")
    cast("AsyncMock", broker._publish_channel).default_exchange.publish.side_effect = [_returned("tasks.DQ.1s"), None]
    broker.publish_timeout_s = 60.0
    broker.declare_probe_timeout_s = 60.0
    _hanging_channels(broker)
    broker.set_publish_deadline(asyncio.get_running_loop().time() + 0.05)
    delivery = _delivery()

    with pytest.raises(BrokerConnectionError, match="shutdown publish deadline"):
        await broker.retry_later(delivery, delay_ms=500, headers={})

    assert _routing_keys(broker) == ["tasks.DQ.1s"]
    cast("AsyncMock", delivery.handle).ack.assert_not_awaited()


@pytest.mark.entscheidung("E-026")
@pytest.mark.befund("PF-21")
async def test_strict_mode_mismatch_while_running_refuses_the_tier_instead_of_failing(swarmq_logs) -> None:
    """Ein anderer Prozess hat die Stufe in der Pause at-most-once angelegt.
    Im Betrieb gilt das wie eine Ablehnung (E-026), nicht fatal; Exit 78
    kommt beim nächsten Start (E-017). Danach kein Versuch mehr."""
    rabbit = FakeRabbit()
    now = [1_000.0]
    broker = await _tier_redeclaration_failed_once(rabbit, now, dead_letter_strategy="at-least-once")
    rabbit.queues["tasks.DQ.1s"] = legacy("tasks")["tasks.DQ"]
    now[0] += 30.0

    await broker.retry_later(_delivery(), delay_ms=500, headers={})
    chains_after_refusal = len(_chains(rabbit))
    now[0] += 30.0
    await broker.retry_later(_delivery(), delay_ms=500, headers={})

    assert _routing_keys(broker) == ["tasks.DQ.1s", "tasks.DQ", "tasks.DQ", "tasks.DQ"]
    assert len(_chains(rabbit)) == chains_after_refusal
    assert broker.delay_tiers_unavailable("tasks") == 1
    [entry] = [e for e in swarmq_logs if e["event"] == "delay_tier_unavailable"]
    assert (entry["tier"], entry["error_type"]) == ("tasks.DQ.1s", "QueueArgumentsMismatch")
    assert rabbit.queues["tasks.DQ.1s"] == legacy("tasks")["tasks.DQ"]


@pytest.mark.befund("PF-21")
async def test_a_refusal_after_the_pause_switches_the_tier_off_without_further_attempts(swarmq_logs) -> None:
    rabbit = FakeRabbit()
    now = [1_000.0]
    broker = await _tier_redeclaration_failed_once(rabbit, now)

    async def refusing_channel(**_: object) -> AsyncMock:
        channel = AsyncMock(is_closed=False)
        channel.declare_queue.side_effect = ChannelAccessRefused("ACCESS_REFUSED - access to queue 'tasks.DQ.1s'")
        return channel

    connection = cast("AsyncMock", broker._connection)
    connection.channel = AsyncMock(side_effect=refusing_channel)
    now[0] += 30.0
    await broker.retry_later(_delivery(), delay_ms=500, headers={})
    opened_after_refusal = connection.channel.await_count
    now[0] += 30.0
    await broker.retry_later(_delivery(), delay_ms=500, headers={})

    assert connection.channel.await_count == opened_after_refusal
    assert _routing_keys(broker) == ["tasks.DQ.1s", "tasks.DQ", "tasks.DQ", "tasks.DQ"]
    assert broker.delay_tiers_unavailable("tasks") == 1
    assert [e["tier"] for e in swarmq_logs if e["event"] == "delay_tier_unavailable"] == ["tasks.DQ.1s"]


@pytest.mark.befund("PF-21")
async def test_declaring_the_family_again_restores_a_pending_tier(swarmq_logs) -> None:
    """Die Familien-Deklaration (nach einem unroutbaren Publish an ``.DQ``
    oder ``.XQ``) deklariert eine ausstehende Stufe mit und heilt sie; danach
    kein eigener Versuch mehr."""
    rabbit = FakeRabbit()
    now = [1_000.0]
    broker = await _tier_redeclaration_failed_once(rabbit, now)
    broker.invalidate_declaration("tasks")
    await broker.declare_queue("tasks")
    chains_after_family = len(_chains(rabbit))
    now[0] += 30.0

    await broker.retry_later(_delivery(), delay_ms=500, headers={})

    assert len(_chains(rabbit)) == chains_after_family
    assert _routing_keys(broker) == ["tasks.DQ.1s", "tasks.DQ", "tasks.DQ.1s"]
    assert [e["tier"] for e in swarmq_logs if e["event"] == "delay_tier_restored"] == ["tasks.DQ.1s"]


@pytest.mark.befund("PF-21")
async def test_a_closed_connection_fails_the_attempt_without_declaring_on_the_publish_channel(swarmq_logs) -> None:
    rabbit = FakeRabbit()
    broker = make_broker(rabbit)
    await broker.declare_queue("tasks")
    publish_channel = cast("AsyncMock", broker._publish_channel)
    publish_channel.default_exchange.publish.side_effect = [_returned("tasks.DQ.1s"), None]
    cast("AsyncMock", broker._connection).is_closed = True
    delivery = _delivery()

    await broker.retry_later(delivery, delay_ms=500, headers={})

    publish_channel.declare_queue.assert_not_awaited()
    assert _routing_keys(broker) == ["tasks.DQ.1s", "tasks.DQ"]
    [entry] = [e for e in swarmq_logs if e["event"] == "delay_tier_redeclare_failed"]
    assert entry["error_type"] == "BrokerConnectionError"


@pytest.mark.befund("PF-21")
async def test_a_deferral_returned_during_the_pause_makes_no_attempt(swarmq_logs) -> None:
    """Review P3-2: Rückstellungen, die ihr Ziel noch bei bereiter Stufe
    gewählt haben, kommen erst nach dem gescheiterten Versuch unroutbar
    zurück; sie gehen ohne zweiten Versuch und ohne zweite WARNING an ``.DQ``."""
    rabbit = FakeRabbit()
    now = [1_000.0]
    broker = await _tier_redeclaration_failed_once(rabbit, now)
    opened = cast("AsyncMock", broker._connection).channel

    assert await broker.restore_delay_tier("tasks.DQ.1s") is False

    opened.assert_not_awaited()
    assert len([e for e in swarmq_logs if e["event"] == "delay_tier_redeclare_failed"]) == 1


@pytest.mark.befund("PF-21")
async def test_a_cancelled_attempt_frees_the_tier_for_the_next_attempt() -> None:
    rabbit = FakeRabbit()
    now = [1_000.0]
    broker = await _tier_redeclaration_failed_once(rabbit, now)
    now[0] += 30.0
    release = _hanging_channels(broker)
    opened = asyncio.Event()
    hanging = cast("AsyncMock", broker._connection).channel.side_effect

    async def channel_opened(**kwargs: object) -> AsyncMock:
        opened.set()
        return cast("AsyncMock", await hanging(**kwargs))

    cast("AsyncMock", broker._connection).channel = AsyncMock(side_effect=channel_opened)
    attempt = asyncio.create_task(broker.restore_delay_tier("tasks.DQ.1s"))
    await opened.wait()
    attempt.cancel()
    with pytest.raises(asyncio.CancelledError):
        await attempt
    release.set()
    cast("AsyncMock", broker._connection).channel = AsyncMock(side_effect=rabbit.open)

    assert await broker.restore_delay_tier("tasks.DQ.1s") is True


@pytest.mark.befund("PF-21")
async def test_the_hint_for_a_strict_mode_mismatch_while_running_names_the_exit_on_restart(swarmq_logs) -> None:
    """Review P3-4: Ein Neustart endet hier mit Exit 78; der allgemeine
    Hinweis „restart the worker“ führte in die Irre."""
    rabbit = FakeRabbit()
    now = [1_000.0]
    broker = await _tier_redeclaration_failed_once(rabbit, now, dead_letter_strategy="at-least-once")
    rabbit.queues["tasks.DQ.1s"] = legacy("tasks")["tasks.DQ"]
    now[0] += 30.0

    await broker.retry_later(_delivery(), delay_ms=500, headers={})

    [entry] = [e for e in swarmq_logs if e["event"] == "delay_tier_unavailable"]
    assert "78" in entry["hint"]
    assert "then restart the worker" not in entry["hint"]


async def test_family_not_declared_by_this_process_uses_the_delay_queue() -> None:
    rabbit = FakeRabbit()
    broker = make_broker(rabbit)

    await broker.retry_later(_delivery(), delay_ms=500, headers={})

    assert _routing_keys(broker) == ["tasks.DQ"]
    assert rabbit.channels == []


@pytest.mark.entscheidung("E-026")
async def test_delay_over_one_hour_stays_in_the_delay_queue() -> None:
    rabbit = FakeRabbit()
    broker = make_broker(rabbit)
    await broker.declare_queue("tasks")

    await broker.retry_later(_delivery(), delay_ms=3_600_001, headers={})

    assert _routing_keys(broker) == ["tasks.DQ"]


@pytest.mark.parametrize(
    ("queue_type", "strategy", "extra"),
    [
        ("quorum", "auto", {"x-dead-letter-strategy": "at-least-once", "x-overflow": "reject-publish"}),
        ("quorum", "legacy", {}),
        ("classic", "auto", {}),
    ],
)
async def test_tier_gets_the_arguments_of_the_delay_queue(
    queue_type: str, strategy: str, extra: dict[str, Any]
) -> None:
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


# --- Review MR 10 ----------------------------------------------------------------


async def test_tiers_are_declared_at_start_and_retry_later_opens_no_channel() -> None:
    """Review MR 10, P1-2: Rechte- und Limit-Fehler zeigen sich beim Start,
    nicht Tage spaeter beim ersten Retry; der Hot-Path kennt keine Kette."""
    rabbit = FakeRabbit()
    broker = make_broker(rabbit)

    await broker.declare_queue("tasks")

    assert [tier for tier in delay_tiers("tasks") if tier in rabbit.queues] == list(delay_tiers("tasks"))
    assert all(rabbit.queues[tier] == rabbit.queues["tasks.DQ"] for tier in delay_tiers("tasks"))
    opened = len(rabbit.channels)
    await broker.retry_later(_delivery(), delay_ms=500, headers={})
    assert _routing_keys(broker) == ["tasks.DQ.1s"]
    assert len(rabbit.channels) == opened


async def test_strict_mode_refuses_an_existing_at_most_once_tier() -> None:
    """Review MR 10, P2-4: der strenge Modus faellt bei einer Stufe nicht
    still auf ``{queue}.DQ`` zurueck; eine at-most-once-Stufe ist derselbe
    Konfigurationsfehler wie eine at-most-once-Bestandsqueue."""
    rabbit = FakeRabbit({"tasks.DQ.4s": legacy("tasks")["tasks.DQ"]})
    broker = make_broker(rabbit, dead_letter_strategy="at-least-once")

    with pytest.raises(QueueArgumentsMismatch) as exc_info:
        await broker.declare_queue("tasks")

    assert exc_info.value.queue == "tasks.DQ.4s"
    assert exc_info.value.argument == "x-dead-letter-strategy"
    assert rabbit.queues["tasks.DQ.4s"] == legacy("tasks")["tasks.DQ"]


async def test_adopted_at_most_once_tier_is_reported_with_the_family(swarmq_logs) -> None:
    rabbit = FakeRabbit({"tasks.DQ.4s": legacy("tasks")["tasks.DQ"]})
    broker = make_broker(rabbit)
    await broker.declare_queue("tasks")

    await broker.report_queue_declarations()

    [warning] = [e for e in swarmq_logs if e["event"] == "queue_dead_letter_at_most_once"]
    assert warning["queues"] == ["tasks.DQ.4s"]


async def test_unroutable_tier_redeclares_only_the_tier_and_names_it(swarmq_logs) -> None:
    """Review MR 10, P2-3: die Familie neu zu deklarieren kostete im
    schlimmsten Fall mehrere Publish-Fristen, und die WARNING nannte die
    Haupt-Queue statt der geloeschten Stufe."""
    rabbit = FakeRabbit()
    broker = make_broker(rabbit)
    await broker.declare_queue("tasks")
    del rabbit.queues["tasks.DQ.1s"]
    cast("AsyncMock", broker._publish_channel).default_exchange.publish.side_effect = [_returned("tasks.DQ.1s"), None]
    declared_at_start = len(_chains(rabbit))

    await broker.retry_later(_delivery(), delay_ms=500, headers={})

    assert _chains(rabbit)[declared_at_start:] == ["tasks.DQ.1s"]
    assert _routing_keys(broker) == ["tasks.DQ.1s", "tasks.DQ.1s"]
    assert "tasks.DQ.1s" in rabbit.queues
    [entry] = [e for e in swarmq_logs if e["event"] == "own_queue_unroutable_redeclaring"]
    assert entry["queue"] == "tasks.DQ.1s"


@pytest.mark.entscheidung("E-026")
async def test_tier_refused_after_an_unroutable_publish_sends_this_and_later_deferrals_to_the_delay_queue(
    swarmq_logs,
) -> None:
    """E-16: Die unroutbare Stufe verliert ihren Eintrag; scheitert ihr
    Neu-Deklarieren an einem Kanalfehler (403), gehen diese und jede weitere
    Rückstellung an ``{queue}.DQ``. Gegenprobe ``declarations.py:250``
    ``discard(name)`` -> ``discard(None)``: rot."""
    rabbit = FakeRabbit()
    broker = make_broker(rabbit)
    await broker.declare_queue("tasks")
    cast("AsyncMock", broker._publish_channel).default_exchange.publish.side_effect = [
        _returned("tasks.DQ.1s"),
        None,
        None,
    ]

    async def refusing_channel(**_: object) -> AsyncMock:
        channel = AsyncMock(is_closed=False)
        channel.declare_queue.side_effect = ChannelAccessRefused("ACCESS_REFUSED - access to queue 'tasks.DQ.1s'")
        return channel

    cast("AsyncMock", broker._connection).channel = AsyncMock(side_effect=refusing_channel)
    first, second = _delivery(), _delivery()

    await broker.retry_later(first, delay_ms=500, headers={})
    await broker.retry_later(second, delay_ms=500, headers={})

    assert _routing_keys(broker) == ["tasks.DQ.1s", "tasks.DQ", "tasks.DQ"]
    assert broker.delay_tiers_unavailable("tasks") == 1
    assert [e["tier"] for e in swarmq_logs if e["event"] == "delay_tier_unavailable"] == ["tasks.DQ.1s"]
    cast("AsyncMock", first.handle).ack.assert_awaited_once()
    cast("AsyncMock", second.handle).ack.assert_awaited_once()


@pytest.mark.entscheidung("E-026")
async def test_declaring_the_family_again_skips_ready_and_refused_tiers() -> None:
    """E-18: Nach dem Verwerfen des Familien-Eintrags deklariert die nächste
    Deklaration die Familie neu (die Kette mit passivem Declare zeigt es für
    Haupt- und Delay-Queue), die Stufen aber
    nicht: bereite bleiben bereit, abgelehnte bleiben aus. Gegenprobe
    ``declarations.py:116`` ``… and tier not in self._unavailable_delay_tiers``
    -> ``or``: rot."""
    rabbit = FakeRabbit({"tasks.DQ.4s": {"x-queue-type": "classic"}})
    broker = make_broker(rabbit)
    await broker.declare_queue("tasks")

    broker.invalidate_declaration("tasks")
    await broker.declare_queue("tasks")

    chains = _chains(rabbit)
    assert [chains.count(name) for name in ("tasks", "tasks.DQ")] == [2, 2]
    assert {tier: chains.count(tier) for tier in delay_tiers("tasks")} == dict.fromkeys(delay_tiers("tasks"), 1)
    assert broker.delay_tiers_unavailable("tasks") == 1
