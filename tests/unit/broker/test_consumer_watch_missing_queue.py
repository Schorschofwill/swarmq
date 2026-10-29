"""Consumer-Wächter: gelöschte konsumierte Queue (PF-20, Option (b), E-083).

Vor jeder Neuanmeldung prüft der Wächter passiv (``queue_exists``, beim Broker
ein Wegwerf-Kanal), ob die Queue noch existiert. Fehlt sie, gibt es kein
``basic.consume``: Es scheiterte an NOT_FOUND und schlösse den gemeinsamen
Consume-Kanal (E-019). Das echte Broker-Verhalten steht in
``tests/integration/broker/test_consumed_queue_deleted.py``.

Test-Liste:
# [x] Queue fehlt → kein consume, ERROR ``consumer_queue_missing`` einmal
# [x] Queue fehlt → raus aus der Restore-Liste des Kanals (kein NOT_FOUND im Reconnect)
# [x] Prüfung scheitert (Verbindung) → ``consumer_reregister_failed``, nächster Takt prüft erneut
# [x] Queue wieder da → zurück in die Restore-Liste, Neuanmeldung mit Grund ``queue_recreated``
# [x] ``missing_queues``: fehlende Queues für ``/ready``, leer nach der Rückkehr
# [x] Broker: prüft passiv auf einem Wegwerf-Kanal, NOT_FOUND heißt fehlt; ``consumer_missing_queues``
# [x] hängende Prüfung → endet nach ``timeout_s`` (Wegwerf-Kanal, nicht der Consume-Kanal), nächster Takt
# [x] Broker ohne Verbindung → Prüfung scheitert mit ``BrokerConnectionError``, gilt nicht als fehlend
# [x] Broker: hängendes Declare, hängendes Close → der Takt endet nach höchstens
#     zweimal ``_RPC_TIMEOUT_S`` (Prüfung, dann Schließen; Review 0.14, P4)
# [x] Broker: Abbruch während des Kanal-Öffnens → der Kanal wird nach dem Öffnen
#     geschlossen, kein Eintrag bleibt offen (Review 0.14, P4)
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock

import pytest
from aio_pika.exceptions import ChannelNotFoundEntity

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.broker.rabbitmq.consumer_watch import _RPC_TIMEOUT_S, ConsumerWatch
from tests.support.consumer_fakes import FakeChannel, FakeQueue, FakeUnderlay
from tests.support.virtual_time import run_virtual

pytestmark = pytest.mark.timeout(5)


class _Existence:
    """``queue_exists``-Fake: welche Queues es beim Broker gibt."""

    def __init__(self, *queues: str) -> None:
        self.queues = set(queues)
        self.asked: list[str] = []

        self.error: Exception | None = None

    async def __call__(self, name: str) -> bool:
        self.asked.append(name)
        if self.error is not None:
            raise self.error
        return name in self.queues


def _watched(existence: _Existence, name: str = "q1", tag: str = "tag-1") -> tuple[ConsumerWatch, FakeQueue, Any]:
    queue = FakeQueue(name, FakeChannel(FakeUnderlay(tag)))
    callback = object()
    watch = ConsumerWatch(queue_exists=existence)
    watch.add(name, queue, callback, tag)
    return watch, queue, callback


async def _delete(watch: ConsumerWatch, queue: FakeQueue, existence: _Existence) -> None:
    """Löscht die Queue beim Broker: ``basic.cancel`` auf offenem Kanal."""
    await watch.tick()
    existence.queues.discard(queue.name)
    queue.channel.underlay.consumers.clear()


@pytest.mark.entscheidung("E-083")
async def test_missing_queue_gets_no_consume_and_one_error(swarmq_logs):
    existence = _Existence("q1")
    watch, queue, _ = _watched(existence)
    await _delete(watch, queue, existence)

    await watch.tick()
    await watch.tick()

    assert queue.consumed == []
    assert queue.cancelled == ["tag-1"]
    [missing] = [e for e in swarmq_logs if e["event"] == "consumer_queue_missing"]
    assert missing["log_level"] == "error"
    assert missing["logger"] == "swarmq.broker.rabbitmq.consumer_watch"
    assert missing["queue"] == "q1"
    assert [e for e in swarmq_logs if e["event"] == "consumer_reregister_failed"] == []


@pytest.mark.entscheidung("E-083")
async def test_missing_queue_leaves_the_restore_list_of_the_channel():
    existence = _Existence("q1")
    watch, queue, _ = _watched(existence)
    await _delete(watch, queue, existence)

    await watch.tick()

    assert "q1" not in queue.channel._queues


async def test_a_failing_existence_check_is_logged_and_retried(swarmq_logs):
    existence = _Existence("q1")
    watch, queue, callback = _watched(existence)
    await watch.tick()
    queue.channel.underlay.consumers.clear()
    existence.error = ConnectionError("channel open failed")

    await watch.tick()

    assert queue.consumed == []
    [failed] = [e for e in swarmq_logs if e["event"] == "consumer_reregister_failed"]
    assert failed["queue"] == "q1"
    assert failed["error_type"] == "ConnectionError"
    assert [e for e in swarmq_logs if e["event"] == "consumer_queue_missing"] == []

    existence.error = None
    await watch.tick()

    assert queue.consumed == [callback]


@pytest.mark.entscheidung("E-083")
async def test_a_recreated_queue_is_readmitted_and_consumed_again(swarmq_logs):
    existence = _Existence("q1")
    watch, queue, callback = _watched(existence)
    counted: list[tuple[str, str]] = []
    watch.on_reregistered = lambda queue_name, reason: counted.append((queue_name, reason))
    await _delete(watch, queue, existence)
    await watch.tick()

    existence.queues.add("q1")
    await watch.tick()

    assert queue.consumed == [callback]
    assert queue.channel._queues["q1"] == {queue}
    assert watch.tags() == [("q1", "q1-new-1")]
    [again] = [e for e in swarmq_logs if e["event"] == "consumer_reregistered"]
    assert again["reason"] == "queue_recreated"
    assert again["old_consumer_tag"] == "tag-1"
    assert counted == [("q1", "queue_recreated")]


@pytest.mark.entscheidung("E-083")
async def test_missing_queues_name_the_deleted_queue_until_it_returns():
    existence = _Existence("q1", "q2")
    watch, queue, _ = _watched(existence)
    other = FakeQueue("q2", FakeChannel(FakeUnderlay("tag-2")))
    watch.add("q2", other, object(), "tag-2")
    assert watch.missing_queues() == []
    await _delete(watch, queue, existence)

    await watch.tick()
    assert watch.missing_queues() == ["q1"]

    existence.queues.add("q1")
    await watch.tick()
    assert watch.missing_queues() == []


def _broker_with(queue: FakeQueue) -> tuple[RabbitMQBroker, AsyncMock]:
    """Broker mit einer Fake-Queue; ``throwaway`` ist der Wegwerf-Kanal der Prüfung."""
    broker = RabbitMQBroker("amqp://guest:guest@localhost/")
    broker._connection = AsyncMock()
    throwaway = AsyncMock()
    broker._connection.channel.return_value = throwaway
    broker._consume_channel = AsyncMock(is_closed=False)
    broker._consume_channel.get_queue.return_value = queue
    return broker, throwaway


@pytest.mark.entscheidung("E-083")
async def test_the_broker_checks_passively_on_a_throwaway_channel():
    queue = FakeQueue("q1", FakeChannel(FakeUnderlay()))
    broker, throwaway = _broker_with(queue)
    await broker.consume("q1", AsyncMock(), concurrency=4)
    await broker.consumer_watch.tick()
    throwaway.declare_queue.side_effect = ChannelNotFoundEntity("NOT_FOUND - no queue 'q1'")
    queue.channel.underlay.consumers.clear()

    await broker.consumer_watch.tick()

    throwaway.declare_queue.assert_awaited_once_with("q1", passive=True)
    throwaway.close.assert_awaited_once()
    assert len(queue.consumed) == 1, "only the consume from consume(), no basic.consume on the missing queue"
    assert broker.consumer_missing_queues() == ["q1"]

    throwaway.declare_queue.side_effect = None
    await broker.consumer_watch.tick()

    assert len(queue.consumed) == 2
    assert broker.consumer_missing_queues() == []


async def test_a_hanging_existence_check_ends_after_the_channel_timeout(swarmq_logs):
    released = asyncio.Event()

    async def hanging(name: str) -> bool:
        await released.wait()
        return True

    queue = FakeQueue("q1", FakeChannel(FakeUnderlay("tag-1")))
    watch = ConsumerWatch(timeout_s=0.01, queue_exists=hanging)
    watch.add("q1", queue, object(), "tag-1")
    await watch.tick()
    queue.channel.underlay.consumers.clear()

    await watch.tick()

    assert queue.consumed == []
    [failed] = [e for e in swarmq_logs if e["event"] == "consumer_reregister_failed"]
    assert failed["error_type"] == "TimeoutError"


async def test_the_existence_check_of_an_unconnected_broker_fails(swarmq_logs):
    queue = FakeQueue("q1", FakeChannel(FakeUnderlay()))
    broker, _ = _broker_with(queue)
    await broker.consume("q1", AsyncMock(), concurrency=4)
    await broker.consumer_watch.tick()
    broker._connection = None  # Verbindung weg, der Kanal des Fakes antwortet noch
    queue.channel.underlay.consumers.clear()

    await broker.consumer_watch.tick()

    [failed] = [e for e in swarmq_logs if e["event"] == "consumer_reregister_failed"]
    assert failed["error_type"] == "BrokerConnectionError"
    assert broker.consumer_missing_queues() == []


async def _hang(*_args: Any, **_kwargs: Any) -> None:
    await asyncio.Event().wait()


def test_a_hanging_check_whose_close_hangs_too_ends_after_the_close_deadline() -> None:
    """Hängt das passive Declare (Alarm mit ``shared``, halboffenes TCP), hängt
    auch das Schließen des Wegwerf-Kanals. Der Takt endet trotzdem: Prüfung und
    Schließen sind je auf ``_RPC_TIMEOUT_S`` begrenzt (virtuelle Uhr). Die
    Frist trifft nur das Warten, nie das Close selbst (F4-30)."""
    aborted: list[object] = []

    async def hanging_close(*_args: Any, **_kwargs: Any) -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            aborted.append(object())
            raise

    async def main() -> float:
        queue = FakeQueue("q1", FakeChannel(FakeUnderlay()))
        broker, throwaway = _broker_with(queue)
        await broker.consume("q1", AsyncMock(), concurrency=4)
        await broker.consumer_watch.tick()
        throwaway.declare_queue.side_effect = _hang
        throwaway.close.side_effect = hanging_close
        queue.channel.underlay.consumers.clear()
        loop = asyncio.get_running_loop()
        start = loop.time()

        await broker.consumer_watch.tick()

        return loop.time() - start

    assert run_virtual(main) <= 2 * _RPC_TIMEOUT_S
    assert aborted == []


def test_a_check_aborted_while_the_channel_opens_closes_the_channel_once_it_is_open() -> None:
    """Ein Abbruch mitten im Öffnen ließe den Kanal halb offen zurück (aiormq
    fängt ``CancelledError`` dort nicht und behält den Eintrag). Das Öffnen
    läuft deshalb zu Ende, und der Kanal wird danach geschlossen, binnen der
    Frist des Schließens."""

    async def main() -> AsyncMock:
        queue = FakeQueue("q1", FakeChannel(FakeUnderlay()))
        broker, throwaway = _broker_with(queue)
        await broker.consume("q1", AsyncMock(), concurrency=4)
        await broker.consumer_watch.tick()

        async def slow_open(*_args: Any, **_kwargs: Any) -> AsyncMock:
            await asyncio.sleep(1.5 * _RPC_TIMEOUT_S)
            return throwaway

        broker._connection.channel.side_effect = slow_open  # type: ignore[union-attr]  # AsyncMock aus _broker_with
        queue.channel.underlay.consumers.clear()

        await broker.consumer_watch.tick()

        return throwaway

    throwaway = run_virtual(main)
    throwaway.declare_queue.assert_not_awaited()
    throwaway.close.assert_awaited_once()
