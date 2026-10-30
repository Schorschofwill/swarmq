"""Charakterisierung: Consumer-Wiederherstellung von aio-pika 10.2 auf RabbitMQ 4.3.6 und 4.2.9 (Plan 2026-10-29-1945, U3, KTD7).

Grün und ohne Produktionscode: Die Tests halten fest, was aio-pika 10.2.1 mit
aiormq 7.2.4 auf den gepinnten Testbrokern tut, wenn der Broker einen
Consumer beendet. Neu gegenüber aio-pika 10.0: ``RobustChannel`` hängt
``_on_consumer_cancel`` an den aiormq-Kanal. Ein ``basic.cancel`` des Brokers
weckt die Verbindungsfabrik (``_request_recovery``); sie läuft im Takt
``reconnect_interval`` (Standard 5 s, hier per URL verkürzt) und ruft
``_restore_consumers``: Für jeden Tag aus ``RobustQueue._consumers``, der im
aiormq-Kanal fehlt, öffnet sie einen Probe-Kanal (aiormq, ``timeout=5``),
deklariert die Queue mit deren Original-Parametern (``RobustQueue._probe``),
schließt den Probe-Kanal und meldet mit demselben Tag neu an
(``Queue.consume(timeout=5)``). Bricht ein Kanal-Close der Wiederherstellung
ab, kommt das ``recovery.cancel()`` aus ``_restore_consumers``.

Beobachtet (je mindestens drei Läufe):

* 4.3.6, Consumer-Timeout ohne swarmq: Der Broker beendet den Consumer mit
  ``basic.cancel``, behält dessen Tag aber auf dem Kanal (Zombie, siehe
  ``test_consumer_timeout.py``). aio-pikas Wiederanmeldung mit demselben Tag
  beantwortet RabbitMQ mit ``Connection.Close(530 NOT_ALLOWED - attempt to
  reuse consumer tag)``: Die ganze Verbindung stirbt. Nach dem Reconnect legt
  der Restore den Tag auf der neuen Verbindung an; danach genau ein Consumer.
  aiormq beantwortet das ``basic.cancel`` hier nicht mit ``CancelOk``, weil
  4.3.6 ``accept_consumer_cancel_ok`` nicht meldet (erst ab 4.3.7).
* 4.3.6 mit swarmq-Wächter: Kommt der Wächter zuerst (eigener Cancel, neuer
  Tag), findet aio-pika keinen fehlenden Tag; kein Probe-Kanal, genau ein
  Consumer, die Verbindung bleibt. Kommt aio-pika zuerst, stirbt die
  Consume-Verbindung wie oben mit 530; danach genau ein Consumer, kein
  zweiter. Beide Reihenfolgen sind hier über ``reconnect_interval`` und den
  Takt des Wächters erzwungen. Mit den Standardwerten (beide 5 s) entscheidet
  die Phase der beiden Takte; aus dem Code gelesen gewinnt jede Seite etwa
  jedes zweite Mal.
* 4.2.9, Consumer-Timeout: Der Broker schließt den Kanal statt ``basic.cancel``
  zu schicken; der Restore legt den Tag auf einem neuen Kanal an. Kein
  Probe-Kanal, die Verbindung bleibt, genau ein Consumer.
* Gelöschte konsumierte Queue (4.3.6 und 4.2.9, swarmq): Der Broker schickt
  ``basic.cancel``; aio-pika öffnet je ``reconnect_interval`` einen
  Probe-Kanal, der passive Probe (swarmq holt die Queue mit ``get_queue``)
  scheitert an NOT_FOUND, die Queue wird nicht neu angelegt. Das geht weiter,
  bis der Takt des Wächters den Tag abmeldet und ``release_queue`` greift;
  danach kein Probe-Kanal mehr. Zahl der Probe-Kanäle also etwa
  ``Wartezeit bis zum Takt / reconnect_interval``; mit den Standardwerten
  (beide 5 s) aus dem Code geschätzt 0 bis 1, gemessen nur mit 0,2 s.
* Wiederanmeldung, deren ``consume`` in die Frist (5 s) läuft (Antwort per
  Patch auf der Client-Seite zurückgehalten, ausgelöst über einen eigenen
  ``basic_cancel`` und ``_request_recovery``): aiormq schickt
  ``Channel.Close(504)`` auf dem Consume-Kanal, der Kanal und mit ihm alle
  Consumer darauf fallen weg, bis der Restore sie neu anlegt (widerspricht
  E-053). Die Verbindung bleibt.
* Der Consume-Kanal schließt, während die Wiederherstellung ihren Probe-Kanal
  schließt (``CloseOk`` des Probe-Kanals per Patch zurückgehalten):
  ``recovery.cancel()`` bricht ``probe.close()`` ab, aiormq schickt ein
  zweites ``Channel.Close(504)`` auf die schon freie Nummer, und RabbitMQ
  beendet die ganze Verbindung mit ``504 CHANNEL_ERROR - expected
  'channel.open'`` (wie F4-30, R1).

Test-Liste:
# [x] 4.3.6 ohne swarmq: Timeout -> aio-pika meldet denselben Tag neu an ->
#     Connection.Close(530 reuse consumer tag) -> nach Reconnect ein Consumer
# [x] 4.2.9 ohne swarmq: Timeout schließt den Kanal, kein Probe-Kanal, ein Consumer
# [x] 4.3.6 mit Wächter zuerst: kein Probe-Kanal, ein Consumer, Verbindung bleibt
# [x] 4.3.6 mit aio-pika zuerst: Consume-Verbindung stirbt mit 530, danach ein Consumer
# [x] gelöschte Queue (4.3.6, 4.2.9): Probe-Kanäle je reconnect_interval bis
#     release_queue, keine Neudeklaration, danach keine Probe mehr
# [x] consume der Wiederanmeldung läuft in die Frist: Channel.Close(504) auf dem
#     Consume-Kanal, Verbindung bleibt (4.3.6, 4.2.9)
# [x] Probe-Close abgebrochen: Verbindung stirbt mit 504 expected 'channel.open'
#     (4.3.6, 4.2.9)
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from contextlib import AsyncExitStack
from dataclasses import dataclass
from typing import Any, override

import aio_pika
import aiormq
import httpx
import pytest
from aio_pika.abc import AbstractIncomingMessage, AbstractRobustQueue
from aio_pika.robust_queue import RobustQueue
from aiormq.exceptions import ConnectionChannelError, ConnectionNotAllowed

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.contracts import Delivery
from tests.support.known_leaks import aiormq_streamwriter_leak
from tests.support.queues import delete_queue_family
from tests.support.services import RABBITMQ_42, RABBITMQ_MAIN, RabbitMQNode, require_rabbitmq
from tests.support.wait import wait_until
from tests.unit._log_capture import capture_swarmq_logs

pytestmark = [pytest.mark.integration, pytest.mark.timeout(60)]

CONSUMER_TIMEOUT_MS = 3000
# Obergrenze für Cancel/Close nach dem Timeout: Timeout plus Prüftakt plus Luft.
TIMEOUT_DEADLINE_S = 12.0
EVENT_DEADLINE_S = 5.0
# aio-pikas Frist für Probe-Kanal und Wiederanmeldung (``timeout=5``) plus Luft.
RECOVERY_RPC_DEADLINE_S = 10.0
FAST_RECOVERY = "reconnect_interval=0.2"
# Länger als jedes Testfenster: aio-pikas Fabrik kommt nicht von selbst zum Zug.
NO_RECOVERY = "reconnect_interval=60"


@pytest.fixture
def rabbitmq43() -> RabbitMQNode:
    return require_rabbitmq(RABBITMQ_MAIN, "4.3.")


@pytest.fixture
def rabbitmq42() -> RabbitMQNode:
    return require_rabbitmq(RABBITMQ_42, "4.2.")


@pytest.fixture(params=["4.2", "4.3"])
def any_rabbitmq(request: pytest.FixtureRequest) -> RabbitMQNode:
    if request.param == "4.2":
        return require_rabbitmq(RABBITMQ_42, "4.2.")
    return require_rabbitmq(RABBITMQ_MAIN, "4.3.")


def _url(node: RabbitMQNode, query: str) -> str:
    return f"{node.amqp_url}?{query}"


def _aiormq_connection(connection: aio_pika.abc.AbstractConnection) -> aiormq.Connection:
    transport = connection.transport
    assert transport is not None
    underlay = transport.connection
    assert isinstance(underlay, aiormq.Connection)
    return underlay


async def _consumer_count(node: RabbitMQNode, queue: str) -> int:
    """Echtzeit-Zahl aus ``queue.declare-ok`` (ohne Statistik-Verzug)."""
    connection = await aio_pika.connect(node.amqp_url)
    async with connection:
        channel = await connection.channel()
        declared = await channel.declare_queue(queue, passive=True)
        count = declared.declaration_result.consumer_count
        assert count is not None
        return int(count)


async def _queue_exists(node: RabbitMQNode, queue: str) -> bool:
    connection = await aio_pika.connect(node.amqp_url)
    async with connection:
        channel = await connection.channel()
        try:
            await channel.declare_queue(queue, passive=True)
        except aio_pika.exceptions.ChannelNotFoundEntity:
            return False
        return True


async def _delete_queue(node: RabbitMQNode, queue: str) -> None:
    """Löscht *queue* über eine fremde Verbindung, wie ein Betreiber."""
    connection = await aio_pika.connect(node.amqp_url)
    async with connection:
        channel = await connection.channel()
        await channel.queue_delete(queue)


def _management_consumer_tags(node: RabbitMQNode, queue: str) -> set[str]:
    response = httpx.get(f"{node.management_url}/consumers/{node.vhost_api}", auth=("guest", "guest"), timeout=5.0)
    response.raise_for_status()
    return {c["consumer_tag"] for c in response.json() if c["queue"]["name"] == queue}


@pytest.fixture
def probes(monkeypatch: pytest.MonkeyPatch) -> list[aiormq.abc.AbstractChannel]:
    """Jeder Probe-Kanal, auf dem aio-pikas Wiederherstellung eine Queue prüft."""
    seen: list[aiormq.abc.AbstractChannel] = []
    original = RobustQueue._probe

    async def counting(self: RobustQueue, channel: aiormq.abc.AbstractChannel) -> None:
        seen.append(channel)
        await original(self, channel)

    monkeypatch.setattr(RobustQueue, "_probe", counting)
    return seen


class _Records(logging.Handler):
    def __init__(self) -> None:
        super().__init__(logging.DEBUG)
        self.messages: list[str] = []

    @override
    def emit(self, record: logging.LogRecord) -> None:
        # Ohne ``repr`` der Argumente: aio-pikas ``OneShotCallback`` scheitert
        # am eigenen ``repr``, sobald der Callback gelaufen ist.
        args = record.args if isinstance(record.args, tuple) else ()
        self.messages.append(" ".join([str(record.msg), *(_safe_text(a) for a in args)]))


def _safe_text(arg: object) -> str:
    if isinstance(arg, (str, BaseException)):
        return str(arg)
    name = getattr(arg, "name", None)  # pamqp-Frames: ``Basic.Consume``
    return name if isinstance(name, str) else type(arg).__name__


@pytest.fixture
def aiormq_records() -> Iterator[list[str]]:
    handler = _Records()
    logger = logging.getLogger("aiormq")
    logger.addHandler(handler)
    try:
        yield handler.messages
    finally:
        logger.removeHandler(handler)


# --- aio-pika ohne swarmq -----------------------------------------------------


@dataclass
class RawConsumer:
    connection: aio_pika.abc.AbstractRobustConnection
    publisher: aio_pika.abc.AbstractRobustChannel
    channel: aio_pika.abc.AbstractRobustChannel
    queue: AbstractRobustQueue
    tag: str
    deliveries: asyncio.Queue[AbstractIncomingMessage]

    async def publish(self, body: bytes) -> None:
        await self.publisher.default_exchange.publish(aio_pika.Message(body), routing_key=self.queue.name)

    async def next_delivery(self) -> AbstractIncomingMessage:
        return await asyncio.wait_for(self.deliveries.get(), EVENT_DEADLINE_S)

    async def underlay(self) -> aiormq.Channel:
        channel = await self.channel.get_underlay_channel()
        assert isinstance(channel, aiormq.Channel)
        return channel


RawConsumerFactory = Callable[..., Awaitable[RawConsumer]]


@pytest.fixture
async def raw_consumer_on() -> AsyncIterator[RawConsumerFactory]:
    """Fabrik wie ``consumer_on`` in ``test_consumer_timeout.py``, mit kurzem
    ``reconnect_interval``: robuste Verbindung, Queue, ein Consumer auf eigenem
    Kanal, Publish über einen zweiten."""
    async with AsyncExitStack() as stack:

        async def _make(
            node: RabbitMQNode, *, timeout_ms: int | None = CONSUMER_TIMEOUT_MS, queue_type: str = "quorum"
        ) -> RawConsumer:
            name = f"ccr_{uuid.uuid4().hex[:8]}"
            if timeout_ms is not None:
                await stack.enter_async_context(node.consumer_timeout_policy(name, timeout_ms))
            stack.push_async_callback(delete_queue_family, node.amqp_url, name)
            connection = await aio_pika.connect_robust(_url(node, FAST_RECOVERY))
            stack.push_async_callback(connection.close)
            publisher = await connection.channel()
            channel = await connection.channel()
            await channel.set_qos(prefetch_count=10)
            queue = await channel.declare_queue(name, durable=True, arguments={"x-queue-type": queue_type})
            deliveries: asyncio.Queue[AbstractIncomingMessage] = asyncio.Queue()

            async def _on_message(message: AbstractIncomingMessage) -> None:
                deliveries.put_nowait(message)

            tag = await queue.consume(_on_message)
            return RawConsumer(connection, publisher, channel, queue, tag, deliveries)

        yield _make


@aiormq_streamwriter_leak  # TS-02: der Broker schließt die Verbindung
@pytest.mark.timeout(40)
async def test_43_aio_pika_reconsumes_the_timed_out_tag_and_the_broker_closes_the_connection(
    rabbitmq43: RabbitMQNode, raw_consumer_on: RawConsumerFactory, probes: list[aiormq.abc.AbstractChannel]
) -> None:
    c = await raw_consumer_on(rabbitmq43)
    await c.publish(b"A")
    await c.next_delivery()
    first = _aiormq_connection(c.connection)
    assert not first.accept_consumer_cancel_ok, "precondition: 4.3.6 does not accept a client CancelOk"

    await asyncio.wait({first.closing}, timeout=TIMEOUT_DEADLINE_S)

    assert first.is_closed, "the connection dies"
    reason = first.closing.exception()
    assert isinstance(reason, ConnectionNotAllowed)
    assert f"attempt to reuse consumer tag '{c.tag}'" in str(reason)
    assert len(probes) == 1, "one probe before the re-consume"

    async def restored() -> bool:
        channel = await c.underlay()
        return channel.connection is not first and c.tag in channel.consumers

    assert await wait_until(restored, timeout=EVENT_DEADLINE_S), "the restore registers the same tag again"
    assert await _consumer_count(rabbitmq43, c.queue.name) == 1


@pytest.mark.timeout(30)
@pytest.mark.parametrize("queue_type", ["quorum", "classic"])
async def test_42_timeout_closes_the_channel_without_a_probe(
    rabbitmq42: RabbitMQNode,
    raw_consumer_on: RawConsumerFactory,
    probes: list[aiormq.abc.AbstractChannel],
    queue_type: str,
) -> None:
    c = await raw_consumer_on(rabbitmq42, queue_type=queue_type)
    await c.publish(b"A")
    await c.next_delivery()
    channel = await c.underlay()
    connection = channel.connection

    await asyncio.wait({channel.closing}, timeout=TIMEOUT_DEADLINE_S)
    again = await c.next_delivery()

    assert channel.is_closed
    assert again.consumer_tag == c.tag
    assert not connection.is_closed
    assert probes == []
    assert await _consumer_count(rabbitmq42, c.queue.name) == 1
    await again.ack()


# --- swarmq mit Consumer-Wächter (4.3.6) ---------------------------------------


@dataclass
class WatchedBroker:
    broker: RabbitMQBroker
    queue: str
    deliveries: asyncio.Queue[Delivery]

    async def next_delivery(self) -> Delivery:
        return await asyncio.wait_for(self.deliveries.get(), EVENT_DEADLINE_S)

    async def consume_underlay(self) -> aiormq.Channel:
        channel = self.broker._consume_channel
        assert channel is not None
        underlay = await channel.get_underlay_channel()
        assert isinstance(underlay, aiormq.Channel)
        return underlay


WatchedBrokerFactory = Callable[..., Awaitable[WatchedBroker]]


@pytest.fixture
async def watched_broker_on() -> AsyncIterator[WatchedBrokerFactory]:
    """``RabbitMQBroker`` mit einem Consumer und Wächter; ``reconnect_interval``
    per URL, der Wächter mit eigenem Takt (3600 s: nur auf Anstoß)."""
    async with AsyncExitStack() as stack:

        async def _make(
            node: RabbitMQNode,
            *,
            query: str,
            watch_interval_s: float,
            timeout_ms: int | None = CONSUMER_TIMEOUT_MS,
            queue_type: str = "quorum",
        ) -> WatchedBroker:
            name = f"ccr_{uuid.uuid4().hex[:8]}"
            if timeout_ms is not None:
                await stack.enter_async_context(node.consumer_timeout_policy(name, timeout_ms))
            stack.push_async_callback(delete_queue_family, node.amqp_url, name)
            broker = RabbitMQBroker(_url(node, query), queue_type=queue_type)
            stack.push_async_callback(broker.close)
            await broker.connect()
            await broker.declare_queue(name)
            deliveries: asyncio.Queue[Delivery] = asyncio.Queue()

            async def _on_delivery(delivery: Delivery) -> None:
                deliveries.put_nowait(delivery)

            await broker.consume(name, _on_delivery, concurrency=10)
            await broker.consumer_watch.tick()  # merkt sich den Kanal
            broker.start_consumer_watch(interval_s=watch_interval_s)
            return WatchedBroker(broker, name, deliveries)

        yield _make


@pytest.mark.timeout(40)
async def test_43_when_the_watch_comes_first_aio_pika_finds_no_missing_tag(
    rabbitmq43: RabbitMQNode, watched_broker_on: WatchedBrokerFactory, probes: list[aiormq.abc.AbstractChannel]
) -> None:
    c = await watched_broker_on(rabbitmq43, query=NO_RECOVERY, watch_interval_s=0.2)
    [(_, old_tag)] = c.broker.consumer_watch.tags()
    await c.broker.publish(c.queue, b"A")
    await c.next_delivery()
    channel = await c.consume_underlay()

    with capture_swarmq_logs() as logs:
        assert await wait_until(
            lambda: any(e["event"] == "consumer_reregistered" for e in logs), timeout=TIMEOUT_DEADLINE_S
        ), "the watch re-registers the cancelled consumer"

    [(_, new_tag)] = c.broker.consumer_watch.tags()
    assert new_tag != old_tag
    again = await c.next_delivery()
    assert again.body == b"A"
    assert isinstance(again.handle, AbstractIncomingMessage)
    assert again.handle.consumer_tag == new_tag
    await c.broker.ack(again)  # sonst liefe der Timeout erneut ab

    # aio-pikas Wiederherstellung von Hand, wie sie die Fabrik nach dem
    # ``basic.cancel`` mit dem Standard-Takt später ausführte.
    robust = c.broker._consume_channel
    assert isinstance(robust, aio_pika.RobustChannel)
    assert await robust._restore_consumers() is False, "nothing to retry"
    assert await c.consume_underlay() is channel
    assert not channel.is_closed
    assert not channel.connection.is_closed
    assert probes == []
    assert await _consumer_count(rabbitmq43, c.queue) == 1
    assert await wait_until(
        lambda: _management_consumer_tags(rabbitmq43, c.queue) == {new_tag}, timeout=TIMEOUT_DEADLINE_S
    )


@aiormq_streamwriter_leak  # TS-02: der Broker schließt die Verbindung
@pytest.mark.timeout(40)
async def test_43_when_aio_pika_comes_first_the_consume_connection_dies_and_one_consumer_remains(
    rabbitmq43: RabbitMQNode, watched_broker_on: WatchedBrokerFactory, probes: list[aiormq.abc.AbstractChannel]
) -> None:
    c = await watched_broker_on(rabbitmq43, query=FAST_RECOVERY, watch_interval_s=3600.0)
    [(_, tag)] = c.broker.consumer_watch.tags()
    await c.broker.publish(c.queue, b"A")
    await c.next_delivery()
    connection = (await c.consume_underlay()).connection

    await asyncio.wait({connection.closing}, timeout=TIMEOUT_DEADLINE_S)

    assert connection.is_closed, "the consume connection dies"
    reason = connection.closing.exception()
    assert isinstance(reason, ConnectionNotAllowed)
    assert f"attempt to reuse consumer tag '{tag}'" in str(reason)
    assert len(probes) == 1

    async def restored() -> bool:
        underlay = await c.consume_underlay()
        return underlay.connection is not connection and tag in underlay.consumers

    assert await wait_until(restored, timeout=EVENT_DEADLINE_S), "the restore registers the same tag again"
    await c.broker.consumer_watch.tick()
    assert c.broker.consumer_watch.tags() == [(c.queue, tag)], "the watch keeps the restored tag"
    assert await _consumer_count(rabbitmq43, c.queue) == 1


# --- Gelöschte konsumierte Queue -----------------------------------------------


@aiormq_streamwriter_leak  # TS-02: der Broker schließt die Probe-Kanäle
@pytest.mark.timeout(40)
async def test_a_deleted_queue_is_probed_each_reconnect_interval_until_release_queue(
    any_rabbitmq: RabbitMQNode, watched_broker_on: WatchedBrokerFactory, probes: list[aiormq.abc.AbstractChannel]
) -> None:
    c = await watched_broker_on(
        any_rabbitmq, query=FAST_RECOVERY, watch_interval_s=3600.0, timeout_ms=None, queue_type="classic"
    )
    channel = await c.consume_underlay()

    await _delete_queue(any_rabbitmq, c.queue)

    assert await wait_until(lambda: len(probes) >= 3, timeout=TIMEOUT_DEADLINE_S), (
        "aio-pika probes the deleted queue again and again"
    )
    assert all(probe.is_closed for probe in probes[:-1]), "each probe channel ends (NOT_FOUND)"
    assert not await _queue_exists(any_rabbitmq, c.queue), "the passive probe does not declare the queue again"

    with capture_swarmq_logs() as logs:
        await c.broker.consumer_watch.tick()  # Wächter: Cancel, Existenzprüfung, release_queue
    assert [e["queue"] for e in logs if e["event"] == "consumer_queue_missing"] == [c.queue]
    released = len(probes)
    # Negativbefund: fünf Takte von reconnect_interval (0,2 s) ohne neuen Probe-Kanal.
    await asyncio.sleep(1.0)

    assert len(probes) - released <= 1, "at most a probe already under way when the watch released the queue"
    settled = len(probes)
    await asyncio.sleep(1.0)
    assert len(probes) == settled, "no probe after release_queue"
    assert not await _queue_exists(any_rabbitmq, c.queue)
    assert not channel.is_closed, "the shared consume channel stays open"
    assert not channel.connection.is_closed


# --- Fristen der Wiederherstellung (Client-Seite, deterministisch) --------------


async def _trigger_recovery(c: RawConsumer) -> aiormq.Channel:
    """Löst aio-pikas Consumer-Wiederherstellung ohne Broker-Timer aus: Der Tag
    verschwindet per eigenem ``basic_cancel`` aus dem aiormq-Kanal, bleibt aber
    in ``RobustQueue._consumers``; dann wie ``_on_consumer_cancel``."""
    channel = await c.underlay()
    await channel.basic_cancel(c.tag)
    assert c.tag not in channel.consumers
    assert c.tag in c.queue._consumers  # type: ignore[attr-defined]  # aio-pikas Liste der Consumer der Queue
    c.connection._request_recovery()  # type: ignore[attr-defined]  # Einstieg von RobustChannel._on_consumer_cancel
    return channel


@pytest.mark.timeout(40)
async def test_a_reconsume_that_runs_into_its_deadline_closes_the_consume_channel_with_504(
    any_rabbitmq: RabbitMQNode,
    raw_consumer_on: RawConsumerFactory,
    monkeypatch: pytest.MonkeyPatch,
    aiormq_records: list[str],
) -> None:
    """E-053: aio-pika ruft ``consume`` mit ``timeout=5`` auf dem
    gemeinsamen Consume-Kanal. Der Test hält das ``ConsumeOk`` der
    Wiederanmeldung im Reader des Kanals zurück, bis aiormq die Frist
    umgesetzt hat."""
    original = aiormq.Channel._get_frame
    hold: list[aiormq.Channel] = []
    release = asyncio.Event()

    async def held_consume_ok(self: aiormq.Channel) -> Any:
        frame = await original(self)
        if hold and self is hold[0] and isinstance(frame, aiormq.spec.Basic.ConsumeOk):
            await release.wait()
        return frame

    monkeypatch.setattr(aiormq.Channel, "_get_frame", held_consume_ok)
    c = await raw_consumer_on(any_rabbitmq, timeout_ms=None, queue_type="classic")
    channel = await c.underlay()
    connection = channel.connection
    hold.append(channel)
    try:
        await _trigger_recovery(c)
        close_event = channel._Channel__close_event  # type: ignore[attr-defined]  # aiormq setzt es beim Close nach der Frist
        assert await wait_until(close_event.is_set, timeout=RECOVERY_RPC_DEADLINE_S, interval=0.05), (
            "aiormq closes the channel after the consume deadline"
        )
    finally:
        release.set()

    await asyncio.wait({channel.closing}, timeout=EVENT_DEADLINE_S)
    assert channel.is_closed
    assert "Closing channel %r because RPC call %s cancelled Channel Basic.Consume" in aiormq_records, aiormq_records
    assert not connection.is_closed, "the connection survives"

    async def restored() -> bool:
        underlay = await c.underlay()
        return underlay is not channel and c.tag in underlay.consumers

    assert await wait_until(restored, timeout=EVENT_DEADLINE_S), "the channel restore consumes again"


@aiormq_streamwriter_leak  # TS-02: der Broker schließt die Verbindung
@pytest.mark.timeout(40)
async def test_closing_the_consume_channel_during_the_probe_close_kills_the_connection_with_504(
    any_rabbitmq: RabbitMQNode,
    raw_consumer_on: RawConsumerFactory,
    monkeypatch: pytest.MonkeyPatch,
    probes: list[aiormq.abc.AbstractChannel],
) -> None:
    """R1: ``recovery.cancel()`` in ``_restore_consumers`` bricht ein laufendes
    ``probe.close()`` ab. Der Test hält das ``CloseOk`` des Probe-Kanals
    zurück (wie F4-30) und lässt den Broker währenddessen den Consume-Kanal
    schließen (Ack auf ein nie vergebenes Tag)."""
    original = aiormq.Channel._on_close_ok_frame
    close_ok_arrived = asyncio.Event()
    release = asyncio.Event()

    async def held_close_ok(self: aiormq.Channel, frame: Any) -> None:
        if probes and self is probes[0]:
            close_ok_arrived.set()
            await release.wait()
        await original(self, frame)

    monkeypatch.setattr(aiormq.Channel, "_on_close_ok_frame", held_close_ok)
    c = await raw_consumer_on(any_rabbitmq, timeout_ms=None, queue_type="classic")
    try:
        channel = await _trigger_recovery(c)
        connection = channel.connection
        assert await wait_until(close_ok_arrived.is_set, timeout=RECOVERY_RPC_DEADLINE_S, interval=0.01), (
            "precondition: the broker answered the probe's Channel.Close"
        )

        await channel.basic_ack(delivery_tag=2**31)
        await asyncio.wait({connection.closing}, timeout=EVENT_DEADLINE_S)
    finally:
        release.set()

    assert connection.is_closed, "the connection dies"
    reason = connection.closing.exception()
    assert isinstance(reason, ConnectionChannelError)
    assert "expected 'channel.open'" in str(reason)
