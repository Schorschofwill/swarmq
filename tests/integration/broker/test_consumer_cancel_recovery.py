"""Consumer-Wiederherstellung von aio-pika 10.2 auf RabbitMQ 4.3.6 und 4.2.9 (Plan 2026-10-29-1945, U3/U7, KTD7, E-089).

Neu gegenüber aio-pika 10.0: ``RobustChannel`` hängt ``_on_consumer_cancel``
an den aiormq-Kanal. Ein ``basic.cancel`` des Brokers weckt die
Verbindungsfabrik (``_request_recovery``); sie läuft im Takt
``reconnect_interval`` (Standard 5 s, hier per URL verkürzt) und ruft
``_restore_consumers``: Für jeden Tag aus ``RobustQueue._consumers``, der im
aiormq-Kanal fehlt, öffnet sie einen Probe-Kanal (aiormq, ``timeout=5``),
deklariert die Queue mit deren Original-Parametern (``RobustQueue._probe``),
schließt den Probe-Kanal und meldet mit demselben Tag neu an
(``Queue.consume(timeout=5)``). Bricht ein Kanal-Close der Wiederherstellung
ab, kommt das ``recovery.cancel()`` aus ``_restore_consumers``.

Die Tests unter „aio-pika ohne swarmq“ und „Fristen der Wiederherstellung“
halten das Verhalten von aio-pika 10.2.1 mit aiormq 7.2.4 selbst fest (U3,
Dokumentation des Upstream-Verhaltens, ohne swarmqs Kanalklasse). Nach diesen
Messungen hat Georg am 2026-10-30 entschieden (E-089): Vom Broker beendete
Consumer meldet nur swarmqs Consumer-Wächter neu an; ``ReleasingRobustChannel``
schaltet aio-pikas Wiederherstellung für swarmqs Kanäle ab (U7). Die Tests
unter „swarmq“ halten dieses Verhalten fest.

Beobachtet an aio-pika ohne swarmq (U3, je mindestens drei Läufe):

* 4.3.6, Consumer-Timeout: Der Broker beendet den Consumer mit
  ``basic.cancel``, behält dessen Tag aber auf dem Kanal (Zombie, siehe
  ``test_consumer_timeout.py``). aio-pikas Wiederanmeldung mit demselben Tag
  beantwortet RabbitMQ mit ``Connection.Close(530 NOT_ALLOWED - attempt to
  reuse consumer tag)``: Die ganze Verbindung stirbt. Nach dem Reconnect legt
  der Restore den Tag auf der neuen Verbindung an; danach genau ein Consumer.
  aiormq beantwortet das ``basic.cancel`` hier nicht mit ``CancelOk``, weil
  4.3.6 ``accept_consumer_cancel_ok`` nicht meldet (erst ab 4.3.7).
* 4.2.9, Consumer-Timeout: Der Broker schließt den Kanal statt ``basic.cancel``
  zu schicken; der Restore legt den Tag auf einem neuen Kanal an. Kein
  Probe-Kanal, die Verbindung bleibt, genau ein Consumer.
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

Vor E-089 (U3) mit swarmq gemessen, heute durch die Tests unter „swarmq“
ersetzt: Kam aio-pika vor dem Wächter, starb die Consume-Verbindung auf 4.3.6
mit 530 (mit den Standardtakten etwa jedes zweite Mal); nach dem Löschen einer
konsumierten Queue öffnete aio-pika je ``reconnect_interval`` einen
Probe-Kanal, bis ``release_queue`` griff.

swarmq (E-089, U7): Kein Probe-Kanal, keine Anmeldung mit dem alten Tag, die
Verbindung bleibt; der Wächter meldet mit neuem Tag an (4.3.6) bzw. behält den
Tag, den aio-pikas Kanal-Restore auf dem neuen Kanal anlegt (4.2.9). Die
Settle-Regel (Plan 2026-10-08-1541, U7/KTD4) und das eigene Abmelden des
Wächters bleiben.

Test-Liste:
# [x] aio-pika 4.3.6: Timeout -> aio-pika meldet denselben Tag neu an ->
#     Connection.Close(530 reuse consumer tag) -> nach Reconnect ein Consumer
# [x] aio-pika 4.2.9: Timeout schließt den Kanal, kein Probe-Kanal, ein Consumer
# [x] aio-pika: consume der Wiederanmeldung läuft in die Frist: Channel.Close(504)
#     auf dem Consume-Kanal, Verbindung bleibt (4.3.6, 4.2.9)
# [x] aio-pika: Probe-Close abgebrochen: Verbindung stirbt mit 504 expected
#     'channel.open' (4.3.6, 4.2.9)
# [x] swarmq 4.3.6, Wächter zuerst: aio-pika findet nichts, ein Consumer, Verbindung bleibt
# [x] swarmq 4.3.6, kurzer reconnect_interval: aio-pika meldet nicht an, kein
#     Probe-Kanal, kein 530; der Wächter meldet neu an, genau ein Consumer, keine
#     doppelte Zustellung
# [x] swarmq 4.3.6: Fabrik bei lebender Verbindung geweckt: nichts angemeldet, kein Probe-Kanal
# [x] swarmq 4.3.6: spätes Ack nach Broker-Cancel wartet auf den eigenen Cancel, kein Zombie
# [x] swarmq 4.2.9, kurzer reconnect_interval: Kanal-Restore mit demselben Tag,
#     Wächter behält ihn, kein Probe-Kanal, Verbindung bleibt, keine doppelte Zustellung
# [x] swarmq, gelöschte Queue (4.3.6, 4.2.9): kein Probe-Kanal, keine
#     Neudeklaration; der Wächter grenzt sie ein (E-083)
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


# --- swarmq mit Consumer-Wächter (E-089) ----------------------------------------


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
    # ``basic.cancel`` mit dem Standard-Takt vor E-089 später ausführte.
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


@pytest.mark.entscheidung("E-089")
@pytest.mark.timeout(40)
async def test_43_aio_pika_leaves_the_cancelled_consumer_to_the_watch_and_the_connection_stays(
    rabbitmq43: RabbitMQNode, watched_broker_on: WatchedBrokerFactory, probes: list[aiormq.abc.AbstractChannel]
) -> None:
    """Vorher (U3): aio-pika kam bei kurzem ``reconnect_interval`` zuerst,
    meldete denselben Tag neu an, und RabbitMQ beendete die Verbindung mit 530."""
    c = await watched_broker_on(rabbitmq43, query=FAST_RECOVERY, watch_interval_s=3600.0)
    [(_, old_tag)] = c.broker.consumer_watch.tags()
    await c.broker.publish(c.queue, b"A")
    await c.next_delivery()
    channel = await c.consume_underlay()
    connection = channel.connection
    channels_before = set(connection.channels)

    assert await wait_until(lambda: old_tag not in channel.consumers, timeout=TIMEOUT_DEADLINE_S), (
        "precondition: the broker cancels the consumer"
    )
    # Negativbefund: zehn Takte von reconnect_interval (0,2 s) ohne Wiederanmeldung durch aio-pika.
    await asyncio.sleep(2.0)

    assert not connection.is_closed, "no 530: aio-pika does not re-consume the zombie's tag"
    assert old_tag not in channel.consumers
    assert probes == []
    assert set(connection.channels) == channels_before, "no probe channel"

    with capture_swarmq_logs() as logs:
        await c.broker.consumer_watch.tick()
    assert [e["reason"] for e in logs if e["event"] == "consumer_reregistered"] == ["broker_cancel"]
    [(_, new_tag)] = c.broker.consumer_watch.tags()
    assert new_tag != old_tag
    again = await c.next_delivery()
    assert again.body == b"A"
    assert isinstance(again.handle, AbstractIncomingMessage)
    assert again.handle.consumer_tag == new_tag
    await c.broker.ack(again)
    await c.broker.publish(c.queue, b"B")
    later = await c.next_delivery()
    assert later.body == b"B"
    await c.broker.ack(later)
    await channel.basic_qos(prefetch_count=10)  # Round-Trip: Acks verarbeitet, keine weitere Zustellung unterwegs
    assert c.deliveries.empty(), "no delivery twice"
    assert not connection.is_closed
    assert await _consumer_count(rabbitmq43, c.queue) == 1
    assert await wait_until(
        lambda: _management_consumer_tags(rabbitmq43, c.queue) == {new_tag}, timeout=TIMEOUT_DEADLINE_S
    )


@pytest.mark.entscheidung("E-089")
@pytest.mark.timeout(40)
async def test_43_a_woken_connection_factory_does_not_re_consume_the_cancelled_tag(
    rabbitmq43: RabbitMQNode, watched_broker_on: WatchedBrokerFactory, probes: list[aiormq.abc.AbstractChannel]
) -> None:
    """Auch wenn aio-pikas Verbindungsfabrik bei lebender Verbindung aufwacht
    (hier von Hand, sonst etwa nach einem gescheiterten Durchgang), meldet sie
    für swarmqs Kanäle keinen Consumer an und öffnet keinen Probe-Kanal."""
    c = await watched_broker_on(rabbitmq43, query=FAST_RECOVERY, watch_interval_s=3600.0)
    [(_, old_tag)] = c.broker.consumer_watch.tags()
    await c.broker.publish(c.queue, b"A")
    await c.next_delivery()
    channel = await c.consume_underlay()
    connection = channel.connection
    channels_before = set(connection.channels)
    assert await wait_until(lambda: old_tag not in channel.consumers, timeout=TIMEOUT_DEADLINE_S), (
        "precondition: the broker cancels the consumer"
    )

    robust = c.broker._consume_channel
    assert isinstance(robust, aio_pika.RobustChannel)
    robust._connection._request_recovery()  # type: ignore[attr-defined]  # Einstieg der Fabrik bei lebender Verbindung
    # Negativbefund: zehn Takte von reconnect_interval (0,2 s).
    await asyncio.sleep(2.0)

    assert not connection.is_closed, "no 530"
    assert old_tag not in channel.consumers
    assert probes == []
    assert set(connection.channels) == channels_before, "no probe channel"
    await c.broker.consumer_watch.tick()
    [(_, new_tag)] = c.broker.consumer_watch.tags()
    assert new_tag != old_tag
    assert await _consumer_count(rabbitmq43, c.queue) == 1


@pytest.mark.timeout(40)
async def test_43_a_late_ack_after_the_broker_cancel_waits_for_the_own_cancel_and_the_connection_stays(
    rabbitmq43: RabbitMQNode, watched_broker_on: WatchedBrokerFactory, probes: list[aiormq.abc.AbstractChannel]
) -> None:
    """Settle-Regel (Plan 2026-10-08-1541, U7/KTD4) bei kurzem
    ``reconnect_interval``: Das späte Ack stößt den Wächter an und geht erst
    nach dessen eigenem Cancel raus; aio-pika kommt nicht dazwischen."""
    c = await watched_broker_on(rabbitmq43, query=FAST_RECOVERY, watch_interval_s=3600.0)
    [(_, old_tag)] = c.broker.consumer_watch.tags()
    await c.broker.publish(c.queue, b"A")
    first = await c.next_delivery()
    channel = await c.consume_underlay()
    assert await wait_until(lambda: old_tag not in channel.consumers, timeout=TIMEOUT_DEADLINE_S), (
        "precondition: the broker cancels the consumer"
    )

    with capture_swarmq_logs() as logs:
        await c.broker.ack(first)  # spätes Ack: erst nach dem eigenen Cancel

    assert [e["event"] for e in logs if e["event"].startswith("delivery_settle")] == ["delivery_settle_deferred"]
    assert old_tag in c.broker.consumer_watch.self_cancelled_tags
    # Das Ack kehrt nach dem eigenen Cancel zurück; die Neuanmeldung im selben Takt folgt.
    assert await wait_until(lambda: bool(c.broker.consumer_watch.tags()), timeout=EVENT_DEADLINE_S)
    [(_, new_tag)] = c.broker.consumer_watch.tags()
    await c.broker.publish(c.queue, b"B")
    got = [await c.next_delivery() for _ in range(2)]
    assert sorted(d.body for d in got) == [b"A", b"B"]
    assert {d.handle.consumer_tag for d in got if isinstance(d.handle, AbstractIncomingMessage)} == {new_tag}
    for delivery in got:
        await c.broker.ack(delivery)
    await channel.basic_qos(prefetch_count=10)  # Round-Trip: Acks verarbeitet
    assert c.deliveries.empty(), "no delivery twice"
    assert not channel.is_closed
    assert not channel.connection.is_closed
    assert probes == []
    assert await wait_until(
        lambda: _management_consumer_tags(rabbitmq43, c.queue) == {new_tag}, timeout=TIMEOUT_DEADLINE_S
    ), "no zombie"


@pytest.mark.timeout(40)
async def test_42_the_channel_restore_keeps_the_tag_and_the_watch_keeps_it(
    rabbitmq42: RabbitMQNode, watched_broker_on: WatchedBrokerFactory, probes: list[aiormq.abc.AbstractChannel]
) -> None:
    """4.2.9 schickt kein ``basic.cancel``, sondern schließt den Kanal; der
    Restore (``_on_close``, wie in aio-pika 10.0) legt den Tag auf dem neuen
    Kanal an. Der Wächter behält ihn, auch bei kurzem ``reconnect_interval``."""
    c = await watched_broker_on(rabbitmq42, query=FAST_RECOVERY, watch_interval_s=3600.0)
    [(_, tag)] = c.broker.consumer_watch.tags()
    await c.broker.publish(c.queue, b"A")
    await c.next_delivery()
    channel = await c.consume_underlay()
    connection = channel.connection

    await asyncio.wait({channel.closing}, timeout=TIMEOUT_DEADLINE_S)
    again = await c.next_delivery()

    assert channel.is_closed, "precondition: the broker closes the channel"
    assert isinstance(again.handle, AbstractIncomingMessage)
    assert again.body == b"A"
    assert again.handle.consumer_tag == tag
    await c.broker.ack(again)
    for _ in range(2):  # zwei Takte: auch ein ersetzter Kanal führte nicht zur Neuanmeldung
        await c.broker.consumer_watch.tick()
    assert c.broker.consumer_watch.tags() == [(c.queue, tag)]
    await c.broker.publish(c.queue, b"B")
    later = await c.next_delivery()
    assert later.body == b"B"
    await c.broker.ack(later)
    restored = await c.consume_underlay()
    await restored.basic_qos(prefetch_count=10)  # Round-Trip: Acks verarbeitet
    assert c.deliveries.empty(), "no delivery twice"
    assert not connection.is_closed
    assert probes == []
    assert await _consumer_count(rabbitmq42, c.queue) == 1


# --- Gelöschte konsumierte Queue -----------------------------------------------


@pytest.mark.entscheidung("E-089")
@pytest.mark.timeout(40)
async def test_a_deleted_queue_gets_no_probe_and_the_watch_fences_it(
    any_rabbitmq: RabbitMQNode, watched_broker_on: WatchedBrokerFactory, probes: list[aiormq.abc.AbstractChannel]
) -> None:
    """Vorher (U3): aio-pika öffnete nach dem ``basic.cancel`` des Brokers je
    ``reconnect_interval`` einen Probe-Kanal, bis ``release_queue`` griff."""
    c = await watched_broker_on(
        any_rabbitmq, query=FAST_RECOVERY, watch_interval_s=3600.0, timeout_ms=None, queue_type="classic"
    )
    channel = await c.consume_underlay()
    [(_, tag)] = c.broker.consumer_watch.tags()
    channels_before = set(channel.connection.channels)

    await _delete_queue(any_rabbitmq, c.queue)
    assert await wait_until(lambda: tag not in channel.consumers, timeout=EVENT_DEADLINE_S), (
        "precondition: the broker cancels the consumer of the deleted queue"
    )
    # Negativbefund: zehn Takte von reconnect_interval (0,2 s) ohne Probe-Kanal.
    await asyncio.sleep(2.0)

    assert probes == []
    assert set(channel.connection.channels) == channels_before, "no probe channel"
    assert not await _queue_exists(any_rabbitmq, c.queue), "nobody declares the queue again"

    with capture_swarmq_logs() as logs:
        await c.broker.consumer_watch.tick()  # Wächter: Cancel, Existenzprüfung, release_queue (E-083)
    assert [e["queue"] for e in logs if e["event"] == "consumer_queue_missing"] == [c.queue]
    assert c.broker.consumer_missing_queues() == [c.queue]
    assert probes == []
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
