"""Charakterisierung: Consumer-Timeout auf RabbitMQ 4.3.6 und 4.2.9 (Plan U5).

Grün und ohne Produktionscode: Die Tests halten fest, wie die gepinnten
Broker zusammen mit roh benutztem aio-pika (10.0.1) reagieren, wenn eine
Nachricht länger unbestätigt bleibt als ``consumer-timeout``. Darauf bauen
KTD3 (Consumer-Wächter) und KTD4 (Settle-Regeln) auf; ändert ein Broker- oder
aio-pika-Upgrade dieses Verhalten, wird diese Datei rot.

Der Timeout kommt pro Testqueue aus einer Policy (``consumer-timeout`` 3000 ms).
Der 4.2-Dienst läuft mit ``channel_tick_interval`` 1000 ms, sonst prüft der
Channel nur alle 60 s. Die Timer-Tests brauchen deshalb 3–5 s und damit weit
mehr als 100 ms; sie sind Integrationstests und bleiben unter dem globalen
pytest-Timeout von 30 s.

Test-Liste:
- [x] 4.3.6: Nach dem Timeout bleibt der Channel offen, der Tag fehlt in
      ``consumers``; ein spätes Ack weckt den Zombie, der wieder Nachrichten
      bekommt — die aiormq stillschweigend verwirft.
- [x] 4.3.6: Eigenes ``queue.cancel`` beendet den Zombie und räumt
      ``RobustQueue._consumers`` auf; ein späteres Ack lässt den Channel offen,
      ein neuer Consumer bekommt alle Nachrichten.
- [x] 4.3.6: Klassische Queues kennen keinen Consumer-Timeout mehr — weder
      Cancel noch Close nach doppelter Frist (auch nicht mit globalem
      ``consumer_timeout``, ad hoc 300 s geprüft).
- [x] 4.2.9 (Quorum und klassisch): Der Timeout schließt den Channel; der Restore legt denselben Tag
      auf einem neuen Channel an; ein spätes Ack wirft
      ``ChannelInvalidStateError``.
- [x] 4.2.9 und 4.3.6: Ein Ack auf ein unbekanntes Delivery-Tag schließt den
      Consume-Channel sofort (deterministischer Ersatz für den Timer) — mit
      demselben Restore-Verhalten wie der 4.2-Timeout.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AsyncExitStack
from dataclasses import dataclass

import aio_pika
import aiormq.abc
import pytest
from aio_pika.abc import AbstractIncomingMessage, AbstractRobustQueue
from aio_pika.exceptions import ChannelInvalidStateError

from tests.support.services import RABBITMQ_42, RABBITMQ_MAIN, RabbitMQNode, require_rabbitmq

pytestmark = [pytest.mark.integration]

CONSUMER_TIMEOUT_MS = 3000
# Obergrenze für das Eintreffen von Cancel/Close: Timeout plus Prüftakt plus Luft.
TIMEOUT_DEADLINE_S = 12.0
EVENT_DEADLINE_S = 5.0


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


async def wait_until(predicate: Callable[[], bool], timeout: float) -> float:
    """Pollt ``predicate`` bis zur Deadline; liefert die verstrichene Zeit.

    Für Zustände ohne eigenes Ereignis (aiormq entfernt den Tag beim
    ``basic.cancel`` still aus ``consumers``). Das kurze Intervall ist ein
    Polling-Takt, keine Wartezeit, von der das Ergebnis abhängt.
    """
    start = time.monotonic()
    deadline = start + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError(f"condition not met within {timeout} s")
        await asyncio.sleep(0.02)
    return time.monotonic() - start


async def close_by_unknown_delivery_tag(channel: aiormq.abc.AbstractChannel) -> None:
    """Schließt den Channel deterministisch: Der Broker beantwortet ein Ack
    auf ein nie vergebenes Delivery-Tag mit ``PRECONDITION_FAILED``."""
    await channel.basic_ack(delivery_tag=2**31)
    await asyncio.wait({channel.closing}, timeout=EVENT_DEADLINE_S)
    assert channel.is_closed


@dataclass
class Consumer:
    publisher: aio_pika.abc.AbstractRobustChannel
    channel: aio_pika.abc.AbstractRobustChannel
    queue: AbstractRobustQueue
    tag: str
    deliveries: asyncio.Queue[AbstractIncomingMessage]

    async def publish(self, body: bytes) -> None:
        await self.publisher.default_exchange.publish(aio_pika.Message(body), routing_key=self.queue.name)

    async def next_delivery(self) -> AbstractIncomingMessage:
        return await asyncio.wait_for(self.deliveries.get(), EVENT_DEADLINE_S)


ConsumerFactory = Callable[..., Awaitable[Consumer]]


@pytest.fixture
async def consumer_on() -> AsyncIterator[ConsumerFactory]:
    """Fabrik: robuste Verbindung, Queue (Standard Quorum, optional mit
    Timeout-Policy), ein Consumer (prefetch 10) auf eigenem Channel, Publish über einen zweiten."""
    async with AsyncExitStack() as stack:

        async def _make(
            node: RabbitMQNode,
            *,
            timeout_ms: int | None = CONSUMER_TIMEOUT_MS,
            queue_type: str = "quorum",
        ) -> Consumer:
            name = f"ct_{uuid.uuid4().hex[:8]}"
            if timeout_ms is not None:
                await stack.enter_async_context(node.consumer_timeout_policy(name, timeout_ms))
            stack.push_async_callback(_delete_queue, node, name)
            connection = await aio_pika.connect_robust(node.amqp_url)
            stack.push_async_callback(connection.close)
            publisher = await connection.channel()
            channel = await connection.channel()
            await channel.set_qos(prefetch_count=10)
            queue = await channel.declare_queue(name, durable=True, arguments={"x-queue-type": queue_type})
            deliveries: asyncio.Queue[AbstractIncomingMessage] = asyncio.Queue()

            async def _on_message(message: AbstractIncomingMessage) -> None:
                deliveries.put_nowait(message)

            tag = await queue.consume(_on_message)
            return Consumer(publisher, channel, queue, tag, deliveries)  # type: ignore[arg-type]

        yield _make


async def _delete_queue(node: RabbitMQNode, name: str) -> None:
    connection = await aio_pika.connect(node.amqp_url)
    async with connection:
        channel = await connection.channel()
        await channel.queue_delete(name)


def _spy_on_dead_tag(channel: aiormq.abc.AbstractChannel, tag: str) -> asyncio.Queue[aiormq.abc.DeliveredMessage]:
    """aiormq verwirft Zustellungen an Tags, die nicht in ``consumers`` stehen.
    Ein Spion unter dem toten Tag macht sichtbar, was der Broker trotzdem schickt."""
    seen: asyncio.Queue[aiormq.abc.DeliveredMessage] = asyncio.Queue()

    async def _spy(message: aiormq.abc.DeliveredMessage) -> None:
        seen.put_nowait(message)

    channel.consumers[tag] = _spy  # type: ignore[attr-defined]
    return seen


# --- 4.3.6: basic.cancel an den Client, Channel bleibt offen ----------------


@pytest.mark.timeout(25)
async def test_43_timeout_cancels_consumer_and_late_ack_wakes_zombie(rabbitmq43, consumer_on):
    c = await consumer_on(rabbitmq43)
    await c.publish(b"A")
    first = await c.next_delivery()
    channel = await c.channel.get_underlay_channel()

    elapsed = await wait_until(lambda: c.tag not in channel.consumers, TIMEOUT_DEADLINE_S)

    assert elapsed >= CONSUMER_TIMEOUT_MS / 1000 - 0.5
    assert not channel.is_closed
    assert await c.channel.get_underlay_channel() is channel
    assert c.tag in c.queue._consumers  # aio-pika merkt nichts

    zombie = _spy_on_dead_tag(channel, c.tag)
    await first.ack()
    await c.publish(b"B")

    woken = await asyncio.wait_for(zombie.get(), EVENT_DEADLINE_S)
    assert isinstance(woken.delivery, aiormq.spec.Basic.Deliver)
    assert woken.delivery.consumer_tag == c.tag
    assert not channel.is_closed
    assert c.deliveries.empty()


@pytest.mark.timeout(25)
async def test_43_own_cancel_ends_zombie_and_later_ack_keeps_channel_open(rabbitmq43, consumer_on):
    c = await consumer_on(rabbitmq43)
    await c.publish(b"A")
    first = await c.next_delivery()
    channel = await c.channel.get_underlay_channel()
    await wait_until(lambda: c.tag not in channel.consumers, TIMEOUT_DEADLINE_S)
    zombie = _spy_on_dead_tag(channel, c.tag)

    await c.queue.cancel(c.tag)

    assert c.tag not in c.queue._consumers
    await first.ack()
    await channel.basic_qos(prefetch_count=10)  # Round-Trip: Broker hat das Ack verarbeitet
    assert not channel.is_closed

    fresh: asyncio.Queue[AbstractIncomingMessage] = asyncio.Queue()

    async def _on_message(message: AbstractIncomingMessage) -> None:
        fresh.put_nowait(message)

    new_tag = await c.queue.consume(_on_message)
    await c.publish(b"B")
    got = [await asyncio.wait_for(fresh.get(), EVENT_DEADLINE_S) for _ in range(2)]

    assert sorted(m.body for m in got) == [b"A", b"B"]
    assert {m.consumer_tag for m in got} == {new_tag}
    assert zombie.empty()
    assert not channel.is_closed


@pytest.mark.timeout(25)
async def test_43_classic_queue_has_no_consumer_timeout(rabbitmq43, consumer_on):
    """4.3 setzt den Timeout nur noch in der Queue um; klassische Queues tun
    es nicht. Gleiche Policy, doppelte Frist: weder Cancel noch Close.
    Die Wartezeit ist hier der Gegenstand der Aussage (Negativbefund)."""
    c = await consumer_on(rabbitmq43, queue_type="classic")
    await c.publish(b"A")
    first = await c.next_delivery()
    channel = await c.channel.get_underlay_channel()

    await asyncio.wait({channel.closing}, timeout=2 * CONSUMER_TIMEOUT_MS / 1000)

    assert not channel.is_closed
    assert c.tag in channel.consumers
    await first.ack()
    await channel.basic_qos(prefetch_count=10)
    assert not channel.is_closed


# --- 4.2.9: Channel-Close, Restore mit demselben Tag ------------------------


@pytest.mark.timeout(25)
@pytest.mark.parametrize("queue_type", ["quorum", "classic"])
async def test_42_timeout_closes_channel_restore_keeps_tag_and_late_ack_raises(rabbitmq42, consumer_on, queue_type):
    c = await consumer_on(rabbitmq42, queue_type=queue_type)
    await c.publish(b"A")
    first = await c.next_delivery()
    channel = await c.channel.get_underlay_channel()

    start = time.monotonic()
    await asyncio.wait({channel.closing}, timeout=TIMEOUT_DEADLINE_S)
    elapsed = time.monotonic() - start

    assert channel.is_closed
    assert elapsed >= CONSUMER_TIMEOUT_MS / 1000 - 0.5
    again = await c.next_delivery()
    assert again.body == b"A"
    assert again.redelivered
    assert again.consumer_tag == c.tag
    restored = await c.channel.get_underlay_channel()
    assert restored is not channel
    assert c.tag in restored.consumers

    with pytest.raises(ChannelInvalidStateError):
        await first.ack()
    await again.ack()


# --- Beide: deterministischer Channel-Close ohne Timer ----------------------


@pytest.mark.timeout(10)
async def test_unknown_delivery_tag_closes_channel_and_restore_keeps_tag(any_rabbitmq, consumer_on):
    c = await consumer_on(any_rabbitmq, timeout_ms=None)
    await c.publish(b"A")
    first = await c.next_delivery()
    channel = await c.channel.get_underlay_channel()

    await close_by_unknown_delivery_tag(channel)

    again = await c.next_delivery()
    assert again.body == b"A"
    assert again.redelivered
    assert again.consumer_tag == c.tag
    restored = await c.channel.get_underlay_channel()
    assert restored is not channel
    assert c.tag in restored.consumers
    with pytest.raises(ChannelInvalidStateError):
        await first.ack()
    await again.ack()
