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

Worker mit Consumer-Wächter (Plan 2026-10-08-1541, U6, R1):
- [x] 4.3.6: Nach dem Timeout konsumiert der Worker weiter, ohne Neustart;
      am Broker hängt genau ein Consumer, kein Zombie.

Settle-Regeln (U7, KTD4):
- [x] 4.3.6: Ein spätes Ack des Originals wartet auf den eigenen Cancel des
      Wächters und weckt keinen Zombie; weitere Nachrichten werden normal
      konsumiert.
- [x] 4.2.9: Ein spätes Ack nach dem Channel-Close fängt
      ``contain_delivery_errors`` ab; der neue Channel bleibt offen.

Ausführungsschutz (U8, KTD5, AE4):
- [x] 4.2.9 und 4.3.6: Ein Task läuft länger als der Consumer-Timeout. Die
      Neuzustellung wird zurückgestellt, solange er läuft, und danach
      übersprungen; der Task lief genau einmal, der Worker konsumiert weiter
      mit genau einem Consumer.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AsyncExitStack
from dataclasses import dataclass

import aio_pika
import aiormq.abc
import httpx
import pytest
from aio_pika.abc import AbstractIncomingMessage, AbstractRobustQueue
from aio_pika.exceptions import ChannelInvalidStateError

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.contracts import Delivery
from swarmq.task import Task, TaskInfo
from swarmq.worker.settle import contain_delivery_errors
from tests.support.services import RABBITMQ_42, RABBITMQ_MAIN, RabbitMQNode, require_rabbitmq
from tests.unit._log_capture import capture_swarmq_logs

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


# --- Worker mit Consumer-Wächter (U6) ----------------------------------------


async def _consumer_count(node: RabbitMQNode, queue: str) -> int:
    """Echtzeit-Zahl aus ``queue.declare-ok`` (ohne Statistik-Verzug)."""
    connection = await aio_pika.connect(node.amqp_url)
    async with connection:
        channel = await connection.channel()
        declared = await channel.declare_queue(queue, passive=True)
        return int(declared.declaration_result.consumer_count)


def _management_consumer_tags(node: RabbitMQNode, queue: str) -> set[str]:
    response = httpx.get(f"{node.management_url}/consumers/%2F", auth=("guest", "guest"), timeout=5.0)
    response.raise_for_status()
    return {c["consumer_tag"] for c in response.json() if c["queue"]["name"] == queue}


@pytest.mark.timeout(40)
async def test_43_worker_keeps_consuming_after_timeout_without_zombie(rabbitmq43, valkey_url):
    queue = f"ct_{uuid.uuid4().hex[:8]}"
    broker = RabbitMQBroker(rabbitmq43.amqp_url)

    class OutlivesTimeout(Task):
        """Läuft, bis der Wächter den vom Broker beendeten Consumer selbst
        abgemeldet hat — deterministisch statt einer festen Schlafdauer. Das
        späte Ack fällt so in die Lage nach dem eigenen Cancel (KTD3)."""

        task_info = TaskInfo(name="ct_outlives_timeout", queues=[queue])

        async def run(self) -> str:
            deadline = time.monotonic() + TIMEOUT_DEADLINE_S
            while not broker.consumer_watch.self_cancelled_tags and time.monotonic() < deadline:
                await asyncio.sleep(0.05)
            return "slow"

    class Quick(Task):
        task_info = TaskInfo(name="ct_quick", queues=[queue])

        async def run(self) -> str:
            return "quick"

    app = SwarmQ(broker, ValkeyBackend(valkey_url), queues=[queue], concurrency=2, consumer_watch_interval_s=0.5)
    try:
        async with rabbitmq43.consumer_timeout_policy(queue, CONSUMER_TIMEOUT_MS), app:
            await app.broker.declare_queue(queue)  # sonst geht der Publish ins Leere
            worker = asyncio.create_task(app.start_worker())
            try:
                slow_id = await app.schedule("ct_outlives_timeout")
                assert await app.get_result(slow_id, timeout=TIMEOUT_DEADLINE_S + 5) == "slow"

                quick_id = await app.schedule("ct_quick")
                assert await app.get_result(quick_id, timeout=EVENT_DEADLINE_S) == "quick"

                assert await _consumer_count(rabbitmq43, queue) == 1
                [(_, current_tag)] = broker.consumer_watch.tags()
                await wait_until(
                    lambda: _management_consumer_tags(rabbitmq43, queue) == {current_tag},
                    TIMEOUT_DEADLINE_S,
                )
            finally:
                worker.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await worker
    finally:
        Task._clear_registered_tasks()
        for name in (queue, f"{queue}.DQ", f"{queue}.XQ"):
            await _delete_queue(rabbitmq43, name)


# --- Settle-Regeln (U7) -------------------------------------------------------


@dataclass
class BrokerConsumer:
    broker: RabbitMQBroker
    queue: str
    deliveries: asyncio.Queue[Delivery]

    async def next_delivery(self) -> Delivery:
        return await asyncio.wait_for(self.deliveries.get(), EVENT_DEADLINE_S)


@pytest.fixture
async def broker_consumer_on() -> AsyncIterator[Callable[[RabbitMQNode], Awaitable[BrokerConsumer]]]:
    """Fabrik: ``RabbitMQBroker`` mit Queue unter Timeout-Policy, einem
    Consumer und gestartetem Wächter, der nur auf Anstoß prüft (langer Takt).
    Ein erster Takt merkt sich den Channel, sodass ein später fehlender Tag
    als ``basic.cancel`` gilt."""
    async with AsyncExitStack() as stack:

        async def _make(node: RabbitMQNode) -> BrokerConsumer:
            name = f"ct_{uuid.uuid4().hex[:8]}"
            await stack.enter_async_context(node.consumer_timeout_policy(name, CONSUMER_TIMEOUT_MS))
            for queue_name in (name, f"{name}.DQ", f"{name}.XQ"):
                stack.push_async_callback(_delete_queue, node, queue_name)
            broker = RabbitMQBroker(node.amqp_url)
            stack.push_async_callback(broker.close)
            await broker.connect()
            await broker.declare_queue(name)
            deliveries: asyncio.Queue[Delivery] = asyncio.Queue()

            async def _on_delivery(delivery: Delivery) -> None:
                deliveries.put_nowait(delivery)

            await broker.consume(name, _on_delivery, concurrency=10)
            await broker.consumer_watch.tick()
            broker.start_consumer_watch(interval_s=3600.0)
            return BrokerConsumer(broker, name, deliveries)

        yield _make


@pytest.mark.timeout(30)
async def test_43_late_ack_waits_for_own_cancel_and_wakes_no_zombie(rabbitmq43, broker_consumer_on):
    c = await broker_consumer_on(rabbitmq43)
    await c.broker.publish(c.queue, b"A")
    first = await c.next_delivery()
    message = first.handle
    [(_, old_tag)] = c.broker.consumer_watch.tags()
    channel = message.channel
    await wait_until(lambda: old_tag not in channel.consumers, TIMEOUT_DEADLINE_S)

    await c.broker.ack(first)  # spätes Ack: erst nach dem eigenen Cancel

    assert old_tag in c.broker.consumer_watch.self_cancelled_tags
    zombie = _spy_on_dead_tag(channel, old_tag)
    await c.broker.publish(c.queue, b"B")
    got = [await c.next_delivery() for _ in range(2)]
    assert sorted(d.body for d in got) == [b"A", b"B"]
    [(_, new_tag)] = c.broker.consumer_watch.tags()
    assert {d.handle.consumer_tag for d in got} == {new_tag}
    for delivery in got:
        await c.broker.ack(delivery)
    assert zombie.empty()
    assert not channel.is_closed
    await wait_until(lambda: _management_consumer_tags(rabbitmq43, c.queue) == {new_tag}, TIMEOUT_DEADLINE_S)


@pytest.mark.timeout(30)
async def test_42_late_ack_after_channel_close_is_contained_and_new_channel_stays_open(rabbitmq42, broker_consumer_on):
    c = await broker_consumer_on(rabbitmq42)
    await c.broker.publish(c.queue, b"A")
    first = await c.next_delivery()
    channel = await c.broker._consume_channel.get_underlay_channel()
    await asyncio.wait({channel.closing}, timeout=TIMEOUT_DEADLINE_S)
    again = await c.next_delivery()
    assert again.body == b"A"

    with capture_swarmq_logs() as logs, contain_delivery_errors():
        await c.broker.ack(first)  # B22: Kanalverlust, keine rohe Exception

    assert [e["event"] for e in logs] == ["delivery_settle_failed"]

    restored = await c.broker._consume_channel.get_underlay_channel()
    assert restored is not channel
    assert not restored.is_closed
    await c.broker.ack(again)
    await c.broker.publish(c.queue, b"B")
    later = await c.next_delivery()
    assert later.body == b"B"
    await c.broker.ack(later)
    await restored.basic_qos(prefetch_count=10)  # Round-Trip: Acks verarbeitet
    assert not restored.is_closed
    assert await _consumer_count(rabbitmq42, c.queue) == 1


# --- Ausführungsschutz (U8, AE4) ----------------------------------------------


@pytest.mark.timeout(60)
async def test_long_task_runs_exactly_once_across_the_consumer_timeout(any_rabbitmq, valkey_url):
    """Der Task läuft, bis der Ausführungsschutz die Neuzustellung des Brokers
    zurückgestellt hat (4.3: nach ``basic.cancel`` und eigenem Cancel, 4.2:
    nach Channel-Close und Restore). Die Running-TTL (2 s) ist kürzer als der
    Lauf -- ohne Verlängerung liefe die Kopie parallel."""
    node = any_rabbitmq
    queue = f"ct_{uuid.uuid4().hex[:8]}"
    broker = RabbitMQBroker(node.amqp_url)
    runs: list[float] = []

    with capture_swarmq_logs() as logs:

        def _seen(event: str) -> bool:
            return any(e["event"] == event for e in logs)

        class OutlivesTimeout(Task):
            task_info = TaskInfo(name="ct_guard_once", queues=[queue])

            async def run(self) -> str:
                runs.append(time.monotonic())
                deadline = time.monotonic() + TIMEOUT_DEADLINE_S
                while not _seen("execution_guard_deferred") and time.monotonic() < deadline:
                    await asyncio.sleep(0.05)
                return "once"

        class Quick(Task):
            task_info = TaskInfo(name="ct_guard_quick", queues=[queue])

            async def run(self) -> str:
                return "quick"

        app = SwarmQ(
            broker,
            ValkeyBackend(valkey_url),
            queues=[queue],
            concurrency=2,
            consumer_watch_interval_s=0.5,
            execution_guard_ttl_s=2.0,
        )
        try:
            async with node.consumer_timeout_policy(queue, CONSUMER_TIMEOUT_MS), app:
                await app.broker.declare_queue(queue)
                worker = asyncio.create_task(app.start_worker())
                try:
                    task_id = await app.schedule("ct_guard_once")
                    assert await app.get_result(task_id, timeout=TIMEOUT_DEADLINE_S + 10) == "once"
                    await wait_until(lambda: _seen("duplicate_delivery_skipped"), TIMEOUT_DEADLINE_S)
                    assert len(runs) == 1
                    assert not _seen("execution_guard_lease_lost")

                    quick_id = await app.schedule("ct_guard_quick")
                    assert await app.get_result(quick_id, timeout=EVENT_DEADLINE_S) == "quick"
                    assert await _consumer_count(node, queue) == 1
                    assert len(runs) == 1
                finally:
                    worker.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await worker
        finally:
            Task._clear_registered_tasks()
            for name in (queue, f"{queue}.DQ", f"{queue}.XQ"):
                await _delete_queue(node, name)
