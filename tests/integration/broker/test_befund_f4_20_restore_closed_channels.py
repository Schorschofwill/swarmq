"""Nachweis F4-20 (behoben in 0.14.0): Nach einem Reconnect öffnet aio-pika geschlossene oder verworfene Kanäle neu.

Register: docs/befunde/fehlersuche-runde-4-2026-10.md (F4-20; Nebenbeobachtung 1
im Spike ``docs/solutions/spikes/2026-10-spike-kanal-close-nach-unblocked.md``,
Gegenprüfung 0.14, Probe A und C).

aio-pikas ``RobustConnection._on_connected`` stellt jeden ``RobustChannel`` ihres
``WeakSet`` wieder her, auch geschlossene; aiormq hält ihn danach stark. Beim
Broker wächst die Kanalzahl mit jedem Reconnect um die geschlossenen, noch nicht
eingesammelten Kanäle (Probe A: 11, dann 12 bis 15). Ein wiederhergestellter
Tiefen-Kanal mit passiv deklarierter, inzwischen gelöschter Queue lässt den
Reconnect endlos an NOT_FOUND scheitern (Probe C, „Failed to reopen channel …
NOT_FOUND“ alle 5 s). Verletzt: ``close_channel_quietly`` (Review MR 10, P0-1)
und „kein Leck“ in E-025 (Nachtrag 2026-10-19) und E-078.

Deterministisch statt GC-Zufall: Die Garbage Collection ist während des Tests
aus, der verworfene Publish-Kanal bleibt zusätzlich stark referenziert. Die
Verbindung trennt der Broker über die Management-API.

Test-Liste:
# [x] Deklaration, Tiefe, Verwurf, Abriss: beim Broker steht danach nur der lebende Publish-Kanal
# [x] Tiefe einer Queue, Queue gelöscht, Abriss: der Reconnect gelingt binnen 10 s
"""

from __future__ import annotations

import gc
import uuid
from collections.abc import Iterator

import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from tests.integration.broker.test_pf19_close_after_unblocked import (
    _aiormq_connection,
    _broker_channels_settle,
    _close_connection_at_broker,
    _open_aiormq_channels,
)
from tests.support.queues import delete_queue_family
from tests.support.wait import wait_until

pytestmark = [pytest.mark.integration, pytest.mark.timeout(90)]

# Probe C: ohne den Befund stand die Verbindung nach rund 4 s wieder.
_RECONNECT_WITHIN_S = 10.0


@pytest.fixture
def no_collection() -> Iterator[None]:
    """Keine Garbage Collection: Ob ein geschlossener ``RobustChannel`` vor dem
    Reconnect eingesammelt ist, wäre sonst Zufall (Spike, Nebenbeobachtung 1)."""
    gc.disable()
    try:
        yield
    finally:
        gc.enable()


async def _reconnected(broker: RabbitMQBroker, dropped: object, timeout: float) -> bool:
    async def ready() -> bool:
        current = _aiormq_connection(broker)
        connection = broker._connection
        return (
            current is not None
            and current is not dropped
            and not current.is_closed
            and connection is not None
            and connection.connected.is_set()
        )

    return await wait_until(ready, timeout=timeout, interval=0.1)


@pytest.mark.befund("F4-20")
@pytest.mark.entscheidung("E-078")
@pytest.mark.usefixtures("no_collection")
async def test_after_a_reconnect_only_the_live_channels_are_open_at_the_broker(rabbitmq_url: str) -> None:
    queue = f"f4-20-{uuid.uuid4().hex[:10]}"
    name = f"f4-20-{uuid.uuid4().hex[:10]}"
    broker = RabbitMQBroker(f"{rabbitmq_url}?name={name}", queue_type="classic")
    await broker.connect()
    try:
        await broker.declare_queue(queue)
        await broker.publish(queue, b"m0")
        assert await broker.queue_depth(queue) == 1
        discarded = broker._publish_channel
        await broker._discard_publish_channel(discarded)

        async def closes_done() -> bool:
            return not broker._pending_channel_closes

        assert await wait_until(closes_done, timeout=10, interval=0.05), "precondition: the discarded channel is closed"
        await broker.publish(queue, b"m1")
        dropped = _aiormq_connection(broker)

        await _close_connection_at_broker(name)
        assert await _reconnected(broker, dropped, timeout=20), "precondition: aio-pika reconnects"
        await broker.publish(queue, b"m2")

        live = _open_aiormq_channels(broker)
        channels = await _broker_channels_settle(name, live)
        assert len(live) == 1, f"only the publish channel is open in aiormq, got {live} (discarded: {discarded})"
        assert channels == live, f"the broker lists only the live channel {live}, got {channels}"
    finally:
        await broker.close()
        await delete_queue_family(rabbitmq_url, queue)


@pytest.mark.befund("F4-20")
@pytest.mark.usefixtures("no_collection")
async def test_a_reconnect_after_the_depth_of_a_deleted_queue_succeeds(rabbitmq_url: str) -> None:
    kept = f"f4-20-{uuid.uuid4().hex[:10]}"
    deleted = f"f4-20-{uuid.uuid4().hex[:10]}"
    name = f"f4-20-{uuid.uuid4().hex[:10]}"
    # Angelegt über eine eigene Verbindung: Die Deklarations-Kanäle des
    # geprüften Brokers legten die gelöschte Queue beim Wiederherstellen sonst
    # selbst neu an, bevor der Tiefen-Kanal an NOT_FOUND scheitert.
    declaring = RabbitMQBroker(rabbitmq_url, queue_type="classic")
    await declaring.connect()
    try:
        await declaring.declare_queue(deleted)
    finally:
        await declaring.close()
    broker = RabbitMQBroker(f"{rabbitmq_url}?name={name}", queue_type="classic")
    await broker.connect()
    try:
        await broker.declare_queue(kept)
        assert await broker.queue_depth(deleted) == 0
        # Der Betreiber löscht die Queue, etwa nach der Meldung einer verwaisten Queue (E-015).
        await delete_queue_family(rabbitmq_url, deleted)
        dropped = _aiormq_connection(broker)

        await _close_connection_at_broker(name)

        assert await _reconnected(broker, dropped, timeout=_RECONNECT_WITHIN_S), (
            f"aio-pika reconnects within {_RECONNECT_WITHIN_S}s instead of failing on NOT_FOUND for {deleted!r}"
        )
        receipt = await broker.publish(kept, b"after")
        assert receipt.queue == kept
    finally:
        await broker.close()
        await delete_queue_family(rabbitmq_url, kept, deleted)
