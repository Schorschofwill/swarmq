"""Leckmessung: Callbacks an ``_closing`` unter aiormq 7.2.4 und aio-pika 10.2.1 (Plan 2026-10-29-1945, U3, U5, U6, KTD2, KTD6).

Seit aiormq 7.2 liefert ``closing`` bei jedem Zugriff einen neuen Beobachter.
Jeder offene Beobachter hält einen Callback am inneren Future ``_closing``,
bis er selbst fertig oder abgebrochen ist
(``tests/unit/broker/test_aiormq_7_2_semantics.py``). Gemessen wird deshalb
die Zahl der Callbacks an ``_closing``, nie an ``closing``.

* KTD2: Bis U5 las swarmq bei jedem Publish ``closing`` des Publish-Kanals
  (FS-08-Wächter) und ließ je Publish einen Callback an ``_closing`` des
  Kanals zurück. U5 hat den Wächter entfernt; aiormq 7.2 beendet einen
  Publish in der Warteschlange der Kanal-Sperre selbst (#240).
* KTD6: aio-pikas ``UnderlayChannel.create`` hängt den ``close_callback`` an
  einen Beobachter von ``connection.closing`` (a), ``UnderlayChannel.close``
  liest ``connection.closing`` noch einmal und erzeugt einen zweiten, leeren
  Beobachter (b). Beide bleiben an ``_closing`` der Verbindung, bis sie
  endet: zwei Callbacks je geschlossenem Kanal; ein vom Broker geschlossener
  ``RobustChannel``, den aio-pika selbst wiederherstellt, lässt (a) zurück.

Stand U3, je drei Läufe: ohne Abhilfe +20 000 bei 10 000 Wegwerf-Kanälen,
+2 000 bei 1 000 Verwürfen, +10 000 an ``_closing`` des Publish-Kanals mit
dem FS-08-Wächter. Von zwei Prototypen (nur im Test) hielt nur Weg 1 (eigener
Beobachter, abgebrochen, sobald der aiormq-Kanal zu ist) alle Messungen flach,
auch den vom Broker geschlossenen ``RobustChannel``; Weg 2 (Beobachter nach
``close()`` abbrechen) erreichte den nicht.

Seit U6 läuft Weg 1 in ``src/swarmq/broker/rabbitmq/underlay.py``: Die
Wegwerf-Kanäle (``open_throwaway_channel``) und die robusten Kanäle auf
swarmqs eigenen Verbindungen (``CHANNEL_CLASS``) halten ihren Beobachter
selbst. Die Messungen laufen über diese echten Pfade und sind ohne Prototyp
flach.

Test-Liste:
# [x] KTD2: Ein Publish in der Warteschlange der Kanal-Sperre endet nach dem
#     Schließen des Kanals sofort mit ChannelInvalidStateError
# [x] KTD2: 10 000 Publishes über RabbitMQBroker -> Callbacks an _closing des
#     Publish-Kanals flach (seit U5 ohne FS-08-Wächter; in U3 xfail strict)
# [x] KTD6: 10 000 Wegwerf-Kanäle -> Callbacks an _closing der Verbindung
#     flach (seit U6; in U3 ohne Prototyp xfail strict)
# [x] KTD6: 1 000 Verwürfe des Publish-Kanals nach Fristriss (E-078) ->
#     flach (seit U6; in U3 ohne Prototyp xfail strict)
# [x] KTD6: vom Broker geschlossener, von aio-pika wiederhergestellter
#     Publish-Kanal -> flach (seit U6)
# [x] KTD6: Das Ende der Verbindung erreicht einen Wegwerf-Kanal weiterhin (close_callbacks)
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator
from typing import Any

import aio_pika
import aiormq
import pytest
from aiormq.exceptions import ChannelInvalidStateError, ChannelPreconditionFailed

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.broker.rabbitmq.connection import close_channel_quietly
from swarmq.broker.rabbitmq.robust_channels import open_throwaway_channel
from tests.support.queues import delete_queue_family
from tests.support.wait import wait_until

pytestmark = [pytest.mark.integration, pytest.mark.timeout(240)]

_PUBLISHES = 10_000
_THROWAWAY_CHANNELS = 10_000
_DISCARDS = 1_000
# Parallelität beim Messen: nur für die Laufzeit, die Messung zählt am Ende.
_BATCH = 50


def _callbacks(future: asyncio.Future[Any]) -> int:
    """Zahl der Done-Callbacks; privat, Python 3.14 bietet keine öffentliche
    Zählung (``_callbacks`` ist ``None`` ohne Callbacks)."""
    return len(future._callbacks or ())


def _inner_closing(connection: aio_pika.abc.AbstractConnection | None) -> asyncio.Future[Any]:
    """``_closing`` der aiormq-Verbindung unter *connection* (nicht ``closing``)."""
    assert connection is not None
    assert connection.transport is not None
    closing: asyncio.Future[Any] = connection.transport.connection._closing  # type: ignore[attr-defined]  # inneres Future, s. Modul-Docstring
    return closing


def restored_by_aio_pika(channel: aio_pika.RobustChannel, old: aiormq.abc.AbstractChannel) -> bool:
    """Hat aio-pika den vom Broker geschlossenen Kanal *old* ersetzt?"""
    return not channel.is_closed and channel._channel is not None and channel._channel.channel is not old


@pytest.fixture
async def broker(rabbitmq_url: str) -> AsyncIterator[tuple[RabbitMQBroker, str]]:
    queue = f"cbflat-{uuid.uuid4().hex[:8]}"
    impl = RabbitMQBroker(rabbitmq_url, queue_type="classic")
    await impl.connect()
    try:
        await impl.declare_queue(queue)
        yield impl, queue
    finally:
        await impl.close()
        await delete_queue_family(rabbitmq_url, queue)


# --- KTD2 ---------------------------------------------------------------------


async def test_a_publish_waiting_on_the_channel_lock_ends_at_once_when_the_channel_closes(rabbitmq_url: str) -> None:
    """KTD2 (aiormq #240) gegen das echte aiormq: Wer beim Schließen in der
    Warteschlange der Kanal-Sperre steht, reiht nichts mehr ein. Das ist die
    Ursache von FS-08; unter 7.0 hing der Publish auf dem toten Kanal."""
    connection = await aio_pika.connect(rabbitmq_url)
    try:
        channel = await connection.channel()
        underlay = await channel.get_underlay_channel()
        assert isinstance(underlay, aiormq.Channel)
        lock = underlay._Channel__lock  # type: ignore[attr-defined]  # die Sperre hinter der Property ``lock``

        async with underlay.lock:
            publish = asyncio.create_task(underlay.basic_publish(b"x", routing_key="cbflat-none"))
            # asyncio.Lock bietet keine API für seine Warteschlange.
            assert await wait_until(lambda: bool(lock._waiters), timeout=5, interval=0.01), (
                "precondition: the publish waits on the channel lock"
            )
            # Der Broker schließt den Kanal: Ack auf ein nie vergebenes Tag.
            await underlay.basic_ack(delivery_tag=2**31)
            await asyncio.wait({underlay.closing}, timeout=5)
            assert underlay.is_closed
            assert isinstance(underlay.closing.exception(), ChannelPreconditionFailed)
            assert not publish.done()

        done, _ = await asyncio.wait({publish}, timeout=1)
        assert done, "the waiting publish ends right after the lock is released"
        with pytest.raises(ChannelInvalidStateError):
            publish.result()
    finally:
        await connection.close()


async def test_publishes_keep_the_callbacks_on_the_publish_channel_flat(broker: tuple[RabbitMQBroker, str]) -> None:
    impl, queue = broker
    await impl.publish(queue, b"warm-up")
    channel = impl._publish_channel
    assert channel is not None
    underlay = await channel.get_underlay_channel()
    inner: asyncio.Future[Any] = underlay._closing  # type: ignore[attr-defined]  # inneres Future
    before = _callbacks(inner)

    for _ in range(_PUBLISHES // _BATCH):
        await asyncio.gather(*(impl.publish(queue, b"x") for _ in range(_BATCH)))

    assert impl._publish_channel is channel
    assert await channel.get_underlay_channel() is underlay, "precondition: one channel for the whole measurement"
    after = _callbacks(inner)
    assert after == before, f"callbacks on the publish channel's _closing: {before} -> {after}"


# --- KTD6 ---------------------------------------------------------------------


async def _throwaway(connection: aio_pika.abc.AbstractConnection) -> None:
    channel = await open_throwaway_channel(connection)
    await close_channel_quietly(channel)


async def test_throwaway_channels_keep_the_callbacks_on_the_connection_flat(
    broker: tuple[RabbitMQBroker, str],
) -> None:
    """Wegwerf-Kanäle über swarmqs Pfad (``open_throwaway_channel``,
    ``close_channel_quietly``), wie Argument-, Existenz- und Tiefenprüfungen."""
    impl, _ = broker
    connection = impl._connection
    inner = _inner_closing(connection)
    before = _callbacks(inner)

    for _ in range(_THROWAWAY_CHANNELS // _BATCH):
        await asyncio.gather(*(_throwaway(connection) for _ in range(_BATCH)))  # type: ignore[arg-type]  # verbunden, s. _inner_closing

    assert _inner_closing(connection) is inner, "precondition: no reconnect during the measurement"
    after = _callbacks(inner)
    assert after == before, f"callbacks on the connection's _closing: {before} -> {after}"


async def test_discarded_publish_channels_keep_the_callbacks_on_the_connection_flat(
    broker: tuple[RabbitMQBroker, str],
) -> None:
    """E-078: Verwurf nach Fristriss über den echten Pfad
    (``_discard_publish_channel`` -> ``release_channel`` ->
    ``_close_channel_detached``); der nächste Publish öffnet einen neuen
    robusten Kanal."""
    impl, queue = broker
    await impl.publish(queue, b"warm-up")
    inner = _inner_closing(impl._connection)
    before = _callbacks(inner)

    for _ in range(_DISCARDS):
        await impl._discard_publish_channel(impl._publish_channel)
        assert await wait_until(lambda: not impl._pending_channel_closes, timeout=5, interval=0.001)
        await impl.publish(queue, b"x")

    assert _inner_closing(impl._connection) is inner, "precondition: no reconnect during the measurement"
    # Der lebende Publish-Kanal hält wie vor der Messung genau seinen einen Beobachter.
    after = _callbacks(inner)
    assert after == before, f"callbacks on the connection's _closing: {before} -> {after}"


async def test_publish_channels_closed_by_the_broker_keep_the_callbacks_on_the_connection_flat(
    broker: tuple[RabbitMQBroker, str],
) -> None:
    """Einen ``RobustChannel``, den der Broker schließt, stellt aio-pika
    selbst wieder her (``_on_close`` -> ``restore``), ohne ``close()``. Der
    alte Unterbau wird nie geschlossen; bei aio-pika bliebe sein Beobachter (a)."""
    impl, queue = broker
    await impl.publish(queue, b"warm-up")
    channel = impl._publish_channel
    assert isinstance(channel, aio_pika.RobustChannel)
    inner = _inner_closing(impl._connection)
    before = _callbacks(inner)

    for _ in range(100):
        underlay = await channel.get_underlay_channel()
        await underlay.basic_ack(delivery_tag=2**31)  # Broker schließt den Kanal
        await asyncio.wait({underlay.closing}, timeout=5)
        assert await wait_until(
            lambda u=underlay: restored_by_aio_pika(channel, u),  # type: ignore[misc]  # Lambda-Default bindet den Kanal
            timeout=5,
            interval=0.005,
        ), "precondition: aio-pika restores the channel"

    assert impl._publish_channel is channel, "precondition: swarmq kept its publish channel"
    assert _inner_closing(impl._connection) is inner, "precondition: no reconnect during the measurement"
    after = _callbacks(inner)
    assert after == before, f"callbacks on the connection's _closing: {before} -> {after}"


async def test_the_end_of_the_connection_still_reaches_a_throwaway_channel(rabbitmq_url: str) -> None:
    """Der eigene Beobachter behält die Aufgabe von (a): Endet die Verbindung,
    erfährt der aio-pika-Kanal davon (``close_callbacks``)."""
    connection = await aio_pika.connect_robust(rabbitmq_url)
    closed = asyncio.Event()

    def on_close(*_: object) -> None:
        closed.set()

    try:
        channel = await open_throwaway_channel(connection)
        channel.close_callbacks.add(on_close)
        inner = _inner_closing(connection)
        assert _callbacks(inner) >= 1, "precondition: the open channel holds its observer"
    finally:
        await connection.close()

    assert await wait_until(closed.is_set, timeout=5, interval=0.01)
    assert channel.is_closed
