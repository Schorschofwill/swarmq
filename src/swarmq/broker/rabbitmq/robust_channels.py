"""Kanaele auf einer ``RobustConnection`` ohne Wiederherstellung nach dem Schliessen (F4-20).

aio-pika 10.0.1: ``RobustConnection._on_connected`` ruft nach jedem Reconnect
``restore()`` fuer jeden ``RobustChannel`` ihres ``WeakSet``, auch fuer
geschlossene; ``RobustChannel.close()`` leert nur das Restored-Event, und
``restore()`` oeffnet den Kanal trotzdem neu. aiormq haelt ihn danach stark:
Jeder Reconnect bringt alle geschlossenen, noch nicht eingesammelten Kanaele
zurueck, samt ihrer robusten Queues. Eine passiv deklarierte, inzwischen
geloeschte Queue laesst den Reconnect dann endlos an NOT_FOUND scheitern.

Zwei Wege:

* Wegwerf-Kanaele (Deklaration, Tiefe, Queue-Status, DLQ) brauchen keine
  Wiederherstellung: ``open_throwaway_channel`` oeffnet einen einfachen
  ``aio_pika.Channel`` (oeffentliches API), der nie im ``WeakSet`` steht. Ein
  vom Broker geschlossener Kanal (NOT_FOUND, 406) bleibt zu.
* Publish- und Consume-Kanal bleiben robust. Verwirft oder schliesst swarmq
  einen davon, meldet ``release_channel`` ihn ab. Das ``WeakSet`` ist privat
  (``_RobustConnection__channels``); fehlt es, gilt fail-open wie beim
  Blocked-Zustand (E-025): Der Kanal kann nach einem Reconnect wieder
  aufgehen wie vor 0.14, einmal je Verbindung meldet das eine WARNING.

Dazu die Restore-Liste der Queues eines ``RobustChannel`` (PF-20, E-083):
``RobustChannel._on_open`` deklariert nach jedem Reconnect jede Queue aus
``_queues`` passiv neu. Fehlt eine konsumierte Queue beim Broker, scheitert das
an NOT_FOUND, schliesst den Consume-Kanal und laesst den Reconnect endlos
scheitern. ``release_queue`` nimmt sie aus der Liste, ``readmit_queue`` legt sie
zurueck, sobald es sie wieder gibt. ``_queues`` ist privat; fehlt es, gilt
fail-open mit einer WARNING je Kanal (Reconnect-Schleife wie vor 0.14).
"""

from __future__ import annotations

import asyncio
import contextlib
import weakref
from typing import Any, cast

import aio_pika

from swarmq.broker.rabbitmq.channel_close import close_without_abort
from swarmq.logging import get_logger

logger = get_logger(__name__)

_CHANNELS_ATTR = "_RobustConnection__channels"
_QUEUES_ATTR = "_queues"

#: Verbindungen, deren ``WeakSet`` schon als unlesbar gemeldet ist.
_unreadable_reported: weakref.WeakSet[Any] = weakref.WeakSet()
#: Kanaele, deren Restore-Liste schon als unlesbar gemeldet ist.
_queues_unreadable_reported: weakref.WeakSet[Any] = weakref.WeakSet()


def open_throwaway_channel(connection: aio_pika.abc.AbstractConnection) -> aio_pika.abc.AbstractChannel:
    """Ein nicht-robuster Kanal auf ``connection``, wie ``connection.channel()``
    noch nicht geoeffnet (erst ``await`` oeffnet ihn).

    Auf einer ``RobustConnection`` ein ``aio_pika.Channel`` statt des
    ``RobustChannel``: Er steht nicht im ``WeakSet`` der Verbindung, ein
    Reconnect oeffnet ihn nicht wieder (F4-20). Jede andere Verbindung liefert
    ohnehin einen nicht-robusten Kanal."""
    if isinstance(connection, aio_pika.RobustConnection):
        return aio_pika.Channel(connection)
    return connection.channel()


async def queue_exists_on_throwaway(
    connection: aio_pika.abc.AbstractConnection, name: str, *, close_timeout_s: float
) -> bool:
    """Passives Declare von *name* auf einem Wegwerf-Kanal; NOT_FOUND heisst
    ``False`` (PF-20, E-083).

    Die Frist der Pruefung setzt der Aufrufer. Ein Abbruch trifft das Oeffnen
    nicht: aiormq faengt dort ``CancelledError`` nicht und behielte den Kanal
    halb offen. Das ``finally`` wartet das Oeffnen ab und schliesst den Kanal,
    beides zusammen hoechstens *close_timeout_s*: Haengt das Declare, haengt
    auch das Close."""
    opening: asyncio.Future[aio_pika.abc.AbstractChannel] = asyncio.ensure_future(open_throwaway_channel(connection))
    try:
        channel = await asyncio.shield(opening)
        await channel.declare_queue(name, passive=True)
    except aio_pika.exceptions.ChannelNotFoundEntity:
        return False
    finally:
        with contextlib.suppress(Exception):
            await asyncio.wait_for(_close_when_open(opening), timeout=close_timeout_s)
    return True


async def _close_when_open(opening: asyncio.Future[aio_pika.abc.AbstractChannel]) -> None:
    channel = await opening
    # Die Frist des Aufrufers trifft nur das Warten, nie das Close (F4-30).
    await close_without_abort(channel, timeout_s=None)


def release_channel(connection: object, channel: object) -> None:
    """Meldet ``channel`` aus dem Wiederherstellungs-``WeakSet`` von
    ``connection`` ab, damit ein Reconnect ihn nicht wieder oeffnet (F4-20).

    Ohne ``RobustConnection`` (Test-Doubles, einfache Verbindung) gibt es
    nichts abzumelden. Fehlt das private ``WeakSet`` (aio-pika geaendert),
    gilt fail-open mit einer WARNING je Verbindung."""
    if not isinstance(connection, aio_pika.RobustConnection):
        return
    channels: object = getattr(connection, _CHANNELS_ATTR, None)
    if isinstance(channels, weakref.WeakSet):
        cast("weakref.WeakSet[object]", channels).discard(channel)
        return
    if connection in _unreadable_reported:
        return
    _unreadable_reported.add(connection)
    logger.warning(
        "robust_channel_release_unavailable",
        hint=(
            "cannot read the channel set of aio-pika's RobustConnection; a closed or discarded "
            "channel may be reopened after a reconnect and stay open at the broker (fail-open)"
        ),
    )


def release_queue(queue: Any, name: str) -> None:
    """Nimmt ``queue`` aus der Restore-Liste ihres Kanals: Ein Reconnect
    deklariert sie nicht mehr passiv neu (PF-20, E-083)."""
    registry = _restore_registry(queue.channel)
    if registry is None:
        return
    members = registry.get(name, set())
    members.discard(queue)
    if not members:
        registry.pop(name, None)


def readmit_queue(queue: Any, name: str) -> None:
    """Legt ``queue`` zurueck in die Restore-Liste ihres Kanals, sobald es sie
    beim Broker wieder gibt (Gegenstueck zu ``release_queue``)."""
    registry = _restore_registry(queue.channel)
    if registry is not None:
        registry.setdefault(name, set()).add(queue)


def _restore_registry(channel: object) -> dict[str, set[Any]] | None:
    """``RobustChannel._queues`` oder ``None``; fehlt es an einem
    ``RobustChannel``, eine WARNING je Kanal (fail-open)."""
    registry: object = getattr(channel, _QUEUES_ATTR, None)
    if isinstance(registry, dict):
        return cast("dict[str, set[Any]]", registry)
    if isinstance(channel, aio_pika.RobustChannel) and channel not in _queues_unreadable_reported:
        _queues_unreadable_reported.add(channel)
        logger.warning(
            "robust_queue_release_unavailable",
            hint=(
                "cannot read the queue list of aio-pika's RobustChannel; a reconnect declares a deleted "
                "consumed queue again and fails on NOT_FOUND until the worker restarts (fail-open)"
            ),
        )
    return None
