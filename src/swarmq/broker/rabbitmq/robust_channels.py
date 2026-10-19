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
"""

from __future__ import annotations

import weakref
from typing import Any, cast

import aio_pika

from swarmq.logging import get_logger

logger = get_logger(__name__)

_CHANNELS_ATTR = "_RobustConnection__channels"

#: Verbindungen, deren ``WeakSet`` schon als unlesbar gemeldet ist.
_unreadable_reported: weakref.WeakSet[Any] = weakref.WeakSet()


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
