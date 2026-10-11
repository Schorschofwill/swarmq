"""Blocked-Zustand der Publish-Verbindung (Plan 0.12.0, MR 16, BZ-16).

Bei einem Ressourcenalarm (Speicher, Platte) meldet RabbitMQ einer
publizierenden Verbindung ``connection.blocked`` und liest bis
``connection.unblocked`` nichts mehr von ihr. aio-pika/aiormq bieten dafuer
kein oeffentliches ``is_blocked``; Spike S6 hat belegt, dass der Zustand
synchron nur ueber das private ``aiormq.Connection.__connection_unblocked``
(``asyncio.Event``, name-mangled) lesbar ist. Der Pfad
``connection.transport.connection`` wird bei jedem Lesen neu gegangen: Ein
robuster Reconnect bringt ein neues ``aiormq.Connection``-Objekt.

Ist der Zustand nicht lesbar (aiormq geaendert), gilt fail-open: Der Publish
laeuft wie vor 0.12, der Broker meldet einmal eine WARNING.
"""

from __future__ import annotations

import asyncio
from typing import Any

_UNBLOCKED_ATTR = "_Connection__connection_unblocked"


class Unreadable:
    """Marker: der Blocked-Zustand ist nicht lesbar (fail-open)."""


UNREADABLE = Unreadable()


def unblocked_event(connection: Any) -> asyncio.Event | Unreadable | None:
    """Das Unblocked-Event der aktuellen aiormq-Verbindung.

    ``None``, solange keine Transport-Verbindung besteht (nicht verbunden,
    Reconnect laeuft) -- dann gibt es nichts zu pruefen, der Publish-Pfad
    behandelt den Reconnect selbst. ``UNREADABLE``, wenn die Verbindung
    besteht, das Event aber fehlt. Kein Roundtrip, kein Warten."""
    transport = getattr(connection, "transport", None)
    underlying = getattr(transport, "connection", None) if transport is not None else None
    if underlying is None:
        return None
    event = getattr(underlying, _UNBLOCKED_ATTR, None)
    if not isinstance(event, asyncio.Event):
        return UNREADABLE
    return event
