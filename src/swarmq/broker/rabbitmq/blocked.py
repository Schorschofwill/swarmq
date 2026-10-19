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
from typing import Any, cast

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


BlockedClose = tuple[asyncio.Event, asyncio.Future[object]]


def blocked_close_waiters(connection: Any) -> BlockedClose | None:
    """Unblocked-Event und ``closing`` derselben aiormq-Verbindung, wenn sie
    gerade blockiert ist (PF-19 a, Nachtrag (3) zu E-025).

    Gelesen beim Verwurf des Publish-Kanals, nicht erst beim Warten: Ein
    Reconnect bringt ein neues aiormq-Objekt, dessen Event das Close des
    alten Kanals nie freigaebe; ``closing`` meldet das Ende des alten.
    ``None`` heisst "nicht blockiert" oder "nicht lesbar" (fail-open, E-025):
    dann sofort schliessen wie bisher."""
    event = unblocked_event(connection)
    if not isinstance(event, asyncio.Event) or event.is_set():
        return None
    closing = getattr(connection.transport.connection, "closing", None)
    if not isinstance(closing, asyncio.Future):
        return None
    return event, cast("asyncio.Future[object]", closing)


async def wait_until_unblocked(blocked: BlockedClose, timeout: float) -> None:
    """Wartet auf ``unblocked`` oder das Ende der Verbindung, hoechstens
    ``timeout``; kein Ausgang ist ein Fehler. Der Hilfs-Task endet mit."""
    unblocked, closing = blocked
    waiter = asyncio.ensure_future(unblocked.wait())
    either: set[asyncio.Future[Any]] = {waiter, closing}
    try:
        await asyncio.wait(either, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
    finally:
        waiter.cancel()
