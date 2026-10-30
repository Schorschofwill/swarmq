"""Kanäle ohne Beobachter-Leck an der Verbindung (Plan 2026-10-29-1945, KTD6 Weg 1, U6).

aio-pika 10.2.1 hängt in ``UnderlayChannel.create`` den ``close_callback`` an
einen Beobachter von ``connection.closing`` (seit aiormq 7.2 liefert jeder
Zugriff einen neuen). ``UnderlayChannel.close`` liest ``connection.closing``
noch einmal, um den Callback zu entfernen, und erzeugt dabei einen zweiten,
leeren Beobachter. Beide bleiben an ``_closing`` der aiormq-Verbindung, bis
sie endet; der erste hält über den Callback den ganzen Kanal. Ein vom Broker
geschlossener ``RobustChannel``, den aio-pika selbst wiederherstellt, wird nie
geschlossen und lässt seinen Beobachter ebenso zurück.

``_ReleasingUnderlay`` hält seinen Beobachter selbst und bricht ihn ab, sobald
der aiormq-Kanal zu ist, gleich auf welchem Weg (eigenes Close, Broker,
abgebrochenes Öffnen). Sein ``close`` liest ``connection.closing`` nicht
erneut. Endet die Verbindung vorher, meldet der Beobachter das dem Kanal wie
bei aio-pika.

Darüber laufen die Wegwerf-Kanäle auf jeder aio-pika-Verbindung
(``open_throwaway_channel``) und Publish- und Consume-Kanal auf swarmqs
eigenen Verbindungen (``CHANNEL_CLASS`` in ``connection.py``), auch beim
Wiederherstellen. Den Publish-Kanal auf einer injizierten Verbindung öffnet
deren eigene Kanalklasse; er bleibt beim Verhalten von aio-pika.

``_open`` ersetzt ``aio_pika.Channel._open`` als Kopie aus 10.2.1, nur mit
``_ReleasingUnderlay``. Passt aio-pika nicht mehr dazu (andere Namen in
``Channel._open`` oder andere Felder in ``UnderlayChannel``), gilt fail-open:
Der Kanal öffnet über aio-pikas eigenes ``_open`` und leckt wie dort, einmal
je Verbindung meldet das eine WARNING. Dass ``closing`` je Zugriff einen
eigenen Beobachter liefert, den abzubrechen niemand sonst stört, hält
``tests/unit/broker/test_aiormq_7_2_semantics.py`` fest; ohne aiormq-Verbindung
(Test-Doubles) öffnet ebenso aio-pikas ``_open``, ohne WARNING.
"""

from __future__ import annotations

import asyncio
import dataclasses
import weakref
from collections.abc import Awaitable, Callable
from typing import Any, override

import aio_pika
import aiormq
from aio_pika.abc import UnderlayChannel
from aio_pika.tools import OneShotCallback
from aiormq.base import Base

from swarmq.logging import get_logger

logger = get_logger(__name__)

#: Namen in ``aio_pika.Channel._open`` (``co_names``) und Felder von
#: ``UnderlayChannel`` in aio-pika 10.2.1, der Vorlage von ``_open``.
_OPEN_NAMES = frozenset({
    "BaseException", "ChannelInvalidStateError", "UnderlayChannel", "_channel", "_channel_number", "_closed",
    "_connection", "_on_close", "_on_open", "asyncio", "close", "connection", "create", "create_future", "done",
    "get_running_loop", "on_return_raises", "publisher_confirms", "ready", "transport",
})  # fmt: skip
_UNDERLAY_FIELDS = frozenset({"channel", "close_callback"})

#: Verbindungen, deren Rückfall auf aio-pikas ``_open`` schon gemeldet ist.
_unfit_reported: weakref.WeakSet[Any] = weakref.WeakSet()


class _ReleasingUnderlay(UnderlayChannel):
    """``UnderlayChannel``, der seinen Beobachter von ``connection.closing`` mit dem Kanal abbricht."""

    @classmethod
    @override
    async def create(
        cls,
        connection: aiormq.abc.AbstractConnection,
        close_callback: Callable[..., Awaitable[Any]],
        **kwargs: Any,
    ) -> UnderlayChannel:
        callback = OneShotCallback(close_callback)
        await connection.ready()
        observer = _observe(connection)
        observer.add_done_callback(callback)

        def release(_: object = None) -> None:
            observer.remove_done_callback(callback)
            observer.cancel()

        try:
            channel = await connection.channel(**kwargs)
        except BaseException:
            release()
            raise
        channel_closing = _observe(channel)
        channel_closing.add_done_callback(callback)
        channel_closing.add_done_callback(release)
        return cls(channel=channel, close_callback=callback)

    @override
    async def close(self, exc: aiormq.abc.ExceptionType | None = None) -> Any:
        if self.close_callback.finished.is_set():
            return
        await self.channel.close(exc)
        await self.close_callback.wait()


def _observe(resource: Any) -> asyncio.Future[Any]:
    """``resource.closing``: seit aiormq 7.2 je Zugriff ein neuer Beobachter (aiormq typisiert ihn ohne Parameter)."""
    observer: asyncio.Future[Any] = resource.closing
    return observer


async def _open(channel: aio_pika.Channel, fallback: Callable[[], Awaitable[None]]) -> None:
    """``aio_pika.Channel._open`` (10.2.1), nur mit ``_ReleasingUnderlay``.

    Ohne aiormq-Verbindung (Test-Doubles) öffnet ``fallback``, aio-pikas
    eigenes ``_open``: Dort ist ``closing`` womöglich ein gemeinsames Future,
    kein Beobachter, und abbrechen darf ihn nur, wer ihn allein hält."""
    transport = channel._connection.transport
    if transport is None:
        raise aiormq.exceptions.ChannelInvalidStateError("No active transport in channel")
    if not isinstance(transport.connection, Base):
        await fallback()
        return
    if not _fits_aio_pika():
        _report_unfit(channel._connection)
        await fallback()
        return
    await transport.ready()
    internals: Any = channel  # aio-pika typisiert ``_on_close`` und ``_closed`` ohne Future-Parameter
    underlay = await _ReleasingUnderlay.create(
        transport.connection,
        internals._on_close,
        publisher_confirms=channel.publisher_confirms,
        on_return_raises=channel.on_return_raises,
        channel_number=channel._channel_number,
    )
    channel._channel = underlay
    try:
        await channel._on_open()
    except BaseException as e:
        await underlay.close(e)
        channel._channel = None
        raise
    if internals._closed.done():
        internals._closed = asyncio.get_running_loop().create_future()


def _fits_aio_pika() -> bool:
    """``True``, solange ``aio_pika.Channel._open`` und ``UnderlayChannel`` der Vorlage 10.2.1 entsprechen."""
    names = frozenset(aio_pika.Channel._open.__code__.co_names)
    fields = frozenset(field.name for field in dataclasses.fields(UnderlayChannel))
    return names == _OPEN_NAMES and fields == _UNDERLAY_FIELDS


def _report_unfit(connection: object) -> None:
    if connection in _unfit_reported:
        return
    _unfit_reported.add(connection)
    logger.warning(
        "channel_observer_release_unavailable",
        hint=(
            "aio-pika's channel opening differs from the version swarmq was built against; channels open the "
            "aio-pika way and each closed channel leaves two observers on the connection until it ends (fail-open)"
        ),
    )


class ReleasingChannel(aio_pika.Channel):
    """``aio_pika.Channel`` ohne Beobachter-Leck an der Verbindung."""

    @override
    async def _open(self) -> None:
        await _open(self, super()._open)


class ReleasingRobustChannel(aio_pika.RobustChannel):
    """``aio_pika.RobustChannel`` ohne Beobachter-Leck, auch beim Wiederherstellen."""

    @override
    async def _open(self) -> None:
        await _open(self, super()._open)
