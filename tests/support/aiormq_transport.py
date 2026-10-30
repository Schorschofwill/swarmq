"""Publish-Kanal mit echtem aio-pika/aiormq-Publish-Code, ohne Netz (B24).

``FakeTransport`` steht fuer die Verbindungsseite eines aiormq-Kanals: Der
echte ``aiormq.Channel.basic_publish`` legt seine Frames in ``write_queue``
und wartet auf zwei Futures, erst auf ``drain_future`` (der Writer hat den
Frame auf den Socket geschrieben), dann auf das Publisher-Confirm. Der Test
spielt den Writer und den Broker:

* ``frame.drain()`` -- der Writer hat den Frame geschrieben;
* ``confirm_all()`` -- der Broker bestaetigt;
* ``nack_all()`` -- der Broker bestaetigt negativ (``Basic.Nack``, wie aiormq
  als ``DeliveryError``);
* ``lose_connection(exc)`` -- die Verbindung reisst; wie aiormqs
  ``FutureStore.reject_all`` scheitern alle offenen Futures mit ``exc``.

Wie aiormq 7.2.4 traegt jeder Kanal das innere Future ``_closing``:
``lose_connection`` schliesst es mit, ``close_channel(exc)`` schliesst nur den
Kanal, ``is_closed`` liest es. Die Sperre ist wie dort ein
Async-Contextmanager, der vor und nach dem Erwerb prueft, ob der Kanal zu ist
(``ChannelInvalidStateError``), und ``basic_publish`` laeuft in aiormqs
``_raise_on_channel_close``. Mit ``closing=True`` ist zusaetzlich die
oeffentliche Eigenschaft ``closing`` lesbar, mit aiormqs Beobachter-Semantik:
Jeder Zugriff liefert einen neuen Beobachter von ``_closing``, und ein
abgebrochener Beobachter entfernt seinen Callback wieder. Ohne bleibt sie
unlesbar (Test-Doubles ohne ``closing``). Wie in aiormq lehnt erst
``lose_connection`` mit dem Schliessen auch die offenen Futures ab;
``close_channel`` allein bildet einen Kanal ohne offene Futures nach.
``lock.waiter_arrived`` meldet, wenn ein zweiter Publish in der
Warteschlange der Kanal-Sperre ankommt.

``FakePublishChannel`` ist die aio-pika-Seite: ein echtes
``aio_pika.Exchange`` als ``default_exchange`` ueber diesem Transport.
"""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from contextlib import AbstractAsyncContextManager
from types import TracebackType
from typing import Any, Literal, cast, override

import aio_pika
import aiormq
from aiormq.abc import ChannelFrame


class ObservableLock(asyncio.Lock):
    """Meldet, wenn ein Wartender ankommt (die Sperre ist schon belegt)."""

    def __init__(self) -> None:
        super().__init__()
        self.waiter_arrived = asyncio.Event()

    @override
    async def acquire(self) -> Literal[True]:
        if self.locked():
            self.waiter_arrived.set()
        return await super().acquire()


class ChannelLock:
    """Die Kanal-Sperre wie in aiormq 7.2.4 (``Channel.lock``): Wer sie per
    ``async with`` betritt, scheitert mit ``ChannelInvalidStateError``, wenn der
    Kanal vor oder nach dem Warten auf die Sperre zu ist. ``acquire``,
    ``release`` und ``waiter_arrived`` reichen an die innere Sperre durch,
    damit ein Test sie selbst belegen kann."""

    def __init__(self, transport: FakeTransport) -> None:
        self._transport = transport
        self._lock = ObservableLock()

    @property
    def waiter_arrived(self) -> asyncio.Event:
        return self._lock.waiter_arrived

    def locked(self) -> bool:
        return self._lock.locked()

    async def acquire(self) -> Literal[True]:
        return await self._lock.acquire()

    def release(self) -> None:
        self._lock.release()

    def _raise_if_closed(self) -> None:
        if self._transport.is_closed:
            raise aiormq.exceptions.ChannelInvalidStateError(f"{self._transport!r} closed")

    async def __aenter__(self) -> None:
        self._raise_if_closed()
        await self._lock.acquire()
        try:
            self._raise_if_closed()
        except BaseException:
            self._lock.release()
            raise

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self._lock.release()


class FakeTransport:
    """Die Attribute, die ``aiormq.Channel.basic_publish`` an ``self`` liest."""

    number = 1
    publisher_confirms = True
    max_content_size = 131072

    def __init__(self, *, closing: bool = False) -> None:
        self._closing_readable = closing
        self._closing: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        # Wie aiormq: die Ausnahme des Schliessens holt ein Callback ab.
        self._closing.add_done_callback(_retrieve)
        # Wie aiormq: nur ein vom Broker geschlossener Kanal setzt den Grund
        # (``_on_close_frame``); ``_raise_on_channel_close`` liest ihn.
        self._close_exception: BaseException | None = None
        self.lock = ChannelLock(self)
        self.delivery_tag = 0
        self.confirmations: OrderedDict[int, Any] = OrderedDict()
        self.message_id_delivery_tag: dict[str, int] = {}
        self.write_queue: asyncio.Queue[ChannelFrame] = asyncio.Queue()
        self._futures: list[asyncio.Future[Any]] = []

    @property
    def is_closed(self) -> bool:
        return self._closing.done()

    @property
    def closing(self) -> asyncio.Future[Any]:
        """Wie ``aiormq.base.Base.closing`` (7.2.4): je Zugriff ein neuer
        Beobachter von ``_closing``; ein abgebrochener Beobachter entfernt
        seinen Callback wieder. Ohne ``closing=True`` unlesbar."""
        if not self._closing_readable:
            msg = "FakeTransport ohne closing=True hat kein lesbares closing"
            raise AttributeError(msg)
        if self._closing.done():
            return self._closing

        future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()

        def on_close(source: asyncio.Future[Any]) -> None:
            if future.done():
                return
            if source.cancelled():
                future.cancel()
            elif (exc := source.exception()) is not None:
                future.set_exception(exc)
            else:
                future.set_result(source.result())

        def on_done(observer: asyncio.Future[Any]) -> None:
            self._closing.remove_done_callback(on_close)
            if not observer.cancelled():
                observer.exception()

        self._closing.add_done_callback(on_close)
        future.add_done_callback(on_done)
        return future

    @closing.setter
    def closing(self, future: asyncio.Future[Any]) -> None:
        """Ersetzt das innere ``_closing`` (etwa durch ein zaehlendes Future)."""
        self._closing_readable = True
        self._closing = future

    def _raise_on_channel_close(self) -> AbstractAsyncContextManager[None]:
        # Der echte Code aus aiormq, nur mit diesem Objekt als Kanal.
        return cast(
            "AbstractAsyncContextManager[None]",
            aiormq.Channel._raise_on_channel_close(cast("aiormq.Channel", self)),
        )

    def create_future(self) -> asyncio.Future[Any]:
        future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        # Wie aiormqs ``FutureStore.create_future`` seit 7.2 (#253): Ein
        # abgelehntes Future, auf das niemand mehr wartet (etwa das Confirm
        # nach einem gescheiterten Drain), meldet asyncio nicht als "never retrieved".
        future.add_done_callback(_retrieve)
        self._futures.append(future)
        return future

    def _split_body(self, body: bytes) -> list[Any]:
        return cast("list[Any]", aiormq.Channel._split_body(cast("aiormq.Channel", self), body))

    async def basic_publish(self, body: bytes, **kwargs: Any) -> Any:
        # Der echte Code aus aiormq, nur mit diesem Objekt als Kanal.
        return await aiormq.Channel.basic_publish(cast("aiormq.Channel", self), body, **kwargs)

    async def next_frame(self) -> ChannelFrame:
        return await self.write_queue.get()

    def confirm_all(self) -> None:
        for future in self.confirmations.values():
            if not future.done():
                future.set_result(aiormq.spec.Basic.Ack())

    def nack_all(self) -> None:
        for future in self.confirmations.values():
            if not future.done():
                future.set_exception(aiormq.exceptions.DeliveryError(None, aiormq.spec.Basic.Nack()))

    def lose_connection(self, exc: BaseException) -> None:
        self.reject_pending(exc)
        self.close_channel(exc)

    def reject_pending(self, exc: BaseException) -> None:
        """Lehnt die offenen Futures ab, ``closing`` bleibt offen (die
        Reihenfolge in aiormqs ``reject_all`` ist nicht festgelegt)."""
        for future in self._futures:
            if not future.done():
                future.set_exception(exc)

    def close_channel(self, exc: BaseException) -> None:
        """Schliesst nur den Kanal (``_closing``), etwa wenn der Kanal schon
        zu ist, bevor aiormq die offenen Futures ablehnt."""
        if not self._closing.done():
            self._closing.set_exception(exc)

    def forget_futures(self) -> None:
        """Gibt die Futures frei (wie aiormq nach dem Ablehnen), damit die
        Garbage Collection sie einsammelt."""
        self._futures.clear()
        self.confirmations.clear()


def _retrieve(future: asyncio.Future[Any]) -> None:
    if not future.cancelled():
        future.exception()


class FakePublishChannel:
    is_closed = False

    def __init__(self, transport: FakeTransport) -> None:
        self.transport = transport
        self.default_exchange = aio_pika.Exchange(cast("aio_pika.abc.AbstractChannel", self), "")
        self.closed = False

    async def get_underlay_channel(self) -> FakeTransport:
        return self.transport

    async def close(self) -> None:
        self.closed = True
