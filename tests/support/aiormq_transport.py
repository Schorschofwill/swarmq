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

Mit ``closing=True`` traegt der Kanal wie ein echter aiormq-Kanal ein
``closing``-Future (FS-08): ``lose_connection`` schliesst es mit,
``close_channel(exc)`` schliesst nur den Kanal. Ohne bleibt der Publish im
Pfad vor FS-08 (Test-Doubles ohne ``closing``). ``lock.waiter_arrived`` meldet,
wenn ein zweiter Publish in der Warteschlange der Kanal-Sperre ankommt.

``FakePublishChannel`` ist die aio-pika-Seite: ein echtes
``aio_pika.Exchange`` als ``default_exchange`` ueber diesem Transport.
"""

from __future__ import annotations

import asyncio
from collections import OrderedDict
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


class FakeTransport:
    """Die Attribute, die ``aiormq.Channel.basic_publish`` an ``self`` liest."""

    number = 1
    publisher_confirms = True
    max_content_size = 131072

    def __init__(self, *, retrieve_rejected: bool = True, closing: bool = False) -> None:
        # ``False``: abgelehnte Futures bleiben unabgeholt wie in aiormq, damit
        # ein Test asyncios "Future exception was never retrieved" sieht.
        self._retrieve_rejected = retrieve_rejected
        if closing:
            self.closing: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
            # Wie aiormq: die Ausnahme des Schliessens holt ein Callback ab.
            self.closing.add_done_callback(_retrieve)
        self.lock = ObservableLock()
        self.delivery_tag = 0
        self.confirmations: OrderedDict[int, Any] = OrderedDict()
        self.message_id_delivery_tag: dict[str, int] = {}
        self.write_queue: asyncio.Queue[ChannelFrame] = asyncio.Queue()
        self._futures: list[asyncio.Future[Any]] = []

    def create_future(self) -> asyncio.Future[Any]:
        future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
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
                if self._retrieve_rejected:
                    # Ein Confirm, auf das nach einem gescheiterten Drain niemand
                    # mehr wartet, meldete asyncio sonst als "never retrieved".
                    future.add_done_callback(_retrieve)

    def close_channel(self, exc: BaseException) -> None:
        """Schliesst nur ``closing`` (falls vorhanden), etwa wenn der Kanal
        schon zu ist, bevor aiormq die offenen Futures ablehnt."""
        closing: asyncio.Future[Any] | None = getattr(self, "closing", None)
        if closing is not None and not closing.done():
            closing.set_exception(exc)

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
