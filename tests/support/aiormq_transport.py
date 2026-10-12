"""Publish-Kanal mit echtem aio-pika/aiormq-Publish-Code, ohne Netz (B24).

``FakeTransport`` steht fuer die Verbindungsseite eines aiormq-Kanals: Der
echte ``aiormq.Channel.basic_publish`` legt seine Frames in ``write_queue``
und wartet auf zwei Futures, erst auf ``drain_future`` (der Writer hat den
Frame auf den Socket geschrieben), dann auf das Publisher-Confirm. Der Test
spielt den Writer und den Broker:

* ``frame.drain()`` -- der Writer hat den Frame geschrieben;
* ``confirm_all()`` -- der Broker bestaetigt;
* ``lose_connection(exc)`` -- die Verbindung reisst; wie aiormqs
  ``FutureStore.reject_all`` scheitern alle offenen Futures mit ``exc``.

``FakePublishChannel`` ist die aio-pika-Seite: ein echtes
``aio_pika.Exchange`` als ``default_exchange`` ueber diesem Transport.
"""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from typing import Any, cast

import aio_pika
import aiormq
from aiormq.abc import ChannelFrame


class FakeTransport:
    """Die Attribute, die ``aiormq.Channel.basic_publish`` an ``self`` liest."""

    number = 1
    publisher_confirms = True
    max_content_size = 131072

    def __init__(self, *, retrieve_rejected: bool = True) -> None:
        # ``False``: abgelehnte Futures bleiben unabgeholt wie in aiormq, damit
        # ein Test asyncios "Future exception was never retrieved" sieht.
        self._retrieve_rejected = retrieve_rejected
        self.lock = asyncio.Lock()
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

    def lose_connection(self, exc: BaseException) -> None:
        for future in self._futures:
            if not future.done():
                future.set_exception(exc)
                if self._retrieve_rejected:
                    # Ein Confirm, auf das nach einem gescheiterten Drain niemand
                    # mehr wartet, meldete asyncio sonst als "never retrieved".
                    future.add_done_callback(_retrieve)

    def forget_futures(self) -> None:
        """Gibt die Futures frei (wie aiormq nach dem Ablehnen), damit die
        Garbage Collection sie einsammelt."""
        self._futures.clear()
        self.confirmations.clear()


def _retrieve(future: asyncio.Future[Any]) -> None:
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
