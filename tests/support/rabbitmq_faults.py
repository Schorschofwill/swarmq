"""Fehlerinjektion für den echten ``RabbitMQBroker`` (Plan U5).

``publish_failure(broker)`` ersetzt den Publish-Kanal über die injizierte
Channel-Factory durch eine Attrappe, deren ``publish`` den transienten
Kanalfehler wirft — ``_publish_with_retry`` erschöpft seine fünf Versuche
und wirft ``BrokerConnectionError``. Ein bloß geschlossener Kanal reicht
nicht, weil ``_ensure_publish_channel`` ihn transparent neu erzeugt.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Iterator
from typing import cast, override

import aio_pika
from aio_pika.exceptions import ChannelInvalidStateError
from aiormq.abc import ChannelFrame

from swarmq.broker.rabbitmq import RabbitMQBroker
from tests.support.aiormq_transport import FakeTransport


class _FailingExchange:
    async def publish(self, message: aio_pika.Message, routing_key: str) -> None:
        raise ChannelInvalidStateError("injected publish failure")


class FailingChannel:
    is_closed = False
    default_exchange = _FailingExchange()

    async def close(self) -> None:
        return None


@contextlib.contextmanager
def publish_failure(broker: RabbitMQBroker) -> Iterator[None]:
    async def _factory() -> aio_pika.abc.AbstractChannel:
        return FailingChannel()  # type: ignore[return-value]

    previous = broker._publish_channel
    broker._publish_channel_factory = _factory
    broker._publish_channel = None
    try:
        yield
    finally:
        broker._publish_channel_factory = None
        broker._publish_channel = previous


class _WrittenAtOnce(asyncio.Queue[ChannelFrame]):
    """``write_queue``, deren Writer jeden Frame sofort auf den Socket bringt."""

    @override
    def put_nowait(self, item: ChannelFrame) -> None:
        item.drain()
        super().put_nowait(item)


class _HangingExchange:
    """Publish, das nie zurückkehrt — der stille Transportausfall aus dem
    Referenzlauf vom 2026-08-28. Der Kanal gilt weiter als offen, es fliegt
    keine Ausnahme, und ohne Deckel wartet der Aufrufer unbegrenzt auf ein
    Publisher-Confirm (Plan 2026-08-28-001, R1/AE1).

    Wie aiormq 7.2 (Plan 2026-10-29-1945, KTD5): Der echte ``basic_publish``
    läuft über ``FakeTransport``; der Frame ist eingereiht und geschrieben
    (``sent``, ``drain_future`` erfüllt), nur das Confirm kommt nie."""

    def __init__(self, channel: HangingChannel) -> None:
        self.publish_started = asyncio.Event()
        self._exchange = aio_pika.Exchange(cast("aio_pika.abc.AbstractChannel", channel), "")

    async def publish(self, message: aio_pika.Message, routing_key: str) -> None:
        self.publish_started.set()
        await self._exchange.publish(message, routing_key=routing_key)


class HangingChannel:
    is_closed = False

    def __init__(self, *, close_hangs: bool = False) -> None:
        self.transport = FakeTransport()
        self.transport.write_queue = _WrittenAtOnce()
        self.default_exchange = _HangingExchange(self)
        self.closed = False
        self._close_hangs = close_hangs

    async def get_underlay_channel(self) -> FakeTransport:
        return self.transport

    async def close(self) -> None:
        if self._close_hangs:
            await asyncio.Event().wait()
        self.closed = True


@contextlib.contextmanager
def hanging_publish(broker: RabbitMQBroker, *, close_hangs: bool = False) -> Iterator[HangingChannel]:
    """Ersetzt den Publish-Kanal durch einen, dessen ``publish`` hängt.

    ``close_hangs`` bildet zusätzlich den Fall ab, in dem auch das Schließen
    auf dem toten Transport nicht zurückkehrt — der Grund, warum der Verwurf
    das Schließen nebenläufig anstößt statt darauf zu warten (KTD3).
    """
    channel = HangingChannel(close_hangs=close_hangs)

    async def _factory() -> aio_pika.abc.AbstractChannel:
        return channel  # type: ignore[return-value]

    previous = broker._publish_channel
    broker._publish_channel_factory = _factory
    broker._publish_channel = None
    try:
        yield channel
    finally:
        broker._publish_channel_factory = None
        broker._publish_channel = previous
