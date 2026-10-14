"""Nachweis FS-09: Der Consumer-Waechter bricht ``basic.cancel`` bzw.
``basic.consume`` nach 2 s ab; aiormq schliesst daraufhin den ganzen
Consume-Channel.

Register: docs/befunde/fehlersuche-runde-2-2026-10.md (FS-09, Quelle N-6).

``ConsumerWatch._reregister``/``_consume`` begrenzen die RPCs per
``asyncio.wait_for(..., timeout=_RPC_TIMEOUT_S)``. ``aiormq.Channel.rpc``
behandelt ``CancelledError``/``TimeoutError`` selbst: Es sendet
``Channel.Close(504, "RPC timeout ...")`` und setzt sein Close-Event. Darauf
liegen alle Consumer des Workers; alle unbestaetigten Zustellungen gehen an
andere Worker, waehrend sie hier noch laufen (Redelivery, spaete Acks enden
in ``delivery_settle_failed``). Neuanmeldungen gibt es gerade in gestoerten
Lagen (Broker unter Last, 4.3-``consumer_timeout``), also genau dann, wenn
der Broker langsamer als 2 s antwortet.

Der Test laesst den echten ``aiormq.Channel`` (Verbindungs-Nachbau ohne
Socket wie ``repro_aiormq_rpc_cancel.py``) die RPCs ausfuehren; der Broker
antwortet nicht innerhalb der Waechterfrist. Erwartet: Der Waechter schickt
kein ``Channel.Close`` und der Channel bleibt nutzbar -- wie lange er auf die
Antwort wartet oder ob er sie im Hintergrund abwartet, laesst der Test offen.

Testliste:
# [x] langsamer basic.cancel (Neuanmeldung nach broker_cancel): kein Channel.Close
# [x] langsamer basic.consume (Neuanmeldung eines abgemeldeten Consumers): kein Channel.Close
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from types import SimpleNamespace
from typing import Any, cast, override

import pytest
from aiormq.abc import AbstractConnection, ExceptionType
from aiormq.base import Base
from aiormq.channel import Channel
from pamqp import commands as spec
from pamqp.frame import unmarshal

from swarmq.broker.rabbitmq.consumer_watch import ConsumerWatch

pytestmark = pytest.mark.timeout(5)

# Waechterfrist im Test (statt 2 s) und Beobachtungsfenster danach.
_FS09_RPC_TIMEOUT_S = 0.02
_FS09_OBSERVE_S = 0.2


class Fs09Connector(Base):
    """Verbindungsseite ohne Broker-Antworten; Frames bleiben in der write_queue."""

    def __init__(self) -> None:
        super().__init__(loop=asyncio.get_running_loop())
        self.publisher_confirms = True
        self.connection_tune = SimpleNamespace(frame_max=131072)
        self.write_queue: asyncio.Queue[Any] = asyncio.Queue()

    @override
    async def _on_close(self, exc: ExceptionType | None = None) -> None:
        return None

    @override
    def __str__(self) -> str:
        return "fs09-connector"


class Fs09Queue:
    """Die Teile von aio-pikas ``RobustQueue``, die der Waechter benutzt."""

    def __init__(self, underlay: Channel) -> None:
        self.underlay = underlay
        self.channel = self

    async def get_underlay_channel(self) -> Channel:
        return self.underlay

    # ``timeout`` wie bei aio-pika: reicht der Waechter eine aiormq-eigene
    # Frist durch, darf der Fake nicht mit ``TypeError`` (ohne Close) enden.
    async def cancel(self, tag: str, timeout: float | None = None) -> None:
        await self.underlay.basic_cancel(tag, timeout=timeout)

    async def consume(self, callback: Any, timeout: float | None = None) -> str:
        ok = await self.underlay.basic_consume("tasks", callback, timeout=timeout)
        return str(ok.consumer_tag)


def _fs09_channel_closes(connector: Fs09Connector) -> list[spec.Channel.Close]:
    """Alle ``Channel.Close``-Methoden, die in die write_queue gingen."""
    closes: list[spec.Channel.Close] = []
    while not connector.write_queue.empty():
        payload = connector.write_queue.get_nowait().payload
        while payload:
            consumed, _channel, frame = unmarshal(payload)
            payload = payload[consumed:]
            if isinstance(frame, spec.Channel.Close):
                closes.append(frame)
    return closes


@pytest.fixture
async def fs09_channel() -> AsyncIterator[tuple[Fs09Connector, Channel]]:
    connector = Fs09Connector()
    channel = Channel(cast("AbstractConnection", connector), 1, publisher_confirms=True)
    try:
        yield connector, channel
    finally:
        await connector.close(None)


async def _fs09_closes_after_tick(watch: ConsumerWatch, connector: Fs09Connector) -> list[spec.Channel.Close]:
    """Ein Takt; laeuft er laenger als das Beobachtungsfenster (der Waechter
    wartet auf die Antwort), wird der Zustand trotzdem danach geprueft und
    der Takt erst dann beendet."""
    tick = asyncio.create_task(watch.tick())
    try:
        await asyncio.wait({tick}, timeout=_FS09_OBSERVE_S)
        if tick.done():
            tick.result()
        return _fs09_channel_closes(connector)
    finally:
        tick.cancel()
        await asyncio.gather(tick, return_exceptions=True)


async def _fs09_callback(_message: Any) -> None:
    return None


@pytest.mark.befund("FS-09")
async def test_fs09_slow_basic_cancel_does_not_close_the_consume_channel(
    fs09_channel: tuple[Fs09Connector, Channel],
) -> None:
    connector, channel = fs09_channel
    watch = ConsumerWatch(timeout_s=_FS09_RPC_TIMEOUT_S)
    channel.consumers["fs09-ctag"] = _fs09_callback
    watch.add("tasks", Fs09Queue(channel), _fs09_callback, "fs09-ctag")
    await watch.tick()  # Tag gesehen: der Channel ist "unveraendert"
    del channel.consumers["fs09-ctag"]  # Broker hat den Consumer beendet (4.3, basic.cancel)

    closes = await _fs09_closes_after_tick(watch, connector)

    assert closes == [], (
        f"Waechter schloss den Consume-Channel: {[(c.reply_code, c.reply_text) for c in closes]} -- "
        "alle unbestaetigten Zustellungen dieses Workers werden neu zugestellt"
    )


@pytest.mark.befund("FS-09")
async def test_fs09_slow_basic_consume_does_not_close_the_consume_channel(
    fs09_channel: tuple[Fs09Connector, Channel],
) -> None:
    connector, channel = fs09_channel
    watch = ConsumerWatch(timeout_s=_FS09_RPC_TIMEOUT_S)
    # Abgemeldet, Neuanmeldung steht aus (``tag=None``): der Takt ruft nur consume.
    watch.add("tasks", Fs09Queue(channel), _fs09_callback, "fs09-ctag")
    watch._entries[0].tag = None

    closes = await _fs09_closes_after_tick(watch, connector)

    assert closes == [], (
        f"Waechter schloss den Consume-Channel: {[(c.reply_code, c.reply_text) for c in closes]} -- "
        "alle unbestaetigten Zustellungen dieses Workers werden neu zugestellt"
    )
