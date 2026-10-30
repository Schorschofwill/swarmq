"""Leckmessung: Callbacks an ``_closing`` unter aiormq 7.2.4 und aio-pika 10.2.1 (Plan 2026-10-29-1945, U3, KTD2, KTD6).

Seit aiormq 7.2 liefert ``closing`` bei jedem Zugriff einen neuen Beobachter.
Jeder offene Beobachter hält einen Callback am inneren Future ``_closing``,
bis er selbst fertig oder abgebrochen ist
(``tests/unit/broker/test_aiormq_7_2_semantics.py``). Gemessen wird deshalb
die Zahl der Callbacks an ``_closing``, nie an ``closing``.

* KTD2: swarmq liest bei jedem Publish ``closing`` des Publish-Kanals
  (FS-08-Wächter, ``delivery._closing_of``) und hängt an den Beobachter
  ``_cancel_children`` (``_CHILDREN_BY_CLOSING``). Der Beobachter endet erst
  mit dem Kanal; jeder Publish lässt einen Callback an ``_closing`` des
  Kanals zurück.
* KTD6: aio-pikas ``UnderlayChannel.create`` hängt den ``close_callback`` an
  einen Beobachter von ``connection.closing`` (a), ``UnderlayChannel.close``
  liest ``connection.closing`` noch einmal, um ihn zu entfernen, und erzeugt
  dabei einen zweiten, leeren Beobachter (b). Beide bleiben an ``_closing``
  der Verbindung, bis sie endet: zwei Callbacks je geschlossenem Kanal.

Beide Wege aus KTD6 laufen hier nur als Prototypen im Test, nicht in ``src/``:

1. ``own_observer``: Ein eigener ``UnderlayChannel`` hält den Beobachter (a)
   selbst und bricht ihn ab, sobald der aiormq-Kanal zu ist, gleich auf
   welchem Weg. Sein ``close`` schließt den aiormq-Kanal und wartet auf den
   ``close_callback``, ohne ``connection.closing`` zu lesen. Im Test ersetzt
   er ``aio_pika.channel.UnderlayChannel``; in ``src/`` hieße das eine
   Kanal-Unterklasse mit eigenem ``_open`` für ``Channel`` und
   ``RobustChannel``.
2. ``cancel_after_close``: Nach jedem ``Channel.close`` bricht ein Wrapper
   die Beobachter an ``_closing`` der Verbindung ab, die den
   ``close_callback`` dieses Kanals tragen oder während des Schließens leer
   entstanden sind.

Ergebnis (Stand U3, je drei Läufe): Ohne Prototyp wächst ``_closing`` der
Verbindung um genau 2 je Kanal (10 000 Wegwerf-Kanäle: +20 000; 1 000
Verwürfe: +2 000), der FS-08-Wächter lässt +10 000 an ``_closing`` des
Publish-Kanals. Beide Prototypen halten beide Kanalmessungen flach (+0).
Weg 2 erreicht aber nur Kanäle, die jemand per ``close()`` schließt: Ein
vom Broker geschlossener ``RobustChannel``, den aio-pika selbst
wiederherstellt, lässt seinen Beobachter (a) weiter zurück (letzter Test).
Weg 1 deckt auch das ab.

Test-Liste:
# [x] KTD2: Ein Publish in der Warteschlange der Kanal-Sperre endet nach dem
#     Schließen des Kanals sofort mit ChannelInvalidStateError
# [x] KTD2: 10 000 Publishes über RabbitMQBroker -> Callbacks an _closing des
#     Publish-Kanals flach (seit U5 ohne FS-08-Wächter; in U3 xfail strict)
# [x] KTD6: 10 000 Wegwerf-Kanäle -> Callbacks an _closing der Verbindung
#     flach (ohne Prototyp xfail strict; je ein Lauf mit Weg 1 und Weg 2)
# [x] KTD6: 1 000 Verwürfe des Publish-Kanals nach Fristriss (E-078) ->
#     flach (ohne Prototyp xfail strict; je ein Lauf mit Weg 1 und Weg 2)
# [x] KTD6: vom Broker geschlossener, von aio-pika wiederhergestellter
#     RobustChannel -> flach nur mit Weg 1 (Weg 2 xfail strict, ohne Prototyp xfail strict)
# [x] KTD6 Weg 1: Das Ende der Verbindung erreicht den Kanal weiterhin (close_callbacks)
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator, Callable
from typing import Any, override

import aio_pika
import aio_pika.channel
import aiormq
import pytest
from aio_pika.abc import UnderlayChannel
from aio_pika.tools import OneShotCallback
from aiormq.exceptions import ChannelInvalidStateError, ChannelPreconditionFailed

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.broker.rabbitmq.connection import close_channel_quietly
from swarmq.broker.rabbitmq.robust_channels import open_throwaway_channel
from tests.support.queues import delete_queue_family
from tests.support.wait import wait_until

pytestmark = [pytest.mark.integration, pytest.mark.timeout(240)]

_PUBLISHES = 10_000
_THROWAWAY_CHANNELS = 10_000
_DISCARDS = 1_000
# Parallelität beim Messen: nur für die Laufzeit, die Messung zählt am Ende.
_BATCH = 50


def _callbacks(future: asyncio.Future[Any]) -> int:
    """Zahl der Done-Callbacks; privat, Python 3.14 bietet keine öffentliche
    Zählung (``_callbacks`` ist ``None`` ohne Callbacks)."""
    return len(future._callbacks or ())


def _inner_closing(connection: aio_pika.abc.AbstractConnection | None) -> asyncio.Future[Any]:
    """``_closing`` der aiormq-Verbindung unter *connection* (nicht ``closing``)."""
    assert connection is not None
    assert connection.transport is not None
    closing: asyncio.Future[Any] = connection.transport.connection._closing  # type: ignore[attr-defined]  # inneres Future, s. Modul-Docstring
    return closing


# --- Prototyp Weg 1: eigener Beobachter -------------------------------------


class _OwnObserverUnderlay(UnderlayChannel):
    """KTD6 Weg 1: hält den Beobachter von ``connection.closing`` selbst.

    Bricht ihn ab, sobald der aiormq-Kanal zu ist (eigenes ``close``, Broker,
    Verbindungsende), und liest ``connection.closing`` beim Schließen nicht
    noch einmal."""

    @classmethod
    @override
    async def create(
        cls,
        connection: aiormq.abc.AbstractConnection,
        close_callback: Callable[..., Any],
        **kwargs: Any,
    ) -> UnderlayChannel:
        callback = OneShotCallback(close_callback)
        await connection.ready()
        observer = connection.closing
        observer.add_done_callback(callback)

        def release(_: object = None) -> None:
            observer.remove_done_callback(callback)
            observer.cancel()

        try:
            channel = await connection.channel(**kwargs)
        except BaseException:
            release()
            raise
        channel_closing = channel.closing
        channel_closing.add_done_callback(callback)
        channel_closing.add_done_callback(release)
        return cls(channel=channel, close_callback=callback)

    @override
    async def close(self, exc: aiormq.abc.ExceptionType | None = None) -> Any:
        if self.close_callback.finished.is_set():
            return
        await self.channel.close(exc)
        await self.close_callback.wait()


# --- Prototyp Weg 2: Beobachter nach dem Schließen abbrechen -----------------


def _observer_of(callback: object) -> asyncio.Future[Any] | None:
    """Der Beobachter hinter aiormqs ``on_close``-Callback (``Base.closing``)."""
    code = getattr(callback, "__code__", None)
    closure = getattr(callback, "__closure__", None)
    if code is None or closure is None or code.co_name != "on_close" or "future" not in code.co_freevars:
        return None
    observer: asyncio.Future[Any] = closure[code.co_freevars.index("future")].cell_contents
    return observer


def _only_aiormq_callbacks(observer: asyncio.Future[Any]) -> bool:
    return all(getattr(cb, "__name__", "") == "on_done" for cb, _ in observer._callbacks or ())


def _close_cancelling_observers(original: Callable[..., Any]) -> Callable[..., Any]:
    """KTD6 Weg 2: ``Channel.close``, danach die Beobachter dieses Kanals abbrechen."""

    async def close(self: aio_pika.Channel, exc: aiormq.abc.ExceptionType | None = None) -> None:
        underlay = self._channel
        if underlay is None:
            await original(self, exc)
            return
        inner: asyncio.Future[Any] = underlay.channel.connection._closing  # type: ignore[attr-defined]  # inneres Future
        before = [cb for cb, _ in inner._callbacks or ()]
        try:
            await original(self, exc)
        finally:
            for cb, _ in list(inner._callbacks or ()):
                observer = _observer_of(cb)
                if observer is None or observer.done():
                    continue
                carries = any(c is underlay.close_callback for c, _ in observer._callbacks or ())
                fresh = all(cb is not old for old in before) and _only_aiormq_callbacks(observer)
                if carries:
                    observer.remove_done_callback(underlay.close_callback)
                if carries or fresh:
                    observer.cancel()

    return close


@pytest.fixture
def prototype(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> str:
    way: str = request.param
    if way == "own_observer":
        monkeypatch.setattr(aio_pika.channel, "UnderlayChannel", _OwnObserverUnderlay)
    elif way == "cancel_after_close":
        monkeypatch.setattr(aio_pika.Channel, "close", _close_cancelling_observers(aio_pika.Channel.close))
    return way


_LEAKS = pytest.mark.xfail(
    strict=True, raises=AssertionError, reason="KTD6: aio-pika 10.2.1 lässt je Kanal zwei Beobachter an _closing"
)
_WAYS = [
    pytest.param("none", marks=_LEAKS, id="ohne-prototyp"),
    pytest.param("own_observer", id="weg1-eigener-beobachter"),
    pytest.param("cancel_after_close", id="weg2-abbrechen-nach-close"),
]


@pytest.fixture
async def broker(rabbitmq_url: str) -> AsyncIterator[tuple[RabbitMQBroker, str]]:
    queue = f"cbflat-{uuid.uuid4().hex[:8]}"
    impl = RabbitMQBroker(rabbitmq_url, queue_type="classic")
    await impl.connect()
    try:
        await impl.declare_queue(queue)
        yield impl, queue
    finally:
        await impl.close()
        await delete_queue_family(rabbitmq_url, queue)


# --- KTD2 ---------------------------------------------------------------------


async def test_a_publish_waiting_on_the_channel_lock_ends_at_once_when_the_channel_closes(rabbitmq_url: str) -> None:
    """KTD2 (aiormq #240) gegen das echte aiormq: Wer beim Schließen in der
    Warteschlange der Kanal-Sperre steht, reiht nichts mehr ein. Das ist die
    Ursache von FS-08; unter 7.0 hing der Publish auf dem toten Kanal."""
    connection = await aio_pika.connect(rabbitmq_url)
    try:
        channel = await connection.channel()
        underlay = await channel.get_underlay_channel()
        assert isinstance(underlay, aiormq.Channel)
        lock = underlay._Channel__lock  # type: ignore[attr-defined]  # die Sperre hinter der Property ``lock``

        async with underlay.lock:
            publish = asyncio.create_task(underlay.basic_publish(b"x", routing_key="cbflat-none"))
            # asyncio.Lock bietet keine API für seine Warteschlange.
            assert await wait_until(lambda: bool(lock._waiters), timeout=5, interval=0.01), (
                "precondition: the publish waits on the channel lock"
            )
            # Der Broker schließt den Kanal: Ack auf ein nie vergebenes Tag.
            await underlay.basic_ack(delivery_tag=2**31)
            await asyncio.wait({underlay.closing}, timeout=5)
            assert underlay.is_closed
            assert isinstance(underlay.closing.exception(), ChannelPreconditionFailed)
            assert not publish.done()

        done, _ = await asyncio.wait({publish}, timeout=1)
        assert done, "the waiting publish ends right after the lock is released"
        with pytest.raises(ChannelInvalidStateError):
            publish.result()
    finally:
        await connection.close()


async def test_publishes_keep_the_callbacks_on_the_publish_channel_flat(broker: tuple[RabbitMQBroker, str]) -> None:
    impl, queue = broker
    await impl.publish(queue, b"warm-up")
    channel = impl._publish_channel
    assert channel is not None
    underlay = await channel.get_underlay_channel()
    inner: asyncio.Future[Any] = underlay._closing  # type: ignore[attr-defined]  # inneres Future
    before = _callbacks(inner)

    for _ in range(_PUBLISHES // _BATCH):
        await asyncio.gather(*(impl.publish(queue, b"x") for _ in range(_BATCH)))

    assert impl._publish_channel is channel
    assert await channel.get_underlay_channel() is underlay, "precondition: one channel for the whole measurement"
    after = _callbacks(inner)
    assert after == before, f"callbacks on the publish channel's _closing: {before} -> {after}"


# --- KTD6 ---------------------------------------------------------------------


async def _throwaway(connection: aio_pika.abc.AbstractConnection) -> None:
    channel = await open_throwaway_channel(connection)
    await close_channel_quietly(channel)


@pytest.mark.parametrize("prototype", _WAYS, indirect=True)
async def test_throwaway_channels_keep_the_callbacks_on_the_connection_flat(
    prototype: str, broker: tuple[RabbitMQBroker, str]
) -> None:
    """Wegwerf-Kanäle über swarmqs Pfad (``open_throwaway_channel``,
    ``close_channel_quietly``), wie Argument-, Existenz- und Tiefenprüfungen."""
    impl, _ = broker
    connection = impl._connection
    inner = _inner_closing(connection)
    before = _callbacks(inner)

    for _ in range(_THROWAWAY_CHANNELS // _BATCH):
        await asyncio.gather(*(_throwaway(connection) for _ in range(_BATCH)))  # type: ignore[arg-type]  # verbunden, s. _inner_closing

    assert _inner_closing(connection) is inner, "precondition: no reconnect during the measurement"
    after = _callbacks(inner)
    assert after == before, f"{prototype}: callbacks on the connection's _closing: {before} -> {after}"


@pytest.mark.parametrize("prototype", _WAYS, indirect=True)
async def test_discarded_publish_channels_keep_the_callbacks_on_the_connection_flat(
    prototype: str, broker: tuple[RabbitMQBroker, str]
) -> None:
    """E-078: Verwurf nach Fristriss über den echten Pfad
    (``_discard_publish_channel`` -> ``release_channel`` ->
    ``_close_channel_detached``); der nächste Publish öffnet einen neuen
    robusten Kanal. Der Prototyp steht vor dem Broker in der Signatur, damit
    pytest ihn zuerst einsetzt: Kanäle aus ``connect`` liefen sonst noch über
    den ungepatchten ``UnderlayChannel`` (gemessen: +2)."""
    impl, queue = broker
    await impl.publish(queue, b"warm-up")
    inner = _inner_closing(impl._connection)
    before = _callbacks(inner)

    for _ in range(_DISCARDS):
        await impl._discard_publish_channel(impl._publish_channel)
        assert await wait_until(lambda: not impl._pending_channel_closes, timeout=5, interval=0.001)
        await impl.publish(queue, b"x")

    assert _inner_closing(impl._connection) is inner, "precondition: no reconnect during the measurement"
    # Der lebende Publish-Kanal hält wie vor der Messung genau seinen einen Beobachter.
    after = _callbacks(inner)
    assert after == before, f"{prototype}: callbacks on the connection's _closing: {before} -> {after}"


@pytest.mark.parametrize(
    "prototype",
    [
        pytest.param("none", marks=_LEAKS, id="ohne-prototyp"),
        pytest.param("own_observer", id="weg1-eigener-beobachter"),
        pytest.param(
            "cancel_after_close",
            marks=pytest.mark.xfail(
                strict=True,
                raises=AssertionError,
                reason="KTD6 Weg 2 greift nur bei close(); den Broker-Close stellt aio-pika selbst wieder her",
            ),
            id="weg2-abbrechen-nach-close",
        ),
    ],
    indirect=True,
)
async def test_robust_channels_closed_by_the_broker_keep_the_callbacks_on_the_connection_flat(
    rabbitmq_url: str, prototype: str
) -> None:
    """Ein ``RobustChannel``, den der Broker schließt, stellt aio-pika selbst
    wieder her (``_on_close`` -> ``restore``), ohne ``close()``. Der alte
    ``UnderlayChannel`` wird nie geschlossen; sein Beobachter (a) bleibt."""
    connection = await aio_pika.connect_robust(rabbitmq_url)
    try:
        channel = await connection.channel()
        assert isinstance(channel, aio_pika.RobustChannel)
        inner = _inner_closing(connection)
        before = _callbacks(inner)

        for _ in range(100):
            underlay = await channel.get_underlay_channel()
            await underlay.basic_ack(delivery_tag=2**31)  # Broker schließt den Kanal
            await asyncio.wait({underlay.closing}, timeout=5)
            assert await wait_until(
                lambda u=underlay: (
                    not channel.is_closed and channel._channel is not None and channel._channel.channel is not u
                ),  # type: ignore[misc]  # Lambda-Default bindet den Kanal
                timeout=5,
                interval=0.005,
            ), "precondition: aio-pika restores the channel"

        assert _inner_closing(connection) is inner, "precondition: no reconnect during the measurement"
        after = _callbacks(inner)
        assert after == before, f"{prototype}: callbacks on the connection's _closing: {before} -> {after}"
    finally:
        await connection.close()


async def test_the_own_observer_still_reports_the_end_of_the_connection_to_the_channel(
    rabbitmq_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Weg 1 behält die Aufgabe des Beobachters (a): Endet die Verbindung,
    erfährt der aio-pika-Kanal davon (``close_callbacks``)."""
    monkeypatch.setattr(aio_pika.channel, "UnderlayChannel", _OwnObserverUnderlay)
    connection = await aio_pika.connect(rabbitmq_url)
    closed = asyncio.Event()

    def on_close(*_: object) -> None:
        closed.set()

    try:
        channel = await connection.channel()
        channel.close_callbacks.add(on_close)
        inner = _inner_closing(connection)
        assert _callbacks(inner) >= 1, "precondition: the open channel holds its observer"
    finally:
        await connection.close()

    assert await wait_until(closed.is_set, timeout=5, interval=0.01)
    assert channel.is_closed
