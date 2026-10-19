"""Nachweis F4-22 (behoben in 0.14.0): Ein im Restore-Fenster ersetzter Kanal bleibt nach dem Reconnect offen und wird bei jedem weiteren Reconnect wiederhergestellt.

Register: docs/befunde/fehlersuche-runde-4-2026-10.md (F4-22; Verdacht C der
Gegenprüfung 0.14, Probe ``probe_c_outage_publish``).

aio-pika 10.0.1: ``RobustConnection._on_connected`` setzt zuerst ``connected``
und stellt danach die Kanäle aus einer Tupel-Kopie des ``WeakSet`` wieder her.
Ein Publish, der in ``_ensure_channel_tracked`` auf ``connection.ready()``
wartet, wacht in diesem Fenster auf, findet den alten Kanal ``is_closed`` und
öffnete vor dem Fix einen neuen. aio-pika stellte den alten trotzdem wieder her,
aiormq hielt ihn stark, und jeder weitere Reconnect brachte beide zurück: Beim
Broker wuchsen die Kanäle auf ``[1, 2]``, ``[1, 2, 3]``, ``[1, 2, 3, 4]``.
Verletzt: E-078, Nachtrag 2026-10-19 („kein Leck auch über einen Reconnect“),
und der F4-20-Text im Changelog 0.14.0 (die Kanalzahl wächst nicht mehr mit
jedem Reconnect).

Echtes aio-pika (``RobustConnection``, ``RobustChannel``) über dem Fake der
aiormq-Verbindung aus dem F4-20-Nachweis; das Öffnen eines Kanals gibt hier
einmal die Kontrolle ab wie das RPC beim echten Broker, sonst gäbe es kein
Fenster. Starke Referenzen auf alle Kanalobjekte halten die Garbage Collection
heraus.

Test-Liste:
# [x] Publish im Restore-Fenster, drei Reconnects: nach jedem genau ein Kanal
# [x] Restore langsamer als die Frist: Ersatz, der alte kommt aus der laufenden Tupel-Kopie trotzdem wieder und wird geschlossen
# [x] Restore scheitert: Ersatz, der alte kommt beim nächsten Reconnect nicht wieder
# [x] Ersatz gerade, als der alte doch noch fertig wurde: der alte wird sofort geschlossen
# [x] broker.close() auf geliehener Verbindung, Restore des alten danach: der alte wird trotzdem geschlossen
# [x] Consume im Restore-Fenster: ein Kanal, beide Consumer
# [x] Consume, Restore langsamer als die Frist: der alte Consume-Kanal bleibt mit seinem Consumer offen
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any, override

import aio_pika
import aiormq.exceptions
import pytest
from yarl import URL

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.broker.rabbitmq import connection as connection_module
from tests.unit.broker.test_befund_f4_20_restore_closed_channels import (
    _FakeAiormq,
    _FakeAiormqChannel,
    _FakeTransport,
)

pytestmark = pytest.mark.timeout(5)


async def _yield_once() -> None:
    """Gibt genau einmal an die Event-Loop ab (ohne ``asyncio.sleep``)."""
    loop = asyncio.get_running_loop()
    turn: asyncio.Future[None] = loop.create_future()
    loop.call_soon(turn.set_result, None)
    await turn


async def _turns(count: int = 20) -> None:
    for _ in range(count):
        await _yield_once()


class _ConsumingChannel(_FakeAiormqChannel):
    """Ein Kanal, der Consumer anmelden kann (Tag zu Queue)."""

    def __init__(self, connection: _FakeAiormq, number: int) -> None:
        super().__init__(connection, number)
        self.consumers: dict[str, str] = {}

    async def basic_consume(self, *, queue: str, consumer_tag: str | None = None, **_: Any) -> SimpleNamespace:
        tag = consumer_tag or f"ctag-{queue}"
        self.consumers[tag] = queue
        return SimpleNamespace(consumer_tag=tag)


class _WindowAiormq(_FakeAiormq):
    """Wie ``_FakeAiormq``, aber ``channel()`` gibt vor dem Öffnen einmal ab:
    Beim echten Broker ist ``Channel.Open`` ein RPC, und genau in diesem Warten
    läuft der wartende Publish."""

    @override
    async def channel(self, **_: Any) -> _FakeAiormqChannel:
        await _yield_once()
        if self.is_closed:
            raise aiormq.exceptions.ConnectionClosed(320, "connection closed")
        number = max(self.channels, default=0) + 1
        channel = _ConsumingChannel(self, number)
        self.channels[number] = channel
        self.opened.append(number)
        return channel

    def consumed_queues(self) -> list[list[str]]:
        """Je offenem Kanal mit Consumern die konsumierten Queues."""
        return [
            sorted(channel.consumers.values())
            for channel in self.channels.values()
            if isinstance(channel, _ConsumingChannel) and channel.consumers
        ]


async def _connected_broker() -> tuple[RabbitMQBroker, aio_pika.RobustConnection, _WindowAiormq]:
    connection = aio_pika.RobustConnection(URL("amqp://guest:guest@localhost/"))
    aiormq_connection = _WindowAiormq()
    connection.transport = _FakeTransport(aiormq_connection)  # type: ignore[assignment]  # Fake statt UnderlayConnection
    connection.connected.set()
    broker = RabbitMQBroker(connection=connection)
    await broker.connect()
    return broker, connection, aiormq_connection


async def _drop(connection: aio_pika.RobustConnection, current: _FakeAiormq) -> None:
    """Abriss wie in aio-pika: Transport weg, ``connected`` aus, alte Verbindung zu."""
    connection.transport = None
    connection.connected.clear()
    await current.drop()
    await _turns()


async def _reconnect(connection: aio_pika.RobustConnection) -> _WindowAiormq:
    """Neuer Transport, dann ``_on_connected``: ``connected`` an, Restore aus der Tupel-Kopie."""
    fresh = _WindowAiormq()
    connection.transport = _FakeTransport(fresh)  # type: ignore[assignment]  # Fake statt UnderlayConnection
    await connection._on_connected()
    await _turns()
    return fresh


@pytest.mark.befund("F4-22")
@pytest.mark.entscheidung("E-078")
async def test_a_publish_in_the_restore_window_leaves_one_channel_after_each_reconnect() -> None:
    broker, connection, current = await _connected_broker()
    seen: list[object] = []  # starke Referenzen: keine Garbage Collection der Kanalobjekte
    try:
        for round_ in range(3):
            seen.append(broker._publish_channel)
            await _drop(connection, current)
            waiting = asyncio.create_task(broker._ensure_publish_channel())
            await _turns()
            assert not waiting.done(), "precondition: the publish waits for the reconnect"

            current = await _reconnect(connection)
            seen.append(await waiting)

            assert sorted(current.channels) == [1], (
                f"round {round_}: after the reconnect exactly one channel is open, got {sorted(current.channels)}"
            )
    finally:
        await broker.close()


class _HeldAiormq(_WindowAiormq):
    """Das erste Öffnen eines Kanals wartet auf ``release``: Der Restore des
    alten Kanals hängt, bis der Test ihn freigibt."""

    def __init__(self) -> None:
        super().__init__()
        self.release = asyncio.Event()
        self._held = False

    @override
    async def channel(self, **kwargs: Any) -> _FakeAiormqChannel:
        if not self._held:
            self._held = True
            await self.release.wait()
        return await super().channel(**kwargs)


class _FailingRestoreAiormq(_HeldAiormq):
    """Wie ``_HeldAiormq``, aber der freigegebene erste Kanal scheitert: Der
    Restore des alten Kanals bricht den Reconnect ab wie ein Abriss mitten im
    Restore."""

    @override
    async def channel(self, **kwargs: Any) -> _FakeAiormqChannel:
        if not self._held:
            self._held = True
            await self.release.wait()
            raise aiormq.exceptions.ConnectionClosed(320, "connection closed during the restore")
        return await super().channel(**kwargs)

    async def close(self, exc: object = None) -> None:
        del exc
        await self.drop()


class _LateRestoreAiormq(_HeldAiormq):
    """Wie ``_HeldAiormq``; der zweite Kanal (der Ersatz) gibt den Restore
    des alten frei und öffnet erst, wenn der fertig ist: Der alte wurde
    gerade noch wiederhergestellt, als swarmq ihn schon ersetzte."""

    def __init__(self) -> None:
        super().__init__()
        self._calls = 0

    @override
    async def channel(self, **kwargs: Any) -> _FakeAiormqChannel:
        self._calls += 1
        if self._calls == 2:
            self.release.set()
            await _turns()
        return await super().channel(**kwargs)


def _shorten_restore_deadline(monkeypatch: pytest.MonkeyPatch) -> None:
    """Kürzt die Frist, die ``_ensure_channel_tracked`` auf Restore und neuen
    Kanal wartet, damit der hängende Restore sie reißt.

    Erst aufrufen, wenn der wartende Aufruf schon in ``connection.ready()``
    hängt und der Reconnect als Task angelegt ist, ohne Abgabe dazwischen: Das
    Warten auf die Verbindung hat die lange Frist schon gelesen und kann vor
    dem Reconnect nicht reißen; das Warten auf den Restore liest die kurze."""
    monkeypatch.setattr(connection_module, "_CHANNEL_READY_TIMEOUT_S", 0.05)


@pytest.mark.befund("F4-22")
async def test_a_channel_restored_after_the_deadline_is_closed_and_only_its_replacement_stays(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker, connection, current = await _connected_broker()
    old = broker._publish_channel
    try:
        await _drop(connection, current)
        waiting = asyncio.create_task(broker._ensure_publish_channel())
        await _turns()
        fresh = _HeldAiormq()
        connection.transport = _FakeTransport(fresh)  # type: ignore[assignment]  # Fake statt UnderlayConnection
        restoring = asyncio.create_task(connection._on_connected())
        _shorten_restore_deadline(monkeypatch)
        replacement = await waiting
        assert replacement is not old, "precondition: the restore missed the deadline, the channel was replaced"

        fresh.release.set()
        await restoring
        await _turns()

        assert fresh.opened == [1, 2], (
            f"the running restore reopens the replaced channel from its tuple copy anyway, opened {fresh.opened}"
        )
        assert sorted(fresh.channels) == [replacement.number], (
            f"only the replacement {replacement.number} stays open, got {sorted(fresh.channels)}"
        )
    finally:
        await broker.close()


@pytest.mark.befund("F4-22")
async def test_a_replaced_channel_whose_restore_failed_does_not_come_back_with_the_next_reconnect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker, connection, current = await _connected_broker()
    old = broker._publish_channel
    try:
        await _drop(connection, current)
        waiting = asyncio.create_task(broker._ensure_publish_channel())
        await _turns()
        failing = _FailingRestoreAiormq()
        connection.transport = _FakeTransport(failing)  # type: ignore[assignment]  # Fake statt UnderlayConnection
        restoring = asyncio.create_task(connection._on_connected())
        _shorten_restore_deadline(monkeypatch)
        replacement = await waiting
        assert replacement is not old, "precondition: the restore missed the deadline, the channel was replaced"
        failing.release.set()
        with pytest.raises(aiormq.exceptions.ConnectionClosed):
            await restoring
        # aio-pika hat die Verbindung geschlossen (``failing.close``); der Rest des Abrisses:
        connection.transport = None
        connection.connected.clear()

        fresh = await _reconnect(connection)

        assert fresh.opened == [1], f"only the replacement is restored, opened {fresh.opened}"
        assert broker._publish_channel is replacement
        assert not replacement.is_closed
    finally:
        await broker.close()


@pytest.mark.befund("F4-22")
async def test_a_replaced_channel_restored_while_its_replacement_opened_is_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker, connection, current = await _connected_broker()
    old = broker._publish_channel
    try:
        await _drop(connection, current)
        waiting = asyncio.create_task(broker._ensure_publish_channel())
        await _turns()
        fresh = _LateRestoreAiormq()
        connection.transport = _FakeTransport(fresh)  # type: ignore[assignment]  # Fake statt UnderlayConnection
        restoring = asyncio.create_task(connection._on_connected())
        _shorten_restore_deadline(monkeypatch)
        replacement = await waiting
        await restoring
        await _turns()

        assert replacement is not old, "precondition: the restore missed the deadline, the channel was replaced"
        assert fresh.opened == [1, 2], f"precondition: the old channel was restored first, opened {fresh.opened}"
        assert sorted(fresh.channels) == [replacement.number], (
            f"only the replacement {replacement.number} stays open, got {sorted(fresh.channels)}"
        )
    finally:
        await broker.close()


@pytest.mark.befund("F4-22")
async def test_a_replaced_channel_restored_after_broker_close_is_closed_on_the_borrowed_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker, connection, current = await _connected_broker()
    old = broker._publish_channel
    closed = False
    try:
        await _drop(connection, current)
        waiting = asyncio.create_task(broker._ensure_publish_channel())
        await _turns()
        fresh = _HeldAiormq()
        connection.transport = _FakeTransport(fresh)  # type: ignore[assignment]  # Fake statt UnderlayConnection
        restoring = asyncio.create_task(connection._on_connected())
        _shorten_restore_deadline(monkeypatch)
        replacement = await waiting
        assert replacement is not old, "precondition: the restore missed the deadline, the channel was replaced"

        await broker.close()
        closed = True
        fresh.release.set()
        await restoring
        await _turns()

        assert len(fresh.opened) == 2, (
            f"precondition: the old channel was restored after the close, opened {fresh.opened}"
        )
        assert sorted(fresh.channels) == [], (
            f"no channel of the closed broker stays open on the borrowed connection, got {sorted(fresh.channels)}"
        )
    finally:
        if not closed:
            await broker.close()


async def _on_delivery(_delivery: object) -> None:
    return None


@pytest.mark.befund("F4-22")
async def test_a_consume_in_the_restore_window_keeps_one_consume_channel_with_both_consumers() -> None:
    broker, connection, current = await _connected_broker()
    try:
        await broker.consume("first", _on_delivery, concurrency=3)
        await _drop(connection, current)
        waiting = asyncio.create_task(broker.consume("second", _on_delivery, concurrency=3))
        await _turns()
        assert not waiting.done(), "precondition: the consume waits for the reconnect"

        fresh = await _reconnect(connection)
        await waiting

        assert fresh.consumed_queues() == [["first", "second"]], (
            f"one consume channel carries both consumers, got {fresh.consumed_queues()}"
        )
        assert len(fresh.channels) == 2, f"publish and consume channel only, got {sorted(fresh.channels)}"
    finally:
        await broker.close()


@pytest.mark.befund("F4-22")
async def test_a_replaced_consume_channel_keeps_its_consumers_when_its_restore_was_late(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Auf dem alten Consume-Kanal hängen die Consumer der früheren Queues
    (ihre ``RobustQueue`` ist an ihn gebunden, der Consumer-Wächter meldet über
    sie neu an). Ihn wie einen ersetzten Publish-Kanal abzumelden und zu
    schließen, hielte diese Queues bis zum Neustart an."""
    broker, connection, current = await _connected_broker()
    try:
        await broker.consume("first", _on_delivery, concurrency=3)
        await _drop(connection, current)
        waiting = asyncio.create_task(broker.consume("second", _on_delivery, concurrency=3))
        await _turns()
        fresh = _HeldAiormq()
        connection.transport = _FakeTransport(fresh)  # type: ignore[assignment]  # Fake statt UnderlayConnection
        restoring = asyncio.create_task(connection._on_connected())
        _shorten_restore_deadline(monkeypatch)
        await waiting
        fresh.release.set()
        await restoring
        await _turns()

        assert sorted(fresh.consumed_queues()) == [["first"], ["second"]], (
            f"the late restored consume channel keeps its consumer next to the new one, got {fresh.consumed_queues()}"
        )

        await _drop(connection, fresh)
        again = await _reconnect(connection)

        assert sorted(again.consumed_queues()) == [["first"], ["second"]], (
            f"both consume channels come back with the next reconnect, got {again.consumed_queues()}"
        )
    finally:
        await broker.close()
