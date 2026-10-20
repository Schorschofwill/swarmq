"""Nachweis F4-24 (behoben in 0.14.1): Ein abgebrochenes Öffnen des Publish- oder Consume-Kanals hinterlässt einen Kanal, den jeder Reconnect wiederherstellt.

Register: docs/befunde/fehlersuche-runde-4-2026-10.md (F4-24; Gegenprüfung
0.14.1, Probe D).

aio-pika 10.0.1 legt den ``RobustChannel`` schon bei ``connection.channel()``
ins ``WeakSet`` der ``RobustConnection``, vor dem Öffnen. Bricht
``_ensure_channel_tracked`` das Öffnen nach Open-Ok und Select-Ok ab (Frist,
Abbruch des Aufrufers), während ``RobustChannel._on_open`` auf ``Basic.Qos``
wartet, schließt aio-pika den Unterbau, der Kanal bleibt aber im ``WeakSet``.
``_on_connected`` stellt ihn nach dem nächsten Reconnect wieder her, aiormq hält
ihn danach stark, und er bleibt als Waise offen. Verletzt: E-078, Nachträge
F4-20 („kein Leck auch über einen Reconnect“) und F4-22, und der Changelog
0.14.0 („only the live publish and consume channels open again“).

Echtes aio-pika über dem Fake der aiormq-Verbindung aus dem F4-20-Nachweis;
``basic_qos`` hängt, solange der Test es will (wie ein Broker, der Open und
Select beantwortet, Qos aber nicht). Die Garbage Collection ist aus.

Test-Liste:
# [x] Publish-Kanal: Öffnen reißt die Frist im Qos, nach dem Reconnect nur der lebende Kanal
# [x] Consume-Kanal: Öffnen reißt die Frist im Qos, nach dem Reconnect nur die lebenden Kanäle
# [x] Abbruch des Aufrufers im Qos: CancelledError geht durch, der Kanal kommt nicht wieder
# [x] erfolgreiches Öffnen: der Kanal bleibt in der Wiederherstellung
"""

from __future__ import annotations

import asyncio
import gc
from collections.abc import Iterator
from typing import Any, cast, override

import aio_pika
import pytest
from yarl import URL

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.broker.rabbitmq import connection as connection_module
from swarmq.exceptions import BrokerConnectionError
from tests.unit.broker.test_befund_f4_20_restore_closed_channels import (
    _FakeAiormq,
    _FakeAiormqChannel,
    _FakeTransport,
    _settle,
)
from tests.unit.broker.test_befund_f4_22_channel_replaced_in_restore_window import _ConsumingChannel

pytestmark = pytest.mark.timeout(5)


class _QosChannel(_ConsumingChannel):
    """``basic_qos`` wartet, solange die Verbindung Qos zurückhält."""

    @override
    async def basic_qos(self, **_: Any) -> None:
        connection = cast("_QosAiormq", self.connection)
        connection.qos_waiting.set()
        await connection.qos_answered.wait()


class _QosAiormq(_FakeAiormq):
    """Beantwortet Open und Select, Qos nur, wenn ``qos_answered`` gesetzt ist."""

    def __init__(self) -> None:
        super().__init__()
        self.qos_answered = asyncio.Event()
        self.qos_answered.set()
        self.qos_waiting = asyncio.Event()

    @override
    async def channel(self, **_: Any) -> _FakeAiormqChannel:
        number = max(self.channels, default=0) + 1
        channel = _QosChannel(self, number)
        self.channels[number] = channel
        self.opened.append(number)
        return channel

    def consumed_queues(self) -> list[list[str]]:
        """Je offenem Kanal mit Consumern die konsumierten Queues."""
        return [
            sorted(channel.consumers.values())
            for channel in self.channels.values()
            if isinstance(channel, _QosChannel) and channel.consumers
        ]

    def withhold_qos(self) -> None:
        self.qos_answered.clear()
        self.qos_waiting.clear()


@pytest.fixture
def no_collection() -> Iterator[None]:
    """Keine Garbage Collection: Ob der abgebrochene ``RobustChannel`` vor dem
    Reconnect eingesammelt ist, wäre sonst Zufall."""
    gc.disable()
    try:
        yield
    finally:
        gc.enable()


async def _connected_broker() -> tuple[RabbitMQBroker, aio_pika.RobustConnection, _QosAiormq]:
    connection = aio_pika.RobustConnection(URL("amqp://guest:guest@localhost/"))
    aiormq_connection = _QosAiormq()
    connection.transport = _FakeTransport(aiormq_connection)  # type: ignore[assignment]  # Fake statt UnderlayConnection
    connection.connected.set()
    broker = RabbitMQBroker(connection=connection)
    await broker.connect()
    return broker, connection, aiormq_connection


async def _reconnect(connection: aio_pika.RobustConnection, dropped: _FakeAiormq) -> _QosAiormq:
    """Der Reconnect wie in aio-pika: Transport weg, alte Verbindung zu, neuer
    Transport, ``_on_connected`` stellt die Kanäle des ``WeakSet`` wieder her."""
    connection.transport = None
    connection.connected.clear()
    await dropped.drop()
    await _settle()
    fresh = _QosAiormq()
    connection.transport = _FakeTransport(fresh)  # type: ignore[assignment]  # Fake statt UnderlayConnection
    await connection._on_connected()
    await _settle()
    return fresh


def _shorten_open_deadline(monkeypatch: pytest.MonkeyPatch) -> None:
    """Kürzt die Frist, mit der ``_ensure_channel_tracked`` einen Kanal öffnet."""
    monkeypatch.setattr(connection_module, "_CHANNEL_READY_TIMEOUT_S", 0.05)


async def _on_delivery(_delivery: object) -> None:
    return None


@pytest.mark.befund("F4-24")
@pytest.mark.entscheidung("E-078")
@pytest.mark.usefixtures("no_collection")
async def test_a_publish_channel_whose_open_missed_the_deadline_does_not_come_back_with_a_reconnect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker, connection, current = await _connected_broker()
    try:
        await broker._discard_publish_channel(broker._publish_channel)
        await asyncio.gather(*broker._pending_channel_closes)
        current.withhold_qos()
        _shorten_open_deadline(monkeypatch)
        with pytest.raises(BrokerConnectionError):
            _ = await broker._ensure_publish_channel()
        assert current.qos_waiting.is_set(), "precondition: the open was aborted while Basic.Qos was pending"
        current.qos_answered.set()
        _ = await broker._ensure_publish_channel()

        fresh = await _reconnect(connection, current)

        assert sorted(fresh.channels) == [1], (
            f"after the reconnect only the live publish channel is open again, got {sorted(fresh.channels)}"
        )
    finally:
        await broker.close()


@pytest.mark.befund("F4-24")
@pytest.mark.entscheidung("E-078")
@pytest.mark.usefixtures("no_collection")
async def test_a_consume_channel_whose_open_missed_the_deadline_does_not_come_back_with_a_reconnect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker, connection, current = await _connected_broker()
    try:
        current.withhold_qos()
        _shorten_open_deadline(monkeypatch)
        with pytest.raises(BrokerConnectionError):
            await broker.consume("tasks", _on_delivery, concurrency=3)
        assert current.qos_waiting.is_set(), "precondition: the open was aborted while Basic.Qos was pending"
        current.qos_answered.set()
        await broker.consume("tasks", _on_delivery, concurrency=3)

        fresh = await _reconnect(connection, current)

        assert sorted(fresh.channels) == [1, 2], (
            f"after the reconnect only the live publish and consume channel are open again, got {sorted(fresh.channels)}"
        )
        assert fresh.consumed_queues() == [["tasks"]]
    finally:
        await broker.close()


@pytest.mark.befund("F4-24")
@pytest.mark.usefixtures("no_collection")
async def test_a_caller_cancelling_the_open_gets_the_cancellation_and_the_channel_does_not_come_back() -> None:
    broker, connection, current = await _connected_broker()
    try:
        await broker._discard_publish_channel(broker._publish_channel)
        await asyncio.gather(*broker._pending_channel_closes)
        current.withhold_qos()
        opening = asyncio.create_task(broker._ensure_publish_channel())
        await current.qos_waiting.wait()

        opening.cancel()
        with pytest.raises(asyncio.CancelledError):
            _ = await opening
        current.qos_answered.set()
        _ = await broker._ensure_publish_channel()
        fresh = await _reconnect(connection, current)

        assert sorted(fresh.channels) == [1], (
            f"after the reconnect only the live publish channel is open again, got {sorted(fresh.channels)}"
        )
    finally:
        await broker.close()


@pytest.mark.befund("F4-24")
@pytest.mark.usefixtures("no_collection")
async def test_a_successfully_opened_channel_is_restored_by_a_reconnect() -> None:
    broker, connection, current = await _connected_broker()
    try:
        await broker._discard_publish_channel(broker._publish_channel)
        await asyncio.gather(*broker._pending_channel_closes)
        opened = await broker._ensure_publish_channel()

        fresh = await _reconnect(connection, current)

        assert sorted(fresh.channels) == [1], f"the opened channel is restored, got {sorted(fresh.channels)}"
        assert broker._publish_channel is opened
        assert not opened.is_closed
    finally:
        await broker.close()
