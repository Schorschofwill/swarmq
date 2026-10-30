"""Nachweis F4-24 (behoben in 0.14.1): Ein abgebrochenes Öffnen des Publish- oder Consume-Kanals hinterlässt beim Broker einen Kanal, den jeder Reconnect wiederherstellt.

Register: docs/befunde/fehlersuche-runde-4-2026-10.md (F4-24; Gegenprüfung
0.14.1, Probe D: Kanäle beim Broker nach dem 1. Reconnect ``[1, 2]``, nach dem
2. ``[1, 2]``, auch mit normaler Garbage Collection).

aio-pika 10.0.1 legt den ``RobustChannel`` schon bei ``connection.channel()``
ins ``WeakSet`` der ``RobustConnection``. Bricht ``_ensure_channel_tracked``
das Öffnen nach Open-Ok und Select-Ok ab, während ``RobustChannel._on_open``
auf ``Basic.Qos`` wartet, schließt aio-pika den Unterbau (beim Broker steht
danach kein Kanal), der Kanal bleibt aber im ``WeakSet``; jeder Reconnect
öffnet ihn wieder. Verletzt: E-078, Nachträge F4-20 und F4-22, Changelog 0.14.0.

Der Broker beantwortet Open und Select, Qos aber nicht: ``aiormq.Channel.basic_qos``
hängt, bis der Test es freigibt. Die Frist des Öffnens ist dafür auf 1 s
verkürzt. Publish- und Consume-Verbindung (BZ-16, Standard) trennt der Broker
über die Management-API, zweimal. Die Garbage Collection ist aus, damit nichts
vom Zufall des Einsammelns abhängt.

Test-Liste:
# [x] Publish-Kanal: Öffnen reißt die Frist im Qos, zwei Abrisse: beim Broker steht nach jedem nur der lebende Kanal
# [x] Consume-Kanal (Worker-Start): ebenso, nur Publish- und Consume-Kanal
"""

from __future__ import annotations

import asyncio
import gc
import uuid
from collections.abc import Awaitable, Callable, Iterator
from typing import Any
from urllib.parse import quote

import aio_pika
import aiormq
import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.broker.rabbitmq import connection as connection_module
from swarmq.exceptions import BrokerConnectionError
from tests.integration.broker.test_pf19_close_after_unblocked import _connection_names, _management
from tests.support.queues import delete_queue_family
from tests.support.wait import wait_until

# exclusive: Die Frist des Öffnens ist auf 1 s verkürzt; ein ausgelasteter
# Broker könnte sie schon vor dem Qos reißen.
pytestmark = [pytest.mark.integration, pytest.mark.timeout(120), pytest.mark.exclusive]

_OPEN_DEADLINE_S = 1.0


@pytest.fixture
def no_collection() -> Iterator[None]:
    gc.disable()
    try:
        yield
    finally:
        gc.enable()


async def _on_delivery(_delivery: object) -> None:
    return None


def _connections(broker: RabbitMQBroker) -> list[aio_pika.abc.AbstractConnection]:
    """Publish-Verbindung und, nach dem ersten ``consume``, die Consume-Verbindung."""
    return [c for c in (broker._connection, broker._consume_connection) if c is not None]


def _underlay(connection: aio_pika.abc.AbstractConnection) -> Any:
    transport = getattr(connection, "transport", None)
    return None if transport is None else transport.connection


def _live_channels(broker: RabbitMQBroker) -> list[list[int]]:
    """Je Verbindung die in aiormq offenen Kanäle."""
    return sorted(sorted(_underlay(c).channels) for c in _connections(broker))


async def _broker_channels(name: str) -> list[list[int]]:
    """Je Broker-Verbindung mit diesem Namen die Kanäle laut Management-API."""
    async with _management() as http:
        result = []
        for listed in await _connection_names(http, name):
            channels = (await http.get(f"/connections/{quote(listed, safe='')}/channels")).json()
            result.append(sorted(int(c["number"]) for c in channels))
    return sorted(result)


async def _broker_channels_settle(name: str, expected: list[list[int]]) -> list[list[int]]:
    """Wartet, bis die Management-API ``expected`` meldet (ihre Statistik hinkt nach)."""
    seen: list[list[int]] = []

    async def settled() -> bool:
        nonlocal seen
        seen = await _broker_channels(name)
        return seen == expected

    await wait_until(settled, timeout=20, interval=0.5)
    return seen


async def _reconnect(broker: RabbitMQBroker, name: str) -> None:
    """Der Broker trennt alle Verbindungen des Brokers; aio-pika verbindet neu."""
    connections = _connections(broker)
    dropped = [_underlay(c) for c in connections]
    names: list[str] = []
    async with _management() as http:

        async def listed() -> bool:
            nonlocal names
            names = await _connection_names(http, name)
            return len(names) == len(connections)

        assert await wait_until(listed, timeout=20, interval=0.2), (
            "precondition: the management API lists the connections"
        )
        for listed_name in names:
            (
                await http.delete(f"/connections/{quote(listed_name, safe='')}", headers={"X-Reason": "f4-24"})
            ).raise_for_status()

    async def back() -> bool:
        return all(
            (current := _underlay(c)) is not None
            and current is not old
            and not current.is_closed
            and c.connected.is_set()
            for c, old in zip(connections, dropped, strict=True)
        )

    assert await wait_until(back, timeout=40, interval=0.05), "precondition: aio-pika reconnects"


async def _open_with_withheld_qos(
    monkeypatch: pytest.MonkeyPatch, open_channel: Callable[[], Awaitable[object]]
) -> None:
    """Öffnet einen Kanal, während der Broker Qos nicht beantwortet; das
    Öffnen reißt die verkürzte Frist."""
    original = aiormq.Channel.basic_qos
    answered = asyncio.Event()
    waiting = asyncio.Event()

    async def withheld_qos(self: Any, *args: Any, **kwargs: Any) -> Any:
        waiting.set()
        await answered.wait()
        return await original(self, *args, **kwargs)

    monkeypatch.setattr(aiormq.Channel, "basic_qos", withheld_qos)
    monkeypatch.setattr(connection_module, "_CHANNEL_READY_TIMEOUT_S", _OPEN_DEADLINE_S)
    try:
        with pytest.raises(BrokerConnectionError):
            await open_channel()
    finally:
        monkeypatch.undo()
    assert waiting.is_set(), "precondition: the open was aborted while Basic.Qos was pending"


@pytest.mark.befund("F4-24")
@pytest.mark.entscheidung("E-078")
@pytest.mark.usefixtures("no_collection")
@pytest.mark.parametrize("kind", ["publish", "consume"])
async def test_a_channel_whose_open_missed_the_deadline_is_not_reopened_at_the_broker_by_reconnects(
    rabbitmq_url: str, monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    queue = f"f4-24-{uuid.uuid4().hex[:10]}"
    name = f"f4-24-{uuid.uuid4().hex[:10]}"
    broker = RabbitMQBroker(f"{rabbitmq_url}?name={name}", queue_type="classic")
    await broker.connect()
    try:
        await broker.declare_queue(queue)
        await broker.publish(queue, b"m0")
        if kind == "publish":
            await broker._discard_publish_channel(broker._publish_channel)
            await _open_with_withheld_qos(monkeypatch, broker._ensure_publish_channel)
        else:
            await _open_with_withheld_qos(monkeypatch, lambda: broker.consume(queue, _on_delivery, concurrency=1))
            await broker.consume(queue, _on_delivery, concurrency=1)

        for round_ in range(2):
            await _reconnect(broker, name)
            await broker.publish(queue, f"m{round_ + 1}".encode())

            live = _live_channels(broker)
            channels = await _broker_channels_settle(name, live)
            assert all(len(numbers) == 1 for numbers in live), (
                f"reconnect {round_ + 1}: one channel per connection is open in aiormq, got {live}"
            )
            assert channels == live, (
                f"reconnect {round_ + 1}: the broker lists only the live channels {live}, got {channels}"
            )
    finally:
        await broker.close()
        await delete_queue_family(rabbitmq_url, queue)
