"""Nachweis F4-22 (behoben in 0.14.0): Ein Publish während eines Verbindungsausfalls lässt nach dem Reconnect einen zweiten Kanal zurück, der mit jedem Reconnect wiederkommt.

Register: docs/befunde/fehlersuche-runde-4-2026-10.md (F4-22; Verdacht C der
Gegenprüfung 0.14, Probe ``probe_c_outage_publish``: Kanäle beim Broker
``[1, 2]``, ``[1, 2, 3]``, ``[1, 2, 3, 4]``).

aio-pika 10.0.1 setzt in ``RobustConnection._on_connected`` zuerst
``connected`` und stellt danach die Kanäle aus einer Tupel-Kopie des
``WeakSet`` wieder her. Ein Publish, der auf ``connection.ready()`` wartet,
wacht in diesem Fenster auf, findet den alten Publish-Kanal ``is_closed`` und
öffnete vor dem Fix einen neuen; aio-pika stellte den alten trotzdem wieder her.
Verletzt: E-078, Nachtrag 2026-10-19 („kein Leck auch über einen Reconnect“).

Ausfall statt sofortigem Reconnect: aio-pika wartet nach jedem
Verbindungsaufbau ``reconnect_interval`` Sekunden, bevor es auf den nächsten
Abriss reagiert. Ein Abriss kurz nach dem (Wieder-)Verbinden lässt die
Verbindung darum bis zum Ende dieser Pause getrennt; ``reconnect_interval=8``
gibt der Management-API Zeit, die neue Verbindung zu führen. Die Garbage
Collection ist aus, damit nichts vom Zufall des Einsammelns abhängt.

Test-Liste:
# [x] drei Ausfälle mit Publish im Ausfall: nach jedem Reconnect genau ein Kanal beim Broker
"""

from __future__ import annotations

import gc
import uuid
from collections.abc import Iterator
from typing import Any
from urllib.parse import quote

import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from tests.integration.broker.test_pf19_close_after_unblocked import (
    _aiormq_connection,
    _broker_channels_settle,
    _management,
    _open_aiormq_channels,
)
from tests.support.queues import delete_queue_family
from tests.support.wait import wait_until

# exclusive: drei Ausfälle mit fester Pause; ein ausgelasteter Broker
# verschöbe das Fenster, in dem der Publish wartet.
pytestmark = [pytest.mark.integration, pytest.mark.timeout(120), pytest.mark.exclusive]

_RECONNECT_INTERVAL_S = 8


@pytest.fixture
def no_collection() -> Iterator[None]:
    gc.disable()
    try:
        yield
    finally:
        gc.enable()


async def _cut_at_broker(connection_name: str, cut: set[str]) -> None:
    """Trennt die offene Verbindung mit diesem ``connection_name`` über die
    Management-API; ``cut`` sind die schon getrennten (Management-Namen)."""
    target: str | None = None
    async with _management() as http:

        async def listed() -> bool:
            nonlocal target
            connections = (await http.get("/connections")).json()
            fresh = [
                c["name"]
                for c in connections
                if c.get("client_properties", {}).get("connection_name") == connection_name and c["name"] not in cut
            ]
            target = fresh[0] if fresh else None
            return target is not None

        assert await wait_until(listed, timeout=20, interval=0.1), (
            "precondition: the management API lists the connection"
        )
        assert target is not None
        cut.add(target)
        (await http.delete(f"/connections/{quote(target, safe='')}", headers={"X-Reason": "f4-22"})).raise_for_status()


@pytest.mark.befund("F4-22")
@pytest.mark.entscheidung("E-078")
@pytest.mark.usefixtures("no_collection")
async def test_a_publish_during_an_outage_leaves_one_channel_at_the_broker_after_each_reconnect(
    rabbitmq_url: str,
) -> None:
    queue = f"f4-22-{uuid.uuid4().hex[:10]}"
    name = f"f4-22-{uuid.uuid4().hex[:10]}"
    broker = RabbitMQBroker(
        f"{rabbitmq_url}?name={name}&reconnect_interval={_RECONNECT_INTERVAL_S}", queue_type="classic"
    )
    await broker.connect()
    cut: set[str] = set()
    try:
        await broker.declare_queue(queue)
        await broker.publish(queue, b"m0")
        for round_ in range(3):
            dropped = _aiormq_connection(broker)
            await _cut_at_broker(name, cut)

            async def disconnected(dropped: Any = dropped) -> bool:
                return bool(dropped.is_closed)

            assert await wait_until(disconnected, timeout=10, interval=0.01), "precondition: the connection dropped"
            connection = broker._connection
            assert connection is not None
            if connection.connected.is_set():
                pytest.fail(f"round {round_}: precondition: aio-pika reconnected at once, no outage")
            receipt = await broker.publish(queue, f"outage-{round_}".encode())
            assert receipt.queue == queue

            live = _open_aiormq_channels(broker)
            channels = await _broker_channels_settle(name, live)
            assert len(live) == 1, f"round {round_}: one channel is open in aiormq after the reconnect, got {live}"
            assert channels == live, f"round {round_}: the broker lists only the live channel {live}, got {channels}"
    finally:
        await broker.close()
        await delete_queue_family(rabbitmq_url, queue)
