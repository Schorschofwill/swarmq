"""Abbruch waehrend des Aufbaus der Consume-Verbindung (Plan 0.12.0, MR 16/18).

Wird ``consume()`` abgebrochen, waehrend ``aio_pika.connect_robust`` die
zweite Verbindung (BZ-16) noch aufbaut, darf keine Verbindung uebrig bleiben:
aio-pika setzt einen abgebrochenen Aufbau sonst im Hintergrund fort und
verbindet sich alle 5 s neu (gefunden beim Gate von MR 18: abgebrochener
Worker-Start im e2e-Test ``test_signal_scheduler_leader``, haengender
Event-Loop beim Schliessen). Grenze: ``connect_robust`` und
``RobustConnection`` gemockt.

Test-Liste:
- [x] Abbruch waehrend ``connect_robust`` -> die angelegte Verbindung wird geschlossen
"""

from __future__ import annotations

import asyncio
from typing import Any, ClassVar
from unittest.mock import AsyncMock, patch

import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker

pytestmark = [pytest.mark.timeout(5)]

_MODULE = "swarmq.broker.rabbitmq.connection.aio_pika"


class _Robust:
    """``RobustConnection``-Ersatz: merkt sich jede Instanz und ``close``."""

    instances: ClassVar[list[_Robust]] = []

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.close = AsyncMock()
        _Robust.instances.append(self)


async def test_cancel_during_consume_connect_closes_the_half_built_connection() -> None:
    _Robust.instances = []
    publish_conn = AsyncMock(is_closed=False)
    building = asyncio.Event()
    never = asyncio.Event()

    async def _connect_robust(url: str, **kwargs: Any) -> Any:
        # Wie aio-pika: erst die Verbindung anlegen, dann aufbauen.
        kwargs.get("connection_class", _Robust)(url)
        building.set()
        await never.wait()
        raise AssertionError("unreachable")

    broker = RabbitMQBroker("amqp://guest:guest@localhost/")
    with patch(f"{_MODULE}.connect_robust", AsyncMock(side_effect=[publish_conn])):
        await broker.connect()
    with (
        patch(f"{_MODULE}.connect_robust", AsyncMock(side_effect=_connect_robust)),
        patch(f"{_MODULE}.RobustConnection", _Robust),
    ):
        task = asyncio.create_task(broker.consume("q1", AsyncMock(), concurrency=1))
        await asyncio.wait_for(building.wait(), timeout=2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    [half_built] = _Robust.instances
    half_built.close.assert_awaited_once()
