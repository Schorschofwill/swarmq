"""Neu-Deklarieren nach einem unroutbaren Publish unter der Shutdown-Restfrist (F4-27).

Beginnt das Herunterfahren, waehrend eine geloeschte Delay-Stufe oder eine
Queue der Familie neu deklariert wird, zieht ``set_publish_deadline`` den
laufenden Versuch nach wie einen laufenden Publish (R12, KTD9). Sonst laeuft
er bis zu seiner eigenen Frist weiter, und erst der Drain bricht ihn ab: Die
``CancelledError`` kaeme in der Pipeline an, deren ``finally`` dann den
Schedfire-Completed-Marker auf eine unbestaetigte Nachricht schriebe.

Test-Liste:
- [x] Stufe: eine waehrend des Versuchs gesetzte Restfrist beendet ihn, Ergebnis wie mit
      vorher gesetzter Restfrist (``BrokerConnectionError``, Original unbestaetigt)
- [x] Familie (``.XQ``): eine waehrend des Versuchs gesetzte Restfrist beendet ihn mit
      ``BrokerConnectionError``
"""

from __future__ import annotations

import asyncio
from typing import cast
from unittest.mock import AsyncMock

import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import BrokerConnectionError
from swarmq.queue_names import main_queue_of
from tests.unit.broker.test_declare_chain import FakeRabbit, make_broker
from tests.unit.broker.test_delay_tiers import _delivery, _returned

pytestmark = pytest.mark.timeout(5)


async def _hanging_redeclaration(routing_key: str) -> tuple[RabbitMQBroker, asyncio.Event, asyncio.Event]:
    """Broker, dessen erster Publish an *routing_key* unroutbar ist und
    dessen Neu-Deklarieren haengt; das Schliessen des Wegwerf-Channels
    suspendiert wie in aiormq (Review F4-27, P1-1). Liefert das Ereignis "Wegwerf-Channel
    geoeffnet" und das zum Freigeben des Haengens."""
    rabbit = FakeRabbit()
    broker = make_broker(rabbit)
    await broker.declare_queue(main_queue_of(routing_key))
    cast("AsyncMock", broker._publish_channel).default_exchange.publish.side_effect = [_returned(routing_key), None]
    broker.publish_timeout_s = 60.0
    broker.declare_probe_timeout_s = 60.0
    opened = asyncio.Event()
    release = asyncio.Event()

    async def hanging_channel(**_: object) -> AsyncMock:
        opened.set()
        channel = AsyncMock(is_closed=False)

        async def declare_queue(*_: object, **__: object) -> None:
            await release.wait()

        async def close(*_: object, **__: object) -> None:
            await asyncio.sleep(0)  # wie aiormq: Schliessen suspendiert immer

        channel.declare_queue = AsyncMock(side_effect=declare_queue)
        channel.close = AsyncMock(side_effect=close)
        return channel

    cast("AsyncMock", broker._connection).channel = AsyncMock(side_effect=hanging_channel)
    return broker, opened, release


async def _set_deadline_once_opened(broker: RabbitMQBroker, opened: asyncio.Event, task: asyncio.Task[None]) -> None:
    """Wie ``_execute_shutdown``: Die Restfrist kommt, waehrend der Versuch laeuft."""
    await opened.wait()
    broker.set_publish_deadline(asyncio.get_running_loop().time() + 0.05)
    await asyncio.wait({task}, timeout=1.0)
    try:
        assert task.done(), "die Restfrist muss den laufenden Versuch beenden"
    finally:
        if not task.done():
            task.cancel()  # wie der Drain; sonst bliebe der Task haengen
            await asyncio.wait({task})


@pytest.mark.befund("F4-27")
async def test_a_shutdown_deadline_set_during_a_tier_redeclaration_bounds_it() -> None:
    broker, opened, _release = await _hanging_redeclaration("tasks.DQ.1s")
    delivery = _delivery()
    task = asyncio.create_task(broker.retry_later(delivery, delay_ms=500, headers={}))

    await _set_deadline_once_opened(broker, opened, task)

    with pytest.raises(BrokerConnectionError, match="shutdown publish deadline"):
        task.result()
    cast("AsyncMock", delivery.handle).ack.assert_not_awaited()


@pytest.mark.befund("F4-27")
async def test_a_shutdown_deadline_set_during_a_family_redeclaration_bounds_it() -> None:
    broker, opened, _release = await _hanging_redeclaration("tasks.XQ")
    delivery = _delivery()
    task = asyncio.create_task(broker.dead_letter(delivery, headers={}))

    await _set_deadline_once_opened(broker, opened, task)

    with pytest.raises(BrokerConnectionError):
        task.result()
    cast("AsyncMock", delivery.handle).ack.assert_not_awaited()
