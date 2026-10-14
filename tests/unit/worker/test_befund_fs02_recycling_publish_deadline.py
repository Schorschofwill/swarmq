"""Nachweis FS-02 (b): Der Recycling-Drain wartet auf Langlaeufer, die
Publish-Endfrist schneidet sie trotzdem ab.

Register: docs/befunde/fehlersuche-runde-2-2026-10.md (FS-02, Quelle N-4).

``request_recycle`` setzt ``_drain_timeout_override`` (groesster
``hard_timeout`` + 30 s), damit ein gesunder Langlaeufer im Recycling-Drain
fertig werden darf (KTD11/R22). ``_execute_shutdown`` setzt die
Publish-Endfrist aber unveraendert auf ``started_at + budget.drain_until``
(``worker/shutdown.py``) -- bei ``shutdown_timeout=30`` unter dem Supervisor
etwa 23 s nach Drain-Beginn. Jeder Task, der danach noch publizieren muss
(Retry-Kopie, DLQ-Kopie, Workflow-Advance), scheitert mit
``PublishIndeterminate``, obwohl der Drain ihn ausdruecklich abwartet.

Zeit ist hier skaliert statt gemockt (wie im bestehenden Recycling-Test in
``test_shutdown_budget_steps.py``): ``shutdown_timeout=0.05`` steht fuer die
30 s, ``_drain_timeout_override=2.0`` fuer die Recycling-Frist. Der Task
endet kurz nach der Publish-Endfrist -- dem "25 s nach Drain-Beginn" des
Szenarios -- und publiziert dann seine Retry-Kopie ueber einen echten
``RabbitMQBroker`` (Writer und Broker spielt ``FakeTransport``).

Mit dem Fix (Plan 2026-10-14-0748, U3) folgt die Publish-Endfrist dem
Drain-Ende; die normale Frist berechnet der Test darum aus
``shutdown_budget`` statt sie vom Broker zu lesen.

Testliste:
# [x] Retry-Publish eines Langlaeufers nach der normalen Publish-Endfrist,
#     aber innerhalb der Recycling-Drain-Frist, scheitert nicht
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.contracts import Delivery
from swarmq.registry import TaskRegistry
from swarmq.shutdown_budget import shutdown_budget
from swarmq.worker import Worker, WorkerState
from tests.support.aiormq_transport import FakePublishChannel, FakeTransport

pytestmark = pytest.mark.timeout(5)


async def _fs02_wire(transport: FakeTransport) -> None:
    """Writer und Broker: jeden Frame sofort schreiben und bestaetigen."""
    while True:
        frame = await transport.next_frame()
        frame.drain()
        transport.confirm_all()


def _fs02_real_broker(transport: FakeTransport) -> RabbitMQBroker:
    broker = RabbitMQBroker("amqp://guest:guest@localhost/")
    broker._connection = Mock(is_closed=False, transport=None)
    broker._connection.ready = AsyncMock()
    broker._publish_channel_factory = AsyncMock(return_value=FakePublishChannel(transport))
    broker.publish_timeout_s = 60.0
    return broker


@pytest.mark.befund("FS-02")
async def test_fs02_long_runner_can_publish_its_retry_during_the_recycling_drain(backend: Any, config: Any) -> None:
    transport = FakeTransport()
    writer = asyncio.create_task(_fs02_wire(transport))
    broker = _fs02_real_broker(transport)
    config.shutdown_timeout = 0.05  # skaliert: steht fuer shutdown_timeout=30
    worker = Worker(broker=broker, backend=backend, registry=TaskRegistry(), config=config)
    worker._drain_timeout_override = 2.0  # skaliert: Recycling-Frist (hard_timeout + 30 s)

    loop = asyncio.get_running_loop()
    past_publish_deadline = asyncio.Event()

    async def _drain_started() -> None:
        # Erster Schritt des Drains; die Publish-Endfrist ist da schon gesetzt.
        assert broker._publish_deadline_at is not None, "Voraussetzung: das Herunterfahren setzt eine Publish-Endfrist"
        # Die normale Frist (ohne Recycling) aus dem Budget berechnen, nicht
        # vom Broker lesen: Mit dem Fix ist dort die Recycling-Frist gesetzt
        # (Plan 2026-10-14-0748, U3). ``started_at`` liegt vor diesem Aufruf,
        # der Zeitpunkt unten also sicher hinter der normalen Frist.
        normal_deadline = loop.time() + shutdown_budget(config.shutdown_timeout, supervised=False).drain_until
        loop.call_at(normal_deadline + 0.005, past_publish_deadline.set)

    # Grenzen des Abbaus, die eine echte Verbindung braeuchten.
    broker.cancel_consume = AsyncMock(side_effect=_drain_started)  # type: ignore[method-assign]  # Grenze: kein Consumer im Unit-Test
    broker.close = AsyncMock()  # type: ignore[method-assign]  # Grenze: keine echte Verbindung

    outcome: list[BaseException | None] = []

    async def _long_runner() -> None:
        await past_publish_deadline.wait()
        delivery = Delivery(body=b'{"fs02": "b"}', headers={}, queue="tasks", handle=AsyncMock(consumer_tag=None))
        try:
            await broker.retry_later(delivery, delay_ms=1000, headers={})
        except Exception as exc:
            outcome.append(exc)
        else:
            outcome.append(None)

    task = asyncio.create_task(_long_runner())
    worker._active_tasks.add(task)
    task.add_done_callback(worker._active_tasks.discard)
    try:
        worker._state = WorkerState.RUNNING
        worker._request_shutdown()
        await asyncio.wait_for(worker._execute_shutdown(), 2.0)
    finally:
        writer.cancel()
        await asyncio.gather(writer, return_exceptions=True)

    assert not task.cancelled(), "Voraussetzung: der Recycling-Drain bricht den Langlaeufer nicht ab"
    assert outcome == [None], (
        f"Retry-Publish im Recycling-Drain scheiterte: {outcome[0]!r} -- das Original wird zurueckgestellt, "
        "obwohl der Drain den Task ausdruecklich abwartet"
    )
