"""Zurueckstellung bei Backend-Ausfall gegen echtes RabbitMQ.

Scheitert ein Gate vor der Ausfuehrung am Backend, legt der Worker eine
bestaetigte Kopie in die Delay-Queue und bestaetigt erst dann das Original;
die Kopie kehrt nach der Wartezeit in die Zustellungs-Queue zurueck.

Test-Liste:
- [x] Kopie kommt nach der Wartezeit zurueck: gleicher Body, alle Header,
      Zurueckstellungszaehler 1, kein mitkopierter ``x-delivery-count``;
      das Original ist bestaetigt (keine zweite Zustellung)
- [x] Publish scheitert -> das Original kommt per Requeue zurueck, keine Kopie
"""

from __future__ import annotations

import contextlib
import time
import uuid
from collections.abc import AsyncIterator
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.config import SwarmQConfig
from swarmq.contracts import Delivery
from swarmq.exceptions import BackendConnectionError
from swarmq.headers import HEADER_BACKEND_DEFER_COUNT
from swarmq.worker import Worker, WorkerState
from swarmq.worker.gates import GateContext
from swarmq.worker.outcome import Redelivered
from tests.contracts._support import Inbox, consume_into
from tests.support.queues import delete_queue_family
from tests.support.rabbitmq_faults import publish_failure

pytestmark = [pytest.mark.timeout(15)]

_BODY = b'{"task": {"id": "t1", "name": "deferred", "queue": "ignored"}, "args": [1], "kwargs": {}}'
_BASE_DELAY_S = 0.3


def _worker(broker: RabbitMQBroker) -> Worker:
    cfg = SwarmQConfig(
        broker_url="amqp://x",
        backend_url="valkey://x",
        backend_defer_base_delay=_BASE_DELAY_S,
    )
    backend = MagicMock()
    backend.is_cancelled = AsyncMock(side_effect=BackendConnectionError("valkey down"))
    registry = MagicMock()
    registry.get_all = MagicMock(return_value=[])
    worker = Worker(broker, backend, registry, cfg)
    worker._state = WorkerState.RUNNING
    return worker


@pytest.fixture
def queue() -> str:
    return f"defer-{uuid.uuid4().hex[:8]}"


@pytest.fixture
async def broker(rabbitmq_url: str, queue: str) -> AsyncIterator[RabbitMQBroker]:
    impl = RabbitMQBroker(rabbitmq_url)
    await impl.connect()
    await impl.declare_queue(queue)
    yield impl
    with contextlib.suppress(Exception):
        await impl.cancel_consume()
    await delete_queue_family(rabbitmq_url, queue)
    await impl.close()


async def _first_delivery(broker: RabbitMQBroker, queue: str, headers: dict[str, Any]) -> tuple[Inbox, Delivery]:
    await broker.publish(queue, _BODY, headers=headers)
    inbox = await consume_into(broker, queue)
    return inbox, await inbox.next()


def _ctx(queue: str) -> GateContext:
    return GateContext(task_id="t1", task_name="deferred", queue=queue)


async def test_deferred_copy_returns_after_delay_with_headers(broker: RabbitMQBroker, queue: str) -> None:
    worker = _worker(broker)
    original = {
        "x-swarmq-retry-count": 2,
        "x-swarmq-workflow-id": "wf-1",
        "x-swarmq-unique-hash": "h1",
        "x-swarmq-enqueued-at": 1700000000,
        # Ein Broker, der Requeues zaehlt (RabbitMQ 3.13), setzt ihn; der
        # Publisher-Wert bleibt in RabbitMQ 4 erhalten -- genau deshalb darf
        # die Kopie ihn nicht mitnehmen.
        "x-delivery-count": 18,
    }
    inbox, delivery = await _first_delivery(broker, queue, original)

    started = time.monotonic()
    outcome = await worker._cancel_gate.apply(delivery, _ctx(queue))

    assert outcome is Redelivered
    copy = await inbox.next(timeout=5.0)
    assert time.monotonic() - started >= _BASE_DELAY_S * 0.8
    assert copy.body == _BODY
    assert "x-delivery-count" not in copy.headers
    assert copy.headers[HEADER_BACKEND_DEFER_COUNT] == 1
    for key in ("x-swarmq-retry-count", "x-swarmq-workflow-id", "x-swarmq-unique-hash", "x-swarmq-enqueued-at"):
        assert copy.headers[key] == original[key]
    await broker.ack(copy)
    await inbox.expect_nothing(within=1.0)  # das Original war bestaetigt
    assert await broker.dead_letter_depth(queue) == 0


async def test_publish_failure_falls_back_to_requeue(broker: RabbitMQBroker, queue: str) -> None:
    worker = _worker(broker)
    inbox, delivery = await _first_delivery(broker, queue, {})

    with publish_failure(broker):
        outcome = await worker._cancel_gate.apply(delivery, _ctx(queue))

    assert outcome is Redelivered
    again = await inbox.next()  # requeue=True → erneute Zustellung des Originals
    assert again.body == _BODY
    assert HEADER_BACKEND_DEFER_COUNT not in again.headers
    await broker.ack(again)
    await inbox.expect_nothing(within=1.0)  # keine Kopie aus der Delay-Queue
