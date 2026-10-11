"""Retry-Kopie gegen echtes RabbitMQ: kein vererbter Zustellzaehler.

Die Retry-Kopie ist eine neue Nachricht mit eigenem Zustellbudget. RabbitMQ 4
behaelt einen vom Publisher gesetzten ``x-delivery-count`` bei (3.13 zaehlt
zudem Requeues); hat die Kopie ihn vom Original geerbt, haelt das
Delivery-Count-Gate sie fuer haengend und schickt sie in den Dead-Letter,
obwohl der Task noch Retries hat.

Der Broker hier (RabbitMQ 4) liefert einen vom Publisher gesetzten Wert bei
der ersten Zustellung und nach ``nack(requeue=True)`` unveraendert aus; erst
eine abgebrochene Zustellung ersetzt ihn durch den eigenen Zaehler. Auf 3.13
zaehlt schon ein Requeue -- eine Kopie mit geerbtem ``limit - 2`` steht nach
einem einzigen Requeue an der Schwelle. Der Test setzt den geerbten Wert
deshalb direkt an die Schwelle (``limit - 1``) und ruft die Retry-Bewertung
ohne vorgeschaltetes Gate auf.

Test-Liste:
- [x] Original mit hohem ``x-delivery-count`` scheitert -> die Retry-Kopie
      kommt ohne Broker-Zaehler zurueck, das Delivery-Count-Gate laesst sie
      durch, das ``.XQ`` bleibt leer
"""

from __future__ import annotations

import contextlib
import uuid
from collections.abc import AsyncIterator
from typing import override
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.config import SwarmQConfig
from swarmq.middleware import MiddlewareChain
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker, WorkerState
from swarmq.worker.gates import GateContext
from swarmq.worker.outcome import Continue, Redelivered
from tests.contracts._support import consume_into
from tests.support.queues import delete_queue_family

pytestmark = [pytest.mark.timeout(15)]

_BODY = b'{"task": {"id": "t1", "name": "flaky", "queue": "ignored"}, "args": [], "kwargs": {}}'
_DELIVERY_LIMIT = 20


class FlakyTask(Task):
    task_info = TaskInfo(name="flaky", queues=["default"], max_retries=3, retry_base_delay=0.2)

    @override
    async def run(self) -> None:
        return None


@pytest.fixture
def queue() -> str:
    return f"retrycopy-{uuid.uuid4().hex[:8]}"


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


def _worker(broker: RabbitMQBroker) -> Worker:
    cfg = SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x", delivery_limit=_DELIVERY_LIMIT)
    backend = MagicMock()
    backend.store_result = AsyncMock()
    backend.notify_result = AsyncMock()
    registry = MagicMock()
    registry.get_all = MagicMock(return_value=[])
    registry.get = MagicMock(return_value=FlakyTask)
    worker = Worker(broker, backend, registry, cfg)
    worker._state = WorkerState.RUNNING
    return worker


async def test_retry_copy_with_inherited_high_delivery_count_is_not_dead_lettered(
    broker: RabbitMQBroker,
    queue: str,
) -> None:
    worker = _worker(broker)
    await broker.publish(queue, _BODY, headers={"x-delivery-count": _DELIVERY_LIMIT - 1})
    inbox = await consume_into(broker, queue)
    original = await inbox.next()
    ctx = GateContext(
        task_id="t1",
        task_name="flaky",
        queue=queue,
        task_info=FlakyTask.task_info,
        task_class=FlakyTask,
    )

    outcome = await worker._retry_handler.evaluate(
        original,
        ctx,
        RuntimeError("boom"),
        "boom",
        {"retry_count": 0},
        MiddlewareChain([]),  # type: ignore[typeddict-item]
    )
    assert outcome is Redelivered

    copy = await inbox.next(timeout=5.0)
    assert copy.headers["x-swarmq-retry-count"] == 1
    # Die Kopie kommt am Gate vorbei und landet nicht im Dead-Letter.
    outcome = await worker._delivery_count_gate.apply(copy, ctx)
    if outcome is Continue:
        await broker.ack(copy)
    assert await broker.dead_letter_depth(queue) == 0, "Retry-Kopie faelschlich im .XQ"
    assert outcome is Continue
    assert "x-delivery-count" not in copy.headers
