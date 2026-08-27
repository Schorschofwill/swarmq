"""Characterization der heutigen Dead-Letter-Fallbacks je Aufrufstelle
(Plan U5, KTD3, Entwurf §7) — gegen echtes RabbitMQ.

Festgeschrieben wird das Verhalten der Worker-Pfade, wenn der Publish an das
Verzögerungs-/Dead-Letter-Ziel scheitert:

- Expiry und Retry fallen auf Redelivery zurück (``nack(requeue=True)``):
  die Nachricht kommt erneut zur Zustellung.
- Lock-Pfade (hier: Render-Fehler des Lock-Namens) fallen auf den DLX-Weg
  zurück (``nack(requeue=False)``): die Nachricht landet in ``{queue}.XQ``
  mit ``x-death``, aber ohne ``x-swarmq-*``-Diagnose-Header.

Phase 2 darf diese Fallbacks nicht vereinheitlichen (R9); U14 entscheidet
nur *ob und wann* (0.2.x).
"""

from __future__ import annotations

import asyncio
import contextlib
import time
import uuid
from collections.abc import AsyncIterator
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import aio_pika
import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.config import SwarmQConfig
from swarmq.contracts import Delivery
from swarmq.headers import HEADER_ENQUEUED_AT
from swarmq.middleware import MessageContext, MiddlewareChain
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker, WorkerState
from swarmq.worker.gates import GateContext
from swarmq.worker.outcome import Continue
from tests.contracts._support import consume_into
from tests.support.rabbitmq_faults import publish_failure


class _LockedTask(Task):
    name = "ctr-locked"

    async def run(self, value: int) -> int:
        return value


def _worker(broker: RabbitMQBroker) -> Worker:
    cfg = SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x")
    backend = MagicMock()
    backend.store_result = AsyncMock()
    backend.notify_result = AsyncMock()
    backend.release_unique = AsyncMock(return_value=True)
    registry = MagicMock()
    registry.get_all = MagicMock(return_value=[])
    worker = Worker(broker, backend, registry, cfg)
    worker._state = WorkerState.RUNNING
    worker._lock_manager = MagicMock()  # Lock-Pfad aktiv; der Render-Fehler kommt vor jedem Acquire
    return worker


_BODY = b'{"task": {"id": "t1", "name": "ctr-locked", "queue": "ignored"}, "args": [1], "kwargs": {}}'


@pytest.fixture
def queue() -> str:
    return f"ctr-fb-{uuid.uuid4().hex[:8]}"


@pytest.fixture
async def broker(rabbitmq_url: str, queue: str) -> AsyncIterator[RabbitMQBroker]:
    impl = RabbitMQBroker(rabbitmq_url)
    await impl.connect()
    await impl.declare_queue(queue)
    yield impl
    with contextlib.suppress(Exception):
        await impl.cancel_consume()
    connection = await aio_pika.connect_robust(rabbitmq_url)
    try:
        channel = await connection.channel()
        for suffix in ("", ".DQ", ".XQ"):
            with contextlib.suppress(Exception):
                await channel.queue_delete(f"{queue}{suffix}")
    finally:
        await connection.close()
    await impl.close()


async def _first_delivery(broker: RabbitMQBroker, queue: str, headers: dict[str, Any]) -> tuple[Any, Delivery]:
    await broker.publish(queue, _BODY, headers=headers)
    inbox = await consume_into(broker, queue)
    return inbox, await inbox.next()


async def test_expiry_publish_failure_falls_back_to_redelivery(broker: RabbitMQBroker, queue: str) -> None:
    worker = _worker(broker)
    inbox, delivery = await _first_delivery(broker, queue, {HEADER_ENQUEUED_AT: int(time.time()) - 3600})
    task_info = TaskInfo(name="ctr-locked", queues=(queue,), max_age=1)

    ctx = GateContext(task_id="t1", task_name="ctr-locked", queue=queue, task_info=task_info, task_class=_LockedTask)
    with publish_failure(broker):
        outcome = await worker._expiry_gate.apply(delivery, ctx)

    assert outcome is not Continue
    again = await inbox.next()  # requeue=True → erneute Zustellung
    assert again.body == _BODY
    await broker.ack(again)


async def test_retry_publish_failure_falls_back_to_redelivery(broker: RabbitMQBroker, queue: str) -> None:
    worker = _worker(broker)
    inbox, delivery = await _first_delivery(broker, queue, {})
    task_info = TaskInfo(name="ctr-locked", queues=(queue,), max_retries=3)
    context: MessageContext = {
        "task_id": "t1",
        "task_name": "ctr-locked",
        "queue": queue,
        "args": [1],
        "kwargs": {},
        "delivery_mode": task_info.delivery.value,
        "retry_count": 0,
    }

    ctx = GateContext(task_id="t1", task_name="ctr-locked", queue=queue, task_info=task_info)
    with publish_failure(broker):
        outcome = await worker._retry_handler.evaluate(
            delivery, ctx, RuntimeError("boom"), "boom", context, MiddlewareChain([])
        )

    assert outcome is not Continue
    again = await inbox.next()
    assert again.body == _BODY
    await broker.ack(again)


async def test_lock_render_failure_with_publish_failure_routes_via_dlx(
    broker: RabbitMQBroker, queue: str, rabbitmq_url: str
) -> None:
    worker = _worker(broker)
    inbox, delivery = await _first_delivery(broker, queue, {"x-swarmq-enqueued-at": 5})
    task_info = TaskInfo(name="ctr-locked", queues=(queue,), lock="user-{missing}", hard_timeout=5)

    ctx = GateContext(
        task_id="t1", task_name="ctr-locked", queue=queue, task_info=task_info, task_class=_LockedTask,
        args=[1], kwargs={}, handler_id=uuid.uuid4(),
    )
    with publish_failure(broker):
        outcome = await worker._lock_gate.apply(delivery, ctx)

    assert outcome is not Continue
    await inbox.expect_nothing()  # nicht redelivered
    letters = await _read_dead_letters(rabbitmq_url, queue)
    assert len(letters) == 1
    body, headers = letters[0]
    assert body == _BODY
    assert "x-death" in headers  # über die Broker-Konfiguration geroutet …
    assert "x-swarmq-lock-render-error" not in headers  # … ohne Diagnose-Header
    assert headers["x-swarmq-enqueued-at"] == 5  # Original-Header bleiben


async def _read_dead_letters(url: str, queue: str) -> list[tuple[bytes, dict[str, Any]]]:
    connection = await aio_pika.connect_robust(url)
    try:
        channel = await connection.channel()
        amqp_queue = await channel.get_queue(f"{queue}.XQ")
        letters: list[tuple[bytes, dict[str, Any]]] = []
        for _ in range(20):
            message = await amqp_queue.get(no_ack=False, fail=False)
            if message is None:
                await asyncio.sleep(0.1)
                message = await amqp_queue.get(no_ack=False, fail=False)
                if message is None:
                    break
            letters.append((message.body, dict(message.headers or {})))
        return letters
    finally:
        await connection.close()
