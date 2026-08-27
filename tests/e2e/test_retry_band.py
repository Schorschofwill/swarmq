"""E2E: Band-Erhalt beim Retry (Plan U8, KTD2 — die einzige benannte
Verhaltenskorrektur der Phase 2).

Eine Nachricht liegt in ``{queue}.high``, ihr Body nennt aber die Basis-Queue
``{queue}`` (Fremd-Producer, oder eine per ``dlq retry`` umgehängte
Nachricht). Der Task scheitert einmal; die Retry-Kopie muss über das
Verzögerungsziel der *Zustellungs-Queue* (``{queue}.high.DQ``) zurück in
``{queue}.high`` gelangen — nicht über ``{queue}.DQ`` in die Basis-Queue.
Bisher las der Worker die Queue aus dem Body und der Retry verlor sein Band.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
import uuid

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.headers import HEADER_ENQUEUED_AT
from swarmq.serialization import encode_json
from swarmq.task import Task, TaskInfo


async def _wait_until(predicate, *, timeout: float = 10.0, interval: float = 0.05) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if predicate():
            return
        await asyncio.sleep(interval)
    raise AssertionError("condition not met in time")


async def test_retry_from_high_band_returns_to_high_band(rabbitmq_url: str, valkey_url: str) -> None:
    queue = f"e2e-band-{uuid.uuid4().hex[:10]}"
    task_name = f"e2e_flaky_high_{uuid.uuid4().hex[:8]}"
    attempts: list[int] = []

    class FlakyHighTask(Task):
        task_info = TaskInfo(
            name=task_name,
            queues=[queue],
            priority="high",
            max_retries=2,
            retry_backoff="fixed",
            retry_base_delay=2.0,
        )

        async def run(self) -> str:
            attempts.append(1)
            if len(attempts) == 1:
                raise RuntimeError("first attempt fails")
            return "ok"

    broker = RabbitMQBroker(rabbitmq_url)
    backend = ValkeyBackend(valkey_url)
    app = SwarmQ(broker, backend, queues=[queue], priorities=True)

    async with app:
        worker_broker = RabbitMQBroker(rabbitmq_url)
        worker_backend = ValkeyBackend(valkey_url)
        worker_app = SwarmQ(worker_broker, worker_backend, queues=[queue], priorities=True)
        worker_task = asyncio.create_task(worker_app.start_worker())
        try:
            await asyncio.sleep(0.7)  # Worker deklariert die Band-Tripel und konsumiert
            # Fremd-Producer: Zustellung in {queue}.high, Body nennt die Basis-Queue.
            task_id = f"band-{uuid.uuid4().hex[:8]}"
            body = {"task": {"id": task_id, "name": task_name, "queue": queue}, "args": [], "kwargs": {}}
            await broker.publish(f"{queue}.high", encode_json(body), headers={HEADER_ENQUEUED_AT: int(time.time())})

            await _wait_until(lambda: len(attempts) == 1)
            await asyncio.sleep(0.3)  # Retry-Kopie liegt jetzt im Verzögerungsziel

            # Band-Erhalt: die Kopie wartet in {queue}.high(.DQ), nicht in {queue}(.DQ).
            assert await broker.queue_depth(f"{queue}.high") == 1
            assert await broker.queue_depth(queue) == 0

            result = await app.get_result(task_id, timeout=20)
            assert result == "ok"
            assert len(attempts) == 2
        finally:
            worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await worker_task
            await worker_broker.close()
            await worker_backend.close()
