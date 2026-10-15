"""Nachweis F3-03 am echten Broker: ``Retry(delay<0)`` über einen echten Worker.

Register: docs/befunde/fehlersuche-runde-3-2026-10.md (F3-03, R3C-02).

Ein Task wirft ``Retry(delay=-5)`` (berechnete Wartezeit wie
``reset_at - time.time()`` nach einem 429). Vor dem Fix ging
``expiration=-5000`` an RabbitMQ; der Broker lehnt das mit
``PRECONDITION_FAILED`` ab und schließt den Publish-Kanal. Parallele
Publishes desselben Brokers scheitern dann mit ``PublishIndeterminate``, und
der Rückfall ``redeliver_in_place`` zählt nicht gegen ``max_retries``: der
Task läuft bis zum Zustelllimit.

Festlegung E-060: die negative Wartezeit wird auf 0 geklemmt und zählt
normal. Geprüft wird der Weg über den Worker (``Retry.__init__`` klemmt,
KTD6), nicht ``broker.retry_later`` direkt. Nebenbei zeigt der Test den
Rundlauf mit ``expiration="0"`` über die Quorum-Delay-Stufe.

Test-Liste:
- [x] ``Retry(delay=-5)``, ``max_retries=2`` -> drei Läufe, danach DLQ;
      20 parallele Publishes des Worker-Brokers scheitern nicht
"""

from __future__ import annotations

import asyncio
import contextlib
import time
import uuid
from typing import override

import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import Retry
from swarmq.headers import HEADER_ENQUEUED_AT
from swarmq.serialization import encode_json
from swarmq.task import Task, TaskInfo
from tests.support.queues import delete_queue_family
from tests.support.wait import wait_until

pytestmark = [pytest.mark.integration, pytest.mark.timeout(30)]

_MAX_RETRIES = 2
_PARALLEL_PUBLISHES = 20
# Nach dem Dead-Letter: so lange auf weitere (falsche) Läufe achten.
_OBSERVE_S = 1.0


def _body(task_id: str, task_name: str, queue: str) -> bytes:
    return encode_json({"task": {"id": task_id, "name": task_name, "queue": queue}, "args": [], "kwargs": {}})


@pytest.mark.befund("F3-03")
@pytest.mark.entscheidung("E-060")
async def test_negative_retry_delay_counts_and_spares_parallel_publishes(rabbitmq_url: str, valkey_url: str) -> None:
    suffix = uuid.uuid4().hex[:10]
    queue = f"f303-{suffix}"
    side_queue = f"f303-side-{suffix}"
    task_name = f"f303_negative_{suffix}"
    runs = 0
    publish_batches: list[asyncio.Task[list[object]]] = []

    worker_broker = RabbitMQBroker(rabbitmq_url)

    async def publish_side_batch() -> list[object]:
        return await asyncio.gather(
            *(worker_broker.publish(side_queue, b"x") for _ in range(_PARALLEL_PUBLISHES)),
            return_exceptions=True,
        )

    class NegativeRetryTask(Task):
        task_info = TaskInfo(name=task_name, queues=[queue], max_retries=_MAX_RETRIES)

        @override
        async def run(self) -> None:
            nonlocal runs
            runs += 1
            if runs == 1:
                # Laufen, während der Retry-Publish auf demselben Broker geht.
                publish_batches.append(asyncio.create_task(publish_side_batch()))
            raise Retry(delay=-5.0)

    producer = RabbitMQBroker(rabbitmq_url)
    await producer.connect()
    worker_backend = ValkeyBackend(valkey_url)
    worker_app = SwarmQ(worker_broker, worker_backend, queues=[queue], concurrency=2)
    worker_task: asyncio.Task[None] | None = None
    try:
        await producer.declare_queue(queue)
        await producer.declare_queue(side_queue)
        worker_task = asyncio.create_task(worker_app.start_worker())

        await producer.publish(
            queue, _body(f"t-{suffix}", task_name, queue), headers={HEADER_ENQUEUED_AT: int(time.time())}
        )

        # Kein Broker-Ereignis meldet "im Dead-Letter"; die Tiefe des .XQ ist
        # die beobachtbare Größe.
        async def dead_lettered() -> bool:
            return await producer.dead_letter_depth(queue) == 1

        assert await wait_until(dead_lettered, timeout=15), f"nicht im Dead-Letter nach {runs} Läufen"
        await wait_until(lambda: runs > _MAX_RETRIES + 1, timeout=_OBSERVE_S)

        assert runs == _MAX_RETRIES + 1, f"{runs} Läufe statt {_MAX_RETRIES + 1}: der Retry zählte nicht"
        assert len(publish_batches) == 1
        results = await asyncio.wait_for(publish_batches[0], timeout=10)
        failures = [r for r in results if isinstance(r, BaseException)]
        assert failures == [], f"parallele Publishes gescheitert: {failures!r}"
    finally:
        if worker_task is not None:
            worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await worker_task
        for batch in publish_batches:
            if not batch.done():
                batch.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await batch
        await worker_broker.close()
        await worker_backend.close()
        await delete_queue_family(rabbitmq_url, queue)
        await delete_queue_family(rabbitmq_url, side_queue)
        await producer.close()
