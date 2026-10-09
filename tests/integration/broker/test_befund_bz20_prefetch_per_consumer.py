"""Nachweis BZ-20: Prefetch gilt pro Consumer, nicht pro Worker.

Register: docs/befunde/betrieb-ueber-zeit-2026-10.md (BZ-20). Der Worker
meldet je konsumierter Queue einen Consumer mit ``prefetch=concurrency`` an,
führt aber höchstens ``concurrency`` Tasks gleichzeitig aus. Bei K Queues
hortet er bis zu K×N unbestätigte Nachrichten, die andere Worker nicht
bekommen und deren Wartezeit gegen den ``consumer_timeout`` läuft.

Erwartet (Nutzersicht): ein Worker mit ``concurrency=1`` hält höchstens eine
unbestätigte Nachricht; alles andere bleibt für andere Worker abholbereit.

Test-Liste:
- [x] Worker mit concurrency=1 und zwei Queues hält höchstens eine Nachricht (BZ-20)
"""

from __future__ import annotations

import asyncio
import contextlib
import time
import uuid

import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.headers import HEADER_ENQUEUED_AT
from swarmq.serialization import encode_json
from swarmq.task import Task, TaskInfo
from tests.support.queues import delete_queue_family
from tests.support.wait import wait_until

pytestmark = [pytest.mark.integration, pytest.mark.timeout(30)]

_MESSAGES_PER_QUEUE = 2
_CONCURRENCY = 1
# Zeit, die der Broker bekommt, um Prefetch-Kontingente auszuliefern.
_SETTLE_S = 3.0


async def _ready_total(broker: RabbitMQBroker, queues: list[str]) -> int:
    return sum([await broker.queue_depth(q) for q in queues])


@pytest.mark.befund("BZ-20")
@pytest.mark.xfail(
    strict=True,
    reason=(
        "BZ-20: der Worker setzt prefetch=concurrency je Queue-Consumer und hortet "
        "bei K Queues bis zu K*concurrency unbestätigte Nachrichten"
    ),
)
async def test_worker_holds_at_most_concurrency_unacked_messages(rabbitmq_url: str, valkey_url: str) -> None:
    suffix = uuid.uuid4().hex[:10]
    queues = [f"bz20-a-{suffix}", f"bz20-b-{suffix}"]
    task_name = f"bz20_block_{suffix}"
    started = asyncio.Event()
    release = asyncio.Event()

    class BlockingTask(Task):
        task_info = TaskInfo(name=task_name, queues=list(queues))

        async def run(self) -> str:
            started.set()
            await release.wait()
            return "ok"

    producer = RabbitMQBroker(rabbitmq_url)
    await producer.connect()
    worker_broker = RabbitMQBroker(rabbitmq_url)
    worker_backend = ValkeyBackend(valkey_url)
    worker_app = SwarmQ(worker_broker, worker_backend, queues=list(queues), concurrency=_CONCURRENCY)
    worker_task: asyncio.Task[None] | None = None
    try:
        for queue in queues:
            await producer.declare_queue(queue)
            for _ in range(_MESSAGES_PER_QUEUE):
                body = {
                    "task": {"id": f"bz20-{uuid.uuid4().hex[:8]}", "name": task_name, "queue": queue},
                    "args": [],
                    "kwargs": {},
                }
                await producer.publish(queue, encode_json(body), headers={HEADER_ENQUEUED_AT: int(time.time())})
        total = len(queues) * _MESSAGES_PER_QUEUE

        worker_task = asyncio.create_task(worker_app.start_worker())
        await asyncio.wait_for(started.wait(), timeout=10)

        # Der Broker meldet Prefetch-Auslieferungen nicht aktiv; die Tiefe
        # der Queues ist die einzige beobachtbare Größe, daher kurzes Pollen.
        ready = total

        async def settled() -> bool:
            nonlocal ready
            ready = await _ready_total(producer, queues)
            return ready <= total - len(queues)

        await wait_until(settled, timeout=_SETTLE_S)

        held = total - ready
        assert held <= _CONCURRENCY, (
            f"worker with concurrency={_CONCURRENCY} holds {held} unacked messages "
            f"across {len(queues)} queues (prefetch is per consumer)"
        )
    finally:
        release.set()
        if worker_task is not None:
            worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await worker_task
        await worker_broker.close()
        await worker_backend.close()
        await delete_queue_family(rabbitmq_url, *queues)
        await producer.close()
