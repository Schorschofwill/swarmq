"""Nachweis BZ-17: unbegrenzter Header ``x-swarmq-traceback``.

Register: docs/befunde/betrieb-ueber-zeit-2026-10.md (BZ-17). Der Retry- und
DLQ-Pfad schreibt ``str(exc)`` ungekürzt in ``x-swarmq-traceback``. Der
AMQP-Content-Header muss in einen Frame passen (``frame_max`` 128 KiB);
sonst schließt der Broker die Verbindung, und jede unbestätigte Nachricht
des Workers wird neu zugestellt.

Erwartet (Nutzersicht): ein Task, der mit 1 MiB Fehlertext scheitert, wird
normal zurückgestellt; ein gleichzeitig laufender Nachbar-Task auf demselben
Worker bleibt unberührt (seine Nachricht geht nicht an die Queue zurück).

Test-Liste:
- [x] riesiger Fehlertext reißt die Nachbarn nicht mit (BZ-17); gemessen
      wird die Tiefe der Haupt-Queue per passivem Declare (Plan 0.12.0, MR 2)
"""

from __future__ import annotations

import asyncio
import contextlib
import time
import uuid

import aio_pika
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

_HUGE_ERROR = "x" * (1024 * 1024)
# So lange beobachten wir die Queue nach dem Fehlschlag auf Rückläufer.
_OBSERVE_S = 3.0


async def _main_queue_depth(rabbitmq_url: str, queue: str) -> int:
    """Bereite Nachrichten nur der Haupt-Queue, per passivem Declare."""
    connection = await aio_pika.connect(rabbitmq_url)
    try:
        channel = await connection.channel()
        declared = await channel.declare_queue(queue, passive=True)
        return int(declared.declaration_result.message_count or 0)
    finally:
        await connection.close()


def _body(task_id: str, task_name: str, queue: str) -> bytes:
    return encode_json({"task": {"id": task_id, "name": task_name, "queue": queue}, "args": [], "kwargs": {}})


@pytest.mark.befund("BZ-17")
async def test_huge_error_text_does_not_requeue_neighbour_tasks(rabbitmq_url: str, valkey_url: str) -> None:
    suffix = uuid.uuid4().hex[:10]
    queue = f"bz17-{suffix}"
    huge_name = f"bz17_huge_{suffix}"
    neighbour_name = f"bz17_neighbour_{suffix}"
    neighbour_started = asyncio.Event()
    neighbour_release = asyncio.Event()
    huge_attempted = asyncio.Event()

    class HugeErrorTask(Task):
        # Langer fester Backoff: die Retry-Kopie bleibt während der
        # Beobachtung in {queue}.DQ.
        task_info = TaskInfo(
            name=huge_name, queues=[queue], max_retries=1, retry_backoff="fixed", retry_base_delay=60.0
        )

        async def run(self) -> None:
            huge_attempted.set()
            raise RuntimeError(_HUGE_ERROR)

    class NeighbourTask(Task):
        task_info = TaskInfo(name=neighbour_name, queues=[queue])

        async def run(self) -> str:
            neighbour_started.set()
            await neighbour_release.wait()
            return "ok"

    producer = RabbitMQBroker(rabbitmq_url)
    await producer.connect()
    worker_broker = RabbitMQBroker(rabbitmq_url)
    worker_backend = ValkeyBackend(valkey_url)
    worker_app = SwarmQ(worker_broker, worker_backend, queues=[queue], concurrency=2)
    worker_task: asyncio.Task[None] | None = None
    try:
        await producer.declare_queue(queue)
        worker_task = asyncio.create_task(worker_app.start_worker())

        headers = {HEADER_ENQUEUED_AT: int(time.time())}
        await producer.publish(queue, _body(f"n-{suffix}", neighbour_name, queue), headers=headers)
        await asyncio.wait_for(neighbour_started.wait(), timeout=10)
        await producer.publish(queue, _body(f"h-{suffix}", huge_name, queue), headers=headers)
        await asyncio.wait_for(huge_attempted.wait(), timeout=10)

        # Kein Broker-Ereignis meldet "Nachricht zurück in der Queue"; die
        # Tiefe der Haupt-Queue ist die beobachtbare Größe, daher Pollen.
        # Nur die Haupt-Queue (passives Declare): ``queue_depth`` zählt
        # ``{queue}.DQ`` mit, dort liegt die gewollte Retry-Kopie.
        requeued = 0

        async def any_requeued() -> bool:
            nonlocal requeued
            requeued = await _main_queue_depth(rabbitmq_url, queue)
            return requeued != 0

        await wait_until(any_requeued, timeout=_OBSERVE_S)

        assert requeued == 0, (
            f"{requeued} message(s) went back to {queue!r} after a task failed with a "
            f"{len(_HUGE_ERROR)}-byte error text — the worker connection was closed "
            "and the neighbour's in-flight delivery was requeued"
        )
    finally:
        neighbour_release.set()
        if worker_task is not None:
            worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await worker_task
        await worker_broker.close()
        await worker_backend.close()
        await delete_queue_family(rabbitmq_url, queue)
        await producer.close()
