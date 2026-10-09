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
- [x] riesiger Fehlertext reißt die Nachbarn nicht mit (BZ-17)
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

pytestmark = [pytest.mark.integration, pytest.mark.timeout(30)]

_HUGE_ERROR = "x" * (1024 * 1024)
# So lange beobachten wir die Queue nach dem Fehlschlag auf Rückläufer.
_OBSERVE_S = 3.0


def _body(task_id: str, task_name: str, queue: str) -> bytes:
    return encode_json({"task": {"id": task_id, "name": task_name, "queue": queue}, "args": [], "kwargs": {}})


async def _delete_queues(url: str, names: list[str]) -> None:
    conn = await aio_pika.connect(url)
    try:
        for name in names:
            ch = await conn.channel()
            try:
                await ch.queue_delete(name)
            except Exception:
                pass
            finally:
                if not ch.is_closed:
                    await ch.close()
    finally:
        await conn.close()


@pytest.mark.befund("BZ-17")
@pytest.mark.xfail(
    strict=True,
    reason=(
        "BZ-17: x-swarmq-traceback ist unbegrenzt; ein 1-MiB-Fehlertext sprengt "
        "frame_max, der Broker schließt die Verbindung und stellt Nachbar-Tasks neu zu"
    ),
)
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
        loop = asyncio.get_running_loop()
        deadline = loop.time() + _OBSERVE_S
        requeued = await producer.queue_depth(queue)
        while requeued == 0 and loop.time() < deadline:
            await asyncio.sleep(0.05)
            requeued = await producer.queue_depth(queue)

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
        await _delete_queues(rabbitmq_url, [queue, f"{queue}.DQ", f"{queue}.XQ"])
        await producer.close()
