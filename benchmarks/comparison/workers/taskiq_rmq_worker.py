"""taskiq-Broker und Tasks des Vergleichs, für ``taskiq worker workers.taskiq_rmq_worker:broker`` und den Erzeuger.

Broker RabbitMQ (taskiq-aio-pika), Ergebnisse in Valkey (taskiq-redis). Die
Queue ist die frische Queue des Laufs; ihr Typ kommt im Profil ``stark``
aus ``garantieprofile.py``, ab Werk bleibt er beim Werkswert von
taskiq-aio-pika. ``ack_type`` setzt der Adapter als ``--ack-type`` der CLI.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
from typing import Any

from taskiq_aio_pika import AioPikaBroker, Queue, QueueType
from taskiq_redis import RedisAsyncResultBackend

from stoerungen import hauptbuch

EINSTELLUNGEN = json.loads(os.environ["VERGLEICH_EINSTELLUNGEN"])

_queue: dict[str, Any] = {"name": os.environ["VERGLEICH_QUEUE"]}
if "queue_type" in EINSTELLUNGEN:
    _queue["type"] = QueueType(EINSTELLUNGEN["queue_type"])

result_backend = RedisAsyncResultBackend(redis_url=os.environ["VERGLEICH_BACKEND_URL"])

broker = AioPikaBroker(
    url=os.environ["VERGLEICH_BROKER_URL"],
    task_queues=[Queue(**_queue)],
).with_result_backend(result_backend)


@broker.task(task_name="noop")
async def noop_task() -> bool:
    return True


@broker.task(task_name="io_task")
async def io_task(sleep_ms: int = 10) -> bool:
    await asyncio.sleep(sleep_ms / 1000)
    return True


@broker.task(task_name="cpu_task")
async def cpu_task(iterations: int = 10_000) -> bool:
    data = b"benchmark"
    for _ in range(iterations):
        data = hashlib.sha256(data).digest()
    return True


@broker.task(task_name="failing_task")
async def failing_task(fail_count: int = 0) -> bool:
    if fail_count > 0:
        raise RuntimeError(f"Intentional failure (fail_count={fail_count})")
    return True


@broker.task(task_name="garantie")
async def garantie(kennung: str, dauer_ms: int = 1000) -> None:
    # Gemeinsame Task des Garantie-Vergleichs (stoerungen/hauptbuch.py).
    await hauptbuch.garantie_async(kennung, dauer_ms)


TASKS = {
    "noop": noop_task,
    "io_task": io_task,
    "cpu_task": cpu_task,
    "failing_task": failing_task,
    "garantie": garantie,
}
