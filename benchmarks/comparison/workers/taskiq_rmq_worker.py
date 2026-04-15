"""TaskIQ worker with RabbitMQ broker for fair comparison against SwarmQ.

Same tasks as taskiq_worker.py, but uses AioPikaBroker instead of
ListQueueBroker. Result backend remains Redis/Valkey (db=1).
"""

from __future__ import annotations

import asyncio
import hashlib

from taskiq_aio_pika import AioPikaBroker
from taskiq_redis import RedisAsyncResultBackend

RABBITMQ_URL = "amqp://guest:guest@localhost:5682/"
VALKEY_URL = "redis://localhost:6389/1"

result_backend = RedisAsyncResultBackend(redis_url=VALKEY_URL)

broker = AioPikaBroker(url=RABBITMQ_URL).with_result_backend(result_backend)


@broker.task
async def noop_task() -> bool:
    return True


@broker.task
async def io_task(sleep_ms: int = 10) -> bool:
    await asyncio.sleep(sleep_ms / 1000)
    return True


@broker.task
async def cpu_task(iterations: int = 10_000) -> bool:
    data = b"benchmark"
    for _ in range(iterations):
        data = hashlib.sha256(data).digest()
    return True


@broker.task
async def failing_task(fail_count: int = 0) -> bool:
    if fail_count > 0:
        raise RuntimeError(f"Intentional failure (fail_count={fail_count})")
    return True
