"""arq-Funktionen und ``WorkerSettings`` des Vergleichs, für ``arq workers.arq_worker.WorkerSettings``.

arq kennt nur Redis-kompatible Server: Queue und Ergebnisse liegen in
Valkey. Beide Profile laufen mit der Werkseinstellung (``garantieprofile.py``).
"""

from __future__ import annotations

import asyncio
import hashlib
import os
from typing import Any

from arq.connections import RedisSettings

QUEUE = os.environ["VERGLEICH_QUEUE"]
REDIS_SETTINGS = RedisSettings.from_dsn(os.environ["VERGLEICH_BACKEND_URL"])


async def noop(ctx: dict[str, Any]) -> bool:
    return True


async def io_task(ctx: dict[str, Any], sleep_ms: int = 10) -> bool:
    await asyncio.sleep(sleep_ms / 1000)
    return True


async def cpu_task(ctx: dict[str, Any], iterations: int = 10_000) -> bool:
    data = b"benchmark"
    for _ in range(iterations):
        data = hashlib.sha256(data).digest()
    return True


async def failing_task(ctx: dict[str, Any], fail_count: int = 0) -> bool:
    if fail_count > 0:
        raise RuntimeError(f"Intentional failure (fail_count={fail_count})")
    return True


class WorkerSettings:
    functions = [noop, io_task, cpu_task, failing_task]
    redis_settings = REDIS_SETTINGS
    queue_name = QUEUE
