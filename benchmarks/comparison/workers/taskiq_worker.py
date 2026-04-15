"""TaskIQ worker task definitions for benchmark comparison.

Tasks are defined at module level using the ``@broker.task`` decorator.
The broker and result backend are configured here so that both the adapter
(``taskiq_sys.py``) and the TaskIQ receiver share the same instances.

Connection uses ``redis://localhost:6389/1`` — the Valkey instance on db=1,
isolated from other benchmark systems.
"""

from __future__ import annotations

import asyncio
import hashlib

from taskiq_redis import ListQueueBroker, RedisAsyncResultBackend

# ---------------------------------------------------------------------------
# Broker & result backend (shared with the adapter via import)
# ---------------------------------------------------------------------------

VALKEY_URL = "redis://localhost:6389/1"

result_backend = RedisAsyncResultBackend(redis_url=VALKEY_URL)

broker = ListQueueBroker(url=VALKEY_URL).with_result_backend(result_backend)


# ---------------------------------------------------------------------------
# Benchmark tasks
# ---------------------------------------------------------------------------


@broker.task
async def noop_task() -> bool:
    """Minimal no-op task. Returns immediately."""
    return True


@broker.task
async def io_task(sleep_ms: int = 10) -> bool:
    """Simulate I/O-bound work with an async sleep."""
    await asyncio.sleep(sleep_ms / 1000)
    return True


@broker.task
async def cpu_task(iterations: int = 10_000) -> bool:
    """Simulate CPU-bound work with repeated SHA-256 hashing."""
    data = b"benchmark"
    for _ in range(iterations):
        data = hashlib.sha256(data).digest()
    return True


@broker.task
async def failing_task(fail_count: int = 0) -> bool:
    """Raise ``RuntimeError`` when *fail_count* > 0.

    This allows scenarios to control whether the task succeeds or fails
    by passing a non-zero ``fail_count`` payload value.
    """
    if fail_count > 0:
        raise RuntimeError(f"Intentional failure (fail_count={fail_count})")
    return True
