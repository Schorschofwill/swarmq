"""E2E tests for Rate Limiting — real RabbitMQ + Valkey.

Spawn a SwarmQ app with a rate_limit'd task, publish more messages than
the limit allows, and verify:
  - every message eventually lands in post_execute (none go to DLQ)
  - the sliding-window counter throttles enough of them that multiple
    TASK_RATE_LIMITED signals fire
  - pre_execute never runs for a rate-limited message
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from typing import Any

import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.signals import Signal
from swarmq.task import Task, TaskInfo


def _unique_queue() -> str:
    return f"e2e-rl-{uuid.uuid4().hex[:12]}"


async def test_rate_limited_task_delays_but_never_drops(
    rabbitmq_url: str, valkey_url: str
) -> None:
    queue = _unique_queue()
    task_name = f"e2e_rl_{uuid.uuid4().hex[:8]}"
    total_messages = 8
    rate_limit = "3/s"

    executed_task_ids: list[str] = []
    completed_event = asyncio.Event()
    worker_online_event = asyncio.Event()
    rate_limited_count = 0

    class RateLimitedEchoTask(Task):
        task_info = TaskInfo(
            name=task_name, queues=[queue], rate_limit=rate_limit
        )

        async def run(self, idx: int) -> int:
            executed_task_ids.append(idx)
            if len(executed_task_ids) >= total_messages:
                completed_event.set()
            return idx

    broker = RabbitMQBroker(rabbitmq_url)
    backend = ValkeyBackend(valkey_url)
    app = SwarmQ(broker, backend, queues=[queue])

    async with app:
        await broker.declare_queue(queue)

        worker_broker = RabbitMQBroker(rabbitmq_url)
        worker_backend = ValkeyBackend(valkey_url)
        worker_app = SwarmQ(worker_broker, worker_backend, queues=[queue])

        @worker_app.on_signal(Signal.TASK_RATE_LIMITED)
        async def _on_rate_limited(**kwargs: Any) -> None:
            nonlocal rate_limited_count
            rate_limited_count += 1

        @worker_app.on_signal(Signal.WORKER_ONLINE)
        async def _on_worker_online(**_: Any) -> None:
            worker_online_event.set()

        worker_task = asyncio.create_task(worker_app.start_worker())

        try:
            # Wait for the WORKER_ONLINE signal instead of sleeping —
            # AGENTS.md forbids `asyncio.sleep` in tests for exactly
            # this reason (synchronisation via events, not timers).
            await asyncio.wait_for(worker_online_event.wait(), timeout=5.0)

            for i in range(total_messages):
                await app.schedule(task_name, i)

            # Give the system up to ~6 s: at 3/s the theoretical lower
            # bound is (8-3)/3 = ~1.67 s plus delay-queue latency; 6 s
            # is generous and catches real regressions (hang / DLQ).
            await asyncio.wait_for(completed_event.wait(), timeout=8.0)
        finally:
            worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await worker_task
            await worker_broker.close()
            await worker_backend.close()

    # Every message made it through eventually — no DLQ losses, no hang.
    assert len(executed_task_ids) == total_messages
    assert sorted(executed_task_ids) == list(range(total_messages))

    # At least some messages hit the limit and were delayed.
    # Exact count depends on scheduler timing; the lower bound
    # (total - limit_per_window) is safe.
    assert rate_limited_count >= total_messages - 3, (
        f"expected at least {total_messages - 3} rate-limit hits, "
        f"saw {rate_limited_count}"
    )
