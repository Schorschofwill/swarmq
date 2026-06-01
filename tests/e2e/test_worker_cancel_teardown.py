"""E2E: cancelling a running worker must not orphan its sub-tasks.

`Worker.start()` launches the embedded scheduler's election + poll loops as
detached `asyncio.create_task`s and blocks on `_shutdown_event.wait()`. The
graceful path (`_request_shutdown` → `_execute_shutdown`) stops them, but a
plain cancel of the `start_worker()` task (tests, ProcessManager reload,
embedded use) used to skip that teardown — leaking the scheduler loops (and the
broker connection tasks), which both produced "Future exception was never
retrieved" noise and left a zombie scheduler competing for leadership.

This pins the cancel path: after cancel, no SchedulerLoop sub-task stays
pending.
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.signals import Signal


def _pending_scheduler_tasks() -> list[str]:
    """Qualnames of still-pending SchedulerLoop sub-tasks on this loop."""
    out: list[str] = []
    for task in asyncio.all_tasks():
        if task.done():
            continue
        qual = getattr(task.get_coro(), "__qualname__", "")
        if "SchedulerLoop." in qual:
            out.append(qual)
    return out


async def test_cancel_does_not_orphan_scheduler_tasks(
    rabbitmq_url: str, valkey_url: str
) -> None:
    queue = f"e2e-cancel-teardown-{uuid.uuid4().hex[:10]}"
    app = SwarmQ(
        RabbitMQBroker(rabbitmq_url),
        ValkeyBackend(valkey_url),
        queues=[queue],
        concurrency=1,
    )
    online = asyncio.Event()

    @app.on_signal(Signal.WORKER_ONLINE)
    async def _on_online(**_: object) -> None:
        online.set()

    async with app:
        await app.broker.declare_queue(queue)
        worker_task = asyncio.create_task(app.start_worker())
        await asyncio.wait_for(online.wait(), timeout=20.0)

        # Crash-style stop: cancel the worker task (no graceful shutdown).
        worker_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker_task

        # Give a cancel-path teardown a moment to settle.
        await asyncio.sleep(0.3)

        leaked = _pending_scheduler_tasks()
        assert not leaked, f"orphaned scheduler sub-tasks after cancel: {leaked}"
