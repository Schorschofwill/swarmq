"""Abschaltbares Dead-Lettering Ende zu Ende (Plan 2026-10-08-1541, U9, R15/KTD7).

Test-Liste:
1. ``dead_letter_terminal_failures=False``: ein terminal fehlschlagender Task
   liefert dem Aufrufer sein Fehlerergebnis, die Nachricht ist bestaetigt,
   und die XQ bleibt leer.
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from collections.abc import AsyncIterator
from typing import Any, override

import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import TaskError
from swarmq.signals import Signal
from swarmq.task import Task, TaskInfo

pytestmark = [pytest.mark.e2e, pytest.mark.timeout(30)]


@contextlib.asynccontextmanager
async def _worker(rabbitmq_url: str, valkey_url: str, queue: str, **config: Any) -> AsyncIterator[None]:
    app = SwarmQ(RabbitMQBroker(rabbitmq_url), ValkeyBackend(valkey_url), queues=[queue], concurrency=1, **config)
    online = asyncio.Event()

    @app.on_signal(Signal.WORKER_ONLINE)
    async def _online(**_: Any) -> None:
        online.set()

    task = asyncio.create_task(app.start_worker())
    try:
        await asyncio.wait_for(online.wait(), timeout=8.0)
        yield
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        await app.broker.close()
        await app.backend.close()


async def test_disabled_dead_lettering_returns_the_error_and_leaves_the_dlq_empty(
    rabbitmq_url: str, valkey_url: str
) -> None:
    queue = f"e2e-no-dlq-{uuid.uuid4().hex[:10]}"
    suffix = uuid.uuid4().hex[:8]

    class FailingTask(Task):
        task_info = TaskInfo(name=f"no_dlq_fail_{suffix}", queues=[queue], max_retries=0)

        @override
        async def run(self) -> None:
            raise RuntimeError("boom without dlq")

    class SentinelTask(Task):
        task_info = TaskInfo(name=f"no_dlq_sentinel_{suffix}", queues=[queue])

        @override
        async def run(self) -> str:
            return "done"

    broker = RabbitMQBroker(rabbitmq_url)
    app = SwarmQ(broker, ValkeyBackend(valkey_url), queues=[queue])
    async with app:
        await broker.declare_queue(queue)
        async with _worker(rabbitmq_url, valkey_url, queue, dead_letter_terminal_failures=False):
            failed_id = await app.schedule(FailingTask.task_info.name)
            sentinel_id = await app.schedule(SentinelTask.task_info.name)

            with pytest.raises(TaskError, match="boom without dlq"):
                await app.get_result(failed_id, timeout=10)
            # concurrency=1: liegt das Ergebnis des Folgetasks vor, ist die
            # fehlgeschlagene Zustellung samt Broker-Verb abgeschlossen.
            assert await app.get_result(sentinel_id, timeout=10) == "done"

        assert await broker.dead_letter_depth(queue) == 0
        assert await broker.queue_depth(queue) == 0
