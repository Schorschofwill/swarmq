"""E2E: ``Retry`` trotz ``retry_exceptions`` (Plan 2026-10-12-1105, MR 2, P8).

Test-Liste:

1. Ein Task mit ``retry_exceptions=[KeyError]`` wirft beim ersten Versuch
   ``Retry(delay=0.1)`` und gelingt beim zweiten -> Ergebnis ``"ok"``,
   ``TASK_RETRYING`` genau einmal. Bisher landete ``Retry`` terminal in der
   DLQ, weil der Filter vor der Sonderbehandlung von ``Retry`` griff.
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from typing import Any, override

import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import Retry
from swarmq.signals import Signal
from swarmq.task import Task, TaskInfo

# Lebendigkeits-Netz fuer das Warten auf Signale, keine Verhaltensfrist.
_EVENT_TIMEOUT = 20.0

pytestmark = [pytest.mark.timeout(60)]


@pytest.fixture
def _cleanup_tasks():
    yield
    Task._clear_registered_tasks()


@pytest.mark.usefixtures("_cleanup_tasks")
async def test_retry_with_retry_exceptions_filter_runs_again(rabbitmq_url: str, valkey_url: str) -> None:
    queue = f"e2e-retry-filter-{uuid.uuid4().hex[:10]}"
    task_name = f"e2e_retry_filter_{uuid.uuid4().hex[:8]}"
    attempts: list[int] = []
    retrying: list[dict[str, Any]] = []

    class RetryDespiteFilter(Task):
        task_info = TaskInfo(name=task_name, queues=[queue], max_retries=2, retry_exceptions=[KeyError])

        @override
        async def run(self) -> str:
            attempts.append(1)
            if len(attempts) == 1:
                raise Retry(delay=0.1)
            return "ok"

    worker_broker = RabbitMQBroker(rabbitmq_url)
    worker_backend = ValkeyBackend(valkey_url)
    worker_app = SwarmQ(worker_broker, worker_backend, queues=[queue], concurrency=1)
    online = asyncio.Event()

    @worker_app.on_signal(Signal.WORKER_ONLINE)
    async def _on_online(**_: Any) -> None:
        online.set()

    @worker_app.on_signal(Signal.TASK_RETRYING)
    async def _on_retrying(**kwargs: Any) -> None:
        retrying.append(kwargs)

    client_app = SwarmQ(RabbitMQBroker(rabbitmq_url), ValkeyBackend(valkey_url), queues=[queue])
    async with client_app:
        worker_task = asyncio.create_task(worker_app.start_worker())
        try:
            await asyncio.wait_for(online.wait(), timeout=_EVENT_TIMEOUT)
            task_id = await client_app.schedule(task_name)

            result = await client_app.get_result(task_id, timeout=_EVENT_TIMEOUT)
        finally:
            worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await worker_task
            await worker_broker.close()
            await worker_backend.close()

    assert result == "ok"
    assert len(attempts) == 2
    assert len(retrying) == 1
    assert retrying[0]["task_id"] == task_id
    assert retrying[0]["delay"] == pytest.approx(0.1)
