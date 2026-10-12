"""U8: workflow primitives x existing features (retry, FAF) end-to-end.

Verifies workflows compose correctly with the retry pipeline and that the
FAF runtime guard fires when a FIRE_AND_FORGET task is smuggled into a
workflow via sig()-by-name (which bypasses the construction-time rejection
in U1). Chord exactly-once-under-redelivery is proven at the Lua /
ChordManager layer in tests/integration/test_chord_lua_scripts.py.
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from typing import override

import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import TaskError
from swarmq.task import DeliveryMode, Task, TaskInfo
from swarmq.workflow import chain, sig

pytestmark = [pytest.mark.e2e, pytest.mark.timeout(30)]


def _q() -> str:
    return f"wfcomb-{uuid.uuid4().hex[:12]}"


@contextlib.asynccontextmanager
async def _worker(rabbitmq_url, valkey_url, queue):
    wb = RabbitMQBroker(rabbitmq_url)
    wbk = ValkeyBackend(valkey_url)
    wapp = SwarmQ(wb, wbk, queues=[queue])
    task = asyncio.create_task(wapp.start_worker())
    try:
        yield
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        await wb.close()
        await wbk.close()


async def test_workflow_x_retry_then_success(rabbitmq_url, valkey_url):
    """A chain step that fails once then succeeds (max_retries=1) must retry
    and the chain must continue to completion."""
    queue = _q()

    class Flaky(Task):
        task_info = TaskInfo(
            name=f"flaky_{queue}",
            queues=[queue],
            max_retries=1,
            retry_base_delay=0.1,
        )
        _attempts = 0

        @override
        async def run(self, seed: str) -> str:
            type(self)._attempts += 1
            if type(self)._attempts == 1:
                raise ValueError("transient")
            return f"{seed}-ok"

    class Tail(Task):
        task_info = TaskInfo(name=f"tail_{queue}", queues=[queue])

        @override
        async def run(self, prev: str) -> str:
            return f"{prev}-tail"

    broker = RabbitMQBroker(rabbitmq_url)
    backend = ValkeyBackend(valkey_url)
    app = SwarmQ(broker, backend, queues=[queue])
    async with app:
        await broker.declare_queue(queue)
        async with _worker(rabbitmq_url, valkey_url, queue):
            handle = await app.apply(chain(Flaky.s("seed"), Tail.s()))
            result = await handle.get(timeout=25)
            assert result == "seed-ok-tail"


async def test_workflow_x_faf_runtime_guard(rabbitmq_url, valkey_url):
    """A FIRE_AND_FORGET task referenced by sig()-by-name (bypassing the
    construction-time rejection) must be caught by the worker runtime guard:
    the workflow is marked failed rather than silently stalling."""
    queue = _q()

    class FafStep(Task):
        task_info = TaskInfo(
            name=f"faf_{queue}",
            queues=[queue],
            delivery=DeliveryMode.FIRE_AND_FORGET,
        )

        @override
        async def run(self) -> str:
            return "ignored"

    class After(Task):
        task_info = TaskInfo(name=f"after_{queue}", queues=[queue])

        @override
        async def run(self, prev: str) -> str:
            return "after"

    broker = RabbitMQBroker(rabbitmq_url)
    backend = ValkeyBackend(valkey_url)
    app = SwarmQ(broker, backend, queues=[queue])
    async with app:
        await broker.declare_queue(queue)
        async with _worker(rabbitmq_url, valkey_url, queue):
            # sig() by name skips the construction-time FAF rejection (U1);
            # the worker's result_ttl==0 guard (U5) must fail the workflow.
            handle = await app.apply(chain(sig(f"faf_{queue}"), sig(f"after_{queue}")))
            with pytest.raises(TaskError):
                await handle.get(timeout=25)
