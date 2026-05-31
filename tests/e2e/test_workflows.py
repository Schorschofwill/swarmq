"""U5/U8: end-to-end workflow roundtrips driven by a real worker.

Verifies the full path: apply() enqueues the frontier, the worker runs each
step and advances the workflow (chain result-injection, chord fan-in), and
handle.get() returns the final result. Against real RabbitMQ + Valkey.
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid

import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import TaskCancelledError, TaskError
from swarmq.task import Task, TaskInfo
from swarmq.workflow import chain, chord, group

pytestmark = [pytest.mark.e2e, pytest.mark.timeout(30)]


def _q() -> str:
    return f"wfe2e-{uuid.uuid4().hex[:12]}"


@contextlib.asynccontextmanager
async def _worker(rabbitmq_url, valkey_url, queue):
    """Run a worker consuming *queue* for the duration of the context."""
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


async def test_chain_roundtrip(rabbitmq_url, valkey_url):
    queue = _q()

    class Step1(Task):
        task_info = TaskInfo(name=f"s1_{queue}", queues=[queue])

        async def run(self, seed: str) -> str:
            return f"{seed}-1"

    class Step2(Task):
        task_info = TaskInfo(name=f"s2_{queue}", queues=[queue])

        async def run(self, prev: str) -> str:
            return f"{prev}-2"

    class Step3(Task):
        task_info = TaskInfo(name=f"s3_{queue}", queues=[queue])

        async def run(self, prev: str) -> str:
            return f"{prev}-3"

    broker = RabbitMQBroker(rabbitmq_url)
    backend = ValkeyBackend(valkey_url)
    app = SwarmQ(broker, backend, queues=[queue])
    async with app:
        await broker.declare_queue(queue)
        async with _worker(rabbitmq_url, valkey_url, queue):
            handle = await app.apply(
                chain(Step1.s("seed"), Step2.s(), Step3.s())
            )
            result = await handle.get(timeout=20)
            assert result == "seed-1-2-3"


async def test_chord_roundtrip(rabbitmq_url, valkey_url):
    queue = _q()

    class Dl(Task):
        task_info = TaskInfo(name=f"dl_{queue}", queues=[queue])

        async def run(self, n: int) -> int:
            return n * 10

    class Merge(Task):
        task_info = TaskInfo(name=f"merge_{queue}", queues=[queue])

        async def run(self, results: list) -> int:
            return sum(results)

    broker = RabbitMQBroker(rabbitmq_url)
    backend = ValkeyBackend(valkey_url)
    app = SwarmQ(broker, backend, queues=[queue])
    async with app:
        await broker.declare_queue(queue)
        async with _worker(rabbitmq_url, valkey_url, queue):
            handle = await app.apply(
                chord(group(Dl.s(1), Dl.s(2), Dl.s(3)), Merge.s())
            )
            result = await handle.get(timeout=20)
            # Merge receives [10, 20, 30] in definition order → 60.
            assert result == 60


async def test_workflow_cancel_stops_chain(rabbitmq_url, valkey_url):
    queue = _q()
    started = asyncio.Event()

    class Slow1(Task):
        task_info = TaskInfo(name=f"slow1_{queue}", queues=[queue])

        async def run(self) -> str:
            started.set()
            return "1"

    class Slow2(Task):
        task_info = TaskInfo(name=f"slow2_{queue}", queues=[queue])

        async def run(self, prev: str) -> str:
            return "2"

    broker = RabbitMQBroker(rabbitmq_url)
    backend = ValkeyBackend(valkey_url)
    app = SwarmQ(broker, backend, queues=[queue])
    async with app:
        await broker.declare_queue(queue)
        async with _worker(rabbitmq_url, valkey_url, queue):
            handle = await app.apply(chain(Slow1.s(), Slow2.s()))
            await handle.cancel()
            # The workflow ends cancelled; get() raises rather than returning.
            with pytest.raises(TaskCancelledError):
                await handle.get(timeout=20)


async def test_chain_step_failure_fails_workflow_with_liveness(rabbitmq_url, valkey_url):
    queue = _q()
    step2_ran = asyncio.Event()

    class Boom(Task):
        task_info = TaskInfo(name=f"boom_{queue}", queues=[queue], max_retries=0)

        async def run(self) -> str:
            raise ValueError("boom")

    class Never(Task):
        task_info = TaskInfo(name=f"never_{queue}", queues=[queue])

        async def run(self, prev: str) -> str:
            step2_ran.set()
            return "never"

    broker = RabbitMQBroker(rabbitmq_url)
    backend = ValkeyBackend(valkey_url)
    app = SwarmQ(broker, backend, queues=[queue])
    async with app:
        await broker.declare_queue(queue)
        async with _worker(rabbitmq_url, valkey_url, queue):
            handle = await app.apply(chain(Boom.s(), Never.s()))
            # Fail-fast + liveness: get() raises within bounded time (the
            # worker marks the workflow failed at the DLQ seam and wakes us) —
            # it does NOT hang to the timeout.
            with pytest.raises(TaskError):
                await handle.get(timeout=20)
            assert not step2_ran.is_set()  # successor never enqueued


async def test_chord_header_failure_does_not_fire_body(rabbitmq_url, valkey_url):
    # AE5 / R16: a chord header member that fails terminally must fail the
    # whole workflow with bounded liveness, and the body must NEVER run.
    queue = _q()
    body_ran = asyncio.Event()

    class Ok(Task):
        task_info = TaskInfo(name=f"ok_{queue}", queues=[queue])

        async def run(self, n: int) -> int:
            return n

    class HeaderBoom(Task):
        task_info = TaskInfo(name=f"hboom_{queue}", queues=[queue], max_retries=0)

        async def run(self, n: int) -> int:
            raise ValueError("header member failed")

    class Body(Task):
        task_info = TaskInfo(name=f"body_{queue}", queues=[queue])

        async def run(self, results: list) -> int:
            body_ran.set()
            return sum(results)

    broker = RabbitMQBroker(rabbitmq_url)
    backend = ValkeyBackend(valkey_url)
    app = SwarmQ(broker, backend, queues=[queue])
    async with app:
        await broker.declare_queue(queue)
        async with _worker(rabbitmq_url, valkey_url, queue):
            handle = await app.apply(
                chord(group(Ok.s(1), HeaderBoom.s(2), Ok.s(3)), Body.s())
            )
            with pytest.raises(TaskError):
                await handle.get(timeout=20)
            # Once get() raises, the workflow is terminal. The failed header
            # member never enters the chord completion set, so the set can
            # never reach N=3 and the body is never enqueued — assertable
            # immediately, no wait needed.
            assert not body_ran.is_set()
