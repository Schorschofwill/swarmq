"""Benchmark task definitions for SwarmQ.

Task classes are created dynamically via factory functions so that each
benchmark run can use a unique queue name, avoiding RabbitMQ collisions
between concurrent or successive runs.

The ``__init_subclass__`` hook on ``Task`` registers every subclass globally.
Creating task classes inside a factory keeps that side-effect intentional and
scoped to the adapter's lifecycle (the adapter saves/restores the registry).
"""

from __future__ import annotations

import asyncio
import hashlib

from swarmq.task import Task, TaskInfo

from stoerungen import hauptbuch


def create_task_classes(queue: str) -> dict[str, type[Task]]:
    """Return a mapping of *task_name -> Task subclass* bound to *queue*.

    Every call produces **new** classes (and therefore new registry entries).
    The caller is responsible for cleaning up ``Task._registered_tasks``
    afterwards — see the adapter's ``task_registry_isolation`` helper.
    """

    class NoopTask(Task):
        task_info = TaskInfo(name="noop", queues=[queue])

        async def run(self) -> bool:
            return True

    class IoTask(Task):
        task_info = TaskInfo(name="io_task", queues=[queue])

        async def run(self, sleep_ms: int = 10) -> bool:
            await asyncio.sleep(sleep_ms / 1000)
            return True

    class CpuTask(Task):
        task_info = TaskInfo(name="cpu_task", queues=[queue])

        async def run(self, iterations: int = 10000) -> bytes:
            data = b"benchmark"
            for _ in range(iterations):
                data = hashlib.sha256(data).digest()
            return data

    class FailingTask(Task):
        task_info = TaskInfo(
            name="failing_task",
            queues=[queue],
            max_retries=3,
            retry_backoff="exponential",
            retry_base_delay=0.05,
        )

        async def run(self, fail_count: int = 0) -> bool:
            raise RuntimeError("Deliberate benchmark failure")

    class GarantieTask(Task):
        task_info = TaskInfo(name="garantie", queues=[queue])

        async def run(self, kennung: str, dauer_ms: int = 1000) -> None:
            await hauptbuch.garantie_async(kennung, dauer_ms)

    return {
        "noop": NoopTask,
        "io_task": IoTask,
        "cpu_task": CpuTask,
        "failing_task": FailingTask,
        "garantie": GarantieTask,
    }
