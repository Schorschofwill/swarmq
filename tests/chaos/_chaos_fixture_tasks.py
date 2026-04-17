"""Fixture module for F1 Worker-Crash chaos tests (U2.1).

The CLI subprocess worker imports this via
``--module tests.chaos._chaos_fixture_tasks``. The queue name comes from
``CLI_TEST_QUEUE`` — same contract as ``tests/e2e/_shutdown_fixture_tasks.py``
and ``tests/integration/_cli_fixture_tasks.py``.

Tasks:
  * ``chaos_sleep`` — sleeps ``seconds`` then returns a marker string.
    Increments ``exec_count:{task_id}`` on each execution; a successor
    worker after SIGKILL can raise that counter above 1.
  * ``chaos_memory_hog`` — allocates roughly ``mb`` megabytes and sleeps
    briefly. Used as a target for an external SIGKILL that simulates an
    OOM-kill (without depending on docker --memory=, which isn't
    portable through the toxiproxy-wrapped chaos stack).
  * ``chaos_segv`` — triggers SIGSEGV via ``ctypes.string_at(0)``.
    ``max_retries=2`` so the task eventually lands in the DLQ.
"""

from __future__ import annotations

import asyncio
import ctypes
import os

from swarmq.task import Task, TaskInfo

_QUEUE = os.environ.get("CLI_TEST_QUEUE", "chaos_test_default")


async def _bump_exec_count(task: Task) -> None:
    """Best-effort per-task execution counter in Valkey."""
    if task._backend is not None and task._task_id is not None:  # noqa: SLF001
        try:
            await task._backend._client.incr(  # noqa: SLF001
                f"exec_count:{task._task_id}"
            )
            await task._backend._client.expire(  # noqa: SLF001
                f"exec_count:{task._task_id}", 300
            )
        except Exception:  # noqa: BLE001
            pass


class ChaosSleepTask(Task):
    """Sleep ``seconds`` then return a marker. Records every execution
    in Valkey so tests can assert at-least-once delivery post-SIGKILL."""

    task_info = TaskInfo(
        name="chaos_sleep",
        queues=[_QUEUE],
        max_retries=0,
    )

    async def run(self, seconds: float) -> str:
        await _bump_exec_count(self)
        await asyncio.sleep(seconds)
        return f"slept:{seconds}"


class ChaosMemoryHogTask(Task):
    """Allocate ~``mb`` megabytes and then sleep — stands in for an
    OOM-bound workload. The chaos test kills the worker externally via
    ``os.kill(pid, SIGKILL)`` once the process has swelled (simulating
    an OOM-killer from the kernel/container runtime). No container
    memory-limit is required."""

    task_info = TaskInfo(
        name="chaos_memory_hog",
        queues=[_QUEUE],
        max_retries=0,
    )

    async def run(self, mb: int, seconds: float) -> str:
        await _bump_exec_count(self)
        # Hold the allocation in scope so it shows up in RSS.
        _ballast = bytearray(mb * 1024 * 1024)  # noqa: F841
        await asyncio.sleep(seconds)
        return f"alloc:{mb}"


class ChaosSegvTask(Task):
    """Dereference a NULL pointer via ctypes to trigger SIGSEGV. The
    worker process dies hard; the broker redelivers the message. After
    ``max_retries=2`` redeliveries the message lands in ``{queue}.XQ``.
    """

    task_info = TaskInfo(
        name="chaos_segv",
        queues=[_QUEUE],
        max_retries=2,
        retry_backoff="fixed",
        retry_base_delay=0.5,
    )

    async def run(self) -> None:
        await _bump_exec_count(self)
        # NULL-deref → SIGSEGV. Python never returns from this call.
        ctypes.string_at(0)
