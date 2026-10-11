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
from typing import override

from swarmq.task import Task, TaskInfo

_QUEUE = os.environ.get("CLI_TEST_QUEUE", "chaos_test_default")


async def _bump_exec_count(task: Task) -> None:
    """Best-effort per-task execution counter in Valkey."""
    if task._backend is not None and task._task_id is not None:
        try:
            await task._backend._client.incr(f"exec_count:{task._task_id}")
            await task._backend._client.expire(f"exec_count:{task._task_id}", 300)
        except Exception:
            pass


class ChaosSleepTask(Task):
    """Sleep ``seconds`` then return a marker. Records every execution
    in Valkey so tests can assert at-least-once delivery post-SIGKILL."""

    task_info = TaskInfo(
        name="chaos_sleep",
        queues=[_QUEUE],
        max_retries=0,
    )

    @override
    async def run(self, seconds: float) -> str:
        await _bump_exec_count(self)
        await asyncio.sleep(seconds)
        return f"slept:{seconds}"


class ChaosMemoryHogTask(Task):
    """Allocate ~``mb`` megabytes and then sleep — stands in for an
    OOM-bound workload. The chaos test kills the worker externally via
    a SIGKILL on the worker child once it has swelled (simulating
    an OOM-killer from the kernel/container runtime). No container
    memory-limit is required."""

    task_info = TaskInfo(
        name="chaos_memory_hog",
        queues=[_QUEUE],
        max_retries=0,
    )

    @override
    async def run(self, mb: int, seconds: float) -> str:
        await _bump_exec_count(self)
        # Hold the allocation in scope so it shows up in RSS.
        _ballast = bytearray(mb * 1024 * 1024)
        await asyncio.sleep(seconds)
        return f"alloc:{mb}"


class ChordHeaderSleepTask(Task):
    """Chord header member: sleep ``seconds`` then echo ``n``. The sleep
    opens a window to SIGKILL the worker mid-fan-in so the member message is
    redelivered to a successor — the exactly-once chord-body scenario."""

    task_info = TaskInfo(
        name="chord_header_sleep",
        queues=[_QUEUE],
        max_retries=0,
    )

    @override
    async def run(self, n: int, seconds: float = 0.0, run_key: str = "") -> int:
        if run_key and self._backend is not None:
            # Starts per run: a crash mid-fan-in shows up as re-runs.
            try:
                await self._backend._client.incr(f"chord_header_runs:{run_key}")
                await self._backend._client.expire(f"chord_header_runs:{run_key}", 300)
            except Exception:
                pass
        if seconds:
            await asyncio.sleep(seconds)
        return n


class ChordBodyCountTask(Task):
    """Chord body: increment a per-run Valkey counter and return the sum of
    the header results. The counter is the exactly-once probe — across any
    number of header-member redeliveries the body must be ENQUEUED (and thus
    run, absent a body-worker crash) exactly once, so the counter must be 1."""

    task_info = TaskInfo(
        name="chord_body_count",
        queues=[_QUEUE],
        max_retries=0,
    )

    @override
    async def run(self, results: list, run_key: str = "") -> int:
        if self._backend is not None:
            try:
                await self._backend._client.incr(f"chord_body:{run_key}")
                await self._backend._client.expire(f"chord_body:{run_key}", 300)
            except Exception:
                pass
        return sum(results)


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

    @override
    async def run(self) -> None:
        await _bump_exec_count(self)
        # NULL-deref → SIGSEGV. Python never returns from this call.
        ctypes.string_at(0)
