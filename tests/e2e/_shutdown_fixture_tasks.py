"""Fixture module for E2E worker-shutdown-under-load tests (U1.9).

Spawned CLI subprocess imports this module via
``--module tests.e2e._shutdown_fixture_tasks``. The queue name is passed
via the ``CLI_TEST_QUEUE`` env var (same contract as
``tests/integration/_cli_fixture_tasks.py``).

Tasks:
  * ``shutdown_sleep`` — sleeps a caller-supplied number of seconds and
    returns a string. Increments a Valkey counter keyed by task_id so
    tests can detect duplicate executions after a SIGKILL redelivery.
  * ``shutdown_always_fail`` — raises ``ValueError`` every time. Used
    for the retry-during-shutdown test. ``max_retries=2`` +
    ``retry_base_delay=1.5`` gives the test enough window to SIGTERM
    worker A while the message is sitting in the delay queue.
"""

from __future__ import annotations

import asyncio
import os

from swarmq.task import Task, TaskInfo

_QUEUE = os.environ.get("CLI_TEST_QUEUE", "shutdown_test_default")


class ShutdownSleepTask(Task):
    """Sleep for ``seconds`` then return a marker string.

    Increments ``exec_count:{task_id}`` in Valkey on every execution so
    the SIGKILL-redelivery test can assert exact-once semantics (each
    task_id should show exactly one execution in the end).
    """

    task_info = TaskInfo(
        name="shutdown_sleep",
        queues=[_QUEUE],
        max_retries=0,
        # Give graceful-shutdown plenty of room: default is 30s, but a
        # SIGTERM test with 10 concurrent 2s sleeps easily fits.
    )

    async def run(self, seconds: float) -> str:
        # Record the execution before sleeping so a SIGKILL mid-sleep
        # still leaves a breadcrumb. We only care about final count
        # after the test completes.
        if self._backend is not None and self._task_id is not None:
            try:
                await self._backend._client.incr(f"exec_count:{self._task_id}")
                # Expire the counter so stray counters from prior runs
                # don't bloat the DB.
                await self._backend._client.expire(f"exec_count:{self._task_id}", 300)
            except Exception:
                # Best-effort — counter failures must not affect the
                # shutdown behaviour we're actually measuring.
                pass
        await asyncio.sleep(seconds)
        return f"slept:{seconds}"


class ShutdownAlwaysFailTask(Task):
    """Always raises. max_retries=2 + retry_base_delay=1.5 lets the
    retry-during-shutdown test SIGTERM worker A while the retry sits in
    the delay queue, then spin up worker B to take over."""

    task_info = TaskInfo(
        name="shutdown_always_fail",
        queues=[_QUEUE],
        max_retries=2,
        retry_backoff="fixed",
        retry_base_delay=1.5,
    )

    async def run(self) -> None:
        raise ValueError("always fails")
