"""TaskIQ adapter for the benchmark comparison suite.

Implements the :class:`TaskQueueSystem` protocol defined in ``base.py``.
TaskIQ is async-native, so the worker runs as an in-process asyncio task
via :class:`taskiq.receiver.Receiver` — no subprocess management needed.

Connection target: ``redis://localhost:6389/1`` (Valkey, db=1).
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from taskiq.receiver import Receiver

from benchmarks.comparison.workers.taskiq_worker import (
    broker,
    cpu_task,
    failing_task,
    io_task,
    noop_task,
)

logger = logging.getLogger(__name__)

# Map benchmark task names to the decorated task objects so that
# ``enqueue()`` can dispatch by string name.
_TASK_REGISTRY: dict[str, Any] = {
    "noop_task": noop_task,
    "io_task": io_task,
    "cpu_task": cpu_task,
    "failing_task": failing_task,
}


class TaskIQSystem:
    """Benchmark adapter for TaskIQ with Redis/Valkey broker and result backend.

    Satisfies the :class:`~benchmarks.comparison.systems.base.TaskQueueSystem`
    protocol.

    Lifecycle
    ---------
    1. ``setup()``        — start the broker (opens Redis connections).
    2. ``start_worker()`` — create a :class:`Receiver` and run it as an
                            ``asyncio.Task`` in the current event loop.
    3. *run benchmarks*   — ``enqueue()`` / ``get_result()`` round-trips.
    4. ``stop_worker()``  — cancel the receiver task.
    5. ``teardown()``     — shut down the broker and flush the database.
    """

    name: str = "taskiq"

    def __init__(self) -> None:
        self._receiver: Receiver | None = None
        self._worker_task: asyncio.Task[None] | None = None

    # ------------------------------------------------------------------
    # Protocol: setup / teardown
    # ------------------------------------------------------------------

    async def setup(self) -> None:
        """Start the broker (opens connections to Redis/Valkey)."""
        await broker.startup()
        logger.info("TaskIQ broker started (url=%s)", broker.url)

    async def teardown(self) -> None:
        """Shut down the broker, flush db=1 to leave a clean state."""
        try:
            await broker.shutdown()
        except Exception:
            logger.exception("Error during broker shutdown")

        # Flush the isolated database so the next run starts clean.
        try:
            import redis.asyncio as aioredis

            conn = aioredis.from_url("redis://localhost:6389/1")
            await conn.flushdb()
            await conn.aclose()
        except Exception:
            logger.debug("Could not flush Valkey db=1 during teardown", exc_info=True)

        logger.info("TaskIQ teardown complete")

    # ------------------------------------------------------------------
    # Protocol: worker lifecycle
    # ------------------------------------------------------------------

    async def start_worker(self) -> None:
        """Start the TaskIQ receiver as an in-process asyncio task."""
        if self._worker_task is not None:
            logger.warning("Worker already running — skipping start_worker()")
            return

        self._receiver = Receiver(broker)

        async def _run_receiver() -> None:
            """Long-running coroutine that listens for and processes tasks."""
            assert self._receiver is not None  # noqa: S101
            try:
                await self._receiver.listen()
            except asyncio.CancelledError:
                logger.debug("TaskIQ receiver cancelled")
            except Exception:
                logger.exception("TaskIQ receiver crashed")

        self._worker_task = asyncio.create_task(_run_receiver())

        # Give the receiver a moment to connect and begin listening.
        await asyncio.sleep(0.1)
        logger.info("TaskIQ worker started (in-process receiver)")

    async def stop_worker(self) -> None:
        """Cancel the receiver task and wait for it to wind down."""
        if self._worker_task is None:
            return

        self._worker_task.cancel()
        try:
            await self._worker_task
        except asyncio.CancelledError:
            pass

        self._worker_task = None
        self._receiver = None
        logger.info("TaskIQ worker stopped")

    # ------------------------------------------------------------------
    # Protocol: enqueue / get_result
    # ------------------------------------------------------------------

    async def enqueue(self, task_name: str, payload: dict) -> str:
        """Dispatch a task by name. Returns the task ID as a string.

        Parameters
        ----------
        task_name:
            One of ``noop_task``, ``io_task``, ``cpu_task``, ``failing_task``.
        payload:
            Keyword arguments forwarded to the task function.
        """
        task_func = _TASK_REGISTRY.get(task_name)
        if task_func is None:
            raise ValueError(
                f"Unknown task {task_name!r}. "
                f"Available: {sorted(_TASK_REGISTRY)}"
            )

        result_handle = await task_func.kiq(**payload)
        return result_handle.task_id

    async def get_result(
        self,
        task_id: str,
        timeout: float = 30.0,
        poll_interval: float = 0.01,
    ) -> Any:
        """Wait for a task result by polling the result backend.

        TaskIQ's ``AsyncResultBackend.get_result`` is used to check for
        completion.  We poll at *poll_interval* seconds to stay consistent
        with the benchmark harness's latency measurement approach.

        Raises
        ------
        TimeoutError
            If the result is not available within *timeout* seconds.
        RuntimeError
            If the task itself raised an exception.
        """
        deadline = asyncio.get_event_loop().time() + timeout

        while True:
            # TaskIQ result backend exposes is_result_ready / get_result
            is_ready = await broker.result_backend.is_result_ready(task_id)  # type: ignore[union-attr]
            if is_ready:
                result = await broker.result_backend.get_result(task_id)  # type: ignore[union-attr]

                # TaskIQ wraps results in a TaskiqResult dataclass.
                # If the task raised, ``result.is_err`` is True.
                if result.is_err:
                    raise RuntimeError(
                        f"Task {task_id} failed: {result.error}"
                    )
                return result.return_value

            if asyncio.get_event_loop().time() >= deadline:
                raise TimeoutError(
                    f"Task {task_id} did not complete within {timeout}s"
                )

            await asyncio.sleep(poll_interval)
