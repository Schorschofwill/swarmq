"""Base protocol for task queue system adapters."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class TaskQueueSystem(Protocol):
    """Structural interface for benchmark system adapters.

    Each library implements this protocol using its native worker model:
    - Async libraries (SwarmQ, TaskIQ): worker as asyncio.Task in-process
    - Sync libraries (Celery, Dramatiq): worker as subprocess
    """

    name: str

    async def setup(self) -> None:
        """Connect to broker/backend, declare queues."""
        ...

    async def teardown(self) -> None:
        """Cleanup: flush queues, close connections."""
        ...

    async def start_worker(self) -> None:
        """Start the worker (asyncio.Task or subprocess)."""
        ...

    async def stop_worker(self) -> None:
        """Stop the worker (cancel task or SIGTERM subprocess)."""
        ...

    async def enqueue(self, task_name: str, payload: dict) -> str:
        """Schedule a task, return task_id."""
        ...

    async def get_result(
        self, task_id: str, timeout: float = 30.0, poll_interval: float = 0.01
    ) -> Any:
        """Poll for result. poll_interval is set by the scenario."""
        ...
