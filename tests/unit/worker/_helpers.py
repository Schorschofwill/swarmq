"""Gemeinsame Helfer der Worker-Unit-Tests (Plan U7)."""

from __future__ import annotations

import time
from typing import Any, override

from swarmq.headers import HEADER_LOCK_WAIT_SINCE
from swarmq.task import Task, TaskInfo


class AddTask(Task):
    task_info = TaskInfo(name="add", queues=["default"])

    @override
    async def run(self, a: int, b: int) -> int:
        return a + b


def _default_payload(
    task_id: str = "abc-123",
    queue: str = "default",
    name: str = "add",
    args: list | None = None,
    kwargs: dict | None = None,
) -> dict:
    return {
        "task": {"id": task_id, "queue": queue, "name": name},
        "args": args if args is not None else [1, 2],
        "kwargs": kwargs if kwargs is not None else {},
    }


def lock_wait_exceeded_headers(config: Any) -> dict[str, Any]:
    """Header einer Nachricht, deren Wartezeit auf einen Lock die Obergrenze
    ``lock_wait_max_s`` ueberschritten hat (Plan 0.12.0, MR 13, BZ-05)."""
    return {HEADER_LOCK_WAIT_SINCE: int(time.time() * 1000) - int(config.lock_wait_max_s * 1000) - 1_000}
