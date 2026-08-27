"""Gemeinsame Helfer der Worker-Unit-Tests (Plan U7)."""

from __future__ import annotations

from swarmq.task import Task, TaskInfo


class AddTask(Task):
    task_info = TaskInfo(name="add", queues=["default"])

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
