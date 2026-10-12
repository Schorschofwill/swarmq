"""PEP 563: Task ohne DI mit unauflösbarem Namen (Tests 3 und 7)."""

from __future__ import annotations

from typing import override

from swarmq.task import Task, TaskInfo


class PlainUndefinedTask(Task):
    task_info = TaskInfo(name="dimod_plain_undefined", queues=["q"])

    @override
    async def run(self, x: Undefined) -> None:  # type: ignore[name-defined]  # noqa: F821 - absichtlich unauflösbar
        pass
