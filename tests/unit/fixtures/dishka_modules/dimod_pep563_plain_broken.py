"""PEP 563: Task ohne DI mit kaputter String-Annotation (Test 4a)."""

from __future__ import annotations

from typing import override

from swarmq.task import Task, TaskInfo


class PlainBrokenTask(Task):
    task_info = TaskInfo(name="dimod_plain_broken", queues=["q"])

    @override
    async def run(self, x: "list[int") -> None:  # type: ignore[valid-type]  # noqa: F722 - absichtlich kaputte Annotation
        pass
