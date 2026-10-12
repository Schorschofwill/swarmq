"""PEP 563: ``Annotated[Svc, FromComponent("c")]`` (Test 1)."""

from __future__ import annotations

from typing import Annotated, override

from dishka import FromComponent

from swarmq.task import Task, TaskInfo


class Svc:
    pass


class ComponentTask(Task):
    task_info = TaskInfo(name="dimod_component", queues=["q"])

    @override
    async def run(self, svc: Annotated[Svc, FromComponent("c")]) -> None:
        pass
