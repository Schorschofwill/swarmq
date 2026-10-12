"""PEP 563: ``FromDishka`` im Text mit unauflösbarem Typ (Test 5)."""

from __future__ import annotations

from typing import override

from dishka import FromDishka

from swarmq.task import Task, TaskInfo


class FromDishkaUndefinedTask(Task):
    task_info = TaskInfo(name="dimod_fromdishka_undefined", queues=["q"])

    @override
    async def run(self, svc: FromDishka[Undefined]) -> None:  # type: ignore[name-defined]  # noqa: F821 - absichtlich unauflösbar
        pass
