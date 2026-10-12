"""PEP 563: ``FromDishka[Svc]`` plus Nicht-DI-Parameter nur unter TYPE_CHECKING (Test 5a)."""

from __future__ import annotations

from typing import TYPE_CHECKING, override

from dishka import FromDishka

from swarmq.task import Task, TaskInfo

if TYPE_CHECKING:
    from decimal import Decimal as OnlyTC


class Svc:
    pass


class FromDishkaTypeCheckingTask(Task):
    task_info = TaskInfo(name="dimod_fromdishka_type_checking", queues=["q"])

    @override
    async def run(self, svc: FromDishka[Svc], x: OnlyTC) -> None:
        pass
