"""PEP 563: Alias ``Depends = FromDishka`` (Test 2)."""

# Phoenix-Muster: Alias ohne Typargument, mypy kennt ihn nicht als generisch.
# mypy: disable-error-code="type-arg"

from __future__ import annotations

from typing import override

from dishka import FromDishka

from swarmq.task import Task, TaskInfo

Depends = FromDishka


class Svc:
    pass


class AliasTask(Task):
    task_info = TaskInfo(name="dimod_alias", queues=["q"])

    @override
    async def run(self, svc: Depends[Svc]) -> None:
        pass
