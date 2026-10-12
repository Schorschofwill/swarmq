"""PEP 563: Alias ``Depends[Svc]`` plus Nicht-DI-Parameter nur unter TYPE_CHECKING (Test 6a)."""

# Phoenix-Muster: Alias ohne Typargument, mypy kennt ihn nicht als generisch.
# mypy: disable-error-code="type-arg"

from __future__ import annotations

from typing import TYPE_CHECKING, override

from dishka import FromDishka

from swarmq.task import Task, TaskInfo

if TYPE_CHECKING:
    from decimal import Decimal as OnlyTC

Depends = FromDishka


class Svc:
    pass


class AliasTypeCheckingTask(Task):
    task_info = TaskInfo(name="dimod_alias_type_checking", queues=["q"])

    @override
    async def run(self, svc: Depends[Svc], x: OnlyTC) -> None:
        pass
