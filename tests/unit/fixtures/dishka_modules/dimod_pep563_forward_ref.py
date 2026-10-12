"""PEP 563: DI-Key mit Forward-Ref auf eine später definierte Klasse (Test 6)."""

# Phoenix-Muster: Alias ohne Typargument, mypy kennt ihn nicht als generisch.
# mypy: disable-error-code="type-arg"

from __future__ import annotations

from typing import Annotated, override

from dishka import FromComponent, FromDishka

from swarmq.task import Task, TaskInfo

Depends = FromDishka


class AliasForwardTask(Task):
    task_info = TaskInfo(name="dimod_alias_forward", queues=["q"])

    @override
    async def run(self, later: Depends[Later]) -> None:
        pass


class ComponentForwardTask(Task):
    task_info = TaskInfo(name="dimod_component_forward", queues=["q"])

    @override
    async def run(self, later: Annotated[Later, FromComponent("c")]) -> None:
        pass


class Later:
    pass
