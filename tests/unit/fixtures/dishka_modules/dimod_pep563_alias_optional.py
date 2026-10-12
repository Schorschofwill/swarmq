"""PEP 563: ``Depends[Svc] | None`` ergibt keinen DI-Key (Test 7a, R18)."""

# Phoenix-Muster: Alias ohne Typargument, mypy kennt ihn nicht als generisch.
# mypy: disable-error-code="type-arg"

from __future__ import annotations

from typing import override

from dishka import FromDishka

from swarmq.task import Task, TaskInfo

Depends = FromDishka


class Svc:
    pass


class AliasOptionalTask(Task):
    task_info = TaskInfo(name="dimod_alias_optional", queues=["q"])

    @override
    async def run(self, svc: Depends[Svc] | None = None) -> None:
        pass
