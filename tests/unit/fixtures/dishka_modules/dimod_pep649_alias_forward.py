"""PEP 649: ``Depends[Later]`` auf eine später definierte Klasse (Test 8a)."""

# Phoenix-Muster: Alias ohne Typargument, mypy kennt ihn nicht als generisch.
# mypy: disable-error-code="type-arg"

from typing import override

from dishka import FromDishka

from swarmq.task import Task, TaskInfo

Depends = FromDishka


class Pep649AliasForwardTask(Task):
    task_info = TaskInfo(name="dimod_pep649_alias_forward", queues=["q"])

    @override
    async def run(self, later: Depends[Later]) -> None:
        pass


class Later:
    pass
