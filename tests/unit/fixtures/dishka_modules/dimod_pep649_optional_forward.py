"""PEP 649: ``FromDishka[Later] | None`` wird ein einziger ForwardRef (Test 8b)."""

from typing import override

from dishka import FromDishka

from swarmq.task import Task, TaskInfo


class Pep649OptionalForwardTask(Task):
    task_info = TaskInfo(name="dimod_pep649_optional_forward", queues=["q"])

    @override
    async def run(self, later: FromDishka[Later] | None = None) -> None:
        pass


class Later:
    pass
