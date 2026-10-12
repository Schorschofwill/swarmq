"""PEP 649 (ohne Future-Import): ``Annotated[Svc, FromComponent("c")]`` (Test 8)."""

from typing import Annotated, override

from dishka import FromComponent

from swarmq.task import Task, TaskInfo


class Svc:
    pass


class Pep649ComponentTask(Task):
    task_info = TaskInfo(name="dimod_pep649_component", queues=["q"])

    @override
    async def run(self, svc: Annotated[Svc, FromComponent("c")]) -> None:
        pass
