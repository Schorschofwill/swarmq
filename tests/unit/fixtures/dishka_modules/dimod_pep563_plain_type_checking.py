"""PEP 563: Task ohne DI mit Import nur unter TYPE_CHECKING (Test 4)."""

from __future__ import annotations

from typing import TYPE_CHECKING, override

from swarmq.task import Task, TaskInfo

if TYPE_CHECKING:
    from decimal import Decimal


class PlainTypeCheckingTask(Task):
    task_info = TaskInfo(name="dimod_plain_type_checking", queues=["q"])

    @override
    async def run(self, amount: Decimal) -> None:
        pass
