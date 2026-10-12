"""Lock-Task ohne ``from __future__ import annotations`` (PEP 649) mit
unauflösbarem Namen in ``run()`` (Plan 2026-10-12-1105, E9, Test 12a).

Wird erst im Test importiert, damit die Registrierung von der
Registry-Fixture zurückgesetzt wird.
"""

from typing import override

from swarmq.task import Task, TaskInfo


class Pep649LockTask(Task):
    task_info = TaskInfo(name="pep649_lock", queues=["default"], lock="user-{lock_id}", hard_timeout=60)

    @override
    async def run(self, x: Undefined, lock_id: int) -> None:  # type: ignore[name-defined]  # noqa: F821 - absichtlich unauflösbar
        return None
