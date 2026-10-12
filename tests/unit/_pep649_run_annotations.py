"""Tasks ohne ``from __future__ import annotations`` (PEP 649) mit
unauflösbaren bzw. kaputten Annotationen in ``run()`` (Plan 2026-10-12-1105,
N2).

Wird erst im Test importiert, damit die Registrierung von der
Registry-Fixture zurückgesetzt wird.
"""

from typing import override

from swarmq.task import Task, TaskInfo


class Pep649UnresolvableTask(Task):
    task_info = TaskInfo(name="n2_pep649", queues=["q"])

    @override
    async def run(self, x: Undefined) -> Missing:  # type: ignore[name-defined]  # noqa: F821 - absichtlich unauflösbar
        pass


class Pep649BrokenReturnTask(Task):
    task_info = TaskInfo(name="n2_pep649_broken", queues=["q"])

    @override
    async def run(self) -> "list[int":  # type: ignore[valid-type]  # noqa: F722 - absichtlich kaputte Annotation
        pass
