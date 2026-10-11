"""Fixture-Modul der Mischbetrieb-Suite (Q2) mit einem Cron, der nie von selbst fällt.

``0 0 1 1 *`` liegt fast immer weit in der Zukunft; die Tests legen den
Fire-Zeitpunkt im Fire-Index selbst auf "jetzt fällig". So zählt jeder Lauf
genau einen erzwungenen Fire, und kein natürlicher Minuten-Fire stört.
Echtheitsprüfung und Aufzeichnung wie in ``_compat_tasks``.
"""

from __future__ import annotations

from swarmq.task import Task, TaskInfo
from tests.compat._compat_tasks import _QUEUE, RUNTIME, _record

CRON_NAME = "compat_cron"


class CompatCronTask(Task):
    task_info = TaskInfo(name=CRON_NAME, queues=[_QUEUE], cron="0 0 1 1 *")

    async def run(self) -> str:
        await _record(CRON_NAME, self._task_id)
        return RUNTIME
