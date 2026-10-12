"""Task-Modul fuer die Reload-Tests mit ``--log-config`` (Plan 2026-10-06-1811, U8).

``reload_sleep`` haelt einen Worker beim Stoppen fest: So laesst sich ein
SIGTERM in die Phase legen, in der die Worker fuer einen beschlossenen Reload
stoppen. Die Queue kommt aus ``CLI_TEST_QUEUE``.
"""

from __future__ import annotations

import asyncio
import os
from typing import override

from swarmq.task import Task, TaskInfo

_QUEUE = os.environ.get("CLI_TEST_QUEUE", "reload_test_default")


class ReloadSleepTask(Task):
    task_info = TaskInfo(name="reload_sleep", queues=[_QUEUE])

    @override
    async def run(self, seconds: float) -> str:
        await asyncio.sleep(seconds)
        return "slept"
