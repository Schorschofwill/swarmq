"""Fixture-Modul fuer den e2e-Test Supervisor-Abbau im Recycling-Drain (FS-30, U9).

Der CLI-Worker importiert es per ``--module tests.e2e._recycling_fixture_tasks``;
die Queue kommt aus ``CLI_TEST_QUEUE``.

Tasks:
  * ``recycle_long_locked`` -- haelt eine Sperre und laeuft lange (deutlich
    laenger als das Shutdown-Budget); vermerkt seinen Start in Valkey.
  * ``recycle_quick`` -- endet sofort und loest mit ``SWARMQ_RECYCLE_MAX_TASKS=1``
    das Recycling aus.
"""

from __future__ import annotations

import asyncio
import os
from typing import TYPE_CHECKING, cast, override

from swarmq.task import Task, TaskInfo

if TYPE_CHECKING:
    from swarmq.backend.valkey import ValkeyBackend

_QUEUE = os.environ.get("CLI_TEST_QUEUE", "recycling_test_default")
RECYCLE_LOCK = "recycle-e2e-lock"
STARTED_KEY = "recycle_e2e_started"


class RecycleLongLocked(Task):
    task_info = TaskInfo(
        name="recycle_long_locked",
        queues=[_QUEUE],
        lock=RECYCLE_LOCK,
        hard_timeout=600,  # ``lock`` verlangt ihn; Recycling-Frist damit 630 s
        max_retries=0,
    )

    @override
    async def run(self) -> str:
        if self._backend is not None:
            await cast("ValkeyBackend", self._backend).client.set(STARTED_KEY, "1", ex=300)
        await asyncio.sleep(600)
        return "nie"


class RecycleQuick(Task):
    task_info = TaskInfo(name="recycle_quick", queues=[_QUEUE])

    @override
    async def run(self) -> str:
        return "ok"
