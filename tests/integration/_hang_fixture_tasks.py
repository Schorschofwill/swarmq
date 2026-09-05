"""Fixture fuer den Lebenszeichen-Test (R21): ein Task, der den Event-Loop synchron blockiert."""

from __future__ import annotations

import os
import time

from swarmq.task import Task, TaskInfo

QUEUE = os.environ.get("HANG_TEST_QUEUE", "hang-fixture")


class LoopBlocker(Task):
    task_info = TaskInfo(name="loop_blocker", queues=[QUEUE], max_retries=0)

    async def run(self, seconds: float = 120.0) -> None:
        time.sleep(seconds)  # blockiert den ganzen Loop — kein Lebenszeichen mehr
