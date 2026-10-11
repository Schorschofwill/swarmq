"""Fixture module for CLI integration tests.

Defines a single ``Task`` subclass so the CLI subprocess has something
to register and run when invoked with ``--module
tests.integration._cli_fixture_tasks``.

The queue name is parameterised via env var ``CLI_TEST_QUEUE`` so each
test run can use a unique queue and avoid bleed-over.
"""

from __future__ import annotations

import os
from typing import override

from swarmq.task import Task, TaskInfo

_QUEUE = os.environ.get("CLI_TEST_QUEUE", "cli_test_default")


class CliEchoTask(Task):
    task_info = TaskInfo(name="cli_echo", queues=[_QUEUE])

    @override
    async def run(self, value: str) -> str:
        return f"echoed:{value}"


_flaky_seen: set[str] = set()


class CliFlakyTask(Task):
    """Scheitert beim ersten Versuch je Task-ID, danach Erfolg (Plan
    2026-10-06-0831, U11): eine Task-ID durchlaeuft so alle vier
    Lebenszyklus-Events."""

    task_info = TaskInfo(name="cli_flaky", queues=[_QUEUE], max_retries=1, retry_base_delay=0.1)

    @override
    async def run(self) -> str:
        if self._task_id not in _flaky_seen:
            _flaky_seen.add(self._task_id)
            raise RuntimeError("first attempt fails")
        return "ok"
