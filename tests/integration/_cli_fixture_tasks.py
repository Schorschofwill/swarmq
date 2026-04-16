"""Fixture module for CLI integration tests.

Defines a single ``Task`` subclass so the CLI subprocess has something
to register and run when invoked with ``--module
tests.integration._cli_fixture_tasks``.

The queue name is parameterised via env var ``CLI_TEST_QUEUE`` so each
test run can use a unique queue and avoid bleed-over.
"""

from __future__ import annotations

import os

from swarmq.task import Task, TaskInfo

_QUEUE = os.environ.get("CLI_TEST_QUEUE", "cli_test_default")


class CliEchoTask(Task):
    task_info = TaskInfo(name="cli_echo", queues=[_QUEUE])

    async def run(self, value: str) -> str:
        return f"echoed:{value}"
