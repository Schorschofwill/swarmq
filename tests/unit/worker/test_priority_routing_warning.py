"""Warnung bei Prioritaets-Tasks ohne ``--priorities`` (Plan 0.12.0, MR 6, BZ-03).

Seit 0.12 verwirft der Broker einen Publish an ``{queue}.high``/``.low`` nicht
mehr still: ohne deklarierte Queue wirft der Producer ``UnroutableMessage``.
Der Hinweis der Startwarnung nennt das.

Test-Liste:
- [x] Hinweis nennt ``UnroutableMessage`` statt stillen Verlust
"""

from __future__ import annotations

import pytest

from swarmq.registry import TaskRegistry
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker

pytestmark = [pytest.mark.timeout(5)]


class HighTask(Task):
    task_info = TaskInfo(name="prio-high", queues=["default"], priority="high")

    async def run(self) -> None:
        return None


def test_priority_warning_hint_names_unroutable_message(broker, backend, config, swarmq_logs):
    registry = TaskRegistry()
    registry.register(HighTask)
    worker = Worker(broker=broker, backend=backend, registry=registry, config=config)

    worker._warn_priority_tasks_without_routing()

    [entry] = [e for e in swarmq_logs if e["event"] == "registered_priority_tasks_with_priorities_disabled"]
    assert "UnroutableMessage" in entry["hint"]
    assert "silent" not in entry["hint"]
