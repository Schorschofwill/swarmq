"""Task-Modul fuer die Logging-e2e-Tests (Plan 2026-10-06-0831, R9-R14).

Wird von ``swarmq worker --module tests.e2e._logging_fixture_tasks`` im
Supervisor, in den Worker-Subprozessen und im Forkserver (``--pool cpu``)
importiert. Die Queue kommt aus ``LOG_E2E_QUEUE``, damit jeder Test eine
eigene Queue nutzt.

``MarkFilter`` ist die Filter-Fabrik fuer die dictConfig-Datei des
``--log-config``-Tests: jede Zeile, die durch den Handler der Datei geht,
traegt ``e2e_marker`` und die PID des erzeugenden Prozesses.
"""

from __future__ import annotations

import asyncio
import logging
import os

from swarmq.logging import get_logger
from swarmq.task import Task, TaskInfo

QUEUE = os.environ.get("LOG_E2E_QUEUE", "log-e2e-default")
MARKER = "from-log-config-file"


class MarkFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.e2e_marker = MARKER
        record.e2e_pid = record.process
        return True


class LogAsyncEcho(Task):
    """Wartet asynchron und loggt eine App-Zeile aus dem Task heraus.

    ``get_logger`` statt ``logging.getLogger``: nur structlog-Logger tragen
    den Task-Kontext (``task_id``, ``compute_child``) aus den Kontextvariablen.
    """

    task_info = TaskInfo(name="log_async_echo", queues=[QUEUE], max_retries=0)

    async def run(self, tag: str) -> dict[str, object]:
        await asyncio.sleep(0.01)
        get_logger("e2e.app").warning("app_task_line", tag=tag)
        return {"tag": tag, "pid": os.getpid()}


class LogCpuSum(Task):
    """Rechnet synchron (CPU-Last) und loggt eine App-Zeile."""

    task_info = TaskInfo(name="log_cpu_sum", queues=[QUEUE], max_retries=0)

    async def run(self, n: int) -> dict[str, object]:
        total = sum(i * i for i in range(n))
        get_logger("e2e.app").warning("app_task_line", tag=f"cpu-{n}")
        return {"total": total, "pid": os.getpid()}
