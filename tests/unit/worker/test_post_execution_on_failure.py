"""E-067: ``post_execution`` läuft nur nach erfolgreichem ``run()`` (F3-28).

Festschreibender Test zur Entscheidung E-067: Wirft ``run()``, bleibt
``post_execution`` aus, und das gespeicherte Ergebnis ist ein Fehler.
Aufräumen für jeden Ausgang gehört in ``try/finally`` oder den
``yield``-Finalizer (Doku: ``website/guides/defining-tasks.md``).

Test-Liste:
- [x] run() wirft ValueError → post_execution läuft nicht, Ergebnis is_error
"""

from __future__ import annotations

from typing import override

import pytest

from swarmq.registry import TaskRegistry
from swarmq.serialization import decode_result
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker
from tests.support.messages import make_message
from tests.unit.worker._helpers import _default_payload


@pytest.mark.timeout(5)
@pytest.mark.entscheidung("E-067")
async def test_run_raises_skips_post_execution(broker, backend, config):
    post_called = False

    class RunFailTask(Task):
        task_info = TaskInfo(name="runfail", queues=["default"])

        @override
        async def run(self) -> str:
            raise ValueError("run failed")

        @override
        async def post_execution(self) -> None:
            nonlocal post_called
            post_called = True

    reg = TaskRegistry()
    reg.register(RunFailTask)
    w = Worker(broker=broker, backend=backend, registry=reg, config=config)

    await w._handle_message(make_message(_default_payload(name="runfail", args=[], kwargs={})))

    assert post_called is False
    stored = decode_result(backend.store_result.call_args[0][1])
    assert stored["is_error"] is True
    assert "run failed" in stored["error_message"]
