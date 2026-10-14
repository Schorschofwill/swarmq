"""Nachweis FS-28: Ein AT_LEAST_ONCE-Workflowschritt mit ``result_ttl=0``
meldet faelschlich „is FIRE_AND_FORGET“.

Register: docs/befunde/fehlersuche-runde-2-2026-10.md (FS-28, Quelle MA-8).

``ResultTail._finish_without_store`` greift bei jedem aufgeloesten
``result_ttl == 0``: FIRE_AND_FORGET ohne TTL, ausdruecklichem
``TaskInfo(result_ttl=0)`` und globalem ``config.result_ttl=0``. Das
Verhalten (Workflow ``failed`` mit ``WorkflowValidationError``) ist nach
E-039 entschieden und bleibt; nur der Text muss die Ursache nennen. Der
FIRE_AND_FORGET-Text bleibt (E-039-Test in ``test_results.py``).

Testliste:
# [x] ``TaskInfo(result_ttl=0)`` mit Workflow-Header: Text nennt ``result_ttl=0``, nicht FIRE_AND_FORGET
# [x] Globales ``config.result_ttl=0``: ebenso
"""

from __future__ import annotations

from typing import Any, override
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.config import SwarmQConfig
from swarmq.headers import HEADER_WORKFLOW_ID, HEADER_WORKFLOW_STEP
from swarmq.registry import TaskRegistry
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker
from swarmq.worker.execution import ExecutionResult
from swarmq.worker.gates import GateContext
from tests.support.messages import make_message
from tests.unit.worker._helpers import _default_payload

pytestmark = pytest.mark.timeout(5)

_FS28_WF_ID = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b"


class Fs28NoStore(Task):
    task_info = TaskInfo(name="fs28_nostore", queues=["default"], result_ttl=0)

    @override
    async def run(self) -> None:
        return None


class Fs28Plain(Task):
    task_info = TaskInfo(name="fs28_plain", queues=["default"])

    @override
    async def run(self) -> None:
        return None


async def _fs28_fail_reason(broker: Any, backend: Any, config: SwarmQConfig, task: type[Task]) -> str:
    registry = TaskRegistry()
    registry.register(task)
    worker = Worker(broker=broker, backend=backend, registry=registry, config=config)
    worker._workflow_advancer = MagicMock()
    worker._workflow_advancer.fail = AsyncMock()
    message = make_message(
        _default_payload(name=task.task_info.name),
        headers={HEADER_WORKFLOW_ID: _FS28_WF_ID, HEADER_WORKFLOW_STEP: "/1"},
    )
    ctx = GateContext(
        task_id="fs28-id", task_name=task.task_info.name, queue="default", task_info=task.task_info, task_class=task
    )
    execution = ExecutionResult(
        result_value=None, is_error=False, is_cancelled=False, error_message="", task_exception=None
    )

    await worker._result_tail.finish(message, ctx, execution)

    backend.store_result.assert_not_awaited()
    worker._workflow_advancer.fail.assert_awaited_once()
    error_type, reason = worker._workflow_advancer.fail.await_args.args[1:3]
    assert error_type == "WorkflowValidationError"
    return str(reason)


@pytest.mark.befund("FS-28")
async def test_fs28_task_result_ttl_zero_names_the_real_cause(broker: Any, backend: Any, config: Any) -> None:
    reason = await _fs28_fail_reason(broker, backend, config, Fs28NoStore)

    assert "result_ttl=0" in reason
    assert "FIRE_AND_FORGET" not in reason


@pytest.mark.befund("FS-28")
async def test_fs28_global_result_ttl_zero_names_the_real_cause(broker: Any, backend: Any) -> None:
    config = SwarmQConfig(
        broker_url="amqp://localhost", backend_url="valkey://localhost", queues=["default"], result_ttl=0
    )

    reason = await _fs28_fail_reason(broker, backend, config, Fs28Plain)

    assert "result_ttl=0" in reason
    assert "FIRE_AND_FORGET" not in reason
