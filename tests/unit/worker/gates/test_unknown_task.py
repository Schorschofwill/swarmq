"""Unbekannter Task (``gates/unknown_task.py``), Plan 0.12.0, MR 3 (BZ-13).

Test-Liste:
- [x] unbekannter Workflow-Schritt geht (Gnadenfrist 0) per Dead-Letter ins
      ``.XQ`` und markiert seinen Workflow als fehlgeschlagen
"""

from __future__ import annotations

import dataclasses
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.headers import HEADER_WORKFLOW_ID, HEADER_WORKFLOW_STEP
from swarmq.registry import TaskRegistry
from swarmq.worker import Worker
from tests.support.messages import make_message
from tests.unit.worker._helpers import _default_payload

pytestmark = pytest.mark.timeout(5)

_WF_ID = "01890a5d-ac96-774b-bcce-b302099a8057"


async def test_unknown_workflow_step_is_routed_and_fails_its_workflow(broker, backend, config) -> None:
    # Gnadenfrist 0 (Plan 0.12.0, MR 11, BZ-06): wie bei ``test_befund_bz13
    # _workflow_hang.py`` Test 1 prueft dieser Test den DLQ-Zweig; mit der
    # Standard-Frist wuerde der Schritt zurueckgestellt.
    worker = Worker(
        broker=broker,
        backend=backend,
        registry=TaskRegistry(),
        config=dataclasses.replace(config, unknown_task_grace_s=0),
    )
    worker._workflow_advancer = MagicMock()
    worker._workflow_advancer.fail = AsyncMock()
    msg = make_message(
        _default_payload(task_id="step-x", name="renamed_away", args=[], kwargs={}),
        headers={HEADER_WORKFLOW_ID: _WF_ID, HEADER_WORKFLOW_STEP: "/2"},
    )

    await worker._handle_message(msg)

    broker.dead_letter.assert_awaited_once()
    worker._workflow_advancer.fail.assert_awaited_once()
    wf_id, error_type, message = worker._workflow_advancer.fail.await_args.args
    assert (wf_id, error_type) == (_WF_ID, "TaskError")
    assert "renamed_away" in message
