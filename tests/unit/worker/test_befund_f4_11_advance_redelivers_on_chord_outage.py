"""Nachweis F4-11 (Worker-Ebene): Ein Valkey-Ausfall im Chord-Store markiert
einen Workflow, dessen Schritte gelungen sind, als ``failed``.

Register: docs/befunde/fehlersuche-runde-4-2026-10.md (F4-11; Verdacht
R4M-D-01; Gegenprüfung ``6-gegenpruefung-M.md``). Ursache an der
Backend-Grenze: ``tests/unit/backend/test_befund_f4_11_chord_errors_wrapped.py``.

Ablauf: Ein Gruppenglied endet erfolgreich, das Lesen des Workflow-Zustands
gelingt, ``chord_complete.lua`` scheitert (Valkey-Neustart im Advance-Fenster
oder ``OOM`` an der ``maxmemory``-Grenze unter ``noeviction``, E-005).
``ChordManager`` reicht den Treiberfehler roh durch; ``ResultTail`` fängt für
den Advance nur ``BackendConnectionError``/``WorkflowStateConflict`` als
vorübergehend und behandelt den rohen Fehler als Programmfehler:
``workflow_advance_failed``, Fail-Mark, Ack.

Erwartet: Redelivery ohne Fail-Mark wie bei jedem Backend-Ausfall im Advance
(``worker/results.py`` „Transient — redelivery retries the idempotent
advance“; ``logging.md:667`` ``workflow_advance_backend_error``); E-040
(Fail-Mark nur, wenn ein Schritt endgültig ohne Erfolg endet).

Echt: ``Worker``/``ResultTail``, ``WorkflowAdvancer``, ``ChordManager``.
Gemockt: Broker und Ergebnis-Backend des Workers (Worker-Fixtures), der
Valkey-Client unter ``ChordManager`` (Skript wirft), der Workflow-Zustand
liegt im ``InMemoryBackend``.

Testliste:
# [x] Transportfehler im Chord-Skript: ``Redelivered``, Workflow nicht ``failed``
# [x] ``OOM`` im Chord-Skript: ``Redelivered``, Workflow nicht ``failed``
"""

from __future__ import annotations

from typing import Any, override
from unittest.mock import AsyncMock, MagicMock

import pytest
from valkey.exceptions import ConnectionError as ValkeyConnectionError
from valkey.exceptions import OutOfMemoryError

from swarmq.backend.valkey.chords import ChordManager
from swarmq.contracts import PublishReceipt
from swarmq.headers import HEADER_WORKFLOW_ID, HEADER_WORKFLOW_STEP
from swarmq.registry import TaskRegistry
from swarmq.serialization import decode_json, encode_json
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker
from swarmq.worker.execution import ExecutionResult
from swarmq.worker.gates import GateContext
from swarmq.worker.outcome import Redelivered
from swarmq.workflow import group
from swarmq.workflow_orchestration import (
    STATUS_FAILED,
    WorkflowAdvancer,
    build_initial_workflow_state,
    initial_frontier,
    workflow_step_task_id,
)
from tests.support.inmemory import FakeClock, InMemoryBackend
from tests.support.messages import make_message
from tests.unit.worker._helpers import _default_payload

pytestmark = pytest.mark.timeout(5)

_WF_ID = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b"


class MemberTask(Task):
    task_info = TaskInfo(name="f411-member", queues=["default"])

    @override
    async def run(self) -> str:
        return "ok"


class _NoopPublisher:
    async def pre_enqueue(self, steps: Any) -> None:
        return None

    async def publish(self, step: Any) -> PublishReceipt:
        return PublishReceipt(queue=step.queue)


async def _group_state() -> tuple[InMemoryBackend, str]:
    """Gruppe aus zwei Gliedern, beide Schritte veröffentlicht; liefert den
    Zustandsspeicher und den Pfad des ersten Glieds."""
    state_store = InMemoryBackend(FakeClock())
    state_store._connected = True  # synchron „verbunden“ wie in test_workflow_advancer.py
    workflow = group(MemberTask.s(), MemberTask.s())
    state = build_initial_workflow_state(_WF_ID, workflow.to_dict())
    frontier = initial_frontier(workflow)
    state["step_tasks"] = {path: workflow_step_task_id(_WF_ID, path) for path, _sig in frontier}
    await state_store.store_workflow_state(_WF_ID, encode_json(state), 60_000)
    return state_store, frontier[0][0]


def _chord_store(error: BaseException) -> ChordManager:
    client = MagicMock()
    client.register_script = MagicMock(return_value=AsyncMock(side_effect=error))
    return ChordManager(client)


@pytest.mark.befund("F4-11")
@pytest.mark.xfail(
    strict=True, raises=AssertionError, reason="F4-11: offen - Valkey-Ausfall im Chord-Store fail-markt den Workflow"
)
@pytest.mark.parametrize(
    "cause",
    [
        pytest.param(ValkeyConnectionError("Error 111 connecting to valkey:6379. Connection refused."), id="transport"),
        pytest.param(OutOfMemoryError("command not allowed when used memory > 'maxmemory'."), id="oom"),
    ],
)
async def test_chord_store_outage_in_the_advance_redelivers_instead_of_failing_the_workflow(
    broker: Any, backend: Any, config: Any, cause: BaseException
) -> None:
    state_store, member_path = await _group_state()
    registry = TaskRegistry()
    registry.register(MemberTask)
    worker = Worker(broker=broker, backend=backend, registry=registry, config=config)
    worker._workflow_advancer = WorkflowAdvancer(
        state_store,
        _chord_store(cause),
        publisher=_NoopPublisher(),
        resolve_queue=lambda _name: "default",
        make_task_id=workflow_step_task_id,
        unpack=lambda raw: raw,
        ttl_ms=60_000,
    )
    task_id = workflow_step_task_id(_WF_ID, member_path)
    msg = make_message(
        _default_payload(name="f411-member", task_id=task_id),
        headers={HEADER_WORKFLOW_ID: _WF_ID, HEADER_WORKFLOW_STEP: member_path},
    )
    ctx = GateContext(
        task_id=task_id, task_name="f411-member", queue="default", task_info=MemberTask.task_info, task_class=MemberTask
    )
    execution = ExecutionResult(
        result_value="ok", is_error=False, is_cancelled=False, error_message="", task_exception=None
    )

    outcome = await worker._result_tail.finish(msg, ctx, execution)

    raw = await state_store.get_workflow_state(_WF_ID)
    assert raw is not None
    status = decode_json(raw)["status"]
    assert status != STATUS_FAILED, (
        f"Workflow nach {type(cause).__name__} im Chord-Skript fail-markiert, obwohl das Glied gelungen ist "
        "(E-040, workflow_advance_backend_error)"
    )
    assert outcome is Redelivered
