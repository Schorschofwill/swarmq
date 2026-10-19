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
# [x] dauerhafte Fehlerantwort (``WRONGTYPE``) im Chord-Skript: ebenfalls
#     ``Redelivered`` bis zum Zustelllimit, kein Fail-Mark (KD7 im Fix-Plan
#     Runde 4, seit 2026-10-19 Regel E-079)
# [x] Chord: Ausfall beim letzten Kopfglied, dann Rückkehr von Valkey: der
#     Chord-Body wird genau einmal eingereiht, auch bei einer weiteren
#     Zustellung desselben Glieds
"""

from __future__ import annotations

from typing import Any, override
from unittest.mock import AsyncMock, MagicMock

import pytest
from valkey.exceptions import ConnectionError as ValkeyConnectionError
from valkey.exceptions import OutOfMemoryError, ResponseError

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
from swarmq.workflow import chord, group
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
    def __init__(self) -> None:
        self.published: list[str] = []

    async def pre_enqueue(self, steps: Any) -> None:
        return None

    async def publish(self, step: Any) -> PublishReceipt:
        self.published.append(step.step_path)
        return PublishReceipt(queue=step.queue)


class _RecoveringChordScript:
    """``chord_complete.lua`` mit Mengen-Semantik (``InMemoryChordStore``);
    wirft, solange ``outage`` gesetzt ist."""

    def __init__(self) -> None:
        self.outage: BaseException | None = None
        members = InMemoryBackend(FakeClock())
        members._connected = True  # synchron „verbunden“ wie der Zustandsspeicher
        self._members = members.chords

    async def __call__(self, *, keys: list[str], args: list[Any]) -> int:
        if self.outage is not None:
            raise self.outage
        member_id, expected, ttl_ms = args
        return int(await self._members.record_member(keys[0], "", member_id, expected=expected, ttl_ms=ttl_ms))


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


async def _finish_member(
    broker: Any,
    backend: Any,
    config: Any,
    state_store: InMemoryBackend,
    member_path: str,
    chord_store: ChordManager,
    publisher: _NoopPublisher | None = None,
) -> Any:
    """Ein erfolgreich beendetes Glied durch ``ResultTail.finish`` schicken."""
    registry = TaskRegistry()
    registry.register(MemberTask)
    worker = Worker(broker=broker, backend=backend, registry=registry, config=config)
    worker._workflow_advancer = WorkflowAdvancer(
        state_store,
        chord_store,
        publisher=publisher if publisher is not None else _NoopPublisher(),
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
    return await worker._result_tail.finish(msg, ctx, execution)


async def _status(state_store: InMemoryBackend) -> str:
    raw = await state_store.get_workflow_state(_WF_ID)
    assert raw is not None
    return str(decode_json(raw)["status"])


@pytest.mark.entscheidung("E-079")
async def test_error_reply_in_the_chord_script_redelivers_instead_of_failing_the_workflow(
    broker: Any, backend: Any, config: Any
) -> None:
    """KD7: eine dauerhafte Fehlerantwort stellt wie bei Lock- und
    Rate-Limit-Gate bis zum Zustelllimit neu zu; kein sofortiges Fail-Mark."""
    state_store, member_path = await _group_state()
    wrongtype = ResponseError("WRONGTYPE Operation against a key holding the wrong kind of value")

    outcome = await _finish_member(broker, backend, config, state_store, member_path, _chord_store(wrongtype))

    assert await _status(state_store) != STATUS_FAILED
    assert outcome is Redelivered


@pytest.mark.befund("F4-11")
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

    outcome = await _finish_member(broker, backend, config, state_store, member_path, _chord_store(cause))

    status = await _status(state_store)
    assert status != STATUS_FAILED, (
        f"Workflow nach {type(cause).__name__} im Chord-Skript fail-markiert, obwohl das Glied gelungen ist "
        "(E-040, workflow_advance_backend_error)"
    )
    assert outcome is Redelivered


async def test_chord_body_runs_exactly_once_after_valkey_returns(broker: Any, backend: Any, config: Any) -> None:
    state_store = InMemoryBackend(FakeClock())
    state_store._connected = True
    workflow = chord(group(MemberTask.s(), MemberTask.s()), MemberTask.s())
    state = build_initial_workflow_state(_WF_ID, workflow.to_dict())
    frontier = initial_frontier(workflow)
    state["step_tasks"] = {path: workflow_step_task_id(_WF_ID, path) for path, _sig in frontier}
    await state_store.store_workflow_state(_WF_ID, encode_json(state), 60_000)
    for path, _sig in frontier:
        await state_store.store_result(workflow_step_task_id(_WF_ID, path), b"ok", 60_000)
    (first, _), (last, _) = frontier
    script = _RecoveringChordScript()
    client = MagicMock()
    client.register_script = MagicMock(return_value=script)
    chord_store = ChordManager(client)
    publisher = _NoopPublisher()

    async def finish(path: str) -> Any:
        return await _finish_member(broker, backend, config, state_store, path, chord_store, publisher)

    await finish(first)
    script.outage = ValkeyConnectionError("Error 111 connecting to valkey:6379. Connection refused.")
    during_outage = await finish(last)
    script.outage = None
    await finish(last)
    await finish(last)  # weitere Zustellung nach dem Abschluss

    assert during_outage is Redelivered
    assert await _status(state_store) != STATUS_FAILED
    assert len(publisher.published) == 1, f"Chord-Body {len(publisher.published)}-mal eingereiht: {publisher.published}"
