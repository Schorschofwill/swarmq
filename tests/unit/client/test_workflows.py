"""Client-Workflow-Tests (Plan U10): Teil-Frontier-Fehler bei ``apply``.

Der Happy-Path von ``apply``/``workflow`` ist als Characterization in
``tests/integration/test_workflow_orchestration.py`` abgedeckt; ``_unpack``
läuft in ``tests/unit/test_workflow_handle.py`` mit. Hier steht das im Plan
(U10) benannte Szenario: ein Publish-Fehler mitten in der initialen Frontier
markiert den Workflow als fehlgeschlagen (statt ihn als verwaistes
``running`` bis zum TTL hängen zu lassen) und reicht den Fehler durch.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from swarmq.client import Client
from swarmq.registry import TaskRegistry
from swarmq.serialization import decode_json, encode_json
from swarmq.task import Task, TaskInfo
from swarmq.workflow import group
from swarmq.workflow_orchestration import STATUS_COMPLETED, STATUS_FAILED, STATUS_RUNNING


class WfA(Task):
    task_info = TaskInfo(name="wf-a", queues=["wfq"])

    async def run(self, x: str) -> str:
        return x


def _client(broker, backend) -> Client:
    registry = TaskRegistry()
    registry.register(WfA)
    return Client(broker=broker, backend=backend, registry=registry)


def _record_cas(backend, results: list[bool]) -> tuple[AsyncMock, list[bytes]]:
    """CAS-Aufrufe aufzeichnen; ``results`` sind die Rueckgaben in Reihenfolge."""
    cas_payloads: list[bytes] = []

    async def _cas(_wf, raw, **kw):
        cas_payloads.append(raw)
        return results.pop(0)

    backend.compare_and_store_workflow_state = AsyncMock(side_effect=_cas)
    return backend.compare_and_store_workflow_state, cas_payloads


async def test_apply_partial_frontier_failure_marks_workflow_failed_via_cas(broker, backend):
    """Publish-Fehler bei Frontier-Mitglied 2/3 -> Status FAILED per CAS
    (R24, KTD11) gegen die Erstanlage ``version: 0`` + Re-Raise."""
    stored: list[bytes] = []
    backend.store_workflow_state = AsyncMock(side_effect=lambda _wf, raw, **kw: stored.append(raw))
    cas, cas_payloads = _record_cas(backend, [True])
    broker.publish = AsyncMock(side_effect=[None, RuntimeError("publish nacked"), None])

    client = _client(broker, backend)
    with pytest.raises(RuntimeError, match="publish nacked"):
        await client.apply(group(WfA.s("a"), WfA.s("b"), WfA.s("c")))

    # Erstanlage: running mit version 0 (blind, store_workflow_state).
    assert len(stored) == 1
    initial = decode_json(stored[0])
    assert initial["status"] == STATUS_RUNNING
    assert initial["version"] == 0
    # Fail-Mark: Compare-and-Set gegen Version 0, Payload version 1.
    assert cas.await_count == 1
    assert cas.await_args.kwargs["expected_version"] == 0
    failed = decode_json(cas_payloads[0])
    assert failed["status"] == STATUS_FAILED
    assert failed["version"] == 1
    assert "initial frontier" in failed["error"]["message"]
    # Nur die Publishes bis zum Fehler sind rausgegangen.
    assert broker.publish.await_count == 2


async def test_apply_fail_mark_conflict_rereads_and_marks_running_state(broker, backend):
    """War ein Advancer schneller (CAS 0), wird der frische Zustand gelesen und
    ein LAUFENDER Zustand erneut als FAILED markiert -- gegen dessen Version."""
    backend.store_workflow_state = AsyncMock()
    cas, cas_payloads = _record_cas(backend, [False, True])
    fresh = {"v": 1, "status": STATUS_RUNNING, "version": 3, "step_tasks": {"/g0": "x", "/g1": "y"}, "error": None}
    backend.get_workflow_state = AsyncMock(return_value=encode_json(fresh))
    broker.publish = AsyncMock(side_effect=RuntimeError("publish nacked"))

    client = _client(broker, backend)
    with pytest.raises(RuntimeError, match="publish nacked"):
        await client.apply(group(WfA.s("a"), WfA.s("b")))

    assert cas.await_count == 2
    assert [c.kwargs["expected_version"] for c in cas.await_args_list] == [0, 3]
    marked = decode_json(cas_payloads[1])
    assert marked["status"] == STATUS_FAILED
    assert marked["version"] == 4
    assert marked["step_tasks"] == {"/g0": "x", "/g1": "y"}  # frischer Bestand, nicht der lokale
    assert "initial frontier" in marked["error"]["message"]


async def test_apply_fail_mark_conflict_leaves_terminal_state_alone(broker, backend):
    """Ist der frische Zustand bereits terminal, wird nichts mehr geschrieben."""
    backend.store_workflow_state = AsyncMock()
    cas, _payloads = _record_cas(backend, [False])
    fresh = {"v": 1, "status": STATUS_COMPLETED, "version": 2, "step_tasks": {}, "error": None}
    backend.get_workflow_state = AsyncMock(return_value=encode_json(fresh))
    broker.publish = AsyncMock(side_effect=RuntimeError("publish nacked"))

    client = _client(broker, backend)
    with pytest.raises(RuntimeError, match="publish nacked"):
        await client.apply(group(WfA.s("a"), WfA.s("b")))

    assert cas.await_count == 1
    backend.store_workflow_state.assert_awaited_once()  # nur die Erstanlage


async def test_apply_frontier_failure_fail_mark_is_best_effort(broker, backend):
    """Schlägt auch das Fail-Mark fehl, gewinnt der ursprüngliche Fehler."""
    backend.store_workflow_state = AsyncMock()
    backend.compare_and_store_workflow_state = AsyncMock(side_effect=RuntimeError("backend down"))
    broker.publish = AsyncMock(side_effect=RuntimeError("publish nacked"))

    client = _client(broker, backend)
    with pytest.raises(RuntimeError, match="publish nacked"):
        await client.apply(group(WfA.s("a"), WfA.s("b")))
