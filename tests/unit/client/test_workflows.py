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
from swarmq.serialization import decode_json
from swarmq.task import Task, TaskInfo
from swarmq.workflow import group
from swarmq.workflow_orchestration import STATUS_FAILED, STATUS_RUNNING


class WfA(Task):
    task_info = TaskInfo(name="wf-a", queues=["wfq"])

    async def run(self, x: str) -> str:
        return x


def _client(broker, backend) -> Client:
    registry = TaskRegistry()
    registry.register(WfA)
    return Client(broker=broker, backend=backend, registry=registry)


async def test_apply_partial_frontier_failure_marks_workflow_failed(broker, backend):
    """Publish-Fehler bei Frontier-Mitglied 2/3 → Status FAILED + Re-Raise."""
    stored: list[bytes] = []
    backend.store_workflow_state = AsyncMock(side_effect=lambda _wf, raw, **kw: stored.append(raw))
    broker.publish = AsyncMock(side_effect=[None, RuntimeError("publish nacked"), None])

    client = _client(broker, backend)
    with pytest.raises(RuntimeError, match="publish nacked"):
        await client.apply(group(WfA.s("a"), WfA.s("b"), WfA.s("c")))

    # Erster Store: running (vor der Frontier); zweiter Store: Fail-Mark.
    assert len(stored) == 2
    assert decode_json(stored[0])["status"] == STATUS_RUNNING
    failed = decode_json(stored[1])
    assert failed["status"] == STATUS_FAILED
    assert "initial frontier" in failed["error"]["message"]
    # Nur die Publishes bis zum Fehler sind rausgegangen.
    assert broker.publish.await_count == 2


async def test_apply_frontier_failure_fail_mark_is_best_effort(broker, backend):
    """Schlägt auch das Fail-Mark fehl, gewinnt der ursprüngliche Fehler."""
    backend.store_workflow_state = AsyncMock(side_effect=[None, RuntimeError("backend down")])
    broker.publish = AsyncMock(side_effect=RuntimeError("publish nacked"))

    client = _client(broker, backend)
    with pytest.raises(RuntimeError, match="publish nacked"):
        await client.apply(group(WfA.s("a"), WfA.s("b")))
