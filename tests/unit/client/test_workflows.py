"""Client-Workflow-Tests (Plan U10): Teil-Frontier-Fehler bei ``apply``.

Der Happy-Path von ``apply``/``workflow`` ist als Characterization in
``tests/integration/test_workflow_orchestration.py`` abgedeckt; ``_unpack``
läuft in ``tests/unit/test_workflow_handle.py`` mit. Hier steht das im Plan
(U10) benannte Szenario: ein Publish-Fehler mitten in der initialen Frontier
markiert den Workflow als fehlgeschlagen (statt ihn als verwaistes
``running`` bis zum TTL hängen zu lassen) und reicht den Fehler durch.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import override
from unittest.mock import AsyncMock

import pytest

from swarmq.client import Client
from swarmq.exceptions import TaskCancelledError
from swarmq.headers import HEADER_ENQUEUED_AT, HEADER_WORKFLOW_ID, HEADER_WORKFLOW_STEP
from swarmq.middleware import Middleware, MiddlewareChain
from swarmq.registry import TaskRegistry
from swarmq.serialization import decode_json, encode_json, encode_result
from swarmq.task import Task, TaskInfo
from swarmq.workflow import group
from swarmq.workflow_orchestration import STATUS_COMPLETED, STATUS_FAILED, STATUS_RUNNING


class WfA(Task):
    task_info = TaskInfo(name="wf-a", queues=["wfq"])

    @override
    async def run(self, x: str) -> str:
        return x


def _client(broker, backend, middleware=None) -> Client:
    registry = TaskRegistry()
    registry.register(WfA)
    chain = MiddlewareChain(list(middleware or []))
    return Client(broker=broker, backend=backend, registry=registry, middleware_chain=chain)


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
    assert cas.await_args is not None
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


class _EnqueueSpy(Middleware):
    """Protokolliert den Enqueue-Pfad der Anfangsfront (U11, R18)."""

    def __init__(self, calls: list[str]) -> None:
        self.calls = calls

    @override
    async def pre_enqueue(self, message) -> None:
        self.calls.append(f"pre:{message['step_path']}:{message['delivery_mode']}")

    @override
    async def post_enqueue(self, message) -> None:
        self.calls.append(f"post:{message['step_path']}")


async def test_apply_runs_pre_enqueue_for_the_whole_frontier_before_persisting(broker, backend):
    """R18: ``pre_enqueue`` laeuft fuer JEDEN Schritt der Anfangsfront, bevor
    der Zustand persistiert wird — lehnt eine Middleware ab, gibt es keinen
    verwaisten ``running``-Zustand und keine Nachricht."""
    calls: list[str] = []
    backend.store_workflow_state = AsyncMock(side_effect=lambda *a, **k: calls.append("store"))
    broker.publish = AsyncMock(side_effect=lambda *a, **k: calls.append("publish"))
    client = _client(broker, backend, [_EnqueueSpy(calls)])

    await client.apply(group(WfA.s("a"), WfA.s("b")))

    assert calls[:3] == ["pre:/g0", "pre:/g1", "store"] or calls[:3] == [
        "pre:/g0:at_least_once",
        "pre:/g1:at_least_once",
        "store",
    ]


async def test_apply_emits_both_signals_per_step_and_no_pseudo_event(broker, backend, monkeypatch):
    """R16/R18: Je Frontschritt feuern ``TASK_ENQUEUED`` (unveraenderter
    Payload) und ``WORKFLOW_STEP_ENQUEUED`` (mit Workflow-Identitaet). Das
    Pseudo-Ereignis mit ``task_name="<workflow>"`` entfaellt — es trug eine
    Workflow-Kennung im ``task_id``-Feld und keinen echten Task."""
    events: list[tuple[str, str, str]] = []
    backend.store_workflow_state = AsyncMock()
    broker.publish = AsyncMock()
    client = _client(broker, backend)

    async def _dispatch(signal, **kwargs):
        events.append((signal.value, kwargs.get("task_name", "-"), kwargs.get("step_path", "-")))

    monkeypatch.setattr(client._signal_dispatcher, "dispatch", AsyncMock(side_effect=_dispatch))

    await client.apply(group(WfA.s("a"), WfA.s("b")))

    assert events == [
        ("task_enqueued", "wf-a", "-"),
        ("workflow_step_enqueued", "wf-a", "/g0"),
        ("task_enqueued", "wf-a", "-"),
        ("workflow_step_enqueued", "wf-a", "/g1"),
    ]


async def test_apply_rejected_by_middleware_persists_nothing(broker, backend):
    """R18: Wirft ``pre_enqueue``, ist der Workflow nie entstanden — kein
    Zustand, keine Nachricht, die Ausnahme erreicht den Aufrufer."""

    class Rejecting(Middleware):
        @override
        async def pre_enqueue(self, message) -> None:
            raise PermissionError("quota exceeded")

    backend.store_workflow_state = AsyncMock()
    broker.publish = AsyncMock()
    client = _client(broker, backend, [Rejecting()])

    with pytest.raises(PermissionError, match="quota exceeded"):
        await client.apply(group(WfA.s("a"), WfA.s("b")))

    backend.store_workflow_state.assert_not_awaited()
    broker.publish.assert_not_awaited()


# E-020 (Nachtrag 2026-10-17, F4-03): Cancel-Flag-TTL der Workflow-Handles des
# reinen Clients.
# Testliste:
# [x] apply mit state_ttl_ms über 24 h: Flag lebt state_ttl_ms
# [x] apply mit state_ttl_ms unter 24 h: Flag 24 h, Zustands-TTL bleibt state_ttl_ms
# [x] workflow ohne cancel_ttl_ms: Flag 24 h
# [x] workflow mit cancel_ttl_ms: Flag lebt cancel_ttl_ms

_DAY_MS = 86_400_000


def _wf_backend(backend) -> None:
    backend.store_workflow_state = AsyncMock()
    backend.get_workflow_state = AsyncMock(return_value=encode_json({"status": STATUS_RUNNING, "version": 0}))
    backend.set_workflow_cancel_flag = AsyncMock(return_value=True)
    backend.notify_workflow = AsyncMock()


def _flag_ttl_ms(backend) -> int:
    call = backend.set_workflow_cancel_flag.await_args
    assert call is not None
    return int(call.kwargs["ttl_ms"])


@pytest.mark.entscheidung("E-020")
@pytest.mark.timeout(5)
async def test_apply_cancel_flag_follows_state_ttl_above_24h(broker, backend):
    _wf_backend(backend)
    client = _client(broker, backend)

    handle = await client.apply(group(WfA.s("a"), WfA.s("b")), state_ttl_ms=2 * _DAY_MS)
    await handle.cancel()

    assert _flag_ttl_ms(backend) == 2 * _DAY_MS


@pytest.mark.entscheidung("E-020")
@pytest.mark.timeout(5)
async def test_apply_cancel_flag_lasts_24h_with_shorter_state_ttl(broker, backend):
    _wf_backend(backend)
    client = _client(broker, backend)

    handle = await client.apply(group(WfA.s("a"), WfA.s("b")), state_ttl_ms=60_000)
    await handle.cancel()

    assert _flag_ttl_ms(backend) == _DAY_MS
    store_call = backend.store_workflow_state.await_args
    assert store_call is not None
    assert store_call.kwargs["ttl_ms"] == 60_000


@pytest.mark.entscheidung("E-020")
@pytest.mark.timeout(5)
async def test_reattached_handle_cancel_flag_defaults_to_24h(broker, backend):
    _wf_backend(backend)
    client = _client(broker, backend)

    handle = await client.workflow("wf-x")
    await handle.cancel()

    assert _flag_ttl_ms(backend) == _DAY_MS


@pytest.mark.entscheidung("E-020")
@pytest.mark.timeout(5)
async def test_reattached_handle_cancel_flag_uses_given_ttl(broker, backend):
    _wf_backend(backend)
    client = _client(broker, backend)

    handle = await client.workflow("wf-x", cancel_ttl_ms=7 * _DAY_MS)
    await handle.cancel()

    assert _flag_ttl_ms(backend) == 7 * _DAY_MS


# ---------------------------------------------------------------------------
# Testlücken Runde 4, Paket U5 (Plan 2026-10-20-1500): ``apply``, ``workflow``
# und die Fehlmarkierung argumentgenau; ``_unpack`` über ``WorkflowHandle``.
# Testliste:
# [x] E-7: apply publiziert je Frontschritt Queue, Body (task_id, args,
#     kwargs) und Workflow-Header; step_tasks im Zustand passen dazu
# [x] E-7: apply bindet die Erstanlage an backend_op_timeout; jede apply
#     erzeugt eine eigene workflow_id
# [x] E-8: workflow ohne Zustand -> WorkflowNotFound
# [x] E-8: workflow liest mit backend_op_timeout, Handle trägt die ID
# [x] E-9: Fehlmarkierung trägt den Ausnahmetyp, CAS mit state_ttl_ms und
#     backend_op_timeout
# [x] E-9: Konflikt-Neulesen mit backend_op_timeout
# [x] E-19: abgebrochenes Endergebnis ohne ``task`` -> TaskCancelledError
# ---------------------------------------------------------------------------

_OP_TIMEOUT = 2.5


def _timed_client(broker, backend) -> Client:
    """Client mit ``backend_op_timeout``, damit die Weitergabe sichtbar ist."""
    registry = TaskRegistry()
    registry.register(WfA)
    return Client(broker=broker, backend=backend, registry=registry, backend_op_timeout=_OP_TIMEOUT)


@pytest.mark.timeout(5)
async def test_apply_publishes_each_frontier_step_with_body_and_workflow_headers(broker, backend, monkeypatch):
    """E-7: Queue, Body und Header jedes Frontschritts sowie ``step_tasks``.

    Gegenproben: ``workflows.py:117`` ``kwargs=signature.kwargs`` →
    ``kwargs={}`` und ``:120`` ``HEADER_WORKFLOW_STEP: step_path`` →
    ``: None`` sind rot.
    """
    from swarmq.client import workflows as workflows_mod

    monkeypatch.setattr(workflows_mod, "time", SimpleNamespace(time=lambda: 1_700_000_000.7))
    stored: list[bytes] = []
    backend.store_workflow_state = AsyncMock(side_effect=lambda _wf, raw, **_kw: stored.append(raw))
    client = _client(broker, backend)

    handle = await client.apply(group(WfA.s("a"), WfA.s(x="b")))

    state = decode_json(stored[0])
    published = [
        (call.args[0], decode_json(call.args[1]), call.kwargs["headers"]) for call in broker.publish.await_args_list
    ]
    assert [(queue, body["args"], body["kwargs"]) for queue, body, _ in published] == [
        ("wfq", ["a"], {}),
        ("wfq", [], {"x": "b"}),
    ]
    assert [body["task"]["name"] for _, body, _ in published] == ["wf-a", "wf-a"]
    assert [headers for _, _, headers in published] == [
        {HEADER_WORKFLOW_ID: handle.workflow_id, HEADER_WORKFLOW_STEP: "/g0", HEADER_ENQUEUED_AT: 1_700_000_000},
        {HEADER_WORKFLOW_ID: handle.workflow_id, HEADER_WORKFLOW_STEP: "/g1", HEADER_ENQUEUED_AT: 1_700_000_000},
    ]
    assert state["step_tasks"] == {
        "/g0": published[0][1]["task"]["id"],
        "/g1": published[1][1]["task"]["id"],
    }


@pytest.mark.timeout(5)
async def test_apply_bounds_the_state_write_and_mints_a_fresh_workflow_id(broker, backend):
    """E-7: Die Erstanlage läuft unter ``backend_op_timeout``; zwei ``apply``
    ergeben zwei verschiedene Workflow-IDs.

    Gegenprobe: ``workflows.py:139`` ``op_timeout=self._backend_op_timeout``
    → ``op_timeout=None`` ist rot.
    """
    backend.store_workflow_state = AsyncMock()
    client = _timed_client(broker, backend)

    first = await client.apply(WfA.s("a"), state_ttl_ms=60_000)
    second = await client.apply(WfA.s("a"), state_ttl_ms=60_000)

    calls = backend.store_workflow_state.await_args_list
    assert [(c.args[0], c.kwargs["ttl_ms"], c.kwargs["op_timeout"]) for c in calls] == [
        (first.workflow_id, 60_000, _OP_TIMEOUT),
        (second.workflow_id, 60_000, _OP_TIMEOUT),
    ]
    assert first.workflow_id != second.workflow_id


@pytest.mark.timeout(5)
async def test_workflow_without_state_raises_workflow_not_found(broker, backend):
    """E-8: Ohne Zustand gibt es keinen Handle, sondern ``WorkflowNotFound``.

    Gegenprobe: ``workflows.py:201`` ``if raw is None:`` → ``if False:`` ist rot.
    """
    from swarmq.exceptions import WorkflowNotFound

    backend.get_workflow_state = AsyncMock(return_value=None)
    client = _client(broker, backend)

    with pytest.raises(WorkflowNotFound, match="wf-gone"):
        await client.workflow("wf-gone")


@pytest.mark.timeout(5)
async def test_workflow_reads_state_within_op_timeout_and_returns_handle_for_the_id(broker, backend):
    """E-8: Das Lesen läuft unter ``backend_op_timeout``; der Handle trägt die
    angefragte ID.

    Gegenproben: ``workflows.py:200`` ``op_timeout=self._backend_op_timeout``
    → ``None`` und ``:207`` ``workflow_id`` (Argument des Handles) → ``None``
    sind rot.
    """
    backend.get_workflow_state = AsyncMock(return_value=encode_json({"status": STATUS_RUNNING, "version": 0}))
    client = _timed_client(broker, backend)

    handle = await client.workflow("wf-x")

    backend.get_workflow_state.assert_awaited_once_with("wf-x", op_timeout=_OP_TIMEOUT)
    assert handle.workflow_id == "wf-x"


@pytest.mark.timeout(5)
async def test_apply_fail_mark_names_the_error_type_and_keeps_ttl_and_op_timeout(broker, backend):
    """E-9: Die Fehlmarkierung trägt den Ausnahmetyp; der CAS läuft mit der
    Zustands-TTL und unter ``backend_op_timeout``.

    Gegenproben: ``workflows.py:186`` ``ttl_ms=state_ttl_ms`` →
    ``ttl_ms=None`` und ``:169`` ``"type"`` → ``"XXtypeXX"`` sind rot.
    """
    backend.store_workflow_state = AsyncMock()
    cas, cas_payloads = _record_cas(backend, [True])
    broker.publish = AsyncMock(side_effect=ConnectionResetError("publish lost"))
    client = _timed_client(broker, backend)

    with pytest.raises(ConnectionResetError):
        await client.apply(group(WfA.s("a"), WfA.s("b")), state_ttl_ms=60_000)

    assert cas.await_args is not None
    assert (cas.await_args.kwargs["ttl_ms"], cas.await_args.kwargs["op_timeout"]) == (60_000, _OP_TIMEOUT)
    assert decode_json(cas_payloads[0])["error"] == {
        "type": "ConnectionResetError",
        "message": "apply() failed to enqueue the initial frontier: publish lost",
    }


@pytest.mark.timeout(5)
async def test_apply_fail_mark_rereads_state_within_op_timeout(broker, backend):
    """E-9: Nach verlorenem CAS liest die Fehlmarkierung den frischen Zustand
    unter ``backend_op_timeout`` und schreibt mit derselben Frist.

    Gegenprobe: ``workflows.py:175`` ``op_timeout=self._backend_op_timeout``
    → ``op_timeout=None`` ist rot.
    """
    backend.store_workflow_state = AsyncMock()
    cas, _payloads = _record_cas(backend, [False, True])
    fresh = {"v": 1, "status": STATUS_RUNNING, "version": 3, "step_tasks": {}, "error": None}
    backend.get_workflow_state = AsyncMock(return_value=encode_json(fresh))
    broker.publish = AsyncMock(side_effect=RuntimeError("publish nacked"))
    client = _timed_client(broker, backend)

    with pytest.raises(RuntimeError, match="publish nacked"):
        await client.apply(group(WfA.s("a"), WfA.s("b")))

    assert backend.store_workflow_state.await_args is not None
    wf_id = backend.store_workflow_state.await_args.args[0]
    backend.get_workflow_state.assert_awaited_once_with(wf_id, op_timeout=_OP_TIMEOUT)
    assert [c.kwargs["op_timeout"] for c in cas.await_args_list] == [_OP_TIMEOUT, _OP_TIMEOUT]


@pytest.mark.timeout(5)
async def test_handle_get_of_cancelled_final_result_without_task_raises_task_cancelled(broker, backend):
    """E-19: Ein abgebrochenes Endergebnis ohne ``task``-Feld meldet
    ``TaskCancelledError`` ohne ID statt eines ``AttributeError``.

    Gegenprobe: ``workflows.py:224`` ``data.get("task", {})`` →
    ``data.get("task", None)`` ist rot (``AttributeError``).
    """
    completed = {"status": STATUS_COMPLETED, "version": 2, "final_result_task_id": "t-last"}
    backend.get_workflow_state = AsyncMock(return_value=encode_json(completed))
    backend.get_result = AsyncMock(
        return_value=encode_result({"result": None, "is_error": False, "is_cancelled": True})
    )
    client = _client(broker, backend)
    handle = await client.workflow("wf-x")

    with pytest.raises(TaskCancelledError) as excinfo:
        await handle.get(timeout=1)

    assert excinfo.value.task_id is None
