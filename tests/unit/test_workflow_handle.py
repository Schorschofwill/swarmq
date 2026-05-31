"""U4: frontier/path navigation + WorkflowHandle behaviour (mocked backend)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.exceptions import (
    ResultNotReadyError,
    TaskCancelledError,
    TaskError,
    WorkflowNotFound,
)
from swarmq.serialization import encode_json, encode_result
from swarmq.task import Task, TaskInfo
from swarmq.workflow import chain, chord, group
from swarmq.workflow_orchestration import (
    STATUS_CANCELLED,
    STATUS_COMPLETED,
    STATUS_FAILED,
    STATUS_RUNNING,
    WorkflowHandle,
    build_initial_workflow_state,
    initial_frontier,
    node_at_path,
)

pytestmark = [pytest.mark.timeout(5)]


class A(Task):
    task_info = TaskInfo(name="a", queues=["default"])

    async def run(self) -> str:
        return "a"


class B(Task):
    task_info = TaskInfo(name="b", queues=["default"])

    async def run(self, x: str = "") -> str:
        return "b"


# --------------------------------------------------------------------------
# initial_frontier + node_at_path
# --------------------------------------------------------------------------


def test_frontier_chain_only_first_step():
    f = initial_frontier(chain(A.s(), B.s()))
    assert [(p, s.task_name) for p, s in f] == [("/0", "a")]


def test_frontier_group_all_members():
    f = initial_frontier(group(A.s(), B.s()))
    assert [(p, s.task_name) for p, s in f] == [("/g0", "a"), ("/g1", "b")]


def test_frontier_chord_header_only():
    f = initial_frontier(chord(group(A.s(), B.s()), B.s()))
    assert [p for p, _ in f] == ["/h/g0", "/h/g1"]


def test_frontier_chain_with_embedded_group_first():
    f = initial_frontier(chain(group(A.s(), A.s()), B.s()))
    assert [p for p, _ in f] == ["/0/g0", "/0/g1"]


def test_node_at_path_roundtrip_chain():
    wf = chain(A.s(), B.s())
    assert node_at_path(wf, "/0").task_name == "a"
    assert node_at_path(wf, "/1").task_name == "b"


def test_node_at_path_chord_header_and_body():
    wf = chord(group(A.s(), B.s()), B.s())
    assert node_at_path(wf, "/h/g0").task_name == "a"
    assert node_at_path(wf, "/b").task_name == "b"


def test_node_at_path_empty_is_root():
    wf = chain(A.s(), B.s())
    assert node_at_path(wf, "") is wf


# --------------------------------------------------------------------------
# WorkflowHandle
# --------------------------------------------------------------------------


def _backend_with_state(state: dict | None) -> MagicMock:
    be = MagicMock()
    raw = encode_json(state) if state is not None else None
    be.get_workflow_state = AsyncMock(return_value=raw)
    be.set_workflow_cancel_flag = AsyncMock(return_value=True)
    be.publish_workflow_notification = AsyncMock()
    be.workflow_notify_channel = MagicMock(return_value="sq:wf:notify:wf-1")
    be.get_result = AsyncMock(return_value=None)
    be.is_workflow_cancelled = AsyncMock(return_value=False)
    be.get_results_batch = AsyncMock(return_value=[])
    return be


def _handle(backend) -> WorkflowHandle:
    from swarmq.client import Client

    return WorkflowHandle(backend, "wf-1", Client._unpack, cancel_ttl_ms=1000)


async def test_status_reads_state():
    state = build_initial_workflow_state("wf-1", {})
    h = _handle(_backend_with_state(state))
    assert await h.status() == STATUS_RUNNING


async def test_status_unknown_raises_not_found():
    h = _handle(_backend_with_state(None))
    with pytest.raises(WorkflowNotFound):
        await h.status()


async def test_cancel_sets_flag_and_notifies():
    be = _backend_with_state(build_initial_workflow_state("wf-1", {}))
    h = _handle(be)
    assert await h.cancel() is True
    be.set_workflow_cancel_flag.assert_awaited_once()
    be.publish_workflow_notification.assert_awaited_once_with("wf-1")


async def test_get_returns_final_result_on_completed():
    state = build_initial_workflow_state("wf-1", {})
    state["status"] = STATUS_COMPLETED
    state["final_result_task_id"] = "task-final"
    be = _backend_with_state(state)
    result_dict = {
        "task": {"id": "task-final", "queue": "default", "name": "b"},
        "is_error": False,
        "is_cancelled": False,
        "error_message": "",
        "result": "the-value",
    }
    be.get_result = AsyncMock(return_value=encode_result(result_dict))
    h = _handle(be)
    assert await h.get(timeout=1.0) == "the-value"


async def test_get_raises_on_failed():
    state = build_initial_workflow_state("wf-1", {})
    state["status"] = STATUS_FAILED
    state["error"] = {"type": "ValueError", "message": "boom"}
    h = _handle(_backend_with_state(state))
    with pytest.raises(TaskError, match="boom"):
        await h.get(timeout=1.0)


async def test_get_raises_on_cancelled():
    state = build_initial_workflow_state("wf-1", {})
    state["status"] = STATUS_CANCELLED
    h = _handle(_backend_with_state(state))
    with pytest.raises(TaskCancelledError):
        await h.get(timeout=1.0)


async def test_get_raises_when_final_result_expired():
    # R6: completed workflow whose final result expired must raise
    # WorkflowResultExpired, not return a misleading None.
    from swarmq.exceptions import WorkflowResultExpired

    state = build_initial_workflow_state("wf-1", {})
    state["status"] = STATUS_COMPLETED
    state["final_result_task_id"] = "task-final"
    be = _backend_with_state(state)
    be.get_result = AsyncMock(return_value=None)  # expired
    h = _handle(be)
    with pytest.raises(WorkflowResultExpired):
        await h.get(timeout=1.0)


async def test_get_times_out_while_running():
    # No pubsub (create_pubsub raises) → poll path → timeout on running state.
    be = _backend_with_state(build_initial_workflow_state("wf-1", {}))
    be.create_pubsub = MagicMock(side_effect=Exception("no pubsub"))
    h = _handle(be)
    with pytest.raises(ResultNotReadyError):
        await h.get(timeout=0.1)


async def test_get_raises_cancelled_when_flag_set_while_running(monkeypatch):
    # R17 liveness: cancel() between steps sets the flag but no advance() has
    # run to write STATUS_CANCELLED. get() must surface it promptly via the
    # cancel-flag check rather than hanging until timeout.
    be = _backend_with_state(build_initial_workflow_state("wf-1", {}))
    be.create_pubsub = MagicMock(side_effect=Exception("no pubsub"))
    be.is_workflow_cancelled = AsyncMock(return_value=True)
    h = _handle(be)
    with pytest.raises(TaskCancelledError):
        await h.get(timeout=5.0)


# --------------------------------------------------------------------------
# children() — R15 read-time per-member introspection
# --------------------------------------------------------------------------


async def test_children_empty_before_any_step():
    h = _handle(_backend_with_state(build_initial_workflow_state("wf-1", {})))
    assert await h.children() == {}


async def test_children_reports_running_and_completed_members():
    state = build_initial_workflow_state("wf-1", {})
    state["step_tasks"] = {"/h/g0": "m0", "/h/g1": "m1"}
    be = _backend_with_state(state)
    # m0 completed (result stored), m1 still running (no result).
    done = encode_result(
        {
            "task": {"id": "m0", "queue": "default", "name": "a"},
            "is_error": False,
            "is_cancelled": False,
            "error_message": "",
            "result": "r0",
        }
    )
    be.get_results_batch = AsyncMock(return_value=[done, None])
    h = _handle(be)
    children = await h.children()
    assert children["/h/g0"] == {"task_id": "m0", "status": "completed", "result": "r0"}
    assert children["/h/g1"] == {"task_id": "m1", "status": "running"}


async def test_children_reports_failed_member():
    state = build_initial_workflow_state("wf-1", {})
    state["step_tasks"] = {"/g0": "m0"}
    be = _backend_with_state(state)
    errored = encode_result(
        {
            "task": {"id": "m0", "queue": "default", "name": "a"},
            "is_error": True,
            "is_cancelled": False,
            "error_message": "boom",
            "result": None,
        }
    )
    be.get_results_batch = AsyncMock(return_value=[errored])
    h = _handle(be)
    children = await h.children()
    assert children["/g0"]["status"] == "failed"
    assert children["/g0"]["task_id"] == "m0"
