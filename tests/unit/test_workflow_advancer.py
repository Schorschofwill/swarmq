"""U5: WorkflowAdvancer DAG logic, driven by an in-memory fake worker.

These tests exercise the full advancement recursion — chain progression
with result injection, immutable suppression, chord fan-in with ordered
results, standalone group aggregation, and the cancel gate — without any
broker/backend infrastructure. A tiny driver loop plays the role of the
worker: it drains published step messages, "runs" each (echoing a result),
stores the result, and calls advance() with the step's path.
"""

from __future__ import annotations

import pytest

from swarmq.serialization import decode_json, decode_result, encode_json, encode_result
from swarmq.task import Task, TaskInfo
from swarmq.workflow import chain, chord, group
from swarmq.workflow_orchestration import (
    STATUS_CANCELLED,
    STATUS_COMPLETED,
    WorkflowAdvancer,
    build_initial_workflow_state,
    initial_frontier,
    workflow_step_task_id,
)

pytestmark = [pytest.mark.timeout(5)]


class Echo(Task):
    task_info = TaskInfo(name="echo", queues=["default"])

    async def run(self, *args) -> list:
        return list(args)


class FakeBackend:
    def __init__(self) -> None:
        self.wf_state: dict[str, bytes] = {}
        self.results: dict[str, bytes] = {}
        self.cancelled: set[str] = set()
        self.notified: list[str] = []

    async def get_workflow_state(self, wf_id, op_timeout=None):
        return self.wf_state.get(wf_id)

    async def store_workflow_state(self, wf_id, data, ttl_ms, op_timeout=None):
        self.wf_state[wf_id] = data

    async def get_result(self, task_id):
        return self.results.get(task_id)

    async def get_results_batch(self, task_ids):
        return [self.results.get(tid) for tid in task_ids]

    async def store_result(self, task_id, data, ttl_ms, op_timeout=None):
        self.results[task_id] = data

    async def is_workflow_cancelled(self, wf_id, op_timeout=None):
        return wf_id in self.cancelled

    async def publish_workflow_notification(self, wf_id):
        self.notified.append(wf_id)


class FakeChord:
    """In-memory equivalent of chord_complete.lua: a distinct-member set
    keyed by (wf_id, chord_id). Mirrors the post-fix Lua contract — returns
    True on EVERY call once the set has reached N (no internal one-shot
    flag); the real one-shot is the advancer's publish-then-mark
    step_tasks["/b"] marker."""

    def __init__(self) -> None:
        self._sets: dict[tuple[str, str], set[str]] = {}

    async def record_and_check_complete(self, wf_id, chord_id, member_id, n, ttl):
        key = (wf_id, chord_id)
        s = self._sets.setdefault(key, set())
        s.add(member_id)
        return len(s) == n


def _unpack(raw: bytes):
    data = decode_result(raw)
    return data["result"]


async def _noop() -> None:
    return None


class Driver:
    """Plays the worker: builds a workflow, seeds initial frontier, then
    drains published step messages — running each, storing its result, and
    advancing the workflow — until no work remains."""

    def __init__(self, workflow):
        self.backend = FakeBackend()
        self.chord = FakeChord()
        self.published: list[tuple[str, bytes, dict]] = []
        self.wf_id = "wf-test"
        self.advancer = WorkflowAdvancer(
            self.backend,
            self.chord,
            publish=self._publish,
            resolve_queue=lambda name: "default",
            make_task_id=workflow_step_task_id,
            unpack=_unpack,
            ttl_ms=60_000,
        )
        self.workflow = workflow

    async def _publish(self, queue, body, headers):
        self.published.append((queue, body, headers))

    async def _run_step(self, body: bytes) -> str:
        msg = decode_json(body)
        task_id = msg["task"]["id"]
        # Echo task: result is the arg list (captures injected predecessor).
        result_value = list(msg["args"])
        self.backend.results[task_id] = encode_result(
            {
                "task": msg["task"],
                "is_error": False,
                "is_cancelled": False,
                "error_message": "",
                "result": result_value,
            }
        )
        return task_id

    async def seed(self):
        state = build_initial_workflow_state(self.wf_id, self.workflow.to_dict())
        step_tasks = {}
        for step_path, _sig in initial_frontier(self.workflow):
            tid = f"seed-{step_path}"
            step_tasks[step_path] = tid
        state["step_tasks"] = step_tasks
        self.backend.wf_state[self.wf_id] = encode_json(state)
        # "Publish" the initial frontier and run it through the driver.
        for step_path, sig in initial_frontier(self.workflow):
            tid = step_tasks[step_path]
            self.backend.results[tid] = encode_result(
                {
                    "task": {"id": tid, "queue": "default", "name": sig.task_name},
                    "is_error": False,
                    "is_cancelled": False,
                    "error_message": "",
                    "result": list(sig.args),
                }
            )
            await self.advancer.advance(self.wf_id, step_path, tid, list(sig.args))

    async def drain(self):
        # Process published messages FIFO until none remain.
        while self.published:
            _queue, body, headers = self.published.pop(0)
            step_path = headers["x-swarmq-workflow-step"]
            task_id = await self._run_step(body)
            result_value = decode_result(self.backend.results[task_id])["result"]
            await self.advancer.advance(self.wf_id, step_path, task_id, result_value)

    async def state(self) -> dict:
        return decode_json(self.backend.wf_state[self.wf_id])

    async def final_value(self):
        st = await self.state()
        fid = st["final_result_task_id"]
        if fid is None:
            return None
        return decode_result(self.backend.results[fid])["result"]


# --------------------------------------------------------------------------
# Chain advancement — R4
# --------------------------------------------------------------------------


async def test_chain_advances_and_completes():
    d = Driver(chain(Echo.s("a"), Echo.s(), Echo.s()))
    await d.seed()
    await d.drain()
    st = await d.state()
    assert st["status"] == STATUS_COMPLETED
    # Each step enqueued exactly once: paths /0, /1, /2.
    assert set(st["step_tasks"].keys()) == {"/0", "/1", "/2"}


async def test_chain_injects_predecessor_result():
    # Echo returns its args; step 2 should receive step 1's result as arg0.
    d = Driver(chain(Echo.s("seed"), Echo.s()))
    await d.seed()
    await d.drain()
    final = await d.final_value()
    # step1 result = ["seed"]; injected into step2 → step2 result = [["seed"]].
    assert final == [["seed"]]


async def test_chain_immutable_suppresses_injection():
    d = Driver(chain(Echo.s("seed"), Echo.si("fixed")))
    await d.seed()
    await d.drain()
    final = await d.final_value()
    # immutable step keeps its own args only — no predecessor injection.
    assert final == ["fixed"]


async def test_cancel_gate_stops_advancement():
    d = Driver(chain(Echo.s("a"), Echo.s(), Echo.s()))
    await d.seed()
    d.backend.cancelled.add(d.wf_id)  # cancel before draining
    await d.drain()
    st = await d.state()
    assert st["status"] == STATUS_CANCELLED


# --------------------------------------------------------------------------
# Chord & group fan-in — R5, R6
# --------------------------------------------------------------------------


async def test_chord_fires_body_with_ordered_results():
    d = Driver(chord(group(Echo.s("a"), Echo.s("b"), Echo.s("c")), Echo.s()))
    await d.seed()
    await d.drain()
    st = await d.state()
    assert st["status"] == STATUS_COMPLETED
    final = await d.final_value()
    # Body receives the ordered list of header results as its injected arg0;
    # Echo wraps args in a list → [[ ["a"], ["b"], ["c"] ]].
    assert final == [[["a"], ["b"], ["c"]]]


async def test_standalone_group_completes_with_aggregate():
    d = Driver(group(Echo.s("x"), Echo.s("y")))
    await d.seed()
    await d.drain()
    st = await d.state()
    assert st["status"] == STATUS_COMPLETED
    final = await d.final_value()
    assert final == [["x"], ["y"]]


async def test_expired_member_result_raises_workflow_result_expired():
    # R6: a chord fan-in where one (completed) member's result has expired
    # must raise WorkflowResultExpired, not silently pass None to the body.
    from swarmq.exceptions import WorkflowResultExpired

    be = FakeBackend()
    ch = FakeChord()
    adv = WorkflowAdvancer(
        be,
        ch,
        publish=lambda *a: _noop(),
        resolve_queue=lambda name: "default",
        make_task_id=lambda wf_id, sp: "t-x",
        unpack=_unpack,
        ttl_ms=60_000,
    )
    wf = chord(group(Echo.s("a"), Echo.s("b")), Echo.s())
    state = build_initial_workflow_state("wf", wf.to_dict())
    state["step_tasks"] = {"/h/g0": "m0", "/h/g1": "m1"}
    be.wf_state["wf"] = encode_json(state)
    # Only m1's result exists; m0 expired before fan-in.
    be.results["m1"] = encode_result(
        {
            "task": {"id": "m1", "queue": "default", "name": "echo"},
            "is_error": False,
            "is_cancelled": False,
            "error_message": "",
            "result": ["b"],
        }
    )
    await adv.advance("wf", "/h/g0", "m0", ["a"])  # 1/2 — no gather yet
    with pytest.raises(WorkflowResultExpired):
        await adv.advance("wf", "/h/g1", "m1", ["b"])  # 2/2 — gathers, m0 gone


async def test_chain_with_embedded_chord():
    d = Driver(
        chain(
            Echo.s("start"),
            chord(group(Echo.s("a"), Echo.s("b")), Echo.s()),
            Echo.s(),
        )
    )
    await d.seed()
    await d.drain()
    st = await d.state()
    assert st["status"] == STATUS_COMPLETED


# --------------------------------------------------------------------------
# Redelivery idempotency — the publish-then-mark step_tasks marker is the
# real one-shot (chord_complete.lua no longer holds a one-shot flag).
# --------------------------------------------------------------------------


async def test_chain_redelivery_does_not_double_enqueue():
    # AT_LEAST_ONCE: a redelivered chain step must not enqueue its successor
    # twice — the persisted next-enqueued marker (step_tasks) suppresses it.
    d = Driver(chain(Echo.s("a"), Echo.s()))
    await d.seed()  # advancing /0 enqueues /1
    assert len(d.published) == 1
    seed_tid = (await d.state())["step_tasks"]["/0"]
    await d.advancer.advance(d.wf_id, "/0", seed_tid, ["a"])  # redelivery
    assert len(d.published) == 1  # successor still enqueued exactly once


async def test_chord_member_redelivery_enqueues_body_once():
    # The chord Lua now reports "complete" on every post-completion call;
    # the body must still be enqueued exactly once, guarded by step_tasks["/b"].
    d = Driver(chord(group(Echo.s("a"), Echo.s("b")), Echo.s()))
    await d.seed()  # both header members advanced → body enqueued once

    def _body_msgs():
        return [p for p in d.published if p[2]["x-swarmq-workflow-step"] == "/b"]

    assert len(_body_msgs()) == 1
    last_tid = (await d.state())["step_tasks"]["/h/g1"]
    # Redeliver the last header member: FakeChord reports complete=True again.
    await d.advancer.advance(d.wf_id, "/h/g1", last_tid, ["b"])
    assert len(_body_msgs()) == 1  # marker prevents a second body enqueue


async def test_workflow_step_task_id_is_deterministic():
    # Determinism is the property that keeps a re-enqueued member's id stable
    # so the chord SADD set cannot be inflated past N under a lost-persist crash.
    assert workflow_step_task_id("wf-1", "/h/g0") == workflow_step_task_id(
        "wf-1", "/h/g0"
    )
    assert workflow_step_task_id("wf-1", "/0") != workflow_step_task_id("wf-1", "/1")
