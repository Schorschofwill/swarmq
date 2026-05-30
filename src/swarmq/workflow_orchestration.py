"""Worker-side workflow orchestration helpers (U3, U5, U6, U7).

This module holds the worker-side machinery that advances workflows —
distinct from the infrastructure-free topology layer in ``workflow.py``
(client/producer side). The :class:`ChordManager` wraps the atomic
``chord_complete.lua`` script; the advancement and depth-guard helpers
added in later units live here too.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from swarmq.exceptions import (
    ResultNotReadyError,
    TaskCancelledError,
    TaskError,
    WorkflowNotFound,
    WorkflowValidationError,
)
from swarmq.lua import load_script
from swarmq.serialization import decode_json, encode_json, encode_result
from swarmq.workflow import Chain, Chord, Group, Signature, WorkflowNode

if TYPE_CHECKING:
    import valkey.asyncio

    from swarmq.backend.valkey import ValkeyBackend


# --------------------------------------------------------------------------
# Workflow state schema (pinned contract — shared by U4 apply, U5 advance,
# U6 fail-mark). Persisted JSON under sq:wf:{workflow_id}.
# --------------------------------------------------------------------------

WORKFLOW_STATE_VERSION = 1

STATUS_RUNNING = "running"
STATUS_COMPLETED = "completed"
STATUS_FAILED = "failed"
STATUS_CANCELLED = "cancelled"

_TERMINAL_STATUSES = frozenset({STATUS_COMPLETED, STATUS_FAILED, STATUS_CANCELLED})


def build_initial_workflow_state(
    workflow_id: str, topology: dict[str, Any]
) -> dict[str, Any]:
    """The state dict written at apply() time.

    Fields:
      * ``status`` — running -> completed/failed/cancelled (terminal).
      * ``topology`` — the full ``node.to_dict()`` DAG, so any worker can
        navigate it without the client.
      * ``final_result_task_id`` — set by the worker (U5) when the terminal
        step's result is stored; ``handle.get()`` resolves the value from it.
      * ``error`` — ``{"type", "message"}`` set by the fail path (U6).
      * ``members`` — chord-path -> ``{"task_ids": [...ordered...],
        "expected_n": N}``; the worker records ordered member task ids so
        fan-in collects results in definition order (R5).
    """
    return {
        "v": WORKFLOW_STATE_VERSION,
        "workflow_id": workflow_id,
        "status": STATUS_RUNNING,
        "topology": topology,
        "final_result_task_id": None,
        "error": None,
        # step path -> task_id for every enqueued step. apply() seeds the
        # initial frontier; the worker (U5) appends as it advances.
        "step_tasks": {},
        "members": {},
    }


class WorkflowHandle:
    """Reattachable handle to a fire-and-forget workflow (R13, R14, R15).

    Constructed by ``Client.apply()`` and reconstructed by
    ``Client.workflow(workflow_id)``. Carries no in-process workflow state
    beyond the id — everything is read from Valkey, so the handle survives
    the originating client process (R10).
    """

    def __init__(
        self,
        backend: ValkeyBackend,
        workflow_id: str,
        unpack: Callable[[bytes], Any],
        cancel_ttl_ms: int,
    ) -> None:
        self._backend = backend
        self.workflow_id = workflow_id
        self._unpack = unpack
        self._cancel_ttl_ms = cancel_ttl_ms

    async def _load_state(self) -> dict[str, Any]:
        raw = await self._backend.get_workflow_state(self.workflow_id)
        if raw is None:
            raise WorkflowNotFound(self.workflow_id)
        return decode_json(raw)  # type: ignore[no-any-return]

    async def status(self) -> str:
        state = await self._load_state()
        return str(state["status"])

    async def children(self) -> dict[str, Any]:
        """Per-member status/result reconciliation (R15). Returns the
        ``members`` map from state (chord-path -> ordered member task ids +
        expected count); empty for a plain chain."""
        state = await self._load_state()
        return dict(state.get("members", {}))

    async def cancel(self) -> bool:
        """Set the workflow-level cancel flag and wake any waiter (R17)."""
        newly = await self._backend.set_workflow_cancel_flag(
            self.workflow_id, ttl_ms=self._cancel_ttl_ms
        )
        await self._backend.publish_workflow_notification(self.workflow_id)
        return newly

    async def get(self, timeout: float = 30.0) -> Any:
        """Block until the workflow reaches a terminal status, then return
        the final result (R13). Raises on failed/cancelled; raises
        ResultNotReadyError on timeout.

        Waits on the stable workflow notification channel (woken by U5
        completion / U6 fail-mark) with a state-key poll fallback — the
        notification is best-effort, the poll guarantees liveness."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        channel = self._backend.workflow_notify_channel(self.workflow_id)

        # valkey's PubSub is untyped; keep it Any so strict mode doesn't flag
        # the get_message/aclose calls as untyped-call errors.
        pubsub: Any
        try:
            pubsub = self._backend.create_pubsub()
            await pubsub.subscribe(channel)
        except Exception:
            pubsub = None  # Pub/Sub unavailable — poll fallback carries liveness.

        try:
            while True:
                state = await self._load_state()
                if state["status"] in _TERMINAL_STATUSES:
                    return await self._resolve_terminal(state)
                remaining = deadline - loop.time()
                if remaining <= 0:
                    raise ResultNotReadyError(
                        f"workflow {self.workflow_id} did not complete "
                        f"within {timeout}s"
                    )
                if pubsub is not None:
                    try:
                        await pubsub.get_message(
                            ignore_subscribe_messages=True,
                            timeout=min(0.2, remaining),
                        )
                        continue
                    except Exception:
                        pubsub = None
                await asyncio.sleep(min(0.05, max(0.0, remaining)))
        finally:
            if pubsub is not None:
                with contextlib.suppress(Exception):
                    await pubsub.aclose()

    async def _resolve_terminal(self, state: dict[str, Any]) -> Any:
        status = state["status"]
        if status == STATUS_CANCELLED:
            raise TaskCancelledError(self.workflow_id)
        if status == STATUS_FAILED:
            err = state.get("error") or {}
            raise TaskError(err.get("message", "workflow failed"))
        # completed — resolve the final result via the terminal task id.
        final_id = state.get("final_result_task_id")
        if final_id is None:
            return None
        raw = await self._backend.get_result(final_id)
        if raw is None:
            return None
        return self._unpack(raw)


# --------------------------------------------------------------------------
# Topology addressing & frontier navigation (shared by U4 apply + U5 advance)
# --------------------------------------------------------------------------
#
# Each node in a persisted topology is addressed by a path string built from
# the root: chain step i -> ``/{i}``; group member i -> ``/g{i}``; chord
# header -> ``/h`` (its Group members are ``/h/g{i}``); chord body -> ``/b``.
# The path is carried on each step message (HEADER_WORKFLOW_STEP) so the
# worker can navigate the topology to decide what to enqueue next.


def node_at_path(root: WorkflowNode, path: str) -> WorkflowNode:
    """Resolve the node addressed by *path* within *root*. Empty string is
    the root itself. Raises WorkflowValidationError on an invalid path."""
    node: WorkflowNode = root
    for seg in [s for s in path.split("/") if s]:
        if seg.startswith("g") and seg[1:].isdigit():
            idx = int(seg[1:])
            if not isinstance(node, Group):
                raise WorkflowValidationError(f"path segment {seg!r} expects a group")
            node = node.members[idx]
        elif seg == "h":
            if not isinstance(node, Chord):
                raise WorkflowValidationError("path segment 'h' expects a chord")
            node = node.header
        elif seg == "b":
            if not isinstance(node, Chord):
                raise WorkflowValidationError("path segment 'b' expects a chord")
            node = node.body
        elif seg.isdigit():
            if not isinstance(node, Chain):
                raise WorkflowValidationError(f"path segment {seg!r} expects a chain")
            node = node.steps[int(seg)]
        else:
            raise WorkflowValidationError(f"unknown path segment {seg!r}")
    return node


def initial_frontier(root: WorkflowNode) -> list[tuple[str, Signature]]:
    """Leaf signatures to enqueue first, each with its topology path.

    The frontier is the set of steps that can run immediately at apply()
    time: the first step of a chain (recursing into an embedded group/chord),
    every member of a group, and the header of a chord. The chord body and
    later chain steps are enqueued later by the worker advancement seam (U5).
    """
    return _frontier(root, "")


def _frontier(node: WorkflowNode, prefix: str) -> list[tuple[str, Signature]]:
    if isinstance(node, Signature):
        return [(prefix or "", node)]
    if isinstance(node, Chain):
        return _frontier(node.steps[0], f"{prefix}/0")
    if isinstance(node, Group):
        out: list[tuple[str, Signature]] = []
        for i, member in enumerate(node.members):
            out.extend(_frontier(member, f"{prefix}/g{i}"))
        return out
    if isinstance(node, Chord):
        return _frontier(node.header, f"{prefix}/h")
    raise WorkflowValidationError(f"unknown node type {type(node).__name__}")


def chord_set_key(wf_id: str, chord_id: str) -> str:
    """Key for the per-chord completed-member set (SADD of task ids)."""
    return f"sq:wf:chord:{wf_id}:{chord_id}"


def chord_flag_key(wf_id: str, chord_id: str) -> str:
    """Key for the one-shot 'body enqueued' flag."""
    return f"sq:wf:chordflag:{wf_id}:{chord_id}"


def split_path(path: str) -> tuple[str, str]:
    """Return ``(parent_path, last_segment)`` for a topology path. The root
    (``""``) has no parent — returns ``("", "")``."""
    if not path:
        return "", ""
    idx = path.rfind("/")
    return path[:idx], path[idx + 1 :]


def inject_result(signature: Signature, result: Any) -> Signature:
    """Return a copy of *signature* with *result* prepended as the first
    positional arg, unless the signature is immutable (R3/R4)."""
    if signature.immutable:
        return signature
    return Signature(
        signature.task_name,
        (result, *signature.args),
        signature.kwargs,
        immutable=signature.immutable,
        _is_fire_and_forget=signature._is_fire_and_forget,
    )


class WorkflowAdvancer:
    """Worker-side workflow advancement (U5). Called at the success seam
    after a workflow-step's result is stored. Drives the topology forward:
    next chain step (with result injection), chord/group fan-in via the
    atomic ChordManager, and terminal completion.

    Collaborators are injected so the DAG logic is unit-testable with fakes.
    ``publish`` is the broker publish coroutine, ``resolve_queue`` maps a
    task name to its queue, ``make_task_id`` mints a new task id, ``unpack``
    decodes a stored result to its value.
    """

    def __init__(
        self,
        backend: ValkeyBackend,
        chord_manager: ChordManager,
        *,
        publish: Callable[[str, bytes, dict[str, Any]], Any],
        resolve_queue: Callable[[str], str],
        make_task_id: Callable[[], str],
        unpack: Callable[[bytes], Any],
        ttl_ms: int,
    ) -> None:
        self._backend = backend
        self._chord = chord_manager
        self._publish = publish
        self._resolve_queue = resolve_queue
        self._make_task_id = make_task_id
        self._unpack = unpack
        self._ttl_ms = ttl_ms

    async def advance(
        self, wf_id: str, completed_path: str, completed_task_id: str, result: Any
    ) -> None:
        """Entry point: a workflow step at *completed_path* finished with
        *result*. Decide and enqueue what runs next, or mark the workflow
        complete. Idempotent against redelivery via the chord set + the
        per-step ``next-enqueued`` markers."""
        from swarmq.workflow import _node_from_dict  # local: avoid cycle at import

        raw = await self._backend.get_workflow_state(wf_id)
        if raw is None:
            return  # state gone (expired/cancelled-cleaned) — nothing to advance
        state: dict[str, Any] = decode_json(raw)
        if state["status"] != STATUS_RUNNING:
            return  # already terminal
        if await self._backend.is_workflow_cancelled(wf_id):
            await self._mark_terminal(wf_id, state, STATUS_CANCELLED, None, None)
            return

        topology = _node_from_dict(state["topology"])
        await self._on_leaf_complete(
            wf_id, state, topology, completed_path, completed_task_id, result
        )

    # --- core recursion -----------------------------------------------------

    async def _on_leaf_complete(
        self,
        wf_id: str,
        state: dict[str, Any],
        topology: WorkflowNode,
        path: str,
        task_id: str,
        result: Any,
    ) -> None:
        if path == "":
            # The whole workflow is a single signature.
            await self._mark_terminal(wf_id, state, STATUS_COMPLETED, task_id, None)
            return

        parent_path, last = split_path(path)
        parent = node_at_path(topology, parent_path)

        if isinstance(parent, Chain):
            i = int(last)
            if i + 1 < len(parent.steps):
                await self._enqueue_node(
                    wf_id, state, parent.steps[i + 1], f"{parent_path}/{i + 1}",
                    inject=result,
                )
                await self._persist(wf_id, state)
            else:
                # Last chain step done → the chain itself completed.
                await self._on_container_complete(
                    wf_id, state, topology, parent_path, result_task_id=task_id,
                    result_value=None,
                )
        elif isinstance(parent, Group):
            await self._on_group_member(
                wf_id, state, topology, parent, parent_path, task_id, result
            )
        elif isinstance(parent, Chord):
            # Only the body resolves to a chord-child leaf ("/b"); header
            # members live under "/h/g{i}" and hit the Group branch.
            await self._on_container_complete(
                wf_id, state, topology, parent_path, result_task_id=task_id,
                result_value=None,
            )
        else:
            await self._mark_terminal(wf_id, state, STATUS_COMPLETED, task_id, None)

    async def _on_group_member(
        self,
        wf_id: str,
        state: dict[str, Any],
        topology: WorkflowNode,
        group: Group,
        group_path: str,
        member_task_id: str,
        result: Any,
    ) -> None:
        expected_n = len(group.members)
        fired = await self._chord.record_and_check_complete(
            wf_id, group_path, member_task_id, expected_n, self._ttl_ms
        )
        if not fired:
            return  # not the last member (or a redelivery) — nothing to do
        # Group complete: gather member results in DEFINITION order from the
        # recorded ordered task-id list (not the unordered chord set), R5.
        member_task_ids = self._ordered_member_ids(state, group_path, expected_n)
        ordered_results = await self._gather_results(member_task_ids)

        # Is this group a chord header? Then enqueue the chord body with the
        # ordered results. Otherwise the group's result is the list itself.
        gp_parent, gp_last = split_path(group_path)
        if gp_last == "h":
            chord_node = node_at_path(topology, gp_parent)
            assert isinstance(chord_node, Chord)
            await self._enqueue_node(
                wf_id, state, chord_node.body, f"{gp_parent}/b",
                inject=ordered_results,
            )
            await self._persist(wf_id, state)
        else:
            await self._on_container_complete(
                wf_id, state, topology, group_path, result_task_id=None,
                result_value=ordered_results,
            )

    async def _on_container_complete(
        self,
        wf_id: str,
        state: dict[str, Any],
        topology: WorkflowNode,
        node_path: str,
        *,
        result_task_id: str | None,
        result_value: Any,
    ) -> None:
        """A composite node (chain/group/chord) at *node_path* finished with
        a result (a stored task id, or an in-memory aggregate value).
        Propagate to its container, or finish the workflow at the root."""
        if node_path == "":
            await self._mark_terminal(
                wf_id, state, STATUS_COMPLETED, result_task_id, result_value
            )
            return

        parent_path, last = split_path(node_path)
        parent = node_at_path(topology, parent_path)

        # Materialise a result value to inject / propagate.
        if result_task_id is not None:
            value = await self._result_value(result_task_id)
        else:
            value = result_value

        if isinstance(parent, Chain):
            i = int(last)
            if i + 1 < len(parent.steps):
                await self._enqueue_node(
                    wf_id, state, parent.steps[i + 1], f"{parent_path}/{i + 1}",
                    inject=value,
                )
                await self._persist(wf_id, state)
            else:
                await self._on_container_complete(
                    wf_id, state, topology, parent_path,
                    result_task_id=result_task_id, result_value=value,
                )
        elif isinstance(parent, Group):
            # A composite that is itself a group member: treat its completion
            # as that member completing (fan-in counts it once via a synthetic
            # member id derived from the node path).
            synthetic_id = await self._materialise(
                wf_id, node_path, result_task_id, value
            )
            await self._on_group_member(
                wf_id, state, topology, parent, parent_path, synthetic_id, value
            )
        elif isinstance(parent, Chord):
            await self._on_container_complete(
                wf_id, state, topology, parent_path,
                result_task_id=result_task_id, result_value=value,
            )
        else:
            await self._mark_terminal(
                wf_id, state, STATUS_COMPLETED, result_task_id, value
            )

    # --- enqueue + state helpers -------------------------------------------

    async def _enqueue_node(
        self,
        wf_id: str,
        state: dict[str, Any],
        node: WorkflowNode,
        node_path: str,
        *,
        inject: Any,
    ) -> None:
        """Publish the frontier of *node* (a chain step / chord body) as
        workflow-step messages, injecting *inject* into mutable leaves.
        publish-then-mark: each step is published (on a publisher-confirms
        channel) before its step-task id is recorded, so a crash never loses
        the step (at worst a rare idempotent double-enqueue)."""
        from swarmq.headers import (
            HEADER_ENQUEUED_AT,
            HEADER_WORKFLOW_ID,
            HEADER_WORKFLOW_STEP,
        )

        for step_path, signature in _frontier(node, node_path):
            # next-enqueued marker (publish-then-mark): skip if already done.
            if step_path in state["step_tasks"]:
                continue
            injected = inject_result(signature, inject)
            task_id = self._make_task_id()
            queue = self._resolve_queue(injected.task_name)
            message = {
                "task": {"id": task_id, "queue": queue, "name": injected.task_name},
                "args": list(injected.args),
                "kwargs": injected.kwargs,
            }
            headers = {
                HEADER_WORKFLOW_ID: wf_id,
                HEADER_WORKFLOW_STEP: step_path,
                HEADER_ENQUEUED_AT: 0,
            }
            await self._publish(queue, encode_json(message), headers)
            state["step_tasks"][step_path] = task_id

    def _ordered_member_ids(
        self, state: dict[str, Any], group_path: str, expected_n: int
    ) -> list[str]:
        """Member task ids in definition order from step_tasks (R5)."""
        out: list[str] = []
        for i in range(expected_n):
            key = f"{group_path}/g{i}"
            tid = state["step_tasks"].get(key)
            if tid is not None:
                out.append(tid)
        return out

    async def _gather_results(self, task_ids: list[str]) -> list[Any]:
        values: list[Any] = []
        for tid in task_ids:
            values.append(await self._result_value(tid))
        return values

    async def _result_value(self, task_id: str) -> Any:
        raw = await self._backend.get_result(task_id)
        if raw is None:
            return None
        return self._unpack(raw)

    async def _materialise(
        self, wf_id: str, node_path: str, result_task_id: str | None, value: Any
    ) -> str:
        """Return a task id whose stored result is *value*. Reuses an existing
        result id when present; otherwise stores a synthetic result so fan-in
        of composite group members has a single id to track."""
        if result_task_id is not None:
            return result_task_id
        synthetic_id = f"{wf_id}:agg:{node_path}"
        result_dict = {
            "task": {"id": synthetic_id, "queue": "<workflow>", "name": "<aggregate>"},
            "is_error": False,
            "is_cancelled": False,
            "error_message": "",
            "result": value,
        }
        await self._backend.store_result(
            synthetic_id, encode_result(result_dict), self._ttl_ms
        )
        return synthetic_id

    async def _mark_terminal(
        self,
        wf_id: str,
        state: dict[str, Any],
        status: str,
        result_task_id: str | None,
        result_value: Any,
    ) -> None:
        state["status"] = status
        if status == STATUS_COMPLETED:
            if result_task_id is None and result_value is not None:
                result_task_id = await self._materialise(wf_id, "", None, result_value)
            state["final_result_task_id"] = result_task_id
        await self._persist(wf_id, state)
        await self._backend.publish_workflow_notification(wf_id)

    async def fail(self, wf_id: str, error_type: str, error_message: str) -> None:
        """Mark the workflow failed and wake any ``get()`` waiter (R16
        fail-fast + liveness). Idempotent: a no-op once terminal. Used by the
        FAF runtime guard (U5) and the worker terminal-failure seam (U6)."""
        raw = await self._backend.get_workflow_state(wf_id)
        if raw is None:
            return
        state: dict[str, Any] = decode_json(raw)
        if state["status"] != STATUS_RUNNING:
            return
        state["status"] = STATUS_FAILED
        state["error"] = {"type": error_type, "message": error_message}
        await self._persist(wf_id, state)
        await self._backend.publish_workflow_notification(wf_id)

    async def _persist(self, wf_id: str, state: dict[str, Any]) -> None:
        await self._backend.store_workflow_state(
            wf_id, encode_json(state), ttl_ms=self._ttl_ms
        )


class ChordManager:
    """Async wrapper around ``chord_complete.lua``.

    One instance per worker, constructed in ``Worker.start()`` after the
    backend connects (the registered ``AsyncScript`` needs a live client).
    Follows the collaborator-owned-script pattern of ``RateLimiter``.
    """

    def __init__(self, client: valkey.asyncio.Valkey) -> None:
        self._client = client
        self._script = client.register_script(load_script("chord_complete"))

    async def record_and_check_complete(
        self,
        wf_id: str,
        chord_id: str,
        member_id: str,
        expected_n: int,
        ttl_ms: int,
    ) -> bool:
        """Record one header-member completion; return True iff this call
        is the one that should enqueue the chord body (R12).

        Idempotent under redelivery: a member already in the completed set
        does not advance the count, and the body-enqueue decision is guarded
        by a one-shot flag set inside the same script. Exactly one call over
        the chord's lifetime returns True.
        """
        result = await self._script(
            keys=[chord_set_key(wf_id, chord_id), chord_flag_key(wf_id, chord_id)],
            args=[member_id, expected_n, ttl_ms],
        )
        return bool(result == 1)
