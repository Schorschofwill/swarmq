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
from swarmq.serialization import decode_json
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
