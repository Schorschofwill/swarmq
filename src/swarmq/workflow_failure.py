"""Workflow-Abschluss und -Fehler (Plan 0.12.0, MR 0; verbatim aus
``workflow_orchestration.py``): Ergebnisse fuer den Fan-in lesen (ein
abgelaufenes Ergebnis ist ``WorkflowResultExpired``), Ergebnisse
materialisieren, den Endzustand setzen, ``fail`` und das Compare-and-Set
``_persist``. Mixin ueber dem ``WorkflowAdvancer``-Zustand.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from swarmq.exceptions import WorkflowResultExpired
from swarmq.logging import get_logger
from swarmq.serialization import decode_json, encode_json, encode_result
from swarmq.workflow_state import STATUS_COMPLETED, STATUS_FAILED, STATUS_RUNNING, _StaleWorkflowState
from swarmq.workflow_topology import aggregate_result_id

if TYPE_CHECKING:
    from swarmq.contracts import ResultBackend

# Q17: derselbe Logger-Name wie ``WorkflowAdvancer.advance`` (Ursprungsmodul).
logger = get_logger("swarmq.workflow_orchestration")


class WorkflowFailure:
    # Mixin über dem Advancer-Zustand; die Attribute setzt ``WorkflowAdvancer.__init__``.
    if TYPE_CHECKING:
        _backend: ResultBackend
        _unpack: Callable[[bytes], Any]
        _ttl_ms: int

        async def _reconcile(self, wf_id: str, local: dict[str, Any]) -> None: ...

    async def _gather_results(self, task_ids: list[str]) -> list[Any]:
        # Single batched fetch preserving definition order; a missing/None
        # entry means the result EXPIRED before fan-in (same contract as
        # _result_value): surface WorkflowResultExpired rather than a None
        # hole (R6). A stored None return is a non-None result dict, so None
        # here unambiguously means "key gone".
        raws = await self._backend.get_results(task_ids)
        values: list[Any] = []
        for tid, raw in zip(task_ids, raws, strict=True):
            if raw is None:
                raise WorkflowResultExpired(
                    f"result for completed step {tid} expired before the "
                    "workflow could consume it; raise the result/workflow TTL"
                )
            values.append(self._unpack(raw))
        return values

    async def _result_value(self, task_id: str) -> Any:
        # Called only for steps the workflow already observed as complete, so
        # a missing key means the result EXPIRED before fan-in/propagation —
        # not "never written" and not "the task returned None" (a None return
        # is stored as a result dict, so get_result yields bytes, not None).
        # Surface it as WorkflowResultExpired (R6) instead of a silent None
        # hole; the worker advance path turns this into a failed workflow.
        raw = await self._backend.get_result(task_id)
        if raw is None:
            raise WorkflowResultExpired(
                f"result for completed step {task_id} expired before the "
                "workflow could consume it; raise the result/workflow TTL"
            )
        return self._unpack(raw)

    async def _materialise(self, wf_id: str, node_path: str, result_task_id: str | None, value: Any) -> str:
        """Return a task id whose stored result is *value*. Reuses an existing
        result id when present; otherwise stores a synthetic result so fan-in
        of composite group members has a single id to track."""
        if result_task_id is not None:
            return result_task_id
        synthetic_id = aggregate_result_id(wf_id, node_path)
        result_dict = {
            "task": {"id": synthetic_id, "queue": "<workflow>", "name": "<aggregate>"},
            "is_error": False,
            "is_cancelled": False,
            "error_message": "",
            "result": value,
        }
        await self._backend.store_result(synthetic_id, encode_result(result_dict), self._ttl_ms)
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
        await self._backend.notify_workflow(wf_id)

    async def fail(self, wf_id: str, error_type: str, error_message: str) -> None:
        """Mark the workflow failed and wake any ``get()`` waiter (R16
        fail-fast + liveness). Idempotent: a no-op once terminal. Used by the
        FAF runtime guard (U5) and the worker terminal-failure seam (U6)."""
        raw = await self._backend.get_workflow_state(wf_id)
        if raw is None:
            # BZ-13: abgelaufen oder aufgeraeumt -- nicht still enden.
            logger.warning("workflow_state_missing", workflow_id=wf_id)
            return
        state: dict[str, Any] = decode_json(raw)
        if state["status"] != STATUS_RUNNING:
            return
        state["status"] = STATUS_FAILED
        state["error"] = {"type": error_type, "message": error_message}
        try:
            await self._persist(wf_id, state)
        except _StaleWorkflowState:
            await self._reconcile(wf_id, state)
            return
        await self._backend.notify_workflow(wf_id)

    async def _persist(self, wf_id: str, state: dict[str, Any]) -> None:
        """Compare-and-Set gegen die gelesene ``version`` (R22; fehlend = 0,
        Dokumente aus 0.5.x). Bei Konflikt :class:`_StaleWorkflowState`."""
        expected = int(state.get("version", 0))
        state["version"] = expected + 1
        stored = await self._backend.compare_and_store_workflow_state(
            wf_id, encode_json(state), ttl_ms=self._ttl_ms, expected_version=expected
        )
        if not stored:
            state["version"] = expected
            raise _StaleWorkflowState(wf_id)
