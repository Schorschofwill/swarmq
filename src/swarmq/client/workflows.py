"""Client-Workflows (Plan U10) — verbatim: apply, workflow, _unpack."""

from __future__ import annotations

import contextlib
import time
import uuid
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from swarmq.client.schedule import ClientBackend, ClientBroker
    from swarmq.middleware import MiddlewareChain
    from swarmq.registry import TaskRegistry
    from swarmq.signals import SignalDispatcher

from swarmq.exceptions import (
    TaskCancelledError,
    TaskError,
)
from swarmq.headers import (
    HEADER_ENQUEUED_AT,
    HEADER_WORKFLOW_ID,
    HEADER_WORKFLOW_STEP,
)
from swarmq.serialization import (
    decode_json,
    decode_result,
    encode_json,
)
from swarmq.workflow import Chain, Chord, Group, Signature
from swarmq.workflow_orchestration import (
    STATUS_FAILED,
    STATUS_RUNNING,
    WorkflowHandle,
    WorkflowStep,
    WorkflowStepEnqueuer,
    build_initial_workflow_state,
    initial_frontier,
    workflow_step_task_id,
)

# R4.1 Workflow state TTL. Workflow topology/status must outlive the
# longest step + retry horizon so reattach (R14) and fan-in (R6) stay
# resolvable. 24h matches the result-TTL default; app.apply() passes
# config.result_ttl * 1000 through.
_DEFAULT_WORKFLOW_TTL_MS = 86_400_000  # 24h


class ClientWorkflows:
    # Mixin über dem Client-Zustand (U10); ``_resolve_queue_and_delivery_mode``
    # ist in ``ClientScheduling`` verbindlich definiert.
    if TYPE_CHECKING:
        _broker: ClientBroker
        _backend: ClientBackend
        _registry: TaskRegistry | None
        _backend_op_timeout: float | None
        _signal_dispatcher: SignalDispatcher

        def _resolve_queue_and_delivery_mode(self, task_name: str, queue: str | None) -> tuple[str, str]: ...

        def _get_chain(self) -> MiddlewareChain: ...

    async def apply(
        self,
        workflow: Signature | Chain | Group | Chord,
        *,
        state_ttl_ms: int = _DEFAULT_WORKFLOW_TTL_MS,
    ) -> WorkflowHandle:
        """Start a workflow fire-and-forget; return a reattachable handle (R10).

        Persists the topology + initial state to Valkey, then enqueues the
        initial frontier (first chain step / all group members / chord
        header) as ``AT_LEAST_ONCE`` workflow-step messages. The worker
        advancement seam drives the rest. Returns immediately — the workflow
        runs independently of this client process.

        State is written BEFORE the frontier is published so a fast-completing
        first step always finds the topology in Valkey when it advances —
        aber NACH ``pre_enqueue`` der ganzen Front (R18), damit eine
        ablehnende Middleware keinen verwaisten Zustand hinterlaesst. Jeder
        Schritt loest nach dem Publish ``TASK_ENQUEUED`` und
        ``WORKFLOW_STEP_ENQUEUED`` aus; das frühere Pseudo-Ereignis mit
        ``task_name="<workflow>"`` entfaellt.
        """
        workflow_id = str(uuid.uuid7())
        frontier = initial_frontier(workflow)  # list[(step_path, Signature)]

        # Generate task ids up front so the persisted state records the
        # initial step->task_id mapping the worker reads when advancing.
        enqueue_plan: list[tuple[str, str, Signature, str]] = []
        step_tasks: dict[str, str] = {}
        for step_path, signature in frontier:
            # Deterministic step id (shared scheme with the worker advancer)
            # so a redelivered step stays idempotent — same id in the chord
            # SADD set, same result key. See workflow_step_task_id.
            task_id = workflow_step_task_id(workflow_id, step_path)
            resolved_queue, _delivery = self._resolve_queue_and_delivery_mode(signature.task_name, None)
            enqueue_plan.append((step_path, task_id, signature, resolved_queue))
            step_tasks[step_path] = task_id

        state = build_initial_workflow_state(workflow_id, workflow.to_dict())
        state["step_tasks"] = step_tasks
        # KTD11: Erstanlage mit Version 0; jede spaetere Schreibung ist ein
        # Compare-and-Set gegen die gelesene Version.
        state["version"] = 0

        enqueued_at = int(time.time())
        steps = [
            WorkflowStep(
                workflow_id=workflow_id,
                step_path=step_path,
                task_id=task_id,
                task_name=signature.task_name,
                queue=resolved_queue,
                args=list(signature.args),
                kwargs=signature.kwargs,
                headers={
                    HEADER_WORKFLOW_ID: workflow_id,
                    HEADER_WORKFLOW_STEP: step_path,
                    HEADER_ENQUEUED_AT: enqueued_at,
                },
            )
            for step_path, task_id, signature, resolved_queue in enqueue_plan
        ]
        enqueuer = WorkflowStepEnqueuer(
            publish=lambda queue, body, headers: self._broker.publish(queue, body, headers=headers),
            dispatch=self._signal_dispatcher.dispatch,
            get_chain=self._get_chain,
        )
        # R18: ``pre_enqueue`` ist ein Gate ueber die ganze Anfangsfront und
        # laeuft VOR der Persistenz — lehnt eine Middleware ab, bleibt kein
        # verwaister ``running``-Zustand zurueck, den niemand mehr weiterdreht.
        await enqueuer.pre_enqueue(steps)
        await self._backend.store_workflow_state(
            workflow_id,
            encode_json(state),
            ttl_ms=state_ttl_ms,
            op_timeout=self._backend_op_timeout,
        )

        try:
            for step in steps:
                await enqueuer.publish(step)
        except Exception as exc:
            # Partial-frontier publish failure: members 1..k-1 are enqueued
            # and the state is already persisted as "running", but the
            # frontier is incomplete so a group/chord can never reach N and
            # the workflow would hang until TTL. Best-effort mark it failed so
            # a reattach sees the failure instead of an orphaned running state.
            # The already-enqueued members run harmlessly (their advance finds
            # a terminal workflow and no-ops).
            with contextlib.suppress(Exception):
                await self._mark_apply_failed(workflow_id, state, exc, state_ttl_ms)
            raise

        return WorkflowHandle(self._backend, workflow_id, self._unpack, cancel_ttl_ms=state_ttl_ms)

    async def _mark_apply_failed(
        self, workflow_id: str, state: dict[str, Any], exc: Exception, state_ttl_ms: int
    ) -> None:
        """Fehlermarkierung per Compare-and-Set gegen die Erstanlage (R24,
        KTD11). War ein Advancer eines schon publizierten Glieds schneller,
        wird der frische Zustand gelesen und nur ein noch LAUFENDER Zustand
        erneut markiert -- ein terminaler bleibt unangetastet."""
        error = {
            "type": type(exc).__name__,
            "message": f"apply() failed to enqueue the initial frontier: {exc}",
        }
        for candidate in (state, None):
            if candidate is None:
                raw = await self._backend.get_workflow_state(workflow_id, op_timeout=self._backend_op_timeout)
                if raw is None:
                    return
                candidate = decode_json(raw)
                if candidate["status"] != STATUS_RUNNING:
                    return
            expected = int(candidate.get("version", 0))
            failed = dict(candidate, status=STATUS_FAILED, error=error, version=expected + 1)
            if await self._backend.compare_and_store_workflow_state(
                workflow_id,
                encode_json(failed),
                ttl_ms=state_ttl_ms,
                expected_version=expected,
                op_timeout=self._backend_op_timeout,
            ):
                return

    async def workflow(self, workflow_id: str) -> WorkflowHandle:
        """Reattach to a running/finished workflow by id (R14).

        Raises :class:`WorkflowNotFound` when no state exists for the id
        (never started, or expired) — never returns a handle over empty
        state.
        """
        raw = await self._backend.get_workflow_state(workflow_id, op_timeout=self._backend_op_timeout)
        if raw is None:
            from swarmq.exceptions import WorkflowNotFound

            raise WorkflowNotFound(workflow_id)
        return WorkflowHandle(
            self._backend,
            workflow_id,
            self._unpack,
            cancel_ttl_ms=_DEFAULT_WORKFLOW_TTL_MS,
        )

    @staticmethod
    def _unpack(raw: bytes) -> Any:
        """Decode a raw result and return the value or raise on cancel/error.

        Priority: is_cancelled > is_error > success. Cancelled results
        raise :class:`TaskCancelledError` even when ``is_error`` is also
        set, because cancel bypasses the normal error pipeline and the
        caller needs to distinguish a user-initiated abort from a task
        failure.
        """
        data = decode_result(raw)
        if data.get("is_cancelled", False):
            task_id = data.get("task", {}).get("id")
            raise TaskCancelledError(task_id)
        if data.get("is_error"):
            raise TaskError(data.get("error_message", "unknown error"))
        return data["result"]
