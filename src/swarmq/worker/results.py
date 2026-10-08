"""Ergebnis-Tail (Plan U20, KTD5/KTD6) — verbatim aus dem Runner.

``result_ttl``-Auflösung, Serialisierung mit Fehler-Fallback, Speichern,
Benachrichtigung, Cancel-Flag löschen (nach dem Speichern) und der
Workflow-Advance-Seam. Backend-/Broker-Fehler → ``Redelivered`` (Redelivery
wiederholt den idempotenten Advance); unerwarteter Advance-Fehler →
Fail-Mark + Ack. publish-then-mark ist seit U20 strukturell über den
``PublishReceipt`` erzwungen (``WorkflowAdvancer.mark_enqueued``, AE5).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

from swarmq.cancellation import build_error_result_dict, build_result_dict
from swarmq.exceptions import (
    BackendConnectionError,
    BrokerConnectionError,
    ResultSerializationError,
    WorkflowStateConflict,
)
from swarmq.headers import HEADER_WORKFLOW_ID, HEADER_WORKFLOW_STEP
from swarmq.logging import _error_fields, get_logger
from swarmq.serialization import encode_result
from swarmq.task import DeliveryMode
from swarmq.worker.gates import GateContext, GateDeps, redeliver_in_place
from swarmq.worker.outcome import DeliveryOutcome, Redelivered, Terminal

if TYPE_CHECKING:
    from swarmq.worker.execution import ExecutionResult

logger = get_logger(__name__)

_FailMarkEvent = Literal["workflow_faf_fail_mark_error", "workflow_fail_mark_failed", "workflow_fail_mark_error"]


def _log_fail_mark_error(event: _FailMarkEvent, **fields: Any) -> None:
    """R18: Event-Namen bleiben feste Literale -- je Aufrufer von
    ``_fail_workflow`` ein eigener Name, wie bisher."""
    if event == "workflow_faf_fail_mark_error":
        logger.error("workflow_faf_fail_mark_error", **fields)
    elif event == "workflow_fail_mark_failed":
        logger.error("workflow_fail_mark_failed", **fields)
    else:
        logger.error("workflow_fail_mark_error", **fields)


class ResultTail:
    def __init__(self, deps: GateDeps, *, workflow_advancer: Any) -> None:
        self._deps = deps
        self._workflow_advancer = workflow_advancer

    @staticmethod
    def _encode_with_fallback(ctx: GateContext, execution: ExecutionResult) -> bytes:
        """Result-Dict serialisieren; bei Serialisierungsfehler den
        Fehler-Envelope speichern (der Aufrufer sieht dann den Fehlertext)."""
        # execution.is_cancelled is a new key in Phase 3b.3; Client._unpack
        # reads it via .get() so pre-R3.6 consumers don't break when absent.
        result_data = build_result_dict(
            task_id=ctx.task_id,
            queue=ctx.queue,
            task_name=ctx.task_name,
            is_error=execution.is_error,
            is_cancelled=execution.is_cancelled,
            error_message=execution.error_message,
            result=None if (execution.is_error or execution.is_cancelled) else execution.result_value,
        )
        try:
            return encode_result(result_data)
        except ResultSerializationError as exc:
            logger.error("result_serialization_failed", **_error_fields(exc))
            result_data = build_error_result_dict(
                task_id=ctx.task_id, queue=ctx.queue, task_name=ctx.task_name,
                error_message=str(exc),
            )
            return encode_result(result_data)

    def _resolve_result_ttl(self, ctx: GateContext) -> int:
        """``result_ttl``-Kaskade: task_info → config → 86400-Fallback;
        FIRE_AND_FORGET ohne explizites TTL speichert nicht (0)."""
        task_info = ctx.task_info
        assert task_info is not None
        result_ttl = task_info.result_ttl
        if result_ttl is None:
            result_ttl = 0 if task_info.delivery is DeliveryMode.FIRE_AND_FORGET else self._deps.config.result_ttl
        if result_ttl is None:
            result_ttl = 86400
        return result_ttl

    def _extract_workflow_id(self, delivery: Any, ctx: GateContext) -> str | None:
        deps = self._deps
        wf_id = deps.extract_valid_workflow_id(delivery)
        if wf_id is None and (delivery.headers or {}).get(HEADER_WORKFLOW_ID) is not None:
            logger.warning(
                "workflow_id_header_invalid",
                task_id=ctx.task_id, task_name=ctx.task_name,
            )
        return wf_id

    async def _finish_without_store(
        self, delivery: Any, ctx: GateContext, execution: ExecutionResult, advancer: Any
    ) -> DeliveryOutcome:
        """``result_ttl=0``-Pfad (FAF-Laufzeit-Guard, R4.1 U5): ein
        FIRE_AND_FORGET-Task als Workflow-Schritt erreicht den Advance-Seam
        nie — der Workflow würde ewig hängen. Construction-time-Rejection
        deckt ``Task.s()``; ``sig()``-by-name fängt nur dieser Pfad. Der
        Workflow wird fail-markiert, damit ``handle.get()`` wirft."""
        deps = self._deps
        task_info = ctx.task_info
        assert task_info is not None
        wf_id_faf = self._extract_workflow_id(delivery, ctx)
        if wf_id_faf is not None and advancer is not None:
            outcome = await self._fail_workflow(
                delivery, ctx, wf_id_faf, advancer, "WorkflowValidationError",
                f"task {ctx.task_name!r} is FIRE_AND_FORGET and cannot "
                "advance a workflow",
                event="workflow_faf_fail_mark_error",
            )
            if outcome is not None:
                # Redelivered: kein Ack, und das Cancel-Flag bleibt stehen,
                # damit die Redelivery den Cancel-Zweig erneut sieht.
                return outcome
        if task_info.delivery is DeliveryMode.AT_LEAST_ONCE:
            await deps.broker.ack(delivery)
        # Even with result_ttl=0 we must clear the cancel flag when the task
        # was cancelled — otherwise it lives until TTL expires and a
        # task_id reuse would silently cancel.
        if execution.is_cancelled:
            await deps.backend.clear_cancel_flag(ctx.task_id, op_timeout=deps.config.backend_op_timeout)
        return Terminal.release_in_finally()

    async def _fail_workflow(
        self, delivery: Any, ctx: GateContext, wf_id: str, advancer: Any,
        error_type: str, reason: str, *, event: _FailMarkEvent,
    ) -> DeliveryOutcome | None:
        """Fehlermarkierung eines Workflows nach R25/KTD12 (U10).

        Scheitert sie transient — Backend weg oder die Konfliktgrenze des
        Compare-and-Set erreicht —, darf die Nachricht nicht bestätigt werden:
        der Workflow bliebe bis zum TTL ``RUNNING`` und ``handle.get()`` liefe
        in seinen Timeout. Bei AT_LEAST_ONCE wird redelivered, bei
        FIRE_AND_FORGET gibt es kein Broker-Verb mehr (der Pfad hat nichts zu
        bestätigen). Jeder andere Fehler ist deterministisch: eine Redelivery
        würde den Task-Körper erneut ausführen und bis zum Delivery-Limit in
        einer Poison-Schleife drehen — also bestätigen und loggen.

        Liefert ``Redelivered``, wenn die Nachricht neu zugestellt wurde,
        sonst ``None`` (der Aufrufer läuft weiter wie bisher)."""
        deps = self._deps
        task_info = ctx.task_info
        assert task_info is not None
        try:
            await advancer.fail(wf_id, error_type, reason)
        except (BackendConnectionError, WorkflowStateConflict) as exc:
            at_least_once = task_info.delivery is DeliveryMode.AT_LEAST_ONCE
            _log_fail_mark_error(event, workflow_id=wf_id, **_error_fields(exc), redelivered=at_least_once)
            if at_least_once:
                await redeliver_in_place(deps, delivery, ctx)
                return Redelivered
        except Exception as exc:
            _log_fail_mark_error(event, workflow_id=wf_id, **_error_fields(exc), redelivered=False)
        return None

    async def _advance_workflow(
        self, delivery: Any, ctx: GateContext, execution: ExecutionResult, advancer: Any
    ) -> DeliveryOutcome | None:
        """R4.1 workflow advancement (U5): nach dem Store, vor dem Ack —
        ein Crash vor dem Ack redelivered, der idempotente Advance läuft
        erneut ohne Verlust oder Doppel-Enqueue. Liefert ein Outcome, wenn
        der Advance den Pfad beendet (Redelivery), sonst ``None``."""
        deps = self._deps
        task_info = ctx.task_info
        assert task_info is not None
        wf_id = self._extract_workflow_id(delivery, ctx)
        if wf_id is None or advancer is None:
            return None
        if execution.is_error or execution.is_cancelled:
            # Fail-fast + liveness (R4.1 U6): ein Fehler-/Cancel-Schritt kann
            # nicht advancen — Workflow synchron fail-markieren, damit
            # handle.get() in endlicher Zeit wirft. Der AT_LEAST_ONCE-
            # Fehlerpfad mit Exception fail-markt bereits im Dead-Letter;
            # dieser Zweig deckt Cancels und Fehler ohne Exception.
            reason = "task was cancelled" if execution.is_cancelled else (
                execution.error_message or "workflow step failed"
            )
            return await self._fail_workflow(
                delivery, ctx, wf_id, advancer, "TaskError", reason,
                event="workflow_fail_mark_failed",
            )
        step_path = (delivery.headers or {}).get(HEADER_WORKFLOW_STEP, "")
        if not isinstance(step_path, str):
            step_path = ""
        try:
            await advancer.advance(wf_id, step_path, ctx.task_id, execution.result_value)
        except (BackendConnectionError, WorkflowStateConflict) as exc:
            # Die Konfliktgrenze des Compare-and-Set (U9) heisst: der Advance
            # ist nicht fertig, nicht gescheitert — wie ein Backend-Ausfall
            # behandeln, nicht fail-markieren.
            if task_info.delivery is DeliveryMode.AT_LEAST_ONCE:
                # Transient — redelivery retries the idempotent advance.
                logger.warning("workflow_advance_backend_error", workflow_id=wf_id, **_error_fields(exc))
                await redeliver_in_place(deps, delivery, ctx)
                return Redelivered
            logger.error("workflow_advance_backend_error", workflow_id=wf_id, **_error_fields(exc))
            return Terminal.release_in_finally()
        except BrokerConnectionError as exc:
            # A workflow-step publish failed transiently (e.g. a
            # publisher-confirm Nack past the broker's retry budget). Do NOT
            # mark the workflow failed — a Nack means the message was not
            # committed, so redelivery re-runs the idempotent advance and
            # re-publishes safely (deterministic step ids + step_tasks).
            if task_info.delivery is DeliveryMode.AT_LEAST_ONCE:
                logger.warning("workflow_advance_broker_error", workflow_id=wf_id, **_error_fields(exc))
                await redeliver_in_place(deps, delivery, ctx)
                return Redelivered
            logger.error("workflow_advance_broker_error", workflow_id=wf_id, **_error_fields(exc))
            return Terminal.release_in_finally()
        except Exception as exc:
            # Unexpected advance bug: fail the workflow so get() raises
            # instead of hanging, then ack (no infinite redelivery loop).
            logger.error(
                "workflow_advance_failed",
                workflow_id=wf_id, **_error_fields(exc),
            )
            return await self._fail_workflow(
                delivery, ctx, wf_id, advancer, type(exc).__name__, str(exc),
                event="workflow_fail_mark_error",
            )
        return None

    async def finish(
        self, delivery: Any, ctx: GateContext, execution: ExecutionResult
    ) -> DeliveryOutcome:
        deps = self._deps
        advancer = self._workflow_advancer()
        task_info = ctx.task_info
        assert task_info is not None

        result_bytes = self._encode_with_fallback(ctx, execution)
        result_ttl = self._resolve_result_ttl(ctx)
        if result_ttl == 0:
            return await self._finish_without_store(delivery, ctx, execution, advancer)

        # Store result, then notify; ack comes last.
        try:
            await deps.backend.store_result(
                ctx.task_id, result_bytes, result_ttl * 1000,
                op_timeout=deps.config.backend_op_timeout,
            )
        except BackendConnectionError as exc:
            if task_info.delivery is DeliveryMode.AT_LEAST_ONCE:
                # Selbstheilend (KTD11): die Neuzustellung speichert erneut.
                logger.warning("backend_connection_error", **_error_fields(exc))
                # Non-terminal — redelivery will re-run the task from
                # scratch; keep the flag so next terminal path can release.
                await redeliver_in_place(deps, delivery, ctx)
                return Redelivered
            # FIRE_AND_FORGET: keine Neuzustellung, das Ergebnis ist verloren.
            logger.error("backend_connection_error", **_error_fields(exc))
            return Terminal.release_in_finally()

        try:
            await deps.backend.notify_result(ctx.task_id)
        except Exception as exc:
            logger.warning(
                "publish_notification_failed",
                **_error_fields(exc),
                task_id=ctx.task_id,
            )

        # Clear the cancel flag AFTER the result is safely stored so a
        # mid-op failure leaves the flag in place and a redelivery
        # re-triggers the cancel branch.
        if execution.is_cancelled:
            await deps.backend.clear_cancel_flag(ctx.task_id, op_timeout=deps.config.backend_op_timeout)

        outcome = await self._advance_workflow(delivery, ctx, execution, advancer)
        if outcome is not None:
            return outcome

        if task_info.delivery is DeliveryMode.AT_LEAST_ONCE:
            await deps.broker.ack(delivery)
        return Terminal.release_in_finally()

