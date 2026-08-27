"""Ergebnis-Tail (Plan U20, KTD5/KTD6) — verbatim aus dem Runner.

``result_ttl``-Auflösung, Serialisierung mit Fehler-Fallback, Speichern,
Benachrichtigung, Cancel-Flag löschen (nach dem Speichern) und der
Workflow-Advance-Seam. Backend-/Broker-Fehler → ``Redelivered`` (Redelivery
wiederholt den idempotenten Advance); unerwarteter Advance-Fehler →
Fail-Mark + Ack. publish-then-mark ist seit U20 strukturell über den
``PublishReceipt`` erzwungen (``WorkflowAdvancer.mark_enqueued``, AE5).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import structlog

from swarmq.cancellation import build_error_result_dict, build_result_dict
from swarmq.exceptions import (
    BackendConnectionError,
    BrokerConnectionError,
    ResultSerializationError,
)
from swarmq.headers import HEADER_WORKFLOW_ID, HEADER_WORKFLOW_STEP
from swarmq.serialization import encode_result
from swarmq.task import DeliveryMode
from swarmq.worker.gates import GateContext, GateDeps
from swarmq.worker.outcome import DeliveryOutcome, Redelivered, Terminal

if TYPE_CHECKING:
    from swarmq.worker.execution import ExecutionResult

logger = structlog.get_logger()


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
            logger.error("result_serialization_failed", error=str(exc))
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
            try:
                await advancer.fail(
                    wf_id_faf,
                    "WorkflowValidationError",
                    f"task {ctx.task_name!r} is FIRE_AND_FORGET and cannot "
                    "advance a workflow",
                )
            except Exception as exc:
                logger.error(
                    "workflow_faf_fail_mark_error",
                    workflow_id=wf_id_faf, error=str(exc),
                )
        if task_info.delivery is DeliveryMode.AT_LEAST_ONCE:
            await deps.broker.ack(delivery)
        # Even with result_ttl=0 we must clear the cancel flag when the task
        # was cancelled — otherwise it lives until TTL expires and a
        # task_id reuse would silently cancel.
        if execution.is_cancelled:
            await deps.backend.clear_cancel_flag(ctx.task_id, op_timeout=deps.config.backend_op_timeout)
        return Terminal.release_in_finally()

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
            try:
                await advancer.fail(wf_id, "TaskError", reason)
            except Exception as exc:
                logger.warning(
                    "workflow_fail_mark_failed", workflow_id=wf_id, error=str(exc)
                )
            return None
        step_path = (delivery.headers or {}).get(HEADER_WORKFLOW_STEP, "")
        if not isinstance(step_path, str):
            step_path = ""
        try:
            await advancer.advance(wf_id, step_path, ctx.task_id, execution.result_value)
        except BackendConnectionError:
            logger.error("workflow_advance_backend_error", workflow_id=wf_id)
            if task_info.delivery is DeliveryMode.AT_LEAST_ONCE:
                # Transient — redelivery retries the idempotent advance.
                await deps.broker.redeliver(delivery)
                return Redelivered
            return Terminal.release_in_finally()
        except BrokerConnectionError:
            # A workflow-step publish failed transiently (e.g. a
            # publisher-confirm Nack past the broker's retry budget). Do NOT
            # mark the workflow failed — a Nack means the message was not
            # committed, so redelivery re-runs the idempotent advance and
            # re-publishes safely (deterministic step ids + step_tasks).
            logger.error("workflow_advance_broker_error", workflow_id=wf_id)
            if task_info.delivery is DeliveryMode.AT_LEAST_ONCE:
                await deps.broker.redeliver(delivery)
                return Redelivered
            return Terminal.release_in_finally()
        except Exception as exc:
            # Unexpected advance bug: fail the workflow so get() raises
            # instead of hanging, then ack (no infinite redelivery loop).
            logger.error(
                "workflow_advance_failed",
                workflow_id=wf_id, error=str(exc),
            )
            try:
                await advancer.fail(wf_id, type(exc).__name__, str(exc))
            except Exception as fail_exc:
                logger.error(
                    "workflow_fail_mark_error",
                    workflow_id=wf_id, error=str(fail_exc),
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
        except BackendConnectionError:
            logger.error("backend_connection_error")
            if task_info.delivery is DeliveryMode.AT_LEAST_ONCE:
                # Non-terminal — redelivery will re-run the task from
                # scratch; keep the flag so next terminal path can release.
                await deps.broker.redeliver(delivery)
                return Redelivered
            return Terminal.release_in_finally()

        try:
            await deps.backend.notify_result(ctx.task_id)
        except Exception as exc:
            logger.warning(
                "publish_notification_failed",
                error=str(exc),
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

