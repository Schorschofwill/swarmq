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

    async def finish(  # noqa: C901, PLR0915  # verbatim aus dem Runner; Follow-Up 0.2.x
        self, delivery: Any, ctx: GateContext, execution: ExecutionResult
    ) -> DeliveryOutcome:
        deps = self._deps
        advancer = self._workflow_advancer()
        assert ctx.task_info is not None
        # Build result dict. execution.is_cancelled is a new key in Phase 3b.3;
        # Client._unpack reads it via .get() so pre-R3.6 consumers
        # don't break when the key is absent.
        result_data = build_result_dict(
            task_id=ctx.task_id,
            queue=ctx.queue,
            task_name=ctx.task_name,
            is_error=execution.is_error,
            is_cancelled=execution.is_cancelled,
            error_message=execution.error_message,
            result=None if (execution.is_error or execution.is_cancelled) else execution.result_value,
        )

        # 7. Serialize result
        try:
            result_bytes = encode_result(result_data)
        except ResultSerializationError as exc:
            logger.error("result_serialization_failed", error=str(exc))
            result_data = build_error_result_dict(
                task_id=ctx.task_id, queue=ctx.queue, task_name=ctx.task_name,
                error_message=str(exc),
            )
            result_bytes = encode_result(result_data)

        # 8. Resolve result_ttl
        result_ttl = ctx.task_info.result_ttl
        if result_ttl is None:
            result_ttl = 0 if ctx.task_info.delivery is DeliveryMode.FIRE_AND_FORGET else deps.config.result_ttl
        if result_ttl is None:
            result_ttl = 86400

        if result_ttl == 0:
            # R4.1 FAF runtime guard (U5): a FIRE_AND_FORGET task used as
            # a workflow step acks before producing a stored result and
            # never reaches the advancement seam below — the workflow
            # would stall forever. Construction-time rejection (U1) covers
            # Task.s(); sig()-by-name can only be caught here. Mark the
            # workflow failed so handle.get() raises instead of hanging.
            wf_id_faf = deps.extract_valid_workflow_id(delivery)
            if wf_id_faf is None and (delivery.headers or {}).get(
                HEADER_WORKFLOW_ID
            ) is not None:
                logger.warning(
                    "workflow_id_header_invalid",
                    task_id=ctx.task_id, task_name=ctx.task_name,
                )
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
            if ctx.task_info.delivery is DeliveryMode.AT_LEAST_ONCE:
                await deps.broker.ack(delivery)
            # Even with result_ttl=0 we must clear the cancel flag
            # when the task was cancelled — otherwise it lives until
            # TTL expires and a ctx.task_id reuse would silently cancel.
            if execution.is_cancelled:
                await deps.backend.clear_cancel_flag(ctx.task_id, op_timeout=deps.config.backend_op_timeout)
            return Terminal.release_in_finally()

        # 9-11. Store result + ack/nack
        try:
            await deps.backend.store_result(
                ctx.task_id, result_bytes, result_ttl * 1000,
                op_timeout=deps.config.backend_op_timeout,
            )
        except BackendConnectionError:
            logger.error("backend_connection_error")
            if ctx.task_info.delivery is DeliveryMode.AT_LEAST_ONCE:
                # Non-terminal — redelivery will re-run the task
                # from scratch; keep the flag so next terminal
                # path can release it.
                await deps.broker.redeliver(delivery)
                return Redelivered
            return Terminal.release_in_finally()

        try:
            await deps.backend.publish_notification(ctx.task_id)
        except Exception as exc:
            logger.warning(
                "publish_notification_failed",
                error=str(exc),
                task_id=ctx.task_id,
            )

        # Clear the cancel flag AFTER the result is safely stored so
        # a mid-op failure leaves the flag in place and a redelivery
        # re-triggers the cancel branch.
        if execution.is_cancelled:
            await deps.backend.clear_cancel_flag(ctx.task_id, op_timeout=deps.config.backend_op_timeout)

        # R4.1 workflow advancement (U5): if this message is a workflow
        # step that succeeded, drive the workflow forward (next chain
        # step / chord fan-in / completion) AFTER the result is stored and
        # BEFORE the ack — so a crash before ack redelivers and the
        # idempotent advance (next-enqueued markers + chord set) re-runs
        # without losing or double-enqueueing.
        wf_id = deps.extract_valid_workflow_id(delivery)
        if wf_id is None and (delivery.headers or {}).get(
            HEADER_WORKFLOW_ID
        ) is not None:
            logger.warning(
                "workflow_id_header_invalid",
                task_id=ctx.task_id, task_name=ctx.task_name,
            )
        if (
            wf_id is not None
            and advancer is not None
            and (execution.is_error or execution.is_cancelled)
        ):
            # R4.1 fail-fast + liveness (U6): a workflow step that
            # reached this point as error/cancelled cannot advance — mark
            # the whole workflow failed synchronously and wake any
            # handle.get() waiter, so it raises within bounded time instead
            # of hanging, and no successor / chord body is enqueued.
            # Since Härtung R2 (P2-2 follow-up) every AT_LEAST_ONCE error
            # with an exception goes through _send_to_dlq (which fail-marks
            # there); this inline branch now covers cancels and the rare
            # error-without-exception shapes.
            reason = "task was cancelled" if execution.is_cancelled else (
                execution.error_message or "workflow step failed"
            )
            try:
                await advancer.fail(wf_id, "TaskError", reason)
            except Exception as exc:
                logger.warning(
                    "workflow_fail_mark_failed", workflow_id=wf_id, error=str(exc)
                )
        elif wf_id is not None and advancer is not None:
            step_path = (delivery.headers or {}).get(HEADER_WORKFLOW_STEP, "")
            if not isinstance(step_path, str):
                step_path = ""
            try:
                await advancer.advance(
                    wf_id, step_path, ctx.task_id, execution.result_value
                )
            except BackendConnectionError:
                logger.error("workflow_advance_backend_error", workflow_id=wf_id)
                if ctx.task_info.delivery is DeliveryMode.AT_LEAST_ONCE:
                    # Transient — redelivery retries the idempotent advance.
                    await deps.broker.redeliver(delivery)
                    return Redelivered
                return Terminal.release_in_finally()
            except BrokerConnectionError:
                # A workflow-step publish failed transiently (e.g. a
                # publisher-confirm Nack that persisted past the broker's
                # retry budget). Do NOT mark the workflow failed — a Nack
                # means the message was not committed, so redelivery
                # re-runs the idempotent advance and re-publishes safely
                # (deterministic step ids + step_tasks markers).
                logger.error("workflow_advance_broker_error", workflow_id=wf_id)
                if ctx.task_info.delivery is DeliveryMode.AT_LEAST_ONCE:
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
                    await advancer.fail(
                        wf_id, type(exc).__name__, str(exc)
                    )
                except Exception as fail_exc:
                    logger.error(
                        "workflow_fail_mark_error",
                        workflow_id=wf_id, error=str(fail_exc),
                    )

        if ctx.task_info.delivery is DeliveryMode.AT_LEAST_ONCE:
            await deps.broker.ack(delivery)
        return Terminal.release_in_finally()

