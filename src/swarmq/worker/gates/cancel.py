"""Pre-Start-Cancel-Gate (Phase 3b.3), Plan U7 — verbatim
aus ``pipeline.py``. Läuft nach dem Schedfire-Gate, vor Semaphore,
Rate-Limit, Lock und FIRE_AND_FORGET-Ack.

Cancel-Flag (Task oder Workflow) gesetzt → Cancel-Ergebnis speichern,
Benachrichtigung, Flag löschen, Schedfire-Claim finalisieren, Dedup-Flag
freigeben, bestätigen, Signal ``TASK_CANCELLED`` nach dem Ack; Outcome
``Terminal.already_released()``. Backend-Fehler und Result-Write-Fehler →
``nack(requeue=True)`` (``Redelivered``). Der Runner behandelt dieses Gate
wie heute *vor* seinem ``try/finally``.
"""

from __future__ import annotations

from typing import Any

import structlog

from swarmq.cancellation import build_cancel_result_dict
from swarmq.exceptions import BackendConnectionError, TaskNotFoundError
from swarmq.serialization import encode_result
from swarmq.signals import Signal
from swarmq.task import DeliveryMode, TaskInfo
from swarmq.worker.gates import GateContext, GateDeps
from swarmq.worker.outcome import Continue, DeliveryOutcome, Redelivered, Terminal

logger = structlog.get_logger()


class CancelGate:
    def __init__(self, deps: GateDeps) -> None:
        self._deps = deps

    async def apply(self, delivery: Any, ctx: GateContext) -> DeliveryOutcome:  # verbatim, U8/0.2.x
        deps = self._deps
        task_id, task_name, queue = ctx.task_id, ctx.task_name, ctx.queue
        try:
            is_cancelled = await deps.backend.is_cancelled(task_id, op_timeout=deps.config.backend_op_timeout)
        except BackendConnectionError:
            # Observability is via the structured log event only:
            # AMQP nack(requeue=True) redelivers the stored
            # message, so in-memory header mutations would be lost anyway.
            logger.error("cancel_check_backend_error", task_id=task_id)
            await deps.broker.redeliver(delivery)
            structlog.contextvars.clear_contextvars()
            return Redelivered

        workflow_cancel = False
        if not is_cancelled:
            # The per-task flag is clear, but the step
            # may belong to a cancelled WORKFLOW. handle.cancel() only
            # sets the workflow flag; group/chord members are all
            # enqueued at apply() time, so without this check every
            # already-enqueued member ran to completion after a cancel.
            wf_id = deps.extract_valid_workflow_id(delivery)
            if wf_id is None:
                return Continue
            try:
                wf_cancelled = await deps.backend.is_workflow_cancelled(
                    wf_id, op_timeout=deps.config.backend_op_timeout
                )
            except BackendConnectionError:
                logger.error("workflow_cancel_check_backend_error", task_id=task_id, workflow_id=wf_id)
                await deps.broker.redeliver(delivery)
                structlog.contextvars.clear_contextvars()
                return Redelivered
            if not wf_cancelled:
                return Continue
            workflow_cancel = True
            logger.info("workflow_step_cancelled_pre_start", task_id=task_id, workflow_id=wf_id)
            # Fall through to the shared cancelled-handling below.
            # clear_cancel_flag(task_id) is an idempotent DEL on a key
            # that does not exist — harmless.

        # Cancel flag is set. Resolve DeliveryMode + result_ttl from
        # registry (unknown tasks fall back to AT_LEAST_ONCE semantics so
        # the client's get_result does not hang forever waiting for a
        # result that will never arrive).
        task_info: TaskInfo | None = None
        try:
            task_class = deps.registry.get(task_name)
            task_info = task_class.task_info
            delivery_mode = task_info.delivery
            # result_ttl falls through: task → global → 86400.
            result_ttl = task_info.result_ttl
            if result_ttl is None:
                result_ttl = 0 if delivery_mode is DeliveryMode.FIRE_AND_FORGET else deps.config.result_ttl
        except TaskNotFoundError:
            result_ttl = deps.config.result_ttl

        try:
            # On the WORKFLOW-cancel path a redelivery of an
            # already-completed step (success stored, ack lost) must not
            # overwrite the original result with a cancel marker — a
            # direct get_result(task_id) would then misreport a step
            # that genuinely ran. The per-task cancel path keeps its
            # unconditional write (the flag only exists pre-/mid-run).
            store_cancel_result = result_ttl > 0
            if store_cancel_result and workflow_cancel:
                existing = await deps.backend.get_result(task_id)
                store_cancel_result = existing is None
            if store_cancel_result:
                result_bytes = encode_result(
                    build_cancel_result_dict(task_id=task_id, queue=queue, task_name=task_name)
                )
                await deps.backend.store_result(
                    task_id, result_bytes, result_ttl * 1000, op_timeout=deps.config.backend_op_timeout
                )
                try:
                    await deps.backend.publish_notification(task_id)
                except Exception as exc:
                    logger.warning("publish_notification_failed", task_id=task_id, error=str(exc))

            # Clear the flag (log+swallow at backend boundary — Unit 3).
            await deps.backend.clear_cancel_flag(task_id, op_timeout=deps.config.backend_op_timeout)

            # If this was a schedfire delivery we claimed in-flight, mark it
            # completed so a redelivery after in-flight-TTL expiry does not
            # re-run the task (Unit 1). User-initiated cancel is
            # terminal — "this fire was processed."
            await deps.finalize_schedfire(delivery, True)

            # R3.5 Unit 11: pre-start cancel is a terminal path — the task
            # never ran; release the dedup flag so a future legitimate
            # schedule() can enqueue again.
            await deps.maybe_release_unique(delivery, task_id, task_info)

            await deps.broker.ack(delivery)

        except BackendConnectionError as exc:
            # Result-write failure: nack(requeue=True) so a redelivery
            # re-runs this branch. Flag stays in Valkey (that's what
            # triggers the re-cancel on redelivery).
            logger.error("cancel_store_result_failed", task_id=task_id, error=str(exc))
            await deps.broker.redeliver(delivery)
            structlog.contextvars.clear_contextvars()
            return Redelivered

        # Signal AFTER the broker ack so a handler that raises doesn't
        # prevent the message from being committed.
        await deps.signals.dispatch(Signal.TASK_CANCELLED, task_id=task_id, task_name=task_name, queue=queue)

        logger.info("task_cancelled_pre_start", task_id=task_id, task_name=task_name)
        structlog.contextvars.clear_contextvars()
        return Terminal.already_released()
