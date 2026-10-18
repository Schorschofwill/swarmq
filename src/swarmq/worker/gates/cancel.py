"""Pre-Start-Cancel-Gate (Phase 3b.3), Plan U7 — verbatim
aus ``pipeline.py``. Läuft nach dem Schedfire-Gate, vor Semaphore,
Rate-Limit, Lock und FIRE_AND_FORGET-Ack.

Cancel-Flag (Task oder Workflow) gesetzt → Cancel-Ergebnis speichern,
Benachrichtigung, Flag löschen, Schedfire-Claim finalisieren, Dedup-Flag
freigeben, bestätigen, Signal ``TASK_CANCELLED`` nach dem Ack; Outcome
``Terminal.already_released()``. Backend-Fehler und Result-Write-Fehler →
Zurueckstellung ueber die Delay-Queue (``backend_defer``, ``Redelivered``;
scheitert deren Publish, ``nack(requeue=True)``). Der Runner behandelt dieses Gate
wie heute *vor* seinem ``try/finally``.
"""

from __future__ import annotations

from typing import Any

from swarmq.cancellation import build_cancel_result_dict
from swarmq.exceptions import BackendConnectionError, BrokerConnectionError, TaskNotFoundError, WorkflowStateConflict
from swarmq.logging import _error_fields, get_logger
from swarmq.serialization import encode_result
from swarmq.signals import Signal
from swarmq.task import DeliveryMode, TaskInfo
from swarmq.worker.gates import GateContext, GateDeps, StepDone
from swarmq.worker.gates.backend_defer import bounded_backend_call, defer_backend_unavailable
from swarmq.worker.outcome import Continue, DeliveryOutcome, Redelivered, Terminal

logger = get_logger(__name__)


def _task_info_and_result_ttl(deps: GateDeps, task_name: str) -> tuple[TaskInfo | None, int]:
    """``TaskInfo`` und Ergebnis-TTL des Cancel-Ergebnisses; ein unbekannter
    Task faellt auf ``config.result_ttl`` zurueck (AT_LEAST_ONCE-Semantik)."""
    try:
        task_info = deps.registry.get(task_name).task_info
    except TaskNotFoundError:
        return None, deps.config.result_ttl
    # result_ttl falls through: task → config.result_ttl (Default DEFAULT_RESULT_TTL_S).
    result_ttl = task_info.result_ttl
    if result_ttl is None:
        result_ttl = 0 if task_info.delivery is DeliveryMode.FIRE_AND_FORGET else deps.config.result_ttl
    return task_info, result_ttl


async def _defer_fail_mark(deps: GateDeps, delivery: Any, ctx: GateContext, exc: BaseException) -> None:
    """Zurueckstellung nach voruebergehend gescheiterter Fehlmarkierung (FS-12).
    Verlorene CAS-Runden (``WorkflowStateConflict``) und ein gescheiterter
    Publish des nachgeholten Advance (``BrokerConnectionError``, F4-01) heilen
    mit Wartezeit wie ein Ausfall und gehen ebenfalls ueber die Delay-Queue."""
    if isinstance(exc, WorkflowStateConflict | BrokerConnectionError):
        outage = BackendConnectionError(f"workflow step not settled: {exc}")
        outage.__cause__ = exc
        exc = outage
    await defer_backend_unavailable(
        deps,
        delivery,
        ctx,
        exc,
        gate="cancel",
        stage="cancel_fail_workflow",
        workflow_id=deps.extract_valid_workflow_id(delivery),
    )


async def _fail_cancelled_step(deps: GateDeps, delivery: Any, ctx: GateContext) -> bool | StepDone:
    """Workflow des per ``cancel(task_id)`` abgebrochenen Schritts
    fehlmarkieren (FS-12); voruebergehende Fehler stellen ueber die
    Delay-Queue zurueck. Ausgaenge wie ``GateDeps.fail_workflow``."""
    if deps.fail_workflow is None:
        return True
    return await deps.fail_workflow(
        delivery,
        ctx,
        "task was cancelled",
        True,  # noqa: FBT003 - Callable-Feld ohne Parameternamen
        redeliver_with=lambda exc: _defer_fail_mark(deps, delivery, ctx, exc),
    )


async def _store_cancel_result(
    deps: GateDeps, ctx: GateContext, result_ttl: int, *, workflow_cancel: bool, step_done: bool
) -> None:
    """Cancel-Ergebnis speichern und melden; nie ueber einem Erfolg (F4-01:
    ``step_done``; Workflow-Cancel: vorhandenes Ergebnis). Backend-Fehler
    gehen an den Aufrufer (Zurueckstellung)."""
    task_id, task_name, queue = ctx.task_id, ctx.task_name, ctx.queue
    # On the WORKFLOW-cancel path a redelivery of an
    # already-completed step (success stored, ack lost) must not
    # overwrite the original result with a cancel marker — a
    # direct get_result(task_id) would then misreport a step
    # that genuinely ran. The per-task cancel path keeps its
    # unconditional write (the flag only exists pre-/mid-run). F4-01: ein
    # schon erfolgreicher Schritt (``step_done``) behaelt sein Ergebnis.
    store_cancel_result = result_ttl > 0 and not step_done
    if store_cancel_result and workflow_cancel:
        existing = await bounded_backend_call(
            deps,
            deps.backend.get_result(task_id),
            what="cancel existing result read",
        )
        store_cancel_result = existing is None
    if store_cancel_result:
        result_bytes = encode_result(build_cancel_result_dict(task_id=task_id, queue=queue, task_name=task_name))
        await deps.backend.store_result(
            task_id, result_bytes, result_ttl * 1000, op_timeout=deps.config.backend_op_timeout
        )
        try:
            await bounded_backend_call(
                deps,
                deps.backend.notify_result(task_id),
                what="cancel result notification",
            )
        except Exception as exc:  # noqa: BLE001 - Benachrichtigung best effort, wird geloggt
            logger.warning("publish_notification_failed", task_id=task_id, **_error_fields(exc))


class CancelGate:
    def __init__(self, deps: GateDeps) -> None:
        self._deps = deps

    async def apply(self, delivery: Any, ctx: GateContext) -> DeliveryOutcome:  # verbatim, U8/0.2.x
        deps = self._deps
        task_id, task_name, queue = ctx.task_id, ctx.task_name, ctx.queue
        try:
            is_cancelled = await deps.backend.is_cancelled(task_id, op_timeout=deps.config.backend_op_timeout)
        except BackendConnectionError as exc:
            # Kein Requeue an derselben Stelle: das verbrauchte das
            # Zustellbudget, nach delivery_limit Zyklen laege die Nachricht
            # still im .XQ. Die Zurueckstellung loggt die eine WARNING-Zeile.
            return await defer_backend_unavailable(deps, delivery, ctx, exc, gate="cancel", stage="cancel_check")

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
            except BackendConnectionError as exc:
                return await defer_backend_unavailable(
                    deps,
                    delivery,
                    ctx,
                    exc,
                    gate="cancel",
                    stage="workflow_cancel_check",
                    workflow_id=wf_id,
                )
            if not wf_cancelled:
                return Continue
            workflow_cancel = True
            logger.info("workflow_step_cancelled_pre_start", task_id=task_id, workflow_id=wf_id)
            # Fall through to the shared cancelled-handling below.
            # clear_cancel_flag(task_id) is an idempotent PEXPIRE LT on a
            # key that does not exist — harmless.

        # Cancel flag is set. Resolve DeliveryMode + result_ttl from
        # registry (unknown tasks fall back to AT_LEAST_ONCE semantics so
        # the client's get_result does not hang forever waiting for a
        # result that will never arrive).
        task_info, result_ttl = _task_info_and_result_ttl(deps, task_name)

        # FS-12 (E-040): ein per ``cancel(task_id)`` vor dem Start
        # abgebrochener Workflow-Schritt markiert den Workflow als
        # fehlgeschlagen, wie der Abbruch waehrend des Laufs. Vor dem
        # Cancel-Ergebnis: die Markierung prueft, ob der Schritt schon
        # erfolgreich erledigt ist, und saehe sonst das eigene Ergebnis.
        # Scheitert sie voruebergehend, wird zurueckgestellt wie an den
        # anderen Backend-Fehlerstellen (kein Requeue an derselben Stelle,
        # dieses Gate laeuft vor dem Delivery-Count-Gate); das Cancel-Flag
        # bleibt stehen, die Kopie sieht diesen Zweig wieder.
        # Der Workflow-Cancel-Zweig bleibt unveraendert (FS-32).
        # F4-01 (E-040, KD6): war der Schritt schon erfolgreich, holt die
        # Markierung seinen Advance nach; dann kein Cancel-Ergebnis ueber dem
        # Erfolg und kein ``TASK_CANCELLED``, sonst wie der Cancel-Pfad.
        step_done = False
        if not workflow_cancel:
            marked = await _fail_cancelled_step(deps, delivery, ctx)
            if marked is False:
                return Redelivered
            step_done = isinstance(marked, StepDone)

        try:
            await _store_cancel_result(deps, ctx, result_ttl, workflow_cancel=workflow_cancel, step_done=step_done)

            # Shorten the flag to at most 1 h (BZ-23; log+swallow at the
            # backend boundary — Unit 3).
            await deps.backend.clear_cancel_flag(task_id, op_timeout=deps.config.backend_op_timeout)

            # If this was a schedfire delivery we claimed in-flight, mark it
            # completed so a redelivery after in-flight-TTL expiry does not
            # re-run the task (Unit 1). User-initiated cancel is
            # terminal — "this fire was processed."
            await deps.finalize_schedfire(ctx, True)  # noqa: FBT003 - Callable-Feld ohne Parameternamen

            # R3.5 Unit 11: pre-start cancel is a terminal path — the task
            # never ran; release the dedup flag so a future legitimate
            # schedule() can enqueue again.
            await deps.maybe_release_unique(delivery, ctx, task_info)

            await deps.broker.ack(delivery)

        except BackendConnectionError as exc:
            # Result-write failure: zurueckstellen, damit die Kopie diesen
            # Zweig erneut durchlaeuft. Flag stays in Valkey (that's what
            # triggers the re-cancel on the deferred copy).
            return await defer_backend_unavailable(
                deps,
                delivery,
                ctx,
                exc,
                gate="cancel",
                stage="cancel_store_result",
            )

        if step_done:
            return Terminal.already_released()

        # Signal AFTER the broker ack so a handler that raises doesn't
        # prevent the message from being committed.
        await deps.signals.dispatch(Signal.TASK_CANCELLED, task_id=task_id, task_name=task_name, queue=queue)

        logger.info("task_cancelled_pre_start", task_id=task_id, task_name=task_name)
        return Terminal.already_released()
