"""Retry-Bewertung und Dead-Letter (Plan U8, KTD3) — Logik verbatim aus
``pipeline.py``, auf die Vertragsverben umgestellt.

``RetryHandler.evaluate`` entscheidet ``NoRetry`` / Retry-Filter / Erschöpfung
→ ``dead_letter``, sonst ``retry_later``; Middleware- und Signal-Hooks feuern in
derselben Reihenfolge wie bisher. Publish-Fehler fallen auf ``redeliver``
zurück (KTD3, unverändert). Ist Dead-Lettering für den Task abgeschaltet
(``TaskInfo.dead_letter``, sonst ``dead_letter_terminal_failures``), laufen
die terminalen Zweige über den Ergebnis-Tail (Plan 2026-10-08-1541, KTD7).

Benannte Verhaltenskorrektur (KTD2): ``retry_later(delivery, …)`` und
``dead_letter(delivery, …)`` veröffentlichen an das Verzögerungs- bzw.
Dead-Letter-Ziel der *Zustellungs-Queue* (``delivery.queue``) — ein Retry aus
``tasks.high`` bleibt im Band ``tasks.high``, statt über die im Body genannte
Queue zu laufen. Abgesichert durch ``tests/e2e/test_retry_band.py``.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

from swarmq.backoff import calculate_backoff
from swarmq.cancellation import build_error_result_dict
from swarmq.exceptions import (
    BackendConnectionError,
    NoRetry,
    Retry,
    SettleFailedAfterPublish,
    TaskNotFoundError,
    WorkflowStateConflict,
)
from swarmq.headers import HEADER_RETRY_COUNT, HEADER_WORKFLOW_ID
from swarmq.logging import _error_fields, get_logger
from swarmq.serialization import encode_result
from swarmq.signals import Signal
from swarmq.task import DeliveryMode
from swarmq.worker.compute.pool import SlotUnavailable
from swarmq.worker.execution import ExecutionResult
from swarmq.worker.gates import GateContext, GateDeps, redeliver_in_place, republish_headers
from swarmq.worker.outcome import DeliveryOutcome, Redelivered

if TYPE_CHECKING:
    from swarmq.middleware import MessageContext, MiddlewareChain

logger = get_logger(__name__)

#: Ergebnis-Tail (``ResultTail.finish``): Speichern, Workflow-Fehler, Ack.
FinishFn = Callable[[Any, GateContext, ExecutionResult], Awaitable[DeliveryOutcome]]


class RetryHandler:
    def __init__(self, deps: GateDeps, *, workflow_advancer: Any, finish: FinishFn) -> None:
        self._deps = deps
        # Getter, weil der Advancer erst in start() entsteht.
        self._workflow_advancer = workflow_advancer
        self._finish = finish

    async def evaluate(
        self,
        delivery: Any,
        ctx: GateContext,
        exc: BaseException,
        error_message: str,
        message_dict: MessageContext,
        chain: MiddlewareChain,
    ) -> DeliveryOutcome:
        """Retry oder Dead-Letter für einen fehlgeschlagenen Task.

        Beide Wege melden ``Redelivered`` an den Runner: die Retry-Kopie ist
        erneut eingereiht (Dedup-Flag bleibt, In-Flight-Claim wird gelöscht),
        und der Dead-Letter-Pfad gibt inline frei (Freigabe im ``finally`` wäre
        doppelt; das In-Flight-Löschen ist dort folgenlos) — heutige Semantik.
        """
        task_info = ctx.task_info
        assert task_info is not None
        deps = self._deps

        # 0. Slot-Fehler vor Job-Uebergabe (KTD4, Plan 2026-09-03-001): der
        # Ready-Handshake des Rechenkinds blieb aus — kein Taskfehler.
        # Redelivery ohne Retry-Zaehler; das Delivery-Limit begrenzt die Schleife.
        if isinstance(exc, SlotUnavailable):
            logger.warning(
                "compute_slot_unavailable_redeliver", task_id=ctx.task_id, **_error_fields(exc),
            )
            await redeliver_in_place(deps, delivery, ctx)
            return Redelivered

        # 1. NoRetry -> direct DLQ
        if isinstance(exc, NoRetry):
            return await self._terminal_failure(delivery, ctx, exc, error_message)

        # 2. Check retry_exceptions filter
        if task_info.retry_exceptions is not None and not isinstance(exc, task_info.retry_exceptions):
            return await self._terminal_failure(delivery, ctx, exc, error_message)

        # 3. Read retry count from message headers
        retry_count = delivery.headers.get(HEADER_RETRY_COUNT, 0)

        # 4. Check if retries exhausted
        if retry_count >= task_info.max_retries:
            return await self._terminal_failure(delivery, ctx, exc, error_message)

        # 5. Calculate delay
        if isinstance(exc, Retry) and exc.delay is not None:
            delay_seconds = exc.delay
        else:
            delay_seconds = calculate_backoff(
                retry_count, task_info.retry_backoff, task_info.retry_base_delay, deps.config.retry_max_delay
            )
        delay_ms = int(delay_seconds * 1000)

        # 5b. Middleware + signal hooks fire BEFORE the delay publish.
        # Synchronize retry_count in message_dict with the on_retry parameter:
        # both represent the NEW attempt number (the retry we're about to do).
        message_dict["retry_count"] = retry_count + 1
        await chain.run_on_retry(message_dict, exception=exc, retry_count=retry_count + 1, delay=delay_seconds)
        await deps.signals.dispatch(
            Signal.TASK_RETRYING,
            task_id=ctx.task_id,
            task_name=ctx.task_name,
            queue=ctx.queue,
            retry_count=retry_count + 1,
            delay=delay_seconds,
        )

        # 6. Retry with incremented retry count. R3.5 Unit 11: merge original
        # headers so x-swarmq-unique-hash (and any future stage-specific
        # header) survives redelivery -- ohne die Zustellzaehler des Brokers
        # (neue Nachricht, eigenes Zustellbudget).
        headers = republish_headers(delivery, ctx)
        headers[HEADER_RETRY_COUNT] = retry_count + 1
        headers["x-swarmq-traceback"] = error_message
        try:
            await deps.broker.retry_later(delivery, delay_ms=delay_ms, headers=headers)
        except Exception as broker_exc:
            logger.error("retry_publish_failed", **_error_fields(broker_exc))
            try:
                await redeliver_in_place(deps, delivery, ctx)
            except Exception:
                pass
            return Redelivered
        logger.info("task_retrying", retry_count=retry_count + 1, max_retries=task_info.max_retries, delay_ms=delay_ms)
        return Redelivered

    async def _terminal_failure(
        self, delivery: Any, ctx: GateContext, exc: BaseException, error_message: str
    ) -> DeliveryOutcome:
        """Weiche nach KTD7 (Plan 2026-10-08-1541): terminaler Task-Fehler in
        die DLQ oder -- abgeschaltet -- ueber den Ergebnis-Tail wie vor MR !12
        (Fehlerergebnis mit ``task_info.result_ttl``, Workflow-Fehler,
        Unique-Freigabe im ``finally``, Ack). Kein zusaetzliches Signal:
        ``TASK_FAILED`` feuerte schon in der Ausfuehrung."""
        task_info = ctx.task_info
        assert task_info is not None
        enabled = task_info.dead_letter
        if enabled is None:
            enabled = self._deps.config.dead_letter_terminal_failures
        if not enabled:
            execution = ExecutionResult(
                result_value=None, is_error=True, is_cancelled=False,
                error_message=error_message, task_exception=exc,
            )
            return await self._finish(delivery, ctx, execution)
        await self.dead_letter(delivery, ctx, error_message, type(exc).__name__)
        return Redelivered

    async def dead_letter(self, delivery: Any, ctx: GateContext, error_message: str, error_type: str) -> None:
        """Store error result in backend, dead-letter, and ack the original.

        ``error_type`` ist der Klassenname der ausloesenden Ausnahme (R17):
        ``task_sent_to_dlq`` traegt damit dasselbe Fehlerschema wie
        ``_error_fields``; ``error`` bleibt der Fehlertext ``error_message``.
        """
        deps = self._deps
        task_id, task_name, queue = ctx.task_id, ctx.task_name, ctx.queue
        # Fetch task_info for result_ttl, release-mode and the delivery mode;
        # unregistered tasks default to completion (see _maybe_release_unique
        # contract) and to AT_LEAST_ONCE (the TaskInfo default).
        try:
            task_info_for_release = deps.registry.get(task_name).task_info
        except TaskNotFoundError:
            task_info_for_release = None

        # Store error result so callers of get_result() get a response
        result_data = build_error_result_dict(
            task_id=task_id, queue=queue, task_name=task_name, error_message=error_message
        )
        try:
            result_bytes = encode_result(result_data)
            # Wie jedes Ergebnis des Tasks: ``task_info.result_ttl`` vor der
            # Konfiguration (Plan 2026-10-08-1541, U9).
            task_ttl = task_info_for_release.result_ttl if task_info_for_release is not None else None
            result_ttl = (task_ttl if task_ttl is not None else deps.config.result_ttl) or 86400
            await deps.backend.store_result(
                task_id, result_bytes, result_ttl * 1000, op_timeout=deps.config.backend_op_timeout
            )
            try:
                await deps.backend.notify_result(task_id)
            except Exception as exc:
                logger.warning("publish_notification_failed", **_error_fields(exc), task_id=task_id)
        except Exception as store_exc:
            logger.warning("dlq_result_store_failed", **_error_fields(store_exc))

        # R4.1 fail-fast + liveness (U6): if this terminally-failed message is
        # a workflow step, mark the whole workflow failed synchronously and
        # wake any handle.get() waiter — so get() raises the error within
        # bounded time instead of hanging to its timeout, and no subsequent
        # step / chord body is enqueued.
        wf_id = deps.extract_valid_workflow_id(delivery)
        if wf_id is None and (delivery.headers or {}).get(HEADER_WORKFLOW_ID) is not None:
            logger.warning("workflow_id_header_invalid", task_id=task_id, task_name=task_name)
        advancer = self._workflow_advancer()
        if wf_id is not None and advancer is not None:
            try:
                await advancer.fail(wf_id, "TaskError", error_message)
            except (BackendConnectionError, WorkflowStateConflict) as wf_exc:
                # R25/KTD12 (U10): ``dead_letter`` bestaetigt das Original —
                # ohne terminalen Workflow-Zustand bliebe der Workflow bis zum
                # TTL ``RUNNING`` und ``handle.get()`` liefe in den Timeout.
                # Die Redelivery wiederholt Fail-Mark und Dead-Letter (beide
                # idempotent); das Delivery-Limit begrenzt die Schleife. Bei
                # FIRE_AND_FORGET gibt es nichts zurueckzuhalten.
                at_least_once = (
                    task_info_for_release is None
                    or task_info_for_release.delivery is DeliveryMode.AT_LEAST_ONCE
                )
                logger.error(
                    "workflow_fail_mark_failed", workflow_id=wf_id,
                    **_error_fields(wf_exc), redelivered=at_least_once,
                )
                if at_least_once:
                    try:
                        await redeliver_in_place(deps, delivery, ctx)
                    except Exception:
                        pass
                    # Non-terminal — das Unique-Flag bleibt stehen, damit der
                    # terminale Pfad der Redelivery es freigibt.
                    return
            except Exception as wf_exc:
                # Deterministisch: eine Redelivery drehte bis zum
                # Delivery-Limit, ohne dass die Markierung je gelingt.
                logger.error(
                    "workflow_fail_mark_failed", workflow_id=wf_id,
                    **_error_fields(wf_exc), redelivered=False,
                )

        # R3.5 Unit 11: preserve all inbound headers so a message that
        # reached DLQ via retry-exhaustion still carries its original
        # x-swarmq-unique-hash for operator introspection (inline
        # release below takes care of the actual flag cleanup).
        headers = {**(dict(delivery.headers) if delivery.headers else {})}
        headers["x-swarmq-traceback"] = error_message
        try:
            await _dead_letter_copy(deps, delivery, headers)
            # Release AFTER successful ack (dead_letter bestätigt das
            # Original). If it raises, the except branch redelivers and
            # the redelivery's terminal path must still be able to
            # release the flag.
            await deps.maybe_release_unique(delivery, task_id, task_info_for_release)
            # Emit the terminal dead-letter signal
            # AFTER a successful publish+ack so operators get a metric
            # (outcome="dlq") and any custom handler fires exactly once
            # per dead-lettered message. The redeliver branch below is
            # non-terminal, so it intentionally emits nothing.
            await deps.signals.dispatch(
                Signal.TASK_DLQ, task_id=task_id, task_name=task_name, queue=queue, error=error_message
            )
        except Exception as broker_exc:
            logger.error("dlq_publish_failed", **_error_fields(broker_exc))
            try:
                await redeliver_in_place(deps, delivery, ctx)
            except Exception:
                pass
            # Non-terminal — leave the flag so the redelivery's terminal
            # path releases it. No release here.
        # KTD11: der Task ist endgueltig fehlgeschlagen -> ERROR.
        logger.error("task_sent_to_dlq", error_type=error_type, error=error_message)


async def _dead_letter_copy(deps: GateDeps, delivery: Any, headers: dict[str, Any]) -> None:
    """Kopie in die XQ, Original bestaetigen. Liegt die Kopie dort und
    scheitert nur das Ack (``SettleFailedAfterPublish``), ist das terminal:
    die Neuzustellung des Originals ueberspringt der Ausfuehrungsschutz als
    ``done`` und gibt nichts frei."""
    try:
        await deps.broker.dead_letter(delivery, headers=headers)
    except SettleFailedAfterPublish as exc:
        logger.warning("dlq_ack_failed_after_publish", **_error_fields(exc))
