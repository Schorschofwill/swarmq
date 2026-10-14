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

import enum
from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING, Any, Literal

from swarmq.backoff import calculate_backoff
from swarmq.cancellation import build_error_result_dict
from swarmq.config import DEFAULT_RESULT_TTL_S, resolve_result_ttl
from swarmq.exceptions import (
    NoRetry,
    Retry,
    TaskNotFoundError,
)
from swarmq.header_text import bounded_header_text
from swarmq.headers import HEADER_RETRY_COUNT, LOOP_STATE_HEADERS
from swarmq.logging import _error_fields, get_logger
from swarmq.serialization import encode_result
from swarmq.signals import Signal
from swarmq.task import DeliveryMode
from swarmq.worker.compute.pool import SlotUnavailable
from swarmq.worker.error_info import error_class_of, gate_error_class
from swarmq.worker.execution import ExecutionResult
from swarmq.worker.gates import GateContext, GateDeps, dead_letter_copy, redeliver_in_place, republish_headers
from swarmq.worker.gates.execution_guard import retry_count_of
from swarmq.worker.outcome import DeliveryOutcome, Redelivered
from swarmq.worker.terminal import fail_workflow_step

if TYPE_CHECKING:
    from swarmq.middleware import MessageContext, MiddlewareChain

logger = get_logger(__name__)


class _ErrorClass(enum.Enum):
    """Default von ``dead_letter(error_class=...)``: die Gate-Pfade uebergeben
    keine Klasse, sie kommt aus ``error_type`` (T17)."""

    FROM_ERROR_TYPE = enum.auto()


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
                "compute_slot_unavailable_redeliver",
                task_id=ctx.task_id,
                **_error_fields(exc),
            )
            await redeliver_in_place(deps, delivery, ctx)
            return Redelivered

        # 1. NoRetry -> direct DLQ
        if isinstance(exc, NoRetry):
            return await self._terminal_failure(delivery, ctx, exc, error_message)

        # 2. Check retry_exceptions filter. ``Retry`` ist eine ausdrueckliche
        # Anweisung des Tasks und gilt unabhaengig vom Filter (P8); das Budget
        # (Schritt 4) gilt weiter.
        if (
            task_info.retry_exceptions is not None
            and not isinstance(exc, Retry)
            and not isinstance(exc, task_info.retry_exceptions)
        ):
            return await self._terminal_failure(delivery, ctx, exc, error_message)

        # 3. Read retry count from message headers; ein nicht ganzzahliger
        # Wert zaehlt wie kein Header (FS-19, KTD15).
        retry_count = retry_count_of(delivery.headers)

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
        # Q6 (BZ-06, BZ-05): der Task lief -- die Rueckstellschleifen
        # ("unbekannt", Lock-Wartezeit) sind beendet, spaetere beginnen mit
        # frischer Frist.
        for state_header in LOOP_STATE_HEADERS:
            headers.pop(state_header, None)
        headers[HEADER_RETRY_COUNT] = retry_count + 1
        headers["x-swarmq-traceback"] = bounded_header_text(error_message)
        try:
            await deps.broker.retry_later(delivery, delay_ms=delay_ms, headers=headers)
        except Exception as broker_exc:  # noqa: BLE001 - Broker gestoert, Rueckfall auf Redeliver
            logger.error("retry_publish_failed", **_error_fields(broker_exc))
            try:
                await redeliver_in_place(deps, delivery, ctx)
            except Exception:  # noqa: BLE001, S110 - Redeliver best effort, Fehler schon geloggt
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
                result_value=None,
                is_error=True,
                is_cancelled=False,
                error_message=error_message,
                task_exception=exc,
            )
            return await self._finish(delivery, ctx, execution)
        await self.dead_letter(delivery, ctx, error_message, type(exc).__name__, error_class=error_class_of(exc))
        return Redelivered

    async def fail_workflow(
        self,
        delivery: Any,
        ctx: GateContext,
        error_message: str,
        may_redeliver: bool = True,
        *,
        error_class: str | None = None,
        redeliver_with: Callable[[BaseException], Awaitable[None]] | None = None,
    ) -> bool:
        """Workflow der Zustellung fehlmarkieren (BZ-13, ``GateDeps.fail_workflow``).
        ``False``: voruebergehend gescheitert und neu zugestellt -- nur bei
        *may_redeliver* und AT_LEAST_ONCE (bzw. unregistriertem Task);
        *redeliver_with* siehe ``fail_workflow_step``."""
        try:
            task_info = self._deps.registry.get(ctx.task_name).task_info
        except TaskNotFoundError:
            task_info = None
        at_least_once = task_info is None or task_info.delivery is DeliveryMode.AT_LEAST_ONCE
        return await fail_workflow_step(
            self._deps,
            self._workflow_advancer(),
            delivery,
            ctx,
            error_message,
            redeliver=may_redeliver and at_least_once,
            log=logger,
            error_class=error_class,
            redeliver_with=redeliver_with,
        )

    async def dead_letter(
        self,
        delivery: Any,
        ctx: GateContext,
        error_message: str,
        error_type: str,
        *,
        may_redeliver: bool = True,
        extra_headers: Mapping[str, Any] | None = None,
        on_publish_failure: Callable[[Exception], Awaitable[None]] | None = None,
        error_class: str | Literal[_ErrorClass.FROM_ERROR_TYPE] | None = _ErrorClass.FROM_ERROR_TYPE,
    ) -> bool:
        """Store error result in backend, dead-letter, and ack the original.

        ``True``, wenn die DLQ-Kopie liegt (auch bei ``SettleFailedAfterPublish``);
        ``False``, wenn die Zustellung stattdessen neu zugestellt wurde oder der
        DLX-Fallback des Aufrufers lief (Plan 0.12.0, MR 11).

        ``error_type`` ist der Klassenname der ausloesenden Ausnahme (R17):
        ``task_sent_to_dlq`` traegt damit dasselbe Fehlerschema wie
        ``_error_fields``; ``error`` bleibt der Fehlertext ``error_message``.
        ``may_redeliver=False`` (am Zustelllimit): eine voruebergehend
        gescheiterte Workflow-Fehlmarkierung stellt nicht neu zu.
        ``extra_headers``: Diagnose-Header der DLQ-Kopie (Lock-Pfade).
        ``on_publish_failure``: ersetzt das ``redeliver`` nach gescheitertem
        DLQ-Publish (Lock-Pfade: terminaler DLX-Fallback, die Ursache ist
        dauerhaft).
        ``error_class`` (P9, T17): Klasse fuer das Fehlerergebnis; ohne
        Angabe (Gate-Pfade) aus ``error_type`` ueber die Tabelle in
        ``error_info`` (nur oeffentliche swarmq-Klassen, sonst keine).
        """
        if error_class is _ErrorClass.FROM_ERROR_TYPE:
            error_class = gate_error_class(error_type)
        deps = self._deps
        task_id, task_name, queue = ctx.task_id, ctx.task_name, ctx.queue
        # Fetch task_info for result_ttl, release-mode and the delivery mode;
        # unregistered tasks default to completion (see _maybe_release_unique
        # contract) and to AT_LEAST_ONCE (the TaskInfo default).
        try:
            task_info_for_release = deps.registry.get(task_name).task_info
        except TaskNotFoundError:
            task_info_for_release = None

        # R4.1 fail-fast + liveness (U6): if this terminally-failed message is
        # a workflow step, mark the whole workflow failed synchronously and
        # wake any handle.get() waiter — so get() raises the error within
        # bounded time instead of hanging to its timeout, and no subsequent
        # step / chord body is enqueued. Scheitert die Markierung
        # voruebergehend, stellt sie AT_LEAST_ONCE-Zustellungen neu zu
        # (non-terminal: das Unique-Flag bleibt stehen, damit der terminale
        # Pfad der Redelivery es freigibt); bei FIRE_AND_FORGET gibt es
        # nichts zurueckzuhalten. Vor dem Speichern des Fehlerergebnisses:
        # die Markierung prueft, ob der Schritt schon erfolgreich erledigt
        # ist (MR 3, BZ-13), und saehe sonst das eigene Fehlerergebnis.
        if not await self.fail_workflow(delivery, ctx, error_message, may_redeliver, error_class=error_class):
            return False

        # Store error result so callers of get_result() get a response
        result_data = build_error_result_dict(
            task_id=task_id, queue=queue, task_name=task_name, error_message=error_message, error_class=error_class
        )
        try:
            result_bytes = encode_result(result_data)
            # Wie jedes Ergebnis des Tasks: ``task_info.result_ttl`` vor der
            # Konfiguration (Plan 2026-10-08-1541, U9).
            task_ttl = task_info_for_release.result_ttl if task_info_for_release is not None else None
            # Anders als der Erfolgspfad: kein FIRE_AND_FORGET-Zweig, und 0
            # faellt hier auf DEFAULT_RESULT_TTL_S statt "nicht speichern".
            result_ttl = (
                resolve_result_ttl(task_ttl, deps.config.result_ttl, fire_and_forget=False) or DEFAULT_RESULT_TTL_S
            )
            await deps.backend.store_result(
                task_id, result_bytes, result_ttl * 1000, op_timeout=deps.config.backend_op_timeout
            )
            try:
                await deps.backend.notify_result(task_id)
            except Exception as exc:  # noqa: BLE001 - Benachrichtigung best effort, wird geloggt
                logger.warning("publish_notification_failed", **_error_fields(exc), task_id=task_id)
        except Exception as store_exc:  # noqa: BLE001 - Ergebnis-Speicher best effort, wird geloggt
            logger.warning("dlq_result_store_failed", **_error_fields(store_exc))

        # R3.5 Unit 11: preserve all inbound headers so a message that
        # reached DLQ via retry-exhaustion still carries its original
        # x-swarmq-unique-hash for operator introspection (inline
        # release below takes care of the actual flag cleanup).
        headers = {**(dict(delivery.headers) if delivery.headers else {})}
        headers["x-swarmq-traceback"] = bounded_header_text(error_message)
        headers.update(extra_headers or {})
        placed = False
        try:
            await dead_letter_copy(deps, delivery, headers)
            placed = True
            # Release AFTER successful ack (dead_letter bestätigt das
            # Original). If it raises, the except branch redelivers and
            # the redelivery's terminal path must still be able to
            # release the flag.
            await deps.maybe_release_unique(delivery, ctx, task_info_for_release)
            # Emit the terminal dead-letter signal
            # AFTER a successful publish+ack so operators get a metric
            # (outcome="dlq") and any custom handler fires exactly once
            # per dead-lettered message. The redeliver branch below is
            # non-terminal, so it intentionally emits nothing.
            await deps.signals.dispatch(
                Signal.TASK_DLQ, task_id=task_id, task_name=task_name, queue=queue, error=error_message
            )
        except Exception as broker_exc:  # noqa: BLE001 - Publish-Fehler fuehrt in den Rueckfall
            if on_publish_failure is not None:
                # Terminaler Fallback des Aufrufers (gibt selbst frei).
                await on_publish_failure(broker_exc)
            else:
                logger.error("dlq_publish_failed", **_error_fields(broker_exc))
                try:
                    await redeliver_in_place(deps, delivery, ctx)
                except Exception:  # noqa: BLE001, S110 - Redeliver best effort, Fehler schon geloggt
                    pass
                # Non-terminal — leave the flag so the redelivery's terminal
                # path releases it. No release here.
        # KTD11: der Task ist endgueltig fehlgeschlagen -> ERROR. Nur wenn die
        # Kopie liegt oder der DLX-Rueckfall des Aufrufers lief (P7b); nach
        # gescheitertem Publish mit Neuzustellung meldet ``dlq_publish_failed``.
        if placed or on_publish_failure is not None:
            logger.error("task_sent_to_dlq", error_type=error_type, error=error_message)
        return placed
