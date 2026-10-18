"""Ergebnis-Tail (Plan U20, KTD5/KTD6) — verbatim aus dem Runner.

``result_ttl``-Auflösung, Serialisierung mit Fehler-Fallback, Speichern,
Benachrichtigung, Cancel-Flag löschen (nach dem Speichern) und der
Workflow-Advance-Seam. Backend-/Broker-Fehler → ``Redelivered`` (Redelivery
wiederholt den idempotenten Advance); unerwarteter Advance-Fehler →
Fail-Mark + Ack. publish-then-mark ist seit U20 strukturell über den
``PublishReceipt`` erzwungen (``WorkflowAdvancer.mark_enqueued``, AE5).
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from swarmq.backoff import backend_defer_delay
from swarmq.cancellation import build_error_result_dict, build_result_dict
from swarmq.config import resolve_result_ttl
from swarmq.exceptions import (
    BackendConnectionError,
    BrokerConnectionError,
    ResultSerializationError,
    UnroutableMessage,
    WorkflowStateConflict,
)
from swarmq.headers import HEADER_WORKFLOW_ID, HEADER_WORKFLOW_STEP
from swarmq.logging import _error_fields, get_logger
from swarmq.serialization import encode_result
from swarmq.task import DeliveryMode
from swarmq.worker.error_info import error_class_of
from swarmq.worker.gates import GateContext, GateDeps, redeliver_in_place
from swarmq.worker.outcome import DeliveryOutcome, Redelivered, Terminal

if TYPE_CHECKING:
    from swarmq.worker.execution import ExecutionResult

logger = get_logger(__name__)

# E-072 (Plan 2026-10-17-2103, KTD7): Speicherversuche eines terminalen
# Ergebnisses je Zustellung (AT_LEAST_ONCE), dazwischen die Wartezeiten der
# Zurückstellung (nominell 1, 2, 4, 8 s).
STORE_ATTEMPTS = 5
# ``result_store_retrying`` als WARNING höchstens einmal je Intervall und
# Worker, dazwischen DEBUG: ein Valkey-Ausfall trifft alle gerade fertigen
# Tasks zugleich.
RETRY_WARNING_INTERVAL_S = 10.0

# Zeitquellen der Speicher-Wiederholung zur Laufzeit; Unit-Tests ersetzen sie
# (``tests/unit/conftest.py``), damit keiner real wartet.
_sleep: Callable[[float], Awaitable[None]] = asyncio.sleep
_monotonic: Callable[[], float] = time.monotonic

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


@dataclass(frozen=True)
class AdvanceFailure:
    """Gescheiterter Advance (``advance_with_result``). ``transient``: der
    Advance ist nicht fertig, nicht gescheitert (Backend weg, verlorene
    CAS-Runden, Publish ohne Confirm) -- eine Neuzustellung wiederholt ihn.
    Sonst (``UnroutableMessage``, unerwarteter Fehler) heilt keine
    Wiederholung: den Workflow mit *error_type* und *reason* fehlmarkieren."""

    transient: bool
    error: Exception

    @property
    def error_type(self) -> str:
        if isinstance(self.error, UnroutableMessage):
            return "UnroutableMessage"
        return type(self.error).__name__

    @property
    def reason(self) -> str:
        return str(self.error)


async def advance_with_result(
    advancer: Any, wf_id: str, delivery: Any, result_value: Any, task_id: str, *, redeliver: bool
) -> AdvanceFailure | None:
    """Advance eines erfolgreichen Schritts mit *result_value*; ``None``, wenn
    er gelang. Gemeinsam für den Ergebnis-Tail und den nachgeholten Advance
    in ``fail_workflow_step`` (F4-01, KTD8). Loggt jeden Fehler; *redeliver*
    sagt, ob der Aufrufer einen vorübergehenden Fehler neu zustellt (WARNING)
    oder nicht (ERROR)."""
    headers: dict[str, Any] = delivery.headers or {}
    step_path = headers.get(HEADER_WORKFLOW_STEP, "")
    if not isinstance(step_path, str):
        step_path = ""
    try:
        await advancer.advance(wf_id, step_path, task_id, result_value)
    except (BackendConnectionError, WorkflowStateConflict) as exc:
        # Die Konfliktgrenze des Compare-and-Set (U9) heisst: der Advance
        # ist nicht fertig, nicht gescheitert — wie ein Backend-Ausfall
        # behandeln, nicht fail-markieren.
        if redeliver:
            logger.warning("workflow_advance_backend_error", workflow_id=wf_id, **_error_fields(exc))
        else:
            logger.error("workflow_advance_backend_error", workflow_id=wf_id, **_error_fields(exc))
        return AdvanceFailure(transient=True, error=exc)
    except UnroutableMessage as exc:
        # Die Queue des naechsten Schritts fehlt (BZ-03, Plan 0.12.0 MR 6).
        # Eine Redelivery aenderte daran nichts und liefe bis zum
        # Zustelllimit: den Workflow sofort fail-markieren.
        logger.error("workflow_advance_unroutable", workflow_id=wf_id, **_error_fields(exc))
        return AdvanceFailure(transient=False, error=exc)
    except BrokerConnectionError as exc:
        # A workflow-step publish failed transiently (e.g. a
        # publisher-confirm Nack past the broker's retry budget). Do NOT
        # mark the workflow failed — a Nack means the message was not
        # committed, so redelivery re-runs the idempotent advance and
        # re-publishes safely (deterministic step ids + step_tasks).
        if redeliver:
            logger.warning("workflow_advance_broker_error", workflow_id=wf_id, **_error_fields(exc))
        else:
            logger.error("workflow_advance_broker_error", workflow_id=wf_id, **_error_fields(exc))
        return AdvanceFailure(transient=True, error=exc)
    except Exception as exc:  # noqa: BLE001 - unerwarteter Advance-Fehler markiert Workflow
        # Unexpected advance bug: fail the workflow so get() raises
        # instead of hanging (no infinite redelivery loop).
        logger.error("workflow_advance_failed", workflow_id=wf_id, **_error_fields(exc))
        return AdvanceFailure(transient=False, error=exc)
    return None


def _error_class(execution: ExecutionResult) -> str | None:
    """``error_class`` eines Fehlerergebnisses (P9): nur mit Task-Exception."""
    return None if execution.task_exception is None else error_class_of(execution.task_exception)


class ResultTail:
    def __init__(self, deps: GateDeps, *, workflow_advancer: Any) -> None:
        self._deps = deps
        self._workflow_advancer = workflow_advancer
        # Zeitpunkt der letzten ``result_store_retrying``-WARNING (Drossel).
        self._retry_warned_at: float | None = None

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
            error_class=_error_class(execution),
        )
        try:
            return encode_result(result_data)
        except ResultSerializationError as exc:
            logger.error("result_serialization_failed", **_error_fields(exc))
            result_data = build_error_result_dict(
                task_id=ctx.task_id,
                queue=ctx.queue,
                task_name=ctx.task_name,
                error_message=str(exc),
                error_class=error_class_of(exc),
            )
            return encode_result(result_data)

    def _resolve_result_ttl(self, ctx: GateContext) -> int:
        """``result_ttl``-Kaskade: task_info → config → ``DEFAULT_RESULT_TTL_S``;
        FIRE_AND_FORGET ohne explizites TTL und 0 speichern nicht (0)."""
        task_info = ctx.task_info
        assert task_info is not None
        return resolve_result_ttl(
            task_info.result_ttl,
            self._deps.config.result_ttl,
            fire_and_forget=task_info.delivery is DeliveryMode.FIRE_AND_FORGET,
        )

    def _extract_workflow_id(self, delivery: Any, ctx: GateContext) -> str | None:
        deps = self._deps
        wf_id = deps.extract_valid_workflow_id(delivery)
        headers: dict[str, Any] = delivery.headers or {}
        if wf_id is None and headers.get(HEADER_WORKFLOW_ID) is not None:
            logger.warning(
                "workflow_id_header_invalid",
                task_id=ctx.task_id,
                task_name=ctx.task_name,
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
            # Meldung nach Ursache (FS-28): FIRE_AND_FORGET oder kein Ergebnis.
            if task_info.delivery is DeliveryMode.FIRE_AND_FORGET:
                cause = "is FIRE_AND_FORGET"
            else:
                cause = "stores no result (result_ttl=0)"
            outcome = await self._fail_workflow(
                delivery,
                ctx,
                wf_id_faf,
                advancer,
                "WorkflowValidationError",
                f"task {ctx.task_name!r} {cause} and cannot advance a workflow",
                event="workflow_faf_fail_mark_error",
            )
            if outcome is not None:
                # Redelivered: kein Ack, und das Cancel-Flag bleibt stehen,
                # damit die Redelivery den Cancel-Zweig erneut sieht.
                return outcome
        if task_info.delivery is DeliveryMode.AT_LEAST_ONCE:
            await deps.broker.ack(delivery)
        # Even with result_ttl=0 the cancel flag is shortened to at most
        # 1 h (BZ-23, PEXPIRE LT) so it does not live its full TTL; a
        # duplicate of the message within that hour stays cancelled.
        if execution.is_cancelled:
            await deps.backend.clear_cancel_flag(ctx.task_id, op_timeout=deps.config.backend_op_timeout)
        return Terminal.release_in_finally()

    async def _fail_workflow(
        self,
        delivery: Any,
        ctx: GateContext,
        wf_id: str,
        advancer: Any,
        error_type: str,
        reason: str,
        *,
        event: _FailMarkEvent,
        error_class: str | None = None,
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
        sonst ``None`` (der Aufrufer läuft weiter wie bisher).
        *error_class* (P9, T13) geht nur mit Wert als Keyword an ``fail``."""
        deps = self._deps
        task_info = ctx.task_info
        assert task_info is not None
        try:
            if error_class is None:
                await advancer.fail(wf_id, error_type, reason)
            else:
                await advancer.fail(wf_id, error_type, reason, error_class=error_class)
        except (BackendConnectionError, WorkflowStateConflict) as exc:
            at_least_once = task_info.delivery is DeliveryMode.AT_LEAST_ONCE
            _log_fail_mark_error(event, workflow_id=wf_id, **_error_fields(exc), redelivered=at_least_once)
            if at_least_once:
                await redeliver_in_place(deps, delivery, ctx)
                return Redelivered
        except Exception as exc:  # noqa: BLE001 - Fehler wird geloggt, terminaler Pfad laeuft weiter
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
            reason = (
                "task was cancelled" if execution.is_cancelled else (execution.error_message or "workflow step failed")
            )
            return await self._fail_workflow(
                delivery,
                ctx,
                wf_id,
                advancer,
                "TaskError",
                reason,
                event="workflow_fail_mark_failed",
                error_class=_error_class(execution),
            )
        at_least_once = task_info.delivery is DeliveryMode.AT_LEAST_ONCE
        failure = await advance_with_result(
            advancer, wf_id, delivery, execution.result_value, ctx.task_id, redeliver=at_least_once
        )
        if failure is None:
            return None
        if failure.transient:
            if at_least_once:
                # Transient — redelivery retries the idempotent advance.
                await redeliver_in_place(deps, delivery, ctx)
                return Redelivered
            return Terminal.release_in_finally()
        # ``UnroutableMessage`` oder unerwarteter Fehler: fail-markieren, dann
        # Ack. Bewusst ``_fail_workflow`` statt ``fail_workflow_step``: dessen
        # Schutz "Schritt schon erledigt" griffe hier immer, denn das Ergebnis
        # dieses Schritts ist bereits gespeichert.
        return await self._fail_workflow(
            delivery,
            ctx,
            wf_id,
            advancer,
            failure.error_type,
            failure.reason,
            event="workflow_fail_mark_error",
        )

    async def _store_once(self, ctx: GateContext, result_bytes: bytes, result_ttl: int) -> None:
        await self._deps.backend.store_result(
            ctx.task_id,
            result_bytes,
            result_ttl * 1000,
            op_timeout=self._deps.config.backend_op_timeout,
        )

    async def _store(self, ctx: GateContext, result_bytes: bytes, result_ttl: int) -> BackendConnectionError | None:
        """Ergebnis speichern; liefert den letzten Fehler, wenn es nicht gelang.

        E-072 (KTD7): bei AT_LEAST_ONCE bis zu ``STORE_ATTEMPTS`` Versuche mit
        den Wartezeiten der Zurückstellung, gedeckelt durch
        ``consumer_timeout_s`` ab ``ctx.received_at``; der Slot bleibt dabei
        belegt. FIRE_AND_FORGET bleibt bei einem Versuch (KD8)."""
        try:
            await self._store_once(ctx, result_bytes, result_ttl)
        except BackendConnectionError as exc:
            task_info = ctx.task_info
            assert task_info is not None
            if task_info.delivery is not DeliveryMode.AT_LEAST_ONCE:
                return exc
            try:
                return await self._retry_store(ctx, result_bytes, result_ttl, exc)
            except asyncio.CancelledError:
                # Drain-Ende (E-001) im Warten oder in einem wiederholten
                # Versuch: nie propagieren -- der Terminal-Default im
                # ``finally`` schriebe sonst Schedfire-Marker und
                # Unique-Freigabe auf eine unbestätigte Nachricht.
                return exc
        return None

    def _log_retrying(self, ctx: GateContext, store_attempt: int, delay_s: float, exc: BaseException) -> None:
        fields = {"task_id": ctx.task_id, "store_attempt": store_attempt, "delay_s": delay_s, **_error_fields(exc)}
        now = _monotonic()
        if self._retry_warned_at is None or now - self._retry_warned_at >= RETRY_WARNING_INTERVAL_S:
            self._retry_warned_at = now
            logger.warning("result_store_retrying", **fields)
        else:
            logger.debug("result_store_retrying", **fields)

    async def _retry_store(
        self, ctx: GateContext, result_bytes: bytes, result_ttl: int, exc: BackendConnectionError
    ) -> BackendConnectionError | None:
        config = self._deps.config
        deadline = ctx.received_at + config.consumer_timeout_s
        for attempt in range(2, STORE_ATTEMPTS + 1):
            delay_s = backend_defer_delay(attempt - 1, config.backend_defer_base_delay, config.backend_defer_max_delay)
            # Der nächste Versuch muss vor ``consumer_timeout_s`` enden,
            # sonst schlösse RabbitMQ den Kanal der unbestätigten Zustellung.
            if _monotonic() + delay_s + config.backend_op_timeout > deadline:
                return exc
            self._log_retrying(ctx, attempt - 1, delay_s, exc)
            await _sleep(delay_s)
            try:
                await self._store_once(ctx, result_bytes, result_ttl)
            except BackendConnectionError as retry_exc:
                exc = retry_exc
                continue
            logger.info("result_store_recovered", task_id=ctx.task_id, store_attempts=attempt)
            return None
        return exc

    async def finish(self, delivery: Any, ctx: GateContext, execution: ExecutionResult) -> DeliveryOutcome:
        deps = self._deps
        advancer = self._workflow_advancer()
        task_info = ctx.task_info
        assert task_info is not None

        result_bytes = self._encode_with_fallback(ctx, execution)
        result_ttl = self._resolve_result_ttl(ctx)
        if result_ttl == 0:
            return await self._finish_without_store(delivery, ctx, execution, advancer)

        # Store result, then notify; ack comes last.
        store_error = await self._store(ctx, result_bytes, result_ttl)
        if store_error is not None:
            if task_info.delivery is DeliveryMode.AT_LEAST_ONCE:
                # Selbstheilend (KTD11): die Neuzustellung speichert erneut.
                logger.warning("backend_connection_error", **_error_fields(store_error))
                # Non-terminal — redelivery will re-run the task from
                # scratch; keep the flag so next terminal path can release.
                await redeliver_in_place(deps, delivery, ctx)
                return Redelivered
            # FIRE_AND_FORGET: keine Neuzustellung, das Ergebnis ist verloren.
            logger.error("backend_connection_error", **_error_fields(store_error))
            return Terminal.release_in_finally()

        try:
            await deps.backend.notify_result(ctx.task_id)
        except Exception as exc:  # noqa: BLE001 - Benachrichtigung best effort, wird geloggt
            logger.warning(
                "publish_notification_failed",
                **_error_fields(exc),
                task_id=ctx.task_id,
            )

        # Shorten the cancel flag (BZ-23: at most 1 h, not deleted) AFTER
        # the result is safely stored so a
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
