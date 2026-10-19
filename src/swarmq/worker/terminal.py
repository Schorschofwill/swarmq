"""Workflow-Fehlmarkierung terminaler Pfade (Plan 0.12.0, MR 3, BZ-13).

Endet ein Workflow-Schritt terminal (Dead-Letter, Ablauf, DLX-Fallback),
muss der Workflow als fehlgeschlagen markiert werden -- sonst bleibt er bis
zur TTL ``RUNNING``, ``handle.get()`` laeuft in den Timeout und ein
Chord-Body nie. ``fail_workflow_step`` ist die gemeinsame Stelle dafuer;
der Logger kommt vom Aufrufer (Q17: ``RetryHandler`` reicht den von
``swarmq.worker.retry`` durch, Event-Namen und Logger bleiben gleich).

War der Schritt schon erfolgreich, kippt der Workflow nicht; ``fail_workflow_step``
holt stattdessen den Advance mit dem gespeicherten Ergebnis nach (F4-01,
E-040, KD6) und meldet ``StepDone``.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from functools import partial
from typing import Any

from swarmq.exceptions import BackendConnectionError, BackendResponseError, WorkflowStateConflict
from swarmq.headers import HEADER_WORKFLOW_ID
from swarmq.logging import _error_fields
from swarmq.serialization import decode_result
from swarmq.worker.gates import GateContext, GateDeps, StepDone, redeliver_in_place
from swarmq.worker.results import advance_with_result


async def fail_workflow_step(
    deps: GateDeps,
    advancer: Any,
    delivery: Any,
    ctx: GateContext,
    error_message: str,
    *,
    redeliver: bool,
    log: Any,
    error_class: str | None = None,
    redeliver_with: Callable[[BaseException], Awaitable[None]] | None = None,
) -> bool | StepDone:
    """Markiert den Workflow der Zustellung als fehlgeschlagen.

    ``True``: weiter mit dem terminalen Pfad (markiert, kein Workflow-Schritt
    oder ein deterministischer Fehler, den eine Wiederholung nicht heilt).
    ``False``: die Markierung (bzw. der nachgeholte Advance) scheiterte
    voruebergehend (Backend weg, verlorene CAS-Runden) und *redeliver* war
    gesetzt -- die Zustellung ist per ``redeliver`` zurueckgegeben, der
    Aufrufer endet nicht-terminal. Das Zustelllimit begrenzt die Schleife.
    ``StepDone``: das Erfolgsergebnis des Schritts ist schon gespeichert; der
    Workflow wird nicht fehlmarkiert, sondern mit dem gespeicherten Ergebnis
    weitergeschaltet (F4-01, E-040, KD6), siehe ``StepDone``.

    *error_class* (P9, T13) geht nur mit Wert als Keyword an ``fail``.
    *redeliver_with* ersetzt den Requeue an derselben Stelle: es bekommt die
    voruebergehende Ausnahme und gibt die Zustellung selbst zurueck (das
    Cancel-Gate stellt ueber die Delay-Queue zurueck, FS-12)."""
    wf_id = deps.extract_valid_workflow_id(delivery)
    if wf_id is None:
        headers: dict[str, Any] = delivery.headers or {}
        if headers.get(HEADER_WORKFLOW_ID) is not None:
            log.warning("workflow_id_header_invalid", task_id=ctx.task_id, task_name=ctx.task_name)
        return True
    if advancer is None:
        return True
    stored = await _stored_success(deps, ctx.task_id)
    if stored is not None:
        # Spaetes Duplikat eines erledigten Schritts (at-least-once, Replay)
        # oder ein Erfolg, dessen Advance scheiterte: nicht kippen, sondern
        # den Advance mit dem gespeicherten Ergebnis nachholen (idempotent,
        # bei einem schon weitergeschalteten Workflow ein No-op).
        log.info("workflow_fail_skipped_step_done", workflow_id=wf_id, task_id=ctx.task_id)
        return await _advance_done_step(
            deps, delivery, ctx, wf_id, advancer, stored, redeliver=redeliver, log=log, redeliver_with=redeliver_with
        )
    if error_class is None:
        fail = partial(advancer.fail, wf_id, "TaskError", error_message)
    else:
        fail = partial(advancer.fail, wf_id, "TaskError", error_message, error_class=error_class)
    marked = await _mark_failed(
        deps, delivery, ctx, wf_id, fail, redeliver=redeliver, log=log, redeliver_with=redeliver_with
    )
    return marked is not False


async def _advance_done_step(
    deps: GateDeps,
    delivery: Any,
    ctx: GateContext,
    wf_id: str,
    advancer: Any,
    stored: dict[str, Any],
    *,
    redeliver: bool,
    log: Any,
    redeliver_with: Callable[[BaseException], Awaitable[None]] | None,
) -> bool | StepDone:
    """Nachgeholter Advance eines erledigten Schritts (F4-01, KTD8)."""
    failure = await advance_with_result(
        advancer, wf_id, delivery, stored.get("result"), ctx.task_id, redeliver=redeliver
    )
    if failure is None:
        log.info("workflow_advance_caught_up", workflow_id=wf_id, task_id=ctx.task_id)
        return StepDone.ACK
    if failure.transient:
        if redeliver:
            await _redeliver(deps, delivery, ctx, failure.error, redeliver_with)
            return False
        if not isinstance(failure.error, BackendResponseError):
            # Zustelllimit (E-040, Restfall): der Workflow bleibt ``running``,
            # die DLQ-Kopie haelt die Zustellung fuer den Betreiber fest.
            return StepDone.DEAD_LETTER
        # Fehlerantwort am Zustelllimit (E-079, 0.13.1): unter dem Limit wie
        # ein Ausfall neu zugestellt, hier heilt keine Wiederholung mehr --
        # fehlmarkieren wie ``UnroutableMessage``.
    # ``UnroutableMessage`` oder unerwarteter Fehler: keine Wiederholung
    # heilt ihn -- den Workflow mit der Ursache fehlmarkieren (wie der
    # Ergebnis-Tail), das Erfolgsergebnis bleibt.
    fail = partial(advancer.fail, wf_id, failure.error_type, failure.reason)
    marked = await _mark_failed(
        deps, delivery, ctx, wf_id, fail, redeliver=redeliver, log=log, redeliver_with=redeliver_with
    )
    if marked is None:
        return StepDone.DEAD_LETTER
    return StepDone.ACK if marked else False


async def _mark_failed(
    deps: GateDeps,
    delivery: Any,
    ctx: GateContext,
    wf_id: str,
    fail: Callable[[], Awaitable[Any]],
    *,
    redeliver: bool,
    log: Any,
    redeliver_with: Callable[[BaseException], Awaitable[None]] | None,
) -> bool | None:
    """Fehlmarkierung *fail* ausfuehren. ``True``: markiert; ``False``:
    voruebergehend gescheitert und neu zugestellt; ``None``: gescheitert,
    nicht neu zugestellt (geloggt)."""
    try:
        await fail()
    except (BackendConnectionError, WorkflowStateConflict) as wf_exc:
        # R25/KTD12 (U10): ohne terminalen Workflow-Zustand bliebe der
        # Workflow bis zum TTL ``RUNNING``. Die Redelivery wiederholt
        # Fail-Mark und terminalen Pfad (beide idempotent).
        log.error("workflow_fail_mark_failed", workflow_id=wf_id, **_error_fields(wf_exc), redelivered=redeliver)
        if redeliver:
            await _redeliver(deps, delivery, ctx, wf_exc, redeliver_with)
            return False
        return None
    except Exception as wf_exc:  # noqa: BLE001 - Fehler wird geloggt, kein Redeliver
        # Deterministisch: eine Redelivery drehte bis zum Zustelllimit, ohne
        # dass die Markierung je gelingt.
        log.error("workflow_fail_mark_failed", workflow_id=wf_id, **_error_fields(wf_exc), redelivered=False)
        return None
    return True


async def _redeliver(
    deps: GateDeps,
    delivery: Any,
    ctx: GateContext,
    exc: BaseException,
    redeliver_with: Callable[[BaseException], Awaitable[None]] | None,
) -> None:
    try:
        if redeliver_with is None:
            await redeliver_in_place(deps, delivery, ctx)
        else:
            await redeliver_with(exc)
    except Exception:  # noqa: BLE001, S110 - Redeliver best effort, Fehler schon geloggt
        pass


async def _stored_success(deps: GateDeps, task_id: str) -> dict[str, Any] | None:
    """Das gespeicherte Erfolgsergebnis von *task_id* (dekodiert), sonst
    ``None``. Fehlt es, ist es ein Fehler-/Cancel-Ergebnis oder scheitert
    das Lesen, gilt der Schritt als nicht erledigt (Markierung wie bisher)."""
    try:
        raw = await asyncio.wait_for(deps.backend.get_result(task_id), timeout=deps.config.backend_op_timeout)
        if raw is None:
            return None
        result: dict[str, Any] = decode_result(raw)
        if result.get("is_error", True) or result.get("is_cancelled", False):
            return None
    except Exception:  # noqa: BLE001 - im Zweifel kein Erfolg
        return None
    return result


__all__ = ["fail_workflow_step"]
