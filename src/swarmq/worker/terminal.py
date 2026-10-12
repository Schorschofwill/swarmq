"""Workflow-Fehlmarkierung terminaler Pfade (Plan 0.12.0, MR 3, BZ-13).

Endet ein Workflow-Schritt terminal (Dead-Letter, Ablauf, DLX-Fallback),
muss der Workflow als fehlgeschlagen markiert werden -- sonst bleibt er bis
zur TTL ``RUNNING``, ``handle.get()`` laeuft in den Timeout und ein
Chord-Body nie. ``fail_workflow_step`` ist die gemeinsame Stelle dafuer;
der Logger kommt vom Aufrufer (Q17: ``RetryHandler`` reicht den von
``swarmq.worker.retry`` durch, Event-Namen und Logger bleiben gleich).
"""

from __future__ import annotations

import asyncio
from typing import Any

from swarmq.exceptions import BackendConnectionError, WorkflowStateConflict
from swarmq.headers import HEADER_WORKFLOW_ID
from swarmq.logging import _error_fields
from swarmq.serialization import decode_result
from swarmq.worker.gates import GateContext, GateDeps, redeliver_in_place


async def fail_workflow_step(
    deps: GateDeps,
    advancer: Any,
    delivery: Any,
    ctx: GateContext,
    error_message: str,
    *,
    redeliver: bool,
    log: Any,
) -> bool:
    """Markiert den Workflow der Zustellung als fehlgeschlagen.

    ``True``: weiter mit dem terminalen Pfad (markiert, kein Workflow-Schritt
    oder ein deterministischer Fehler, den eine Wiederholung nicht heilt).
    ``False``: die Markierung scheiterte voruebergehend (Backend weg,
    verlorene CAS-Runden) und *redeliver* war gesetzt -- die Zustellung ist
    per ``redeliver`` zurueckgegeben, der Aufrufer endet nicht-terminal. Das
    Zustelllimit begrenzt die Schleife."""
    wf_id = deps.extract_valid_workflow_id(delivery)
    if wf_id is None:
        headers: dict[str, Any] = delivery.headers or {}
        if headers.get(HEADER_WORKFLOW_ID) is not None:
            log.warning("workflow_id_header_invalid", task_id=ctx.task_id, task_name=ctx.task_name)
        return True
    if advancer is None:
        return True
    if await _step_succeeded(deps, ctx.task_id):
        # Spaetes Duplikat eines erledigten Schritts (at-least-once, Replay):
        # der Workflow ist weitergelaufen und darf nicht kippen.
        log.info("workflow_fail_skipped_step_done", workflow_id=wf_id, task_id=ctx.task_id)
        return True
    try:
        await advancer.fail(wf_id, "TaskError", error_message)
    except (BackendConnectionError, WorkflowStateConflict) as wf_exc:
        # R25/KTD12 (U10): ohne terminalen Workflow-Zustand bliebe der
        # Workflow bis zum TTL ``RUNNING``. Die Redelivery wiederholt
        # Fail-Mark und terminalen Pfad (beide idempotent).
        log.error("workflow_fail_mark_failed", workflow_id=wf_id, **_error_fields(wf_exc), redelivered=redeliver)
        if redeliver:
            try:
                await redeliver_in_place(deps, delivery, ctx)
            except Exception:  # noqa: BLE001, S110 - Redeliver best effort, Fehler schon geloggt
                pass
            return False
    except Exception as wf_exc:  # noqa: BLE001 - Fehler wird geloggt, kein Redeliver
        # Deterministisch: eine Redelivery drehte bis zum Zustelllimit, ohne
        # dass die Markierung je gelingt.
        log.error("workflow_fail_mark_failed", workflow_id=wf_id, **_error_fields(wf_exc), redelivered=False)
    return True


async def _step_succeeded(deps: GateDeps, task_id: str) -> bool:
    """``True``, wenn fuer *task_id* ein Erfolgsergebnis gespeichert ist.
    Fehlt es, ist es ein Fehler-/Cancel-Ergebnis oder scheitert das Lesen,
    gilt der Schritt als nicht erledigt (Markierung wie bisher)."""
    try:
        raw = await asyncio.wait_for(deps.backend.get_result(task_id), timeout=deps.config.backend_op_timeout)
        if raw is None:
            return False
        result = decode_result(raw)
        return not result.get("is_error", True) and not result.get("is_cancelled", False)
    except Exception:  # noqa: BLE001 - im Zweifel kein Erfolg
        return False


__all__ = ["fail_workflow_step"]
