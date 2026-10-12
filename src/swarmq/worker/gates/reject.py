"""Ablehnung durch den Worker als Kopie mit Ablaufzeit (Plan 2026-10-12-1105, MR 8, P5b).

Nachrichten, die der Worker nicht ausfuehren kann (``undecodable``,
``middleware_chain_factory``, ``body_too_large``), legt er wie jede andere
Dead-Letter-Kopie ueber ``broker.dead_letter`` ins ``.XQ``: mit der
Dead-Letter-Frist als Ablaufzeit und dem Ablage-Stempel (KTD6). Grund und
Fehler reisen im bestehenden ``x-swarmq-traceback`` als ``"<reason>: <error>"``
(T16, kein neuer Header).
"""

from __future__ import annotations

from typing import Any

from swarmq.header_text import bounded_header_text
from swarmq.signals import Signal
from swarmq.worker.gates import GateDeps, dead_letter_copy, fail_workflow_best_effort, route_via_dlx


async def reject_to_dead_letter(
    deps: GateDeps,
    delivery: Any,
    *,
    reason: str,
    error: str,
    task_id: str | None = None,
    task_name: str | None = None,
) -> None:
    """Kopie ins ``.XQ``, danach ``TASK_REJECTED`` mit *reason* und die
    Workflow-Fehlmarkierung best effort (wie ``route_via_dlx``). Scheitert
    die Kopie, folgt ``route_via_dlx`` mit demselben Grund; es feuert das
    Signal selbst, das Signal kommt also genau einmal."""
    headers = {**(dict(delivery.headers) if delivery.headers else {})}
    headers["x-swarmq-traceback"] = bounded_header_text(f"{reason}: {error}")
    try:
        await dead_letter_copy(deps, delivery, headers)
    except Exception:  # noqa: BLE001 - jeder Kopierfehler faellt auf den DLX-Weg von 0.12 zurueck
        await route_via_dlx(deps, delivery, reason=reason, task_id=task_id, task_name=task_name, error=error)
        return
    await deps.signals.dispatch(
        Signal.TASK_REJECTED,
        queue=getattr(delivery, "queue", None),
        reason=reason,
        task_id=task_id,
        task_name=task_name,
        error=error,
    )
    await fail_workflow_best_effort(deps, delivery, task_id, task_name, error or reason)
