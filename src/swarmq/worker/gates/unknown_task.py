"""Lookup unbekannter Tasks (Plan 0.12.0, MR 0; verbatim aus ``pipeline.py``).

Ein nicht registrierter Task geht ueber den DLX-Fallback. Der Logger kommt vom
Aufrufer (``Worker._lookup_task_or_route`` reicht den Logger von
``swarmq.worker.pipeline`` durch, Q17), damit Logger-Name und Patch-Pfad
unveraendert bleiben.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from swarmq.exceptions import TaskNotFoundError
from swarmq.worker.gates import GateDeps, route_via_dlx

if TYPE_CHECKING:
    from swarmq.registry import TaskRegistry
    from swarmq.task import Task


async def lookup_task_or_route(
    registry: TaskRegistry,
    deps: GateDeps,
    message: Any,
    task_name: str,
    task_id: str | None = None,
    *,
    log: Any,
) -> type[Task] | None:
    """Registry-Lookup; unbekannte Tasks gehen über den DLX-Fallback
    (``None`` = behandelt). Der Body ist hier dekodiert, also trägt das
    Signal die Kennung -- ohne sie wäre die Nachricht im ``.XQ`` nicht
    wiederzufinden (B14 des Loadsuite-Backlogs)."""
    try:
        return registry.get(task_name)
    except TaskNotFoundError:
        log.error("task_not_found")
        await route_via_dlx(
            deps,
            message,
            reason="unregistered_task",
            task_id=task_id,
            task_name=task_name,
            error=f"task {task_name!r} is not registered",
        )
        return None
