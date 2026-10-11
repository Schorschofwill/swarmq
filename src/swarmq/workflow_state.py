"""Zustandsschema der Workflows (U12) — der festgeschriebene Vertrag.

Das persistierte JSON unter ``sq:wf:{workflow_id}`` ist die gemeinsame
Sprache von ``Client.apply``, dem ``WorkflowAdvancer`` im Worker und dem
Fail-Mark-Pfad. Es steht hier fuer sich, weil alle drei es lesen und
schreiben und keiner es besitzt; ``workflow_orchestration`` re-exportiert
die oeffentlichen Namen.
"""

from __future__ import annotations

from typing import Any

# --------------------------------------------------------------------------
# Workflow state schema (pinned contract — shared by U4 apply, U5 advance,
# U6 fail-mark). Persisted JSON under sq:wf:{workflow_id}.
# --------------------------------------------------------------------------

WORKFLOW_STATE_VERSION = 1

STATUS_RUNNING = "running"
STATUS_COMPLETED = "completed"
STATUS_FAILED = "failed"
STATUS_CANCELLED = "cancelled"

_TERMINAL_STATUSES = frozenset({STATUS_COMPLETED, STATUS_FAILED, STATUS_CANCELLED})

# R23: Konfliktgrenze des Compare-and-Set. Jede Runde gewinnt mindestens ein
# Schreiber, ein Advancer verliert also hoechstens so oft, wie parallele
# Zweige schreiben. Darueber: WorkflowStateConflict -> Redelivery.
WORKFLOW_CAS_MAX_ROUNDS = 25


class _StaleWorkflowState(Exception):
    """Intern (KTD11): der lokale Schnappschuss ist gegenueber dem Backend
    veraltet -- ein Compare-and-Set schlug fehl. ``advance()`` faengt sie und
    merged die lokalen Aenderungen in den frischen Zustand (R23)."""


class _WorkflowAlreadyTerminal(Exception):
    """Intern (R24): die frische Lesung vor dem Fan-in fand einen terminalen
    Zustand. ``advance()`` faengt sie und endet ohne Schreibung."""


def build_initial_workflow_state(workflow_id: str, topology: dict[str, Any]) -> dict[str, Any]:
    """The state dict written at apply() time.

    Fields:
      * ``status`` — running -> completed/failed/cancelled (terminal).
      * ``topology`` — the full ``node.to_dict()`` DAG, so any worker can
        navigate it without the client.
      * ``final_result_task_id`` — set by the worker (U5) when the terminal
        step's result is stored; ``handle.get()`` resolves the value from it.
      * ``error`` — ``{"type", "message"}`` set by the fail path (U6).

    Per-member introspection (R15, ``handle.children()``) is reconstructed at
    read time from ``step_tasks`` + result existence — there is no separate
    ``members`` field to keep transactionally in sync.
    """
    return {
        "v": WORKFLOW_STATE_VERSION,
        "workflow_id": workflow_id,
        "status": STATUS_RUNNING,
        "topology": topology,
        "final_result_task_id": None,
        "error": None,
        # step path -> task_id for every enqueued step. apply() seeds the
        # initial frontier; the worker (U5) appends as it advances.
        "step_tasks": {},
    }
