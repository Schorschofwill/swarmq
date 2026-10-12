"""Reine Topologie-Helfer (U12, KTD13): aus ``test_workflow_advancer.py``
verschobene Faelle plus der Re-Export-Vertrag.

Der Modul-Split ist ein Refactor ohne Verhaltenswechsel — die Faelle sind
dieselben, nur an ihrem neuen Ort. Der Re-Export-Test ist nicht kosmetisch:
``workflow_orchestration`` importiert die Namen selbst nicht mehr, ein
``ruff --fix`` haelt sie ohne die ``X as X``-Form fuer toten Import und
loescht sie — und bricht damit jeden bestehenden Importweg.
"""

from __future__ import annotations

from swarmq import workflow_orchestration as wo
from swarmq.workflow_enqueue import WorkflowStep, WorkflowStepEnqueuer
from swarmq.workflow_state import STATUS_RUNNING, build_initial_workflow_state
from swarmq.workflow_topology import (
    aggregate_result_id,
    initial_frontier,
    inject_result,
    node_at_path,
    split_path,
    workflow_step_task_id,
)


def test_workflow_step_task_id_is_deterministic():
    # Determinism is the property that keeps a re-enqueued member's id stable
    # so the chord SADD set cannot be inflated past N under a lost-persist crash.
    assert workflow_step_task_id("wf-1", "/h/g0") == workflow_step_task_id("wf-1", "/h/g0")
    assert workflow_step_task_id("wf-1", "/0") != workflow_step_task_id("wf-1", "/1")


def test_workflow_orchestration_reexports_the_moved_names():
    """Die bestehenden Importwege (``client/workflows.py``, ``worker/*``,
    ``app.py``, ``__init__.py``) zeigen weiter auf ``workflow_orchestration``."""
    assert wo.workflow_step_task_id is workflow_step_task_id
    assert wo.aggregate_result_id is aggregate_result_id
    assert wo.split_path is split_path
    assert wo.node_at_path is node_at_path
    assert wo.initial_frontier is initial_frontier
    assert wo.inject_result is inject_result
    assert wo.WorkflowStep is WorkflowStep
    assert wo.WorkflowStepEnqueuer is WorkflowStepEnqueuer
    assert wo.build_initial_workflow_state is build_initial_workflow_state
    assert wo.STATUS_RUNNING is STATUS_RUNNING
