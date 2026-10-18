"""Workflow-Maschine gegen die In-Memory-Fakes (Plan 2026-10-17-0941, U8, R7).

Invarianten und erlaubte Abweichungen stehen im Docstring von
``tests/property/stateful/workflow_machine.py``. Profil ``quick`` (100
Beispiele) mit 30 Schritten je Beispiel; die deterministischen Abläufe aus
``workflow_scenarios.py`` halten die Testszenarien der Einheit fest. Der
Nachweis zu F4-01 (R4Z-02) läuft als ``xfail(strict=True)``; der zu F4-16
(R4Z-01, E-073) ist seit 0.13.0 behoben und grün, R4Z-03 (NICHT HALTBAR) hat keinen Nachweis (Register ``docs/befunde/fehlersuche-runde-4-2026-10.md``).
"""

from __future__ import annotations

import os
from collections.abc import Callable, Coroutine, Iterator
from typing import Any

import pytest
from hypothesis import settings
from hypothesis.stateful import run_state_machine_as_test

from tests.property.stateful import workflow_scenarios
from tests.property.stateful.workflow_machine import WorkflowEnv, WorkflowMachine

# 60 s im Profil ``quick`` (KTD2); ``thorough`` (10 000 Beispiele) manuell.
_TIMEOUT_S = 60 if os.environ.get("HYPOTHESIS_PROFILE", "quick") == "quick" else 1800
pytestmark = pytest.mark.timeout(_TIMEOUT_S)

OpenEnv = Callable[[], Coroutine[Any, Any, WorkflowEnv]]
Scenario = Callable[[WorkflowMachine], None]


@pytest.fixture
def machine(open_env: OpenEnv) -> Iterator[WorkflowMachine]:
    machine = WorkflowMachine(open_env)
    yield machine
    machine.teardown()


def test_workflow_machine_holds_its_invariants(open_env: OpenEnv) -> None:
    run_state_machine_as_test(lambda: WorkflowMachine(open_env), settings=settings(stateful_step_count=30))


@pytest.mark.parametrize("scenario", workflow_scenarios.SCENARIOS.values(), ids=workflow_scenarios.SCENARIOS.keys())
def test_scenario(machine: WorkflowMachine, scenario: Scenario) -> None:
    scenario(machine)


def test_wrong_reference_is_caught(open_env: OpenEnv) -> None:
    machine = workflow_scenarios.ReferenceOff(open_env)
    try:
        workflow_scenarios.reference_off_is_caught(machine)
    finally:
        machine.teardown()


@pytest.mark.befund("F4-01")
@pytest.mark.xfail(
    strict=True, raises=AssertionError, reason="F4-01: offen - Dead-Letter nach Erfolg ohne Advance laesst running"
)
def test_dead_letter_after_unadvanced_success(machine: WorkflowMachine) -> None:
    workflow_scenarios.dead_letter_after_unadvanced_success(machine)


@pytest.mark.entscheidung("E-073")
@pytest.mark.befund("F4-16")
def test_unmarked_member_completing_first_completes_the_workflow(machine: WorkflowMachine) -> None:
    workflow_scenarios.unmarked_member_completing_first_completes_the_workflow(machine)


@pytest.mark.parametrize("sabotage", workflow_scenarios.SABOTAGES.values(), ids=workflow_scenarios.SABOTAGES.keys())
def test_sabotage_is_caught(machine: WorkflowMachine, sabotage: Scenario) -> None:
    sabotage(machine)
