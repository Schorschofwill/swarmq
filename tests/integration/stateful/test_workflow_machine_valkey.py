"""Workflow-Maschine gegen Valkey (Plan 2026-10-17-0941, U8, R7, KTD2).

``ChordManager`` (``chord_complete.lua``, über ``ValkeyBackend.chords``) und
das Compare-and-Set des ``ValkeyBackend`` (``workflow_state_cas.lua``) unter
derselben Maschine wie gegen die Fakes; Invarianten und erlaubte Abweichungen
im Docstring von ``tests/property/stateful/workflow_machine.py``. Der Broker
bleibt ``InMemoryBroker``. Wenige Beispiele (``max_examples=8``); eine
Abweichung zwischen Fake und Valkey ist ein Verdacht (R7).

Die Zufallssuche sperrt hier die Regel ``concurrent_advance``
(``SerialWorkflowMachine``): Gegen Valkey bestimmt die echte Reihenfolge der
Antworten, welche Zustellungen danach offen sind, und damit die erlaubten
Regeln; Hypothesis meldete das als ``FlakyStrategyDefinition``. Die
Überlappung zweier Advancer deckt der Ablauf ``two_advancers_on_the_last_member``.
"""

from __future__ import annotations

from collections.abc import Callable, Coroutine, Iterator
from typing import Any

import pytest
from hypothesis import HealthCheck, settings
from hypothesis.stateful import run_state_machine_as_test

from tests.property.stateful import workflow_scenarios
from tests.property.stateful.workflow_machine import WorkflowEnv, WorkflowMachine

pytestmark = pytest.mark.timeout(60)


class SerialWorkflowMachine(WorkflowMachine):
    allow_concurrent = False


OpenEnv = Callable[[], Coroutine[Any, Any, WorkflowEnv]]
Scenario = Callable[[WorkflowMachine], None]

VALKEY_SETTINGS = settings(
    max_examples=8,
    deadline=None,
    stateful_step_count=30,
    suppress_health_check=[HealthCheck.too_slow],
)


@pytest.fixture
def machine(open_env: OpenEnv) -> Iterator[WorkflowMachine]:
    machine = WorkflowMachine(open_env)
    yield machine
    machine.teardown()


def test_workflow_machine_holds_its_invariants_against_valkey(open_env: OpenEnv) -> None:
    run_state_machine_as_test(lambda: SerialWorkflowMachine(open_env), settings=VALKEY_SETTINGS)


@pytest.mark.parametrize("scenario", workflow_scenarios.SCENARIOS.values(), ids=workflow_scenarios.SCENARIOS.keys())
def test_scenario_against_valkey(machine: WorkflowMachine, scenario: Scenario) -> None:
    scenario(machine)


@pytest.mark.xfail(strict=True, raises=AssertionError, reason="R4Z-01: Gruppenglied ohne persistierten Marker")
def test_unmarked_member_completing_first_against_valkey(machine: WorkflowMachine) -> None:
    workflow_scenarios.unmarked_member_completing_first(machine)


@pytest.mark.xfail(strict=True, raises=AssertionError, reason="R4Z-02: Dead-Letter nach Erfolg ohne Advance")
def test_dead_letter_after_unadvanced_success_against_valkey(machine: WorkflowMachine) -> None:
    workflow_scenarios.dead_letter_after_unadvanced_success(machine)


def test_an_example_does_not_see_the_state_of_its_predecessor(open_env: OpenEnv) -> None:
    first = WorkflowMachine(open_env)
    try:
        wf = first.start(workflow_scenarios.CHORD3)
        first.drain()
        wf_id = wf.wf_id
    finally:
        first.teardown()

    second = WorkflowMachine(open_env)  # öffnet nur auf leerer Datenbank
    try:
        assert second.run(second.env.backend.get_workflow_state(wf_id)) is None
        second.verify()
    finally:
        second.teardown()


@pytest.mark.parametrize("sabotage", workflow_scenarios.SABOTAGES.values(), ids=workflow_scenarios.SABOTAGES.keys())
def test_sabotage_is_caught_against_valkey(machine: WorkflowMachine, sabotage: Scenario) -> None:
    sabotage(machine)
