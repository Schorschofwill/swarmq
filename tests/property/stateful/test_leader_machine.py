"""Leader- und Fire-Index-Maschine gegen die In-Memory-Fakes (Plan
2026-10-17-0941, U7, R7).

Invarianten und erlaubte Abweichungen stehen im Docstring von
``tests/property/stateful/leader_machine.py``. Profil ``quick`` (100
Beispiele) mit 30 Schritten je Beispiel; die deterministischen Abläufe aus
``leader_scenarios.py`` halten die Testszenarien der Einheit fest.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Coroutine, Iterator
from typing import Any

import pytest
from hypothesis import settings
from hypothesis.stateful import run_state_machine_as_test

from tests.property.stateful import leader_scenarios
from tests.property.stateful.leader_machine import LeaderEnv, LeaderMachine

_TIMEOUT_S = 60 if os.environ.get("HYPOTHESIS_PROFILE", "quick") == "quick" else 1800
pytestmark = pytest.mark.timeout(_TIMEOUT_S)

OpenEnv = Callable[[], Coroutine[Any, Any, LeaderEnv]]


@pytest.fixture
def machine(open_env: OpenEnv) -> Iterator[LeaderMachine]:
    machine = LeaderMachine(open_env)
    yield machine
    machine.teardown()


def test_leader_machine_holds_its_invariants(open_env: OpenEnv) -> None:
    run_state_machine_as_test(lambda: LeaderMachine(open_env), settings=settings(stateful_step_count=30))


_SCENARIOS = leader_scenarios.SCENARIOS | leader_scenarios.FAKE_ONLY_SCENARIOS


@pytest.mark.parametrize("scenario", _SCENARIOS.values(), ids=_SCENARIOS.keys())
def test_scenario(machine: LeaderMachine, scenario: Callable[[LeaderMachine], None]) -> None:
    scenario(machine)


@pytest.mark.parametrize("sabotage", leader_scenarios.SABOTAGES.values(), ids=leader_scenarios.SABOTAGES.keys())
def test_sabotage_is_caught(machine: LeaderMachine, sabotage: Callable[[LeaderMachine], None]) -> None:
    sabotage(machine)
