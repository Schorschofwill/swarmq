"""Retry- und DLQ-Ketten-Maschine gegen die In-Memory-Fakes (Plan
2026-10-17-0941, U9, R7).

Invarianten und erlaubte Abweichungen stehen im Docstring von
``tests/property/stateful/retry_chain_machine.py``. Profil ``quick`` (100
Beispiele) mit 30 Schritten je Beispiel; die deterministischen Abläufe aus
``retry_chain_scenarios.py`` halten die Testszenarien der Einheit fest.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Coroutine, Iterator
from typing import Any

import pytest
from hypothesis import settings
from hypothesis.stateful import run_state_machine_as_test

from tests.property.stateful import retry_chain_scenarios
from tests.property.stateful.retry_chain_machine import ChainEnv, RetryChainMachine

_TIMEOUT_S = 60 if os.environ.get("HYPOTHESIS_PROFILE", "quick") == "quick" else 1800
pytestmark = pytest.mark.timeout(_TIMEOUT_S)

OpenEnv = Callable[[], Coroutine[Any, Any, ChainEnv]]


def test_retry_chain_machine_holds_its_invariants(open_env: OpenEnv) -> None:
    run_state_machine_as_test(lambda: RetryChainMachine(open_env), settings=settings(stateful_step_count=30))


@pytest.fixture
def machine(open_env: OpenEnv) -> Iterator[RetryChainMachine]:
    machine = RetryChainMachine(open_env)
    yield machine
    machine.teardown()


@pytest.mark.parametrize(
    "scenario", retry_chain_scenarios.SCENARIOS.values(), ids=retry_chain_scenarios.SCENARIOS.keys()
)
def test_scenario(machine: RetryChainMachine, scenario: Callable[[RetryChainMachine], None]) -> None:
    scenario(machine)


def test_lenient_count_model_is_caught(open_env: OpenEnv) -> None:
    machine = retry_chain_scenarios.LenientCountModel(open_env)
    try:
        retry_chain_scenarios.lenient_model_is_caught(machine)
    finally:
        machine.teardown()


@pytest.mark.xfail(strict=True, reason="Verdacht R4Z-nn (Fehlersuche Runde 4), Gegenprüfung in U17")
@pytest.mark.parametrize("suspect", retry_chain_scenarios.SUSPECTS.values(), ids=retry_chain_scenarios.SUSPECTS.keys())
def test_suspect(machine: RetryChainMachine, suspect: Callable[[RetryChainMachine], None]) -> None:
    suspect(machine)
