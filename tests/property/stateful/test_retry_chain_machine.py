"""Retry- und DLQ-Ketten-Maschine gegen die In-Memory-Fakes (Plan
2026-10-17-0941, U9, R7).

Invarianten und erlaubte Abweichungen stehen im Docstring von
``tests/property/stateful/retry_chain_machine.py``. Profil ``quick`` (100
Beispiele) mit 30 Schritten je Beispiel; die deterministischen Abläufe aus
``retry_chain_scenarios.py`` halten die Testszenarien der Einheit fest.
Die Nachweise zu F4-02 (R4Z-04) sind seit 0.13.0 (U12) grün; R4Z-05 ist
eine ENTSCHEIDUNG (E-051) und hier festgeschrieben (Register
``docs/befunde/fehlersuche-runde-4-2026-10.md``).
"""

from __future__ import annotations

import os
from collections.abc import Callable, Coroutine, Iterator
from typing import Any

import pytest
from hypothesis import settings

from tests.property.stateful import retry_chain_scenarios
from tests.property.stateful.retry_chain_machine import ChainEnv, RetryChainMachine
from tests.support.stateful import run_machine

_TIMEOUT_S = 60 if os.environ.get("HYPOTHESIS_PROFILE", "quick") == "quick" else 1800
pytestmark = pytest.mark.timeout(_TIMEOUT_S)

OpenEnv = Callable[[], Coroutine[Any, Any, ChainEnv]]


def test_retry_chain_machine_holds_its_invariants(open_env: OpenEnv) -> None:
    run_machine(lambda: RetryChainMachine(open_env), settings=settings(stateful_step_count=30))


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


@pytest.mark.befund("F4-02")
@pytest.mark.parametrize(
    "nachweis", retry_chain_scenarios.F4_02_NACHWEISE.values(), ids=retry_chain_scenarios.F4_02_NACHWEISE.keys()
)
def test_f4_02_redelivery_at_the_limit_stays_a_duplicate(
    machine: RetryChainMachine, nachweis: Callable[[RetryChainMachine], None]
) -> None:
    nachweis(machine)


@pytest.mark.entscheidung("E-051")
def test_republished_retry_copy_with_text_count_is_skipped_as_superseded(machine: RetryChainMachine) -> None:
    retry_chain_scenarios.republished_retry_copy_with_text_count_is_skipped_as_superseded(machine)
