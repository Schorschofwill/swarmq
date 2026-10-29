"""Lock-Maschine gegen die In-Memory-Fakes (Plan 2026-10-17-0941, U6, R7).

Invarianten und erlaubte Abweichungen stehen im Docstring von
``tests/property/stateful/lock_machine.py``. Profil ``quick`` (100 Beispiele)
mit 30 Schritten je Beispiel; die deterministischen Abläufe aus
``lock_scenarios.py`` halten die Testszenarien der Einheit fest.

Marker ``entscheidung`` nur, wo der Ablauf genau die Entscheidung prüft
(E-033, E-034 Nachtrag FS-13, E-052 Unique-Rückbau). Die Keeper-Abläufe
tragen keinen: Die Regel ``advance`` prüft dabei die genauen Fristen des
Keepers (I1, I4), die E-022 nicht festlegt. Ohne Marker bleiben auch der
nachgebildete Startpunkt (E-034 genau einmal) und der Owner-Check.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Coroutine, Iterator
from typing import Any

import pytest
from hypothesis import settings

from tests.property.stateful import lock_scenarios
from tests.property.stateful.lock_machine import LockEnv, LockMachine
from tests.support.stateful import run_machine

# 60 s im Profil ``quick`` (KTD2); ``thorough`` (10 000 Beispiele, manuell nach
# docs/testing/test-strategy.md) braucht rund zwei Minuten.
_TIMEOUT_S = 60 if os.environ.get("HYPOTHESIS_PROFILE", "quick") == "quick" else 1800
pytestmark = pytest.mark.timeout(_TIMEOUT_S)

OpenEnv = Callable[[], Coroutine[Any, Any, LockEnv]]


@pytest.fixture
def machine(open_env: OpenEnv) -> Iterator[LockMachine]:
    machine = LockMachine(open_env)
    yield machine
    machine.teardown()


def test_lock_machine_holds_its_invariants(open_env: OpenEnv) -> None:
    run_machine(lambda: LockMachine(open_env), settings=settings(stateful_step_count=30))


def test_two_owners_with_expiry_in_between(machine: LockMachine) -> None:
    lock_scenarios.two_owners_with_expiry_in_between(machine)


def test_ticking_keeper_holds_the_lock_far_past_its_ttl(machine: LockMachine) -> None:
    lock_scenarios.ticking_keeper_holds_the_lock_far_past_its_ttl(machine)


def test_keeper_extends_before_expiry_and_reports_loss_after(machine: LockMachine) -> None:
    lock_scenarios.keeper_extends_before_expiry_and_reports_loss_after(machine)


def test_foreign_tokens_change_nothing(machine: LockMachine) -> None:
    lock_scenarios.foreign_tokens_change_nothing(machine)


def test_model_without_owner_check_is_caught(open_env: OpenEnv) -> None:
    machine = lock_scenarios.OwnerCheckOmitted(open_env)
    try:
        lock_scenarios.model_without_owner_check_is_caught(machine)
    finally:
        machine.teardown()


@pytest.mark.entscheidung("E-034")
def test_end_before_start_frees_the_start_flag(machine: LockMachine) -> None:
    lock_scenarios.end_before_start_frees_the_start_flag(machine)


def test_start_mode_releases_exactly_once(machine: LockMachine) -> None:
    lock_scenarios.start_mode_releases_exactly_once(machine)


@pytest.mark.entscheidung("E-033")
def test_unique_for_none_never_claims(machine: LockMachine) -> None:
    lock_scenarios.unique_for_none_never_claims(machine)


@pytest.mark.entscheidung("E-052")
def test_reply_lost_claim_never_touches_a_foreign_flag(machine: LockMachine) -> None:
    lock_scenarios.reply_lost_claim_never_touches_a_foreign_flag(machine)
