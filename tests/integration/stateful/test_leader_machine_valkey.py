"""Leader- und Fire-Index-Maschine gegen Valkey (Plan 2026-10-17-0941, U7,
R7, KTD2).

``ValkeyScheduleStore`` (``sched_add/poll/commit/replan.lua``, Handover,
Cron-Meldungen) und ``ValkeyLockStore`` (``lock_acquire/renew/release.lua``)
unter derselben Maschine wie gegen die Fakes; Invarianten und erlaubte
Abweichungen im Docstring von ``tests/property/stateful/leader_machine.py``.
Wenige Beispiele (``max_examples=8``); der Broker bleibt ``InMemoryBroker``.
Eine Abweichung zwischen Fake und Valkey ist ein Verdacht (R7).

Die Cron-Meldungen führt Valkey mit Serverzeit; der Zeitsprung der Umgebung
lässt sie nicht veralten. Der Ablauf zum Zurückhalten verwaister Crons
(E-014) läuft deshalb nur gegen die Fakes.
"""

from __future__ import annotations

from collections.abc import Callable, Coroutine, Iterator
from typing import Any

import pytest
from hypothesis import HealthCheck, settings
from hypothesis.stateful import run_state_machine_as_test

from tests.property.stateful import leader_scenarios
from tests.property.stateful.leader_machine import CLI_CRON, LeaderEnv, LeaderMachine

pytestmark = pytest.mark.timeout(60)

OpenEnv = Callable[[], Coroutine[Any, Any, LeaderEnv]]

VALKEY_SETTINGS = settings(
    max_examples=8,
    deadline=None,
    stateful_step_count=30,
    suppress_health_check=[HealthCheck.too_slow],
)


@pytest.fixture
def machine(open_env: OpenEnv) -> Iterator[LeaderMachine]:
    machine = LeaderMachine(open_env)
    yield machine
    machine.teardown()


def test_leader_machine_holds_its_invariants_against_valkey(open_env: OpenEnv) -> None:
    run_state_machine_as_test(lambda: LeaderMachine(open_env), settings=VALKEY_SETTINGS)


@pytest.mark.parametrize("scenario", leader_scenarios.SCENARIOS.values(), ids=leader_scenarios.SCENARIOS.keys())
def test_scenario_against_valkey(machine: LeaderMachine, scenario: Callable[[LeaderMachine], None]) -> None:
    scenario(machine)


def test_an_example_does_not_see_the_state_of_its_predecessor(open_env: OpenEnv) -> None:
    first = LeaderMachine(open_env)
    try:
        first.election_tick("A")
        first.add_cli_cron()
        first.verify()
    finally:
        first.teardown()

    second = LeaderMachine(open_env)  # öffnet nur auf leerer Datenbank
    try:
        second.verify()
        assert second.run(second.env.backend.schedules.fire_ts(CLI_CRON)) is None
        second.election_tick("B")
        assert second.cands["B"].loop.state == "leader"
    finally:
        second.teardown()


@pytest.mark.parametrize("sabotage", leader_scenarios.SABOTAGES.values(), ids=leader_scenarios.SABOTAGES.keys())
def test_sabotage_is_caught_against_valkey(machine: LeaderMachine, sabotage: Callable[[LeaderMachine], None]) -> None:
    sabotage(machine)
