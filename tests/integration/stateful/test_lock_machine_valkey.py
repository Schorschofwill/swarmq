"""Lock-Maschine gegen Valkey (Plan 2026-10-17-0941, U6, R7, KTD2).

``ValkeyLockStore`` (``lock_acquire/release/renew/extend.lua``) und die
Unique-Claims des ``ValkeyBackend`` (``SET NX GET PX``, ``unique_release.lua``)
unter derselben Maschine wie gegen die Fakes; Invarianten und erlaubte
Abweichungen im Docstring von ``tests/property/stateful/lock_machine.py``.
Wenige Beispiele (``max_examples=8``); unter ``tests/integration/`` ist kein
Hypothesis-Profil registriert. Eine Abweichung zwischen Fake und Valkey ist
ein Verdacht (R7). Die Valkey-Varianten tragen keinen ``entscheidung``-Marker;
festgeschrieben werden Entscheidungen in der Fake-Stufe.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine, Iterator
from typing import Any

import pytest
import valkey.asyncio as valkey_asyncio
from hypothesis import HealthCheck, settings

from tests.property.stateful import lock_scenarios
from tests.property.stateful.lock_machine import LockEnv, LockMachine
from tests.support.stateful import run_machine

pytestmark = pytest.mark.timeout(60)

OpenEnv = Callable[[], Coroutine[Any, Any, LockEnv]]

VALKEY_SETTINGS = settings(
    max_examples=8,
    deadline=None,
    stateful_step_count=30,
    suppress_health_check=[HealthCheck.too_slow],
)


@pytest.fixture
def machine(open_env: OpenEnv) -> Iterator[LockMachine]:
    machine = LockMachine(open_env)
    yield machine
    machine.teardown()


def test_lock_machine_holds_its_invariants_against_valkey(open_env: OpenEnv) -> None:
    run_machine(lambda: LockMachine(open_env), settings=VALKEY_SETTINGS)


@pytest.mark.parametrize("scenario", lock_scenarios.SCENARIOS.values(), ids=lock_scenarios.SCENARIOS.keys())
def test_scenario_against_valkey(machine: LockMachine, scenario: Callable[[LockMachine], None]) -> None:
    scenario(machine)


def test_model_without_owner_check_is_caught_against_valkey(open_env: OpenEnv) -> None:
    machine = lock_scenarios.OwnerCheckOmitted(open_env)
    try:
        lock_scenarios.model_without_owner_check_is_caught(machine)
    finally:
        machine.teardown()


async def _dbsize(url: str) -> int:
    client = valkey_asyncio.Valkey.from_url(url)
    try:
        return int(await client.dbsize())
    finally:
        await client.aclose()


def test_an_example_does_not_see_the_state_of_its_predecessor(open_env: OpenEnv, valkey_url: str) -> None:
    first = LockMachine(open_env)
    try:
        assert first.start("w1", "res-a", 90_000) is not None
        first.schedule(1)
    finally:
        first.teardown()

    assert asyncio.run(_dbsize(valkey_url)) == 0, "teardown hat die Datenbank nicht geleert"

    second = LockMachine(open_env)
    try:
        assert second.run(second.env.backend.locks.current_owner("res-a")) is None
        assert second.schedule(1) not in {message.task_id for message in first.messages}
        second.verify()
    finally:
        second.teardown()
