"""Retry- und DLQ-Ketten-Maschine gegen Valkey (Plan 2026-10-17-0941, U9,
R7, KTD2).

``ValkeyExecutionGuardStore`` (``guard_claim.lua``, ``guard_finish.lua``,
``lock_renew.lua``) und ``ValkeyRateLimitStore`` (``sliding_window_counter.lua``)
unter derselben Maschine wie gegen die Fakes; Invarianten und erlaubte
Abweichungen im Docstring von ``tests/property/stateful/retry_chain_machine.py``.
Ergebnisse (``store_result``/``get_result``) und Cancel-Flags gehen ebenfalls
an Valkey. Der Broker bleibt ``InMemoryBroker`` (``ChainBroker``). Wenige
Beispiele (``max_examples=8``); unter ``tests/integration/`` ist kein
Hypothesis-Profil registriert. Eine Abweichung zwischen Fake und Valkey ist
ein Verdacht (R7).

Zeit im Rate-Limit: ``RateLimiter.check`` liest die Serverzeit (``TIME``)
und reicht sie dem Skript als ``now_ms``; der Zeitsprung der Umgebung
(``age_keys``) verschiebt nur Fristen, nicht die Serverzeit. Die Maschine
gibt dem ``ValkeyRateLimitStore`` deshalb einen Client, dessen ``time()`` die
``FakeClock`` liefert (``FakeClockTime``); alle anderen Befehle gehen an
den echten Client. Das Skript rechnet damit Fenster und ``retry_after_ms``
exakt wie das Modell, die Fenster-Keys altern mit ``advance``.
"""

from __future__ import annotations

from collections.abc import Callable, Coroutine, Iterator
from typing import TYPE_CHECKING, Any, cast

import pytest
from hypothesis import HealthCheck, settings
from hypothesis.stateful import run_state_machine_as_test

from swarmq.backend.valkey.rate_limit import ValkeyRateLimitStore
from swarmq.keyspace import KeySpace
from tests.property.stateful import retry_chain_scenarios
from tests.property.stateful.retry_chain_machine import ChainEnv, RetryChainMachine
from tests.support.inmemory import FakeClock

if TYPE_CHECKING:
    import valkey.asyncio as valkey_asyncio

    from swarmq.backend.valkey import ValkeyBackend

pytestmark = pytest.mark.timeout(60)

OpenEnv = Callable[[], Coroutine[Any, Any, ChainEnv]]

VALKEY_SETTINGS = settings(
    max_examples=8,
    deadline=None,
    stateful_step_count=30,
    suppress_health_check=[HealthCheck.too_slow],
)


class FakeClockTime:
    """Valkey-Client, dessen ``TIME`` die ``FakeClock`` liefert."""

    def __init__(self, client: valkey_asyncio.Valkey, clock: FakeClock) -> None:
        self._client = client
        self._clock = clock

    async def time(self) -> tuple[int, int]:
        now_ms = self._clock.now_ms()
        return now_ms // 1000, (now_ms % 1000) * 1000

    def __getattr__(self, name: str) -> Any:
        return getattr(self._client, name)


def clocked_rate_limit(env: ChainEnv) -> ValkeyRateLimitStore:
    backend = cast("ValkeyBackend", env.backend)
    client = cast("valkey_asyncio.Valkey", FakeClockTime(backend.client, env.clock))
    return ValkeyRateLimitStore(client, keys=KeySpace(backend.namespace))


def chain_machine(open_env: OpenEnv) -> RetryChainMachine:
    return RetryChainMachine(open_env, rate_limit=clocked_rate_limit)


@pytest.fixture
def machine(open_env: OpenEnv) -> Iterator[RetryChainMachine]:
    machine = chain_machine(open_env)
    yield machine
    machine.teardown()


def test_retry_chain_machine_holds_its_invariants_against_valkey(open_env: OpenEnv) -> None:
    run_state_machine_as_test(lambda: chain_machine(open_env), settings=VALKEY_SETTINGS)


@pytest.mark.parametrize(
    "scenario", retry_chain_scenarios.SCENARIOS.values(), ids=retry_chain_scenarios.SCENARIOS.keys()
)
def test_scenario_against_valkey(machine: RetryChainMachine, scenario: Callable[[RetryChainMachine], None]) -> None:
    scenario(machine)


@pytest.mark.befund("F4-02")
@pytest.mark.xfail(
    strict=True, raises=AssertionError, reason="F4-02: offen - Zustelllimit-Gate liegt vor dem Ausfuehrungsschutz"
)
@pytest.mark.parametrize(
    "nachweis", retry_chain_scenarios.F4_02_NACHWEISE.values(), ids=retry_chain_scenarios.F4_02_NACHWEISE.keys()
)
def test_f4_02_redelivery_at_the_limit_stays_a_duplicate_against_valkey(
    machine: RetryChainMachine, nachweis: Callable[[RetryChainMachine], None]
) -> None:
    nachweis(machine)


@pytest.mark.entscheidung("E-051")
def test_republished_retry_copy_with_text_count_is_skipped_as_superseded_against_valkey(
    machine: RetryChainMachine,
) -> None:
    retry_chain_scenarios.republished_retry_copy_with_text_count_is_skipped_as_superseded(machine)


def test_lenient_count_model_is_caught_against_valkey(open_env: OpenEnv) -> None:
    machine = retry_chain_scenarios.LenientCountModel(open_env, rate_limit=clocked_rate_limit)
    try:
        retry_chain_scenarios.lenient_model_is_caught(machine)
    finally:
        machine.teardown()


def test_rate_limit_store_reads_the_fake_clock(machine: RetryChainMachine) -> None:
    """Die Lösung für die Serverzeit: ein Fenster später ist der Zähler frei,
    obwohl real keine Minute verging."""
    store = machine.rate_store
    decisions = [machine.run(store.check("probe", max_requests=1, window_ms=60_000)) for _ in range(2)]
    assert [d.allowed for d in decisions] == [True, False]
    machine.advance(120)
    assert machine.run(store.check("probe", max_requests=1, window_ms=60_000)).allowed


def test_an_example_does_not_see_the_state_of_its_predecessor(open_env: OpenEnv) -> None:
    first = chain_machine(open_env)
    try:
        retry_chain_scenarios.three_failures_with_two_retries_end_in_one_dead_letter(first)
    finally:
        first.teardown()

    second = chain_machine(open_env)  # öffnet nur auf leerer Datenbank
    try:
        second.publish("t1")
        assert second.run(second.env.backend.get_result("t1")) is None
        assert second.deliver(second.pending[0], retry_chain_scenarios.ERROR).reason == "retry"
        retry_chain_scenarios.verify(second)
    finally:
        second.teardown()
