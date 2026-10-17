"""Treiber für Hypothesis-Zustandsmaschinen über die async Fakes
(Fehlersuche Runde 4, Plan 2026-10-17-0941, U6, KTD1).

``RuleBasedStateMachine`` kennt keine async Regeln; der Treiber hält einen
``asyncio.Runner`` und führt je Regel eine Coroutine darauf aus.

Test-Liste:
- [x] ``run`` führt eine Coroutine aus und liefert ihr Ergebnis
- [x] eine Task aus einer Regel lebt in der nächsten weiter (ein Loop je
      Maschine); ``close`` bricht sie ab, ohne Warnung
- [x] eine Regel, die wirft, lässt die Maschine mit dieser Exception scheitern
- [x] ``teardown`` schließt die Umgebung genau einmal, auch nach einer
      werfenden Regel
- [x] scheitert das Öffnen der Umgebung, bleibt kein Loop offen
- [x] In-Memory-Umgebung: verbundenes Backend; ``advance`` stellt die Uhr
      vor, ein Lock läuft mit ihr ab; ``close`` trennt das Backend
"""

from __future__ import annotations

import asyncio

import pytest
from hypothesis import Phase, settings
from hypothesis.stateful import rule, run_state_machine_as_test

from swarmq.exceptions import BackendConnectionError
from tests.support.inmemory import FakeClock
from tests.support.stateful import AsyncDriver, AsyncStateMachine, open_inmemory_env

pytestmark = pytest.mark.timeout(5)


def test_run_returns_the_result_of_the_coroutine() -> None:
    async def answer() -> int:
        return 42

    driver = AsyncDriver()
    try:
        assert driver.run(answer()) == 42
    finally:
        driver.close()


def test_a_task_started_in_one_rule_survives_into_the_next_and_close_cancels_it() -> None:
    started: list[asyncio.Task[bool]] = []
    gate = asyncio.Event()

    async def start() -> None:
        started.append(asyncio.create_task(gate.wait()))

    async def still_pending() -> bool:
        return not started[0].done()

    driver = AsyncDriver()
    driver.run(start())
    assert driver.run(still_pending()) is True

    driver.close()

    assert started[0].cancelled()


_ONE_STEP = settings(max_examples=1, stateful_step_count=1, database=None, phases=[Phase.generate], deadline=None)


class _RecordingEnv:
    """Umgebung mit FakeClock, die ihr Schließen zählt."""

    def __init__(self) -> None:
        self.clock = FakeClock()
        self.closed = 0

    async def advance(self, seconds: float) -> None:
        await self.clock.advance(seconds)

    async def close(self) -> None:
        self.closed += 1


class _Boom(Exception):
    pass


def test_a_rule_that_raises_fails_the_machine_with_that_exception() -> None:
    class Raising(AsyncStateMachine[_RecordingEnv]):
        @rule()
        def explode(self) -> None:
            async def boom() -> None:
                raise _Boom

            self.run(boom())

    envs: list[_RecordingEnv] = []

    async def open_env() -> _RecordingEnv:
        envs.append(_RecordingEnv())
        return envs[-1]

    with pytest.raises(_Boom):
        run_state_machine_as_test(lambda: Raising(open_env), settings=_ONE_STEP)

    assert envs, "keine Umgebung geöffnet"
    assert all(env.closed == 1 for env in envs)


def test_failing_to_open_the_env_leaves_no_loop_behind() -> None:
    class Plain(AsyncStateMachine[_RecordingEnv]):
        @rule()
        def noop(self) -> None:
            pass

    async def broken_env() -> _RecordingEnv:
        raise _Boom

    with pytest.raises(_Boom):
        run_state_machine_as_test(lambda: Plain(broken_env), settings=_ONE_STEP)


async def test_inmemory_env_advances_clock_and_stores_together() -> None:
    env = await open_inmemory_env()
    try:
        start = env.clock.now_ms()
        token = await env.backend.locks.acquire("res", owner="a", ttl_ms=2_000, max_concurrency=1)
        assert token is not None

        await env.advance(2)

        assert env.clock.now_ms() == start + 2_000
        assert await env.backend.locks.current_owner("res") is None
    finally:
        await env.close()
    with pytest.raises(BackendConnectionError):
        await env.backend.ping()
