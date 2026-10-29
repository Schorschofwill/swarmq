"""Treiber für Hypothesis-Zustandsmaschinen über async Subjekte
(Fehlersuche Runde 4, Plan 2026-10-17-0941, U6, KTD1).

``RuleBasedStateMachine`` kennt keine async Regeln. ``AsyncStateMachine``
hält je Beispiel einen eigenen Loop (``AsyncDriver`` über ``asyncio.Runner``);
jede Regel führt ihre Coroutine mit ``self.run(coro)`` aus. Die Umgebung
(Store-Factory, KTD2) öffnet die Maschine im Konstruktor aus einer
Coroutine-Factory, also im eigenen Loop; ``teardown`` schließt erst die
Subjekte (``close_subjects``), dann die Umgebung, dann den Loop, auch nach
einer werfenden Regel.

Gebrauch::

    run_machine(lambda: MyMachine(open_env), settings=...)

``open_env`` kommt als Fixture aus ``tests/property/stateful/conftest.py``
(``open_inmemory_env``) oder ``tests/integration/stateful/conftest.py``
(Valkey).
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine
from typing import Any, Protocol, cast, override

import hypothesis
from hypothesis.stateful import RuleBasedStateMachine, run_state_machine_as_test

from tests.support.inmemory import FakeClock, InMemoryBackend


class StatefulEnv(Protocol):
    """Was eine Store-Factory einer Maschine liefert: die Uhr des Modells und
    das Vorstellen der Zeit für Uhr und Stores."""

    @property
    def clock(self) -> FakeClock: ...

    async def advance(self, seconds: float) -> None: ...

    async def close(self) -> None: ...


class InMemoryEnv:
    """Store-Factory Fake (KTD2): ``InMemoryBackend`` mit seinen Stores
    gegen eine ``FakeClock``; ``advance`` stellt nur die Uhr vor. Die Fristen
    im Fake sind exakt (keine Toleranz)."""

    ttl_tolerance_ms = 0

    def __init__(self, clock: FakeClock, backend: InMemoryBackend) -> None:
        self.clock = clock
        self.backend = backend

    async def advance(self, seconds: float) -> None:
        await self.clock.advance(seconds)

    async def close(self) -> None:
        await self.backend.close()


async def open_inmemory_env() -> InMemoryEnv:
    clock = FakeClock()
    backend = InMemoryBackend(clock)
    await backend.connect()
    return InMemoryEnv(clock, backend)


class AsyncDriver:
    """Hält einen ``asyncio.Runner``; jede Regel führt ihre Coroutine darauf aus."""

    def __init__(self) -> None:
        self._runner = asyncio.Runner()

    def run[T](self, coro: Coroutine[Any, Any, T]) -> T:
        return self._runner.run(coro)

    def close(self) -> None:
        self._runner.close()


class AsyncStateMachine[E: StatefulEnv](RuleBasedStateMachine):
    """Zustandsmaschine mit eigenem Loop je Beispiel."""

    def __init__(self, open_env: Callable[[], Coroutine[Any, Any, E]]) -> None:
        super().__init__()
        self.driver = AsyncDriver()
        try:
            self.env = self.driver.run(open_env())
        except BaseException:
            # Ohne Maschine ruft Hypothesis kein ``teardown``.
            self.driver.close()
            raise

    def run[T](self, coro: Coroutine[Any, Any, T]) -> T:
        return self.driver.run(coro)

    async def close_subjects(self) -> None:
        """Haken für Maschinen: eigene Subjekte schließen, bevor die Umgebung
        schließt (etwa Lock-Keeper, Clients)."""

    @override
    def teardown(self) -> None:
        """Subjekte, Umgebung und Loop schließen, auch nach einer werfenden
        Regel; offene Tasks bricht ``asyncio.Runner.close`` ab."""
        try:
            self.run(self.close_subjects())
        finally:
            try:
                self.run(self.env.close())
            finally:
                self.driver.close()


def run_machine(factory: Callable[[], RuleBasedStateMachine], *, settings: hypothesis.settings) -> None:
    """``run_state_machine_as_test`` für eine Factory statt einer Klasse.

    hypothesis typisiert ``state_machine_factory`` seit 6.170 als
    ``type[RuleBasedStateMachine]``, nimmt laut Doku und Code aber alles, was
    eine Maschine liefert; die Maschinen hier brauchen ``open_env`` im
    Konstruktor.
    """
    run_state_machine_as_test(cast("type[RuleBasedStateMachine]", factory), settings=settings)
