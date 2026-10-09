"""``tests.support.wait.wait_until``: Pollen ohne feste Wartezeit (Plan 0.12.0, MR 1).

Ersetzt die ``asyncio.sleep``-Schleifen der Nachweis-Tests. Das Ergebnis ist
ein bool, damit Beobachtungs-Tests ("bleibt X innerhalb der Frist aus?")
nach der Frist selbst asserten können.

Test-Liste:
- [x] Prädikat sofort wahr -> True
- [x] Prädikat nie wahr -> False nach der Frist
- [x] Prädikat wird nach einigen Runden wahr -> True, vor der Frist
- [x] async Prädikat wird ausgewertet (awaitable Ergebnis)
- [x] async Prädikat wird nach einigen Runden wahr -> True
"""

from __future__ import annotations

import time

import pytest

from tests.support.wait import wait_until

pytestmark = [pytest.mark.timeout(5)]


async def test_true_predicate_returns_true() -> None:
    assert await wait_until(lambda: True, timeout=1.0) is True


async def test_false_predicate_returns_false_after_timeout() -> None:
    start = time.monotonic()
    assert await wait_until(lambda: False, timeout=0.05, interval=0.01) is False
    assert time.monotonic() - start >= 0.05


async def test_predicate_becoming_true_returns_true_before_timeout() -> None:
    calls = 0

    def third_call_succeeds() -> bool:
        nonlocal calls
        calls += 1
        return calls >= 3

    start = time.monotonic()
    assert await wait_until(third_call_succeeds, timeout=2.0, interval=0.001) is True
    assert calls == 3
    assert time.monotonic() - start < 1.0


async def test_async_predicate_is_awaited() -> None:
    async def never() -> bool:
        return False

    assert await wait_until(never, timeout=0.02, interval=0.005) is False


async def test_async_predicate_becoming_true_returns_true() -> None:
    calls = 0

    async def second_call_succeeds() -> bool:
        nonlocal calls
        calls += 1
        return calls >= 2

    assert await wait_until(second_call_succeeds, timeout=1.0, interval=0.001) is True
    assert calls == 2
