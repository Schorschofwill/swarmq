"""Fake-Clock des In-Memory-Stubs (Entwurf §8.3).

Steuert TTLs, ``retry_later``-Fälligkeit und Sliding-Window-Fenster. Die
Contract-Klassen erhalten sie als ``clock``-Fixture; gegen echte Dienste
ist ``clock`` stattdessen echte Wartezeit (``RealClock``).
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable

TickListener = Callable[[int], Awaitable[None]]


class FakeClock:
    def __init__(self, start_ms: int = 1_700_000_000_000) -> None:
        self._now_ms = start_ms
        self._listeners: list[TickListener] = []

    def now_ms(self) -> int:
        return self._now_ms

    def subscribe(self, listener: TickListener) -> None:
        self._listeners.append(listener)

    async def advance(self, seconds: float) -> None:
        self._now_ms += round(seconds * 1000)
        for listener in list(self._listeners):
            await listener(self._now_ms)
        await asyncio.sleep(0)  # ausgelöste Zustellungs-Tasks laufen an


class RealClock:
    def now_ms(self) -> int:
        return int(time.time() * 1000)

    async def advance(self, seconds: float) -> None:
        await asyncio.sleep(seconds)
