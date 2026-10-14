"""Virtuelle Uhr fuer Scheduler-Tests (FS-31): eine eigene Event-Loop, deren
``time()`` beim Warten vorspringt. 30 s Wartezeit kosten nur Millisekunden."""

from __future__ import annotations

import asyncio
import selectors
from collections.abc import Callable, Coroutine
from typing import Any, override


class _FastForwardSelector(selectors.DefaultSelector):
    """Wartet nie in Echtzeit: liegt nichts an, springt die Uhr vor."""

    def __init__(self) -> None:
        super().__init__()
        self.clock = 0.0

    @override
    def select(self, timeout: float | None = None) -> list[tuple[selectors.SelectorKey, int]]:
        ready = super().select(0)
        if ready:
            return ready
        if timeout is None:
            raise RuntimeError("virtuelle Uhr: nichts geplant, die Loop wuerde ewig warten")
        self.clock += max(timeout, 0.0)
        return []


class _VirtualTimeLoop(asyncio.SelectorEventLoop):
    def __init__(self) -> None:
        self._selector_ff = _FastForwardSelector()
        super().__init__(selector=self._selector_ff)

    @override
    def time(self) -> float:
        return self._selector_ff.clock


def run_virtual[T](main: Callable[[], Coroutine[Any, Any, T]]) -> T:
    """Fuehrt *main* auf einer frischen Loop mit virtueller Uhr aus (Start bei 0)."""
    loop = _VirtualTimeLoop()
    try:
        return loop.run_until_complete(main())
    finally:
        loop.close()
