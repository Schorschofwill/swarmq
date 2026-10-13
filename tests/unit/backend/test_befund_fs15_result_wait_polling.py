"""Nachweis FS-15: ``pubsub.subscribe()`` in ``get_result()`` hat keine Frist.

Register: docs/befunde/fehlersuche-runde-2-2026-10.md (FS-15; Quelle MC-2,
Skript ``mc/mc_wait_poll.py``).

``ValkeyResults.wait_for_result`` (``backend/valkey/results.py``) ruft
``pubsub.subscribe()`` ohne Deadline auf. Der Docstring sagt zu, dass ein
haengendes Valkey den Aufrufer nicht ueber ``timeout`` hinaus festhaelt; mit
den Wiederholungen von valkey-py dauert es real 30 s und mehr laenger.

Das Polling alle 5 ms nach dem 1-s-Pub/Sub-Fenster und das ignorierte
``polling`` sind dagegen entschieden (Anforderung R3, Plan 2026-04-15-005,
Commit c18a144, ``api.md``) und hier nicht Gegenstand (Gegenpruefung C).

Die Wartezeit laeuft auf einer virtuellen Uhr (eigene Event-Loop, deren
``time()`` beim Warten vorspringt). Gemockt sind nur der valkey-Client und
die Zeit.

Testliste:
# [x] haengendes subscribe(): wait_for_result endet spaetestens kurz nach timeout
"""

from __future__ import annotations

import asyncio
import selectors
from collections.abc import Awaitable, Callable, Coroutine
from typing import Any, override
from unittest.mock import AsyncMock, Mock

import pytest

from swarmq.backend.valkey import ValkeyBackend

pytestmark = pytest.mark.timeout(5)


class _FastForwardSelector(selectors.DefaultSelector):
    """Wartet nie in Echtzeit: liegt nichts an, springt die Uhr der Loop um
    die angefragte Wartezeit vor."""

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


def _run_virtual[T](main: Callable[[], Coroutine[Any, Any, T]]) -> T:
    loop = _VirtualTimeLoop()
    try:
        return loop.run_until_complete(main())
    finally:
        loop.close()


class _SilentPubSub:
    """Pub/Sub ohne Benachrichtigung; ``get_message`` wartet seine Frist ab."""

    def __init__(self, subscribe: Callable[[str], Awaitable[None]] | None = None) -> None:
        self._subscribe = subscribe

    async def subscribe(self, channel: str) -> None:
        if self._subscribe is not None:
            await self._subscribe(channel)

    async def get_message(self, ignore_subscribe_messages: bool = True, timeout: float = 0.0) -> None:
        await asyncio.sleep(timeout)

    async def unsubscribe(self, channel: str) -> None:
        return None

    async def aclose(self) -> None:
        return None


def _backend(pubsub: _SilentPubSub) -> tuple[ValkeyBackend, AsyncMock]:
    client = AsyncMock()
    client.get = AsyncMock(return_value=None)
    client.pubsub = Mock(return_value=pubsub)
    return ValkeyBackend(client=client), client.get


@pytest.mark.befund("FS-15")
@pytest.mark.xfail(strict=True, reason="FS-15: offen - pubsub.subscribe() ohne Frist")
def test_hanging_subscribe_does_not_outlast_the_timeout() -> None:
    async def _hang(_channel: str) -> None:
        await asyncio.Event().wait()

    backend, _ = _backend(_SilentPubSub(subscribe=_hang))

    async def _main() -> tuple[bool, float]:
        loop = asyncio.get_running_loop()
        started = loop.time()
        try:
            # Aeussere Schranke nur, damit der Test endet: 60 s virtuelle Zeit.
            await asyncio.wait_for(backend.wait_for_result("fs15-task", timeout=2.0), timeout=60.0)
        except TimeoutError:
            return False, loop.time() - started
        return True, loop.time() - started

    finished, elapsed = _run_virtual(_main)
    assert finished, f"wait_for_result(timeout=2.0) haengt im subscribe(): nach {elapsed:.0f} s nicht beendet"
    assert elapsed <= 2.5
