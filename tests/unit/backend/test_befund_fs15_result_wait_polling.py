"""Nachweis FS-15: ``get_result()`` liest Pub/Sub nur in der ersten Sekunde und
pollt danach per GET alle 5 ms; ``polling`` wird ignoriert, ``subscribe()``
hat keine Frist.

Register: docs/befunde/fehlersuche-runde-2-2026-10.md (FS-15; Quelle MC-2,
Skript ``mc/mc_wait_poll.py``).

``ValkeyResults.wait_for_result`` (``backend/valkey/results.py``) wertet
Pub/Sub nur im Fenster ``_PUBSUB_WINDOW_S = 1.0`` aus und pollt danach mit
``_FALLBACK_POLL_S = 0.005``: rund 200 GET/s je Wartendem, bei 100 parallel
wartenden Clients etwa 20 000 GET/s Dauerlast. ``Client.get_result`` nimmt
``polling`` nur noch aus API-Kompatibilitaet an. ``pubsub.subscribe()``
laeuft ohne Deadline; ein haengendes Valkey haelt den Aufrufer ueber
``timeout`` hinaus fest.

Die Wartezeiten laufen auf einer virtuellen Uhr (eigene Event-Loop, deren
``time()`` beim Warten vorspringt): Sekunden Wartezeit kosten so nur
Millisekunden, ohne Schlafen in Echtzeit. Gemockt sind nur der valkey-Client
und die Zeit.

Testliste:
# [x] 10 s Warten ohne Ergebnis: hoechstens ein GET je 0,1 s (plus Erst-GET)
# [x] Client.get_result(polling=1.0): hoechstens ein GET je Sekunde
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
from swarmq.client import Client
from swarmq.exceptions import ResultNotReadyError

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
@pytest.mark.xfail(strict=True, reason="FS-15: offen - nach 1 s Pub/Sub-Fenster GET alle 5 ms")
def test_waiting_ten_seconds_polls_at_most_every_100ms() -> None:
    backend, get = _backend(_SilentPubSub())

    async def _main() -> bytes | None:
        return await backend.wait_for_result("fs15-task", timeout=10.0)

    assert _run_virtual(_main) is None
    assert get.await_count <= 1 + 10.0 / 0.1, (
        f"{get.await_count} GETs in 10 s Wartezeit (~{get.await_count / 9:.0f}/s nach dem Pub/Sub-Fenster)"
    )


@pytest.mark.befund("FS-15")
@pytest.mark.xfail(strict=True, reason="FS-15: offen - Client.get_result ignoriert polling")
def test_client_get_result_honours_the_polling_interval() -> None:
    backend, get = _backend(_SilentPubSub())
    client = Client(broker=AsyncMock(), backend=backend)

    async def _main() -> None:
        with pytest.raises(ResultNotReadyError):
            await client.get_result("fs15-task", timeout=10.0, polling=1.0)

    _run_virtual(_main)
    assert get.await_count <= 1 + 10, f"polling=1.0 wirkungslos: {get.await_count} GETs in 10 s"


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
