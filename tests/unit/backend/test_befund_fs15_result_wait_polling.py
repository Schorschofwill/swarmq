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
# [x] haengendes subscribe() bei vorhandenem Ergebnis: Ergebnis kommt nach dem
#     Pub/Sub-Fenster (1 s), nicht erst nach Ablauf von timeout als None
# [x] haengendes unsubscribe() im Teardown: Rueckkehr <= timeout + epsilon
# [x] verworfener PubSub (subscribe zu langsam) wird im Hintergrund geschlossen,
#     ohne dass eine Task haengen bleibt ("Task was destroyed")
# [x] wait_for_workflow mit haengendem subscribe(): False spaetestens nach timeout
# [x] wait_for_workflow mit haengendem unsubscribe(): Rueckkehr <= timeout + epsilon
# [x] Abbruch des Aufrufers waehrend subscribe(): PubSub wird trotzdem geschlossen
# [x] nie abonnierter PubSub (subscribe haengt oder scheitert): kein
#     unsubscribe() (wuerde neu verbinden), nur aclose()
# [x] close() des Backends mit haengendem Hintergrund-Closer endet zuegig,
#     keine Task bleibt zurueck; ein fast fertiger Closer darf zu Ende laufen
# [x] haengendes subscribe() und timeout < Pub/Sub-Fenster: Rueckkehr nach
#     timeout, nicht nach dem Fenster
# [x] Mindestwartezeit des Teardowns: nach Timeout-Ende sind unsubscribe()
#     und aclose() schon gelaufen
# [x] Abbruch des Aufrufers waehrend des Teardowns: CancelledError geht
#     durch, der Teardown laeuft im Hintergrund zu Ende
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

    def __init__(
        self,
        subscribe: Callable[[str], Awaitable[None]] | None = None,
        unsubscribe: Callable[[str], Awaitable[None]] | None = None,
    ) -> None:
        self._subscribe = subscribe
        self._unsubscribe = unsubscribe
        self.closed = False
        self.unsubscribe_calls = 0

    async def subscribe(self, channel: str) -> None:
        if self._subscribe is not None:
            await self._subscribe(channel)

    async def get_message(self, ignore_subscribe_messages: bool = True, timeout: float = 0.0) -> None:
        await asyncio.sleep(timeout)

    async def unsubscribe(self, channel: str) -> None:
        self.unsubscribe_calls += 1
        if self._unsubscribe is not None:
            await self._unsubscribe(channel)

    async def aclose(self) -> None:
        self.closed = True


def _backend(pubsub: _SilentPubSub) -> tuple[ValkeyBackend, AsyncMock]:
    client = AsyncMock()
    client.get = AsyncMock(return_value=None)
    client.pubsub = Mock(return_value=pubsub)
    return ValkeyBackend(client=client), client.get


@pytest.mark.befund("FS-15")
@pytest.mark.entscheidung("E-030")
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


def _hang_forever() -> Callable[[str], Awaitable[None]]:
    async def _hang(_channel: str) -> None:
        await asyncio.Event().wait()

    return _hang


@pytest.mark.entscheidung("E-030")
def test_hanging_subscribe_still_finds_an_existing_result_within_the_pubsub_window() -> None:
    backend, get = _backend(_SilentPubSub(subscribe=_hang_forever()))
    get.return_value = b"ready"

    async def _main() -> tuple[bytes | None, float]:
        loop = asyncio.get_running_loop()
        started = loop.time()
        raw = await asyncio.wait_for(backend.wait_for_result("fs15-ready", timeout=30.0), timeout=60.0)
        return raw, loop.time() - started

    raw, elapsed = _run_virtual(_main)
    assert raw == b"ready"
    assert elapsed <= 1.5


@pytest.mark.entscheidung("E-030")
def test_hanging_unsubscribe_in_teardown_does_not_outlast_the_timeout() -> None:
    pubsub = _SilentPubSub(unsubscribe=_hang_forever())
    backend, _ = _backend(pubsub)

    async def _main() -> tuple[bytes | None, float, set[asyncio.Task[Any]]]:
        loop = asyncio.get_running_loop()
        started = loop.time()
        raw = await asyncio.wait_for(backend.wait_for_result("fs15-teardown", timeout=2.0), timeout=60.0)
        elapsed = loop.time() - started
        return raw, elapsed, await _settle_background_tasks()

    raw, elapsed, pending = _run_virtual(_main)
    assert raw is None
    assert elapsed <= 2.5
    # Der Teardown laeuft im Hintergrund zu Ende und trennt die Verbindung.
    assert pending == set()
    assert pubsub.closed


async def _settle_background_tasks() -> set[asyncio.Task[Any]]:
    """Wartet (virtuell) auf alle anderen Tasks der Loop; liefert die nach
    einer Minute noch haengenden. Leer heisst: beim Schliessen der Loop
    bleibt keine Task zurueck ("Task was destroyed but it is pending")."""
    others = asyncio.all_tasks() - {asyncio.current_task()}
    if not others:
        return set()
    _, pending = await asyncio.wait(others, timeout=60.0)
    return pending


def test_pubsub_discarded_after_a_slow_subscribe_is_closed_in_the_background() -> None:
    pubsub = _SilentPubSub(subscribe=_hang_forever())
    backend, _ = _backend(pubsub)

    async def _main() -> tuple[bytes | None, set[asyncio.Task[Any]]]:
        raw = await asyncio.wait_for(backend.wait_for_result("fs15-discard", timeout=2.0), timeout=60.0)
        return raw, await _settle_background_tasks()

    raw, pending = _run_virtual(_main)
    assert raw is None
    assert pending == set()
    assert pubsub.closed


@pytest.mark.entscheidung("E-030")
def test_wait_for_workflow_with_hanging_subscribe_ends_at_the_timeout() -> None:
    pubsub = _SilentPubSub(subscribe=_hang_forever())
    backend, _ = _backend(pubsub)

    async def _main() -> tuple[bool | None, float, set[asyncio.Task[Any]]]:
        loop = asyncio.get_running_loop()
        started = loop.time()
        try:
            notified = await asyncio.wait_for(backend.wait_for_workflow("fs15-wf", timeout=2.0), timeout=60.0)
        except TimeoutError:
            return None, loop.time() - started, set()
        elapsed = loop.time() - started
        return notified, elapsed, await _settle_background_tasks()

    notified, elapsed, pending = _run_virtual(_main)
    assert notified is False, f"wait_for_workflow(timeout=2.0) haengt im subscribe(): nach {elapsed:.0f} s"
    assert elapsed <= 2.5
    assert pending == set()
    assert pubsub.closed


def test_wait_for_workflow_with_hanging_unsubscribe_ends_at_the_timeout() -> None:
    pubsub = _SilentPubSub(unsubscribe=_hang_forever())
    backend, _ = _backend(pubsub)

    async def _main() -> tuple[bool | None, float, set[asyncio.Task[Any]]]:
        loop = asyncio.get_running_loop()
        started = loop.time()
        try:
            notified = await asyncio.wait_for(backend.wait_for_workflow("fs15-wf", timeout=2.0), timeout=60.0)
        except TimeoutError:
            return None, loop.time() - started, set()
        elapsed = loop.time() - started
        return notified, elapsed, await _settle_background_tasks()

    notified, elapsed, pending = _run_virtual(_main)
    assert notified is False, f"wait_for_workflow(timeout=2.0) haengt im unsubscribe(): nach {elapsed:.0f} s"
    assert elapsed <= 2.5
    assert pending == set()
    assert pubsub.closed


@pytest.mark.parametrize("waiter", ["wait_for_result", "wait_for_workflow"])
def test_caller_cancelled_during_subscribe_still_closes_the_pubsub(waiter: str) -> None:
    subscribing = asyncio.Event()

    async def _hang(_channel: str) -> None:
        subscribing.set()
        await asyncio.Event().wait()

    pubsub = _SilentPubSub(subscribe=_hang)
    backend, _ = _backend(pubsub)

    async def _main() -> tuple[bool, set[asyncio.Task[Any]]]:
        wait = getattr(backend, waiter)
        task = asyncio.create_task(wait("fs15-cancel", timeout=30.0))
        await subscribing.wait()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            cancelled = True
        else:
            cancelled = False
        return cancelled, await _settle_background_tasks()

    cancelled, pending = _run_virtual(_main)
    assert cancelled
    assert pending == set()
    assert pubsub.closed


async def _fail(_channel: str) -> None:
    raise ConnectionError("valkey nicht erreichbar")


@pytest.mark.parametrize("subscribe", [_hang_forever(), _fail], ids=["haengt", "scheitert"])
def test_never_subscribed_pubsub_is_closed_without_unsubscribe(subscribe: Callable[[str], Awaitable[None]]) -> None:
    # unsubscribe() verbaende einen getrennten PubSub neu, nur um UNSUBSCRIBE
    # zu senden; aclose() trennt die Verbindung und beendet das Abonnement.
    pubsub = _SilentPubSub(subscribe=subscribe)
    backend, _ = _backend(pubsub)

    async def _main() -> tuple[bytes | None, set[asyncio.Task[Any]]]:
        raw = await asyncio.wait_for(backend.wait_for_result("fs15-never", timeout=2.0), timeout=60.0)
        return raw, await _settle_background_tasks()

    raw, pending = _run_virtual(_main)
    assert raw is None
    assert pending == set()
    assert pubsub.closed
    assert pubsub.unsubscribe_calls == 0


def test_backend_close_does_not_leave_a_hanging_background_closer() -> None:
    pubsub = _SilentPubSub(unsubscribe=_hang_forever())
    backend, _ = _backend(pubsub)

    async def _main() -> tuple[float, set[asyncio.Task[Any]]]:
        loop = asyncio.get_running_loop()
        # Der Teardown haengt in unsubscribe() und laeuft im Hintergrund weiter.
        await asyncio.wait_for(backend.wait_for_result("fs15-close", timeout=2.0), timeout=60.0)
        started = loop.time()
        await asyncio.wait_for(backend.close(), timeout=60.0)
        return loop.time() - started, asyncio.all_tasks() - {asyncio.current_task()}

    elapsed, leftover = _run_virtual(_main)
    assert elapsed <= 0.5
    assert leftover == set()


def test_backend_close_lets_a_nearly_finished_closer_complete() -> None:
    async def _slow(_channel: str) -> None:
        await asyncio.sleep(0.08)

    pubsub = _SilentPubSub(unsubscribe=_slow)
    backend, _ = _backend(pubsub)

    async def _main() -> None:
        # Der Teardown braucht etwas laenger als die Mindestwartezeit.
        await asyncio.wait_for(backend.wait_for_result("fs15-drain", timeout=2.0), timeout=60.0)
        await asyncio.wait_for(backend.close(), timeout=60.0)

    _run_virtual(_main)
    assert pubsub.closed


@pytest.mark.entscheidung("E-030")
def test_hanging_subscribe_with_timeout_below_the_pubsub_window_ends_at_the_timeout() -> None:
    pubsub = _SilentPubSub(subscribe=_hang_forever())
    backend, _ = _backend(pubsub)

    async def _main() -> tuple[bytes | None, float, set[asyncio.Task[Any]]]:
        loop = asyncio.get_running_loop()
        started = loop.time()
        raw = await asyncio.wait_for(backend.wait_for_result("fs15-short", timeout=0.5), timeout=60.0)
        elapsed = loop.time() - started
        return raw, elapsed, await _settle_background_tasks()

    raw, elapsed, pending = _run_virtual(_main)
    assert raw is None
    # Die Frist des Abonnements ist die Restfrist, nicht das 1-s-Fenster.
    assert elapsed <= 0.6
    assert pending == set()
    assert pubsub.closed


@pytest.mark.parametrize("waiter", ["wait_for_result", "wait_for_workflow"])
@pytest.mark.entscheidung("E-030")
def test_teardown_after_the_timeout_still_gets_its_minimum_grace(waiter: str) -> None:
    # Auch wenn die Deadline schon erreicht ist, wartet der Aufrufer kurz auf
    # den Teardown: ein antwortendes Valkey ist danach sauber abgemeldet.
    async def _answers_quickly(_channel: str) -> None:
        await asyncio.sleep(0.01)

    pubsub = _SilentPubSub(unsubscribe=_answers_quickly)
    backend, _ = _backend(pubsub)

    async def _main() -> tuple[int, bool]:
        wait = getattr(backend, waiter)
        await asyncio.wait_for(wait("fs15-grace", timeout=2.0), timeout=60.0)
        return pubsub.unsubscribe_calls, pubsub.closed

    unsubscribe_calls, closed = _run_virtual(_main)
    assert unsubscribe_calls == 1
    assert closed


@pytest.mark.parametrize("waiter", ["wait_for_result", "wait_for_workflow"])
def test_caller_cancelled_during_teardown_lets_the_teardown_finish_in_the_background(waiter: str) -> None:
    tearing_down = asyncio.Event()
    unsubscribed = asyncio.Event()

    async def _slow(_channel: str) -> None:
        tearing_down.set()
        await asyncio.sleep(1.0)
        unsubscribed.set()

    pubsub = _SilentPubSub(unsubscribe=_slow)
    backend, _ = _backend(pubsub)

    async def _main() -> tuple[bool, set[asyncio.Task[Any]]]:
        wait = getattr(backend, waiter)
        task = asyncio.create_task(wait("fs15-cancel-teardown", timeout=2.0))
        await tearing_down.wait()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            cancelled = True
        else:
            cancelled = False
        return cancelled, await _settle_background_tasks()

    cancelled, pending = _run_virtual(_main)
    assert cancelled
    assert pending == set()
    assert unsubscribed.is_set()
    assert pubsub.closed
