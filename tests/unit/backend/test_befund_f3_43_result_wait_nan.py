"""Nachweis F3-43: ``get_result(timeout=nan)`` bringt die Event-Loop zum
Absturz, wenn das GET haengt.

Register: docs/befunde/fehlersuche-runde-3-2026-10.md (F3-43; Quelle Review U4).

``ValkeyResults.wait_for_result`` (``backend/valkey/results.py``) bindet jedes
GET per ``asyncio.wait_for(timeout=Restfrist)``. Bei ``timeout=nan`` ist die
Restfrist ``nan``; ``remaining <= 0`` ist nie wahr, also landet ``nan`` als
Timer in der Loop. Ist er der einzige faellige Timer, rechnet die Loop daraus
die Selector-Wartezeit, und ``epoll`` wirft ``ValueError: cannot convert
float NaN to integer`` aus ``run_until_complete``. Bei schnell antwortendem
Valkey endet derselbe Aufruf mit ``ResultNotReadyError``. Ein negatives
``timeout`` liest gar nicht.

Erwartet (E-030, Nachtrag Review U4; Gleichlauf mit ``get_results``): Eine
Frist ohne positive Restfrist (negativ, ``nan``) gilt als abgelaufen, genau
ein Lesen, dann ``ResultNotReadyError``; ``inf`` bleibt unbegrenzt.

Die Wartezeit laeuft auf einer virtuellen Uhr (eigene Event-Loop, deren
``time()`` beim Warten vorspringt). Eine dort eingeplante ``nan``-Frist
laesst die Loop sofort abstuerzen; die echte stuerzt ab, sobald der Timer
vorne im Heap liegt. Gemockt sind nur der valkey-Client und die Zeit.

Testliste:
# [x] get_result(timeout=-1): genau ein GET, dann ResultNotReadyError
# [x] get_result(timeout=nan) mit langsamem GET: kein Absturz, genau ein GET,
#     dann ResultNotReadyError
# [x] get_result(timeout=nan) mit vorhandenem Ergebnis: Ergebnis
# [x] wait_for_workflow(timeout=nan) mit langsamem subscribe: False, kein Absturz
# [x] get_result(timeout=inf): wartet unbegrenzt, Ergebnis nach 2 s kommt an
"""

from __future__ import annotations

import asyncio
import contextvars
import math
import selectors
from collections.abc import Callable, Coroutine
from typing import Any, override
from unittest.mock import AsyncMock, Mock

import pytest

from swarmq.backend.valkey import ValkeyBackend
from swarmq.client import Client
from swarmq.exceptions import ResultNotReadyError
from swarmq.serialization import encode_result

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
            msg = "virtuelle Uhr: nichts geplant, die Loop wuerde ewig warten"
            raise RuntimeError(msg)
        self.clock += max(timeout, 0.0)
        return []


class _LoopCrashError(BaseException):
    """Steht fuer den Absturz der Event-Loop an einer nan-Frist."""


class _VirtualTimeLoop(asyncio.SelectorEventLoop):
    def __init__(self) -> None:
        self._selector_ff = _FastForwardSelector()
        super().__init__(selector=self._selector_ff)

    @override
    def time(self) -> float:
        return self._selector_ff.clock

    @override
    def call_at[*Ts](
        self,
        when: float,
        callback: Callable[[*Ts], object],
        *args: *Ts,
        context: contextvars.Context | None = None,
    ) -> asyncio.TimerHandle:
        # Ein nan-Timer bringt die echte Loop zum Absturz, sobald er vorne
        # im Heap liegt (``select(nan)`` -> ``ValueError`` aus ``epoll``);
        # ob er das tut, haengt von der Heap-Ordnung ab. Darum stuerzt die
        # virtuelle Loop schon beim Einplanen ab, an jedem ``except
        # Exception`` des Produktionscodes vorbei.
        if math.isnan(when):
            raise _LoopCrashError
        return super().call_at(when, callback, *args, context=context)


def _run_virtual[T](main: Callable[[], Coroutine[Any, Any, T]]) -> T:
    loop = _VirtualTimeLoop()
    try:
        return loop.run_until_complete(main())
    finally:
        loop.close()


class _SilentPubSub:
    """Pub/Sub ohne Benachrichtigung; ``subscribe`` dauert *subscribe_s*."""

    def __init__(self, subscribe_s: float = 0.0) -> None:
        self._subscribe_s = subscribe_s
        self.subscribe_calls = 0

    async def subscribe(self, _channel: str) -> None:
        self.subscribe_calls += 1
        await asyncio.sleep(self._subscribe_s)

    async def get_message(self, ignore_subscribe_messages: bool = True, timeout: float = 0.0) -> None:
        await asyncio.sleep(timeout)

    async def unsubscribe(self, _channel: str) -> None:
        pass

    async def aclose(self) -> None:
        pass


def _backend(
    get: Callable[[str], Coroutine[Any, Any, bytes | None]], pubsub: _SilentPubSub
) -> tuple[ValkeyBackend, Mock]:
    valkey = AsyncMock()
    valkey.get = AsyncMock(side_effect=get)
    valkey.pubsub = Mock(return_value=pubsub)
    return ValkeyBackend(client=valkey), valkey.get


def _client(get: Callable[[str], Coroutine[Any, Any, bytes | None]], pubsub: _SilentPubSub) -> tuple[Client, Mock]:
    backend, get_mock = _backend(get, pubsub)
    return Client(broker=AsyncMock(), backend=backend), get_mock


async def _never_ready(_key: str) -> bytes | None:
    return None


@pytest.mark.befund("F3-43")
@pytest.mark.entscheidung("E-030")
def test_get_result_with_negative_timeout_reads_once_then_raises() -> None:
    client, get = _client(_never_ready, _SilentPubSub())

    async def _main() -> None:
        await client.get_result("f3-43-neg", timeout=-1.0)

    with pytest.raises(ResultNotReadyError):
        _run_virtual(_main)
    assert get.await_count == 1


@pytest.mark.befund("F3-43")
@pytest.mark.entscheidung("E-030")
def test_get_result_with_nan_timeout_and_slow_get_reads_once_then_raises() -> None:
    async def _slow_get(_key: str) -> bytes | None:
        await asyncio.sleep(0.5)
        return None

    client, get = _client(_slow_get, _SilentPubSub())

    async def _main() -> None:
        await client.get_result("f3-43-nan", timeout=float("nan"))

    with pytest.raises(ResultNotReadyError):
        _run_virtual(_main)
    assert get.await_count == 1


@pytest.mark.befund("F3-43")
@pytest.mark.entscheidung("E-030")
def test_wait_for_workflow_with_nan_timeout_returns_false_without_crash() -> None:
    pubsub = _SilentPubSub(subscribe_s=0.5)
    backend, _ = _backend(_never_ready, pubsub)

    async def _main() -> bool:
        return await backend.wait_for_workflow("f3-43-wf", timeout=float("nan"))

    assert _run_virtual(_main) is False


@pytest.mark.entscheidung("E-030")
def test_get_result_with_nan_timeout_returns_an_existing_result() -> None:
    async def _ready(_key: str) -> bytes | None:
        return encode_result({"result": 42, "is_error": False})

    client, get = _client(_ready, _SilentPubSub())

    async def _main() -> Any:
        return await client.get_result("f3-43-ready", timeout=float("nan"))

    assert _run_virtual(_main) == 42
    assert get.await_count == 1


@pytest.mark.entscheidung("E-030")
def test_get_result_with_inf_timeout_waits_beyond_the_pubsub_window() -> None:
    """Charakterisierung: ``inf`` ist ein unbegrenztes Warten (E-030)."""

    async def _ready_after_two_seconds(_key: str) -> bytes | None:
        if asyncio.get_running_loop().time() < 2.0:
            return None
        return encode_result({"result": "spaet", "is_error": False})

    client, _ = _client(_ready_after_two_seconds, _SilentPubSub())

    async def _main() -> tuple[Any, float]:
        value = await client.get_result("f3-43-inf", timeout=math.inf)
        return value, asyncio.get_running_loop().time()

    value, finished_at = _run_virtual(_main)
    assert value == "spaet"
    assert 2.0 <= finished_at < 2.1
