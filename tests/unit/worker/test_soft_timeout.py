"""Soft-Timeout-Huelle (FS-05, Plan 2026-10-14-0748 U18, KTD14; E-045).

``run_with_soft_timeout`` legt ``asyncio.wait_for`` um ``run()``. Nur der
eigene Ablauf der Frist wird ``SoftTimeoutError``; eine ``TimeoutError`` aus
dem Task selbst bleibt, was sie ist.

Testliste:
# [x] TimeoutError-Unterklasse des Tasks (mit soft_timeout) bleibt dasselbe Objekt
# [x] ohne soft_timeout: Rueckgabewert und TimeoutError des Tasks unveraendert
# [x] abgelaufene Frist -> SoftTimeoutError "soft timeout after Xs"; im Task
#     kam CancelledError an
# [x] Task verschluckt die CancelledError -> normale Rueckgabe (Erfolg)
# [x] die durchgereichte TimeoutError traegt keine Huelle im Kontext
# [x] soft_timeout=0 -> SoftTimeoutError ohne "never awaited"-Warnung
# [x] Abbruch von aussen waehrend der Frist bleibt CancelledError
# [x] Grenzfall E-045: Task faengt den Abbruch und wirft eigene TimeoutError
#     -> genau diese TimeoutError, kein SoftTimeoutError
"""

from __future__ import annotations

import asyncio
import inspect
from typing import cast

import pytest

from swarmq.exceptions import SoftTimeoutError
from swarmq.worker.soft_timeout import run_with_soft_timeout

pytestmark = pytest.mark.timeout(5)

# ``TaskInfo.soft_timeout`` ist als ``int`` annotiert; die Huelle nimmt jede Zahl.
_FAST_S = cast("int", 0.01)


class _ServerTimeoutError(TimeoutError):
    """Wie ``aiohttp.ServerTimeoutError``: eine Unterklasse von ``TimeoutError``."""


async def test_task_timeout_error_subclass_passes_through_unchanged() -> None:
    raised = _ServerTimeoutError("upstream did not answer")

    async def run() -> None:
        raise raised

    with pytest.raises(_ServerTimeoutError) as info:
        await run_with_soft_timeout(run(), 60)

    assert info.value is raised


async def test_without_soft_timeout_the_value_passes_through() -> None:
    async def run() -> str:
        return "done"

    assert await run_with_soft_timeout(run(), None) == "done"


async def test_without_soft_timeout_a_task_timeout_error_passes_through_unchanged() -> None:
    raised = TimeoutError("own deadline")

    async def run() -> None:
        raise raised

    with pytest.raises(TimeoutError) as info:
        await run_with_soft_timeout(run(), None)

    assert info.value is raised


async def test_expired_soft_timeout_becomes_soft_timeout_error_and_cancels_run() -> None:
    seen: list[type[BaseException]] = []

    async def run() -> None:
        try:
            await asyncio.Event().wait()
        except BaseException as exc:
            seen.append(type(exc))
            raise

    with pytest.raises(SoftTimeoutError, match=r"soft timeout after 0\.01s"):
        await run_with_soft_timeout(run(), _FAST_S)

    assert seen == [asyncio.CancelledError]


async def test_task_that_swallows_the_cancel_returns_normally() -> None:
    """E-045-Folge: Wer die ``CancelledError`` verschluckt, liefert ein Ergebnis."""

    async def run() -> str:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            return "partial"
        return "never"

    assert await run_with_soft_timeout(run(), _FAST_S) == "partial"


async def test_passed_through_task_timeout_error_carries_no_wrapper_context() -> None:
    async def run() -> None:
        raise TimeoutError("own deadline")

    with pytest.raises(TimeoutError) as info:
        await run_with_soft_timeout(run(), 60)

    assert info.value.__context__ is None
    assert info.value.__cause__ is None


async def test_zero_soft_timeout_is_a_soft_timeout_without_unawaited_coroutine() -> None:
    """``wait_for`` mit Frist 0 startet die Huelle nicht; ``run()`` darf nicht
    als "never awaited" liegen bleiben (Warnungen sind Fehler)."""

    async def run() -> str:
        return "never"

    coro = run()
    with pytest.raises(SoftTimeoutError, match="soft timeout after 0s"):
        await run_with_soft_timeout(coro, 0)

    assert inspect.getcoroutinestate(coro) == inspect.CORO_CLOSED


async def test_outer_cancel_during_the_soft_timeout_stays_a_cancel() -> None:
    started = asyncio.Event()

    async def run() -> None:
        started.set()
        await asyncio.Event().wait()

    outer = asyncio.create_task(run_with_soft_timeout(run(), 60))
    await started.wait()
    outer.cancel()

    with pytest.raises(asyncio.CancelledError):
        await outer


@pytest.mark.entscheidung("E-045")
async def test_task_raising_its_own_timeout_error_after_the_cancel_keeps_it() -> None:
    """Grenzfall E-045: Faengt ``run()`` die ``CancelledError`` des Soft-Timeouts
    und wirft selbst ``TimeoutError``, zaehlt das als Task-Timeout."""
    raised = TimeoutError("own after cancel")

    async def run() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            raise raised from None

    with pytest.raises(TimeoutError) as info:
        await run_with_soft_timeout(run(), _FAST_S)

    assert info.value is raised
    assert not isinstance(info.value, SoftTimeoutError)
