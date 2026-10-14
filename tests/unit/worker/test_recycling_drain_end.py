"""Drain-Ende im Recycling (FS-02 Teil b, Plan 2026-10-14-0748, U3, KTD3).

Eine einzige Quelle liefert das Drain-Ende ab Beginn des Herunterfahrens:
ohne Recycling ``drain_until`` aus dem Budget, mit Recycling-Override
``started_at + override``. Publish-Endfrist und Drain-Wartezeit folgen ihr,
und das Ende der Aufraeumschritte wird danach neu angesetzt -- sonst bekaeme
jeder Schritt nach einem langen Recycling-Drain nur die Untergrenze.

Zeit ist skaliert (wie ``test_shutdown_budget_steps.py``).

Testliste:
- [x] Recycling: die Publish-Endfrist ist started_at + Override, nicht drain_until
- [x] ohne Recycling: die Publish-Endfrist bleibt started_at + drain_until (Bestand)
- [x] nach einem langen Recycling-Drain bekommt ein Aufraeumschritt die Reserve ab Drain-Ende
- [x] nach einem kurzen Recycling-Drain verkuerzt sich das Aufraeumbudget nicht
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock

import pytest

from swarmq.shutdown_budget import shutdown_budget
from swarmq.worker import WorkerState
from swarmq.worker import lifecycle as _lc

pytestmark = pytest.mark.timeout(5)


def _until(worker: Any, seconds: float) -> asyncio.Task[bool]:
    """Laufender Task, der nach *seconds* von selbst endet."""
    done = asyncio.Event()
    asyncio.get_running_loop().call_later(seconds, done.set)
    task = asyncio.create_task(done.wait())
    worker._active_tasks.add(task)
    task.add_done_callback(worker._active_tasks.discard)
    return task


async def _shutdown(worker: Any) -> float:
    worker._state = WorkerState.RUNNING
    worker._request_shutdown()
    started = asyncio.get_running_loop().time()
    await asyncio.wait_for(worker._execute_shutdown(), 2.0)
    return started


def _deadlines(broker: Any) -> list[float | None]:
    return [call.args[0] for call in broker.set_publish_deadline.call_args_list]


async def test_recycling_publish_deadline_follows_the_drain_end(worker: Any, broker: Any) -> None:
    worker._config.shutdown_timeout = 0.02
    worker._drain_timeout_override = 5.0

    started = await _shutdown(worker)

    first, last = _deadlines(broker)
    assert first is not None
    assert started + 5.0 <= first < started + 5.1
    assert last is None


async def test_without_recycling_the_publish_deadline_stays_at_drain_until(worker: Any, broker: Any) -> None:
    worker._config.shutdown_timeout = 0.02
    drain_until = shutdown_budget(0.02, supervised=False).drain_until

    started = await _shutdown(worker)

    first, _last = _deadlines(broker)
    assert first is not None
    assert started + drain_until <= first < started + drain_until + 0.05


async def test_cleanup_after_a_long_recycling_drain_gets_the_reserve_from_the_drain_end(
    worker: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(_lc, "_SHUTDOWN_STEP_MIN_S", 0.001)
    worker._config.shutdown_timeout = 0.04  # Reserve nach dem Drain: 0.02 s
    worker._drain_timeout_override = 2.0
    _ = _until(worker, 0.06)  # endet nach dem ganzen Budget, vor der Recycling-Frist
    seen: list[float] = []

    async def _stop() -> None:
        seen.append(worker._step_timeout())

    worker._scheduler = AsyncMock()
    worker._scheduler.stop.side_effect = _stop

    await _shutdown(worker)

    (step_timeout,) = seen
    assert step_timeout > 0.01, f"Aufraeumschritt bekam nur {step_timeout}s statt der Reserve ab Drain-Ende"


async def test_a_short_recycling_drain_does_not_shrink_the_cleanup_budget(
    worker: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(_lc, "_SHUTDOWN_STEP_MIN_S", 0.001)
    worker._config.shutdown_timeout = 2.0  # Reserve 1 s, Ende des Budgets bei 2 s
    worker._drain_timeout_override = 5.0
    seen: list[float] = []

    async def _stop() -> None:
        seen.append(worker._step_timeout())

    worker._scheduler = AsyncMock()
    worker._scheduler.stop.side_effect = _stop

    await _shutdown(worker)  # kein laufender Task: der Drain endet sofort

    (step_timeout,) = seen
    assert step_timeout > 1.5


async def test_sigterm_drain_never_moves_the_cleanup_end_past_the_budget(
    worker: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """E-018: Ohne Recycling bleibt das Ende bei ``finish_by``, auch wenn ein
    abgebrochener Task das Drain-Ende hinauszoegert -- sonst laege der Abbau
    unter dem Supervisor hinter dessen SIGKILL."""
    monkeypatch.setattr(_lc, "_SHUTDOWN_STEP_MIN_S", 0.05)
    worker._config.shutdown_timeout = 0.04  # Drain bis 0.02 s, Ende bei 0.04 s
    finish_by = shutdown_budget(0.04, supervised=False).finish_by

    async def _slow_to_cancel() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            late = asyncio.Event()
            asyncio.get_running_loop().call_later(0.03, late.set)
            await late.wait()

    task = asyncio.create_task(_slow_to_cancel())
    worker._active_tasks.add(task)
    task.add_done_callback(worker._active_tasks.discard)

    started = await _shutdown(worker)

    assert worker._shutdown_end <= started + finish_by + 0.01
