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
- [x] Recycling unter dem Supervisor: Reserve ab Drain-Ende aus dem Budget mit
      ``finish_by = sigkill_at`` (Review U3, testing)
- [x] Override ``0`` heisst kein Override: Publish-Endfrist bei drain_until
      (Review U3, testing)
"""

from __future__ import annotations

import asyncio
import multiprocessing
from collections.abc import Iterator
from typing import Any
from unittest.mock import AsyncMock

import pytest

from swarmq import supervision as sv
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


@pytest.fixture
def supervised() -> Iterator[None]:
    """Worker unter dem Supervisor: Budget endet bei dessen SIGKILL."""
    sv.set_current_channel(sv.SupervisorChannel.create(multiprocessing.get_context("spawn")))
    try:
        yield
    finally:
        sv.set_current_channel(None)


async def _cleanup_after_long_recycling_drain(worker: Any, total: float) -> tuple[float, float]:
    """Recycling-Drain, dessen einziger Task erst nach dem ganzen Budget
    *total* endet. Liefert ``(step_timeout, seit_task_ende)``: das Zeitlimit
    des ersten Aufraeumschritts und die Zeit, die zwischen Task-Ende und
    diesem Schritt verging.

    Deterministisch statt mit Zeitpuffer: Das neu angesetzte Ende liegt bei
    Drain-Ende + Reserve, und das Drain-Ende liegt nie vor dem Task-Ende.
    Also gilt ``step_timeout + seit_task_ende >= Reserve`` unabhaengig davon,
    wie langsam die Maschine ist (Review U3: Flake-Risiko der Fassung mit
    fester Schwelle)."""
    worker._config.shutdown_timeout = total
    worker._drain_timeout_override = 5.0  # Recycling-Frist weit hinter dem Budget
    task = _until(worker, 1.25 * total)  # endet nach dem ganzen Budget, vor der Recycling-Frist
    loop = asyncio.get_running_loop()
    done_at: list[float] = []
    task.add_done_callback(lambda _t: done_at.append(loop.time()))
    seen: list[tuple[float, float]] = []

    async def _stop() -> None:
        seen.append((worker._step_timeout(), loop.time()))

    worker._scheduler = AsyncMock()
    worker._scheduler.stop.side_effect = _stop

    await _shutdown(worker)

    ((step_timeout, stop_at),) = seen
    (task_done_at,) = done_at
    return step_timeout, stop_at - task_done_at


async def test_cleanup_after_a_long_recycling_drain_gets_the_reserve_from_the_drain_end(
    worker: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(_lc, "_SHUTDOWN_STEP_MIN_S", 0.001)
    budget = shutdown_budget(0.04, supervised=False)
    reserve = budget.finish_by - budget.drain_until  # 0.02 s

    step_timeout, since_task_end = await _cleanup_after_long_recycling_drain(worker, 0.04)

    assert step_timeout + since_task_end >= reserve, (
        f"Aufraeumschritt bekam nur {step_timeout}s statt der Reserve ab Drain-Ende"
    )
    assert step_timeout <= reserve


async def test_supervised_recycling_cleanup_gets_the_supervised_reserve_from_the_drain_end(
    worker: Any, monkeypatch: pytest.MonkeyPatch, supervised: None
) -> None:
    """Unter dem Supervisor endet das Budget bei ``sigkill_at``; die Reserve
    nach dem Recycling-Drain kommt aus diesem Budget, nicht aus dem
    unbeaufsichtigten (sonst liefe der Abbau hinter den SIGKILL)."""
    monkeypatch.setattr(_lc, "_SHUTDOWN_STEP_MIN_S", 0.001)
    budget = shutdown_budget(0.04, supervised=True)
    assert budget.finish_by == budget.sigkill_at < 0.04, "Voraussetzung: beaufsichtigtes Budget"
    reserve = budget.finish_by - budget.drain_until  # 0.01 s, unbeaufsichtigt waeren es 0.02 s

    step_timeout, since_task_end = await _cleanup_after_long_recycling_drain(worker, 0.04)

    assert step_timeout + since_task_end >= reserve
    assert step_timeout <= reserve


async def test_override_zero_means_no_recycling_drain(worker: Any, broker: Any) -> None:
    worker._config.shutdown_timeout = 0.02
    worker._drain_timeout_override = 0
    drain_until = shutdown_budget(0.02, supervised=False).drain_until

    started = await _shutdown(worker)

    first, _last = _deadlines(broker)
    assert first is not None
    assert started + drain_until <= first < started + drain_until + 0.05


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
