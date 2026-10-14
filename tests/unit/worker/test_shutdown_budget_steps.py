"""Shutdown-Budget im Worker (BZ-18, Plan 0.12.0, MR 17).

``shutdown_timeout`` ist das Gesamtbudget ab SIGTERM. Der Drain endet so,
dass Sperrenfreigabe, Leader-Uebergabe und Verbindungsschluss noch darin
liegen; jeder Aufraeumschritt hat ein Zeitlimit.

Test-Liste:
- [x] unter dem Supervisor endet der Drain vor dessen SIGKILL (Reserve 2 s anteilig)
- [x] haengender Scheduler-Stopp: begrenzt, Broker und Backend werden trotzdem geschlossen
- [x] haengende Lock-Freigabe: begrenzt (WARNING ``shutdown_lock_release_failed``),
      der Keeper schliesst trotzdem (MR 13), der Scheduler stoppt danach
- [x] haengender Abschluss des Ausfuehrungsschutzes: begrenzt, Sperren werden danach freigegeben
- [x] haengende abgetrennte Lock-Freigaben: Warten begrenzt auf das Budget (nicht mehr mindestens 5 s)
- [x] haengender Broker-Schluss (zwei Verbindungen, MR 16): begrenzt, das Backend schliesst trotzdem
- [x] haengender Backend-Schluss: begrenzt, der Worker endet als STOPPED
- [x] haengender Abbau des Rechen-Pools: begrenzt, Sperren werden danach freigegeben
- [x] haengender Abbau des DI-Containers: begrenzt, Broker und Backend schliessen danach
- [x] Task, der den Abbruch ignoriert: Warten auf ihn begrenzt auf das Budget (nicht fest 1 s)
- [x] Recycling-Drain ist ausgenommen: er wartet seine eigene Frist ab, ohne Abbruch
- [x] ausgeschoepftes Budget: jeder Schritt bekommt trotzdem die Untergrenze
- [x] ... und die Unteraufgaben des Schedulers werden abgebrochen (``abort``), keine Waisen
"""

from __future__ import annotations

import asyncio
import multiprocessing
import uuid
from typing import Any
from unittest.mock import AsyncMock

import pytest

from swarmq.contracts.coordination import LockToken
from swarmq.supervision import SupervisorChannel, set_current_channel
from swarmq.worker import WorkerState
from swarmq.worker import lifecycle as _lc

pytestmark = pytest.mark.timeout(5)


def _hang(worker: Any) -> asyncio.Task[bool]:
    task = asyncio.create_task(asyncio.Event().wait())
    worker._active_tasks.add(task)
    task.add_done_callback(worker._active_tasks.discard)
    return task


@pytest.mark.entscheidung("E-018")
async def test_supervised_drain_ends_before_the_supervisor_kills(worker: Any) -> None:
    loop = asyncio.get_running_loop()
    worker._config.shutdown_timeout = 0.2  # SIGKILL des Supervisors bei 0.1 s, Drain bis 0.05 s
    worker._state = WorkerState.RUNNING
    task = _hang(worker)
    cancelled_at: list[float] = []
    task.add_done_callback(lambda _t: cancelled_at.append(loop.time()))
    set_current_channel(SupervisorChannel.create(multiprocessing.get_context("spawn")))
    try:
        started = loop.time()
        worker._request_shutdown()
        await worker._execute_shutdown()
    finally:
        set_current_channel(None)

    assert cancelled_at
    assert cancelled_at[0] - started < 0.09


async def _forever(*_args: Any) -> None:
    await asyncio.Event().wait()


@pytest.fixture
def short_steps(monkeypatch: pytest.MonkeyPatch) -> None:
    """Untergrenze je Aufraeumschritt verkuerzen (Default 1 s), damit der Test schnell bleibt."""
    monkeypatch.setattr(_lc, "_SHUTDOWN_STEP_MIN_S", 0.01)


async def _shutdown_within(worker: Any, seconds: float) -> None:
    worker._state = WorkerState.RUNNING
    worker._request_shutdown()
    await asyncio.wait_for(worker._execute_shutdown(), seconds)


async def test_hanging_scheduler_stop_is_bounded(worker: Any, broker: Any, backend: Any, short_steps: None) -> None:
    worker._config.shutdown_timeout = 0.04
    scheduler = AsyncMock()
    scheduler.stop.side_effect = _forever
    worker._scheduler = scheduler

    await _shutdown_within(worker, 1.0)

    broker.close.assert_awaited()
    backend.close.assert_awaited()


async def test_scheduler_is_aborted_when_its_stop_hangs(worker: Any, short_steps: None) -> None:
    worker._config.shutdown_timeout = 0.04
    scheduler = AsyncMock()
    scheduler.stop.side_effect = _forever
    worker._scheduler = scheduler

    await _shutdown_within(worker, 1.0)

    scheduler.abort.assert_awaited_once()


async def test_hanging_lock_release_is_bounded_and_the_keeper_still_closes(
    worker: Any, short_steps: None, swarmq_logs: list[dict[str, Any]]
) -> None:
    worker._config.shutdown_timeout = 0.04
    worker._lock_manager = AsyncMock()
    worker._lock_manager.release.side_effect = _forever
    worker._active_handler_locks[uuid.uuid4()] = [LockToken(name="report", owner="tok", max_concurrency=1)]
    closed: list[bool] = []
    worker._lock_keeper.close = lambda: closed.append(True)
    worker._scheduler = AsyncMock()

    await _shutdown_within(worker, 1.0)

    assert closed == [True]
    worker._scheduler.stop.assert_awaited_once()
    assert [e for e in swarmq_logs if e["event"] == "shutdown_lock_release_failed"]


async def test_hanging_execution_guard_drain_is_bounded(
    worker: Any, short_steps: None, swarmq_logs: list[dict[str, Any]]
) -> None:
    worker._config.shutdown_timeout = 0.04
    worker._execution_guard.drain = _forever
    worker._lock_manager = AsyncMock()
    worker._active_handler_locks[uuid.uuid4()] = [LockToken(name="report", owner="tok", max_concurrency=1)]

    await _shutdown_within(worker, 1.0)

    worker._lock_manager.release.assert_awaited_once()
    (event,) = [e for e in swarmq_logs if e["event"] == "shutdown_step_timeout"]
    assert event["step"] == "execution_guard_drain"


async def test_waiting_for_detached_releases_is_bounded_by_the_budget(worker: Any, short_steps: None) -> None:
    worker._config.shutdown_timeout = 0.04
    detached = asyncio.create_task(_forever())
    worker._detached_releases.add(detached)
    try:
        await _shutdown_within(worker, 1.0)
    finally:
        detached.cancel()


@pytest.mark.entscheidung("E-018")
async def test_hanging_broker_close_is_bounded_and_the_backend_still_closes(
    worker: Any, broker: Any, backend: Any, short_steps: None
) -> None:
    worker._config.shutdown_timeout = 0.04
    broker.close.side_effect = _forever

    await _shutdown_within(worker, 1.0)

    backend.close.assert_awaited_once()


async def test_hanging_backend_close_is_bounded(worker: Any, backend: Any, short_steps: None) -> None:
    worker._config.shutdown_timeout = 0.04
    backend.close.side_effect = _forever

    await _shutdown_within(worker, 1.0)

    assert worker._state is WorkerState.STOPPED


async def test_hanging_executor_close_is_bounded(worker: Any, short_steps: None) -> None:
    worker._config.shutdown_timeout = 0.04
    worker._close_executor = _forever
    worker._lock_manager = AsyncMock()
    worker._active_handler_locks[uuid.uuid4()] = [LockToken(name="report", owner="tok", max_concurrency=1)]

    await _shutdown_within(worker, 1.0)

    worker._lock_manager.release.assert_awaited_once()


async def test_hanging_di_container_close_is_bounded(
    worker: Any, broker: Any, monkeypatch: pytest.MonkeyPatch, short_steps: None
) -> None:
    worker._config.shutdown_timeout = 0.04
    container = AsyncMock()
    container.close.side_effect = _forever
    monkeypatch.setattr("swarmq.worker.shutdown.get_registered_container", lambda: container)

    await _shutdown_within(worker, 1.0)

    broker.close.assert_awaited_once()


async def test_waiting_for_a_task_that_ignores_cancellation_is_bounded(worker: Any, short_steps: None) -> None:
    loop = asyncio.get_running_loop()
    worker._config.shutdown_timeout = 0.04
    release = asyncio.Event()

    async def _stubborn() -> None:
        while not release.is_set():
            try:
                await release.wait()
            except asyncio.CancelledError:
                continue

    task = asyncio.create_task(_stubborn())
    worker._active_tasks.add(task)
    try:
        started = loop.time()
        await _shutdown_within(worker, 2.0)
        assert loop.time() - started < 0.5
    finally:
        release.set()
        await task


@pytest.mark.entscheidung("E-018")
async def test_recycling_drain_is_exempt_from_the_budget(worker: Any) -> None:
    worker._config.shutdown_timeout = 0.01
    worker._drain_timeout_override = 2.0
    done = asyncio.Event()
    asyncio.get_running_loop().call_later(0.06, done.set)  # endet nach dem Budget, vor der Recycling-Frist

    async def _slow() -> None:
        await done.wait()

    task = asyncio.create_task(_slow())
    worker._active_tasks.add(task)
    task.add_done_callback(worker._active_tasks.discard)

    await _shutdown_within(worker, 2.0)

    assert done.is_set()
    assert not task.cancelled()


async def test_exhausted_budget_still_gives_each_step_the_floor(worker: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_lc, "_SHUTDOWN_STEP_MIN_S", 0.5)
    worker._config.shutdown_timeout = 0.001
    stopped = asyncio.Event()

    async def _slow_stop() -> None:
        asyncio.get_running_loop().call_later(0.05, stopped.set)  # dauert laenger als das Budget
        await stopped.wait()

    worker._scheduler = AsyncMock()
    worker._scheduler.stop.side_effect = _slow_stop

    await _shutdown_within(worker, 2.0)

    assert stopped.is_set()
