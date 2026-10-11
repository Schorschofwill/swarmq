"""Verdrahtung des Lock-Keepers im Worker (Plan 0.12.0, MR 13, BZ-25).

Test-Liste:
- [x] erfolgreicher Acquire -> der Keeper haelt das Token; nach dem Lauf
      (Freigabe im finally) nicht mehr
- [x] Freigabe liegengebliebener Locks im Shutdown nimmt sie aus dem Keeper;
      der Keeper endet vor ``backend.close``
- [x] Review MR 13 (Hinweis): der Abbruch-Teardown (``_abort_teardown``)
      beendet den Keeper ebenfalls
- [x] ein Gate ohne ``keep_lock`` (Dritt-Verdrahtung) laeuft wie bisher
"""

from __future__ import annotations

from typing import ClassVar, override
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.contracts.coordination import LockToken
from swarmq.registry import TaskRegistry
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker
from tests.support.messages import make_message
from tests.unit.worker._helpers import _default_payload

pytestmark = pytest.mark.timeout(5)


class _Locked(Task):
    task_info = TaskInfo(name="kept", queues=["default"], lock="resource", hard_timeout=60)

    kept_during_run: ClassVar[list[bool]] = []

    @override
    async def run(self) -> None:
        worker = type(self).worker  # type: ignore[attr-defined]
        type(self).kept_during_run.append(worker._lock_keeper.kept == 1)


def _worker(broker, backend, config) -> Worker:
    registry = TaskRegistry()
    registry.register(_Locked)
    worker = Worker(broker=broker, backend=backend, registry=registry, config=config)
    worker._lock_manager = MagicMock()
    worker._lock_manager.acquire = AsyncMock(
        side_effect=lambda name, *, owner, ttl_ms, max_concurrency: LockToken(name, owner, max_concurrency)
    )
    worker._lock_manager.release = AsyncMock(return_value=True)
    _Locked.worker = worker  # type: ignore[attr-defined]
    _Locked.kept_during_run = []
    return worker


async def test_acquired_lock_is_kept_while_the_handler_runs(broker, backend, config) -> None:
    worker = _worker(broker, backend, config)

    await worker._handle_message(make_message(_default_payload(name="kept", args=[], kwargs={})))

    assert _Locked.kept_during_run == [True]
    assert worker._lock_keeper.kept == 0
    worker._lock_manager.release.assert_awaited_once()


async def test_shutdown_drops_leftover_locks_and_closes_the_keeper_before_backend_close(
    broker,
    backend,
    config,
) -> None:
    import uuid

    from swarmq.worker.lifecycle import WorkerState

    worker = _worker(broker, backend, config)
    worker._state = WorkerState.RUNNING
    token = LockToken(name="res-left", owner="tok-left", max_concurrency=1)
    worker._active_handler_locks[uuid.uuid4()] = [token]
    worker._lock_keeper.keep(token, 90_000, task_id="t", task_name="kept")
    kept_at_close: list[int] = []
    backend.close = AsyncMock(side_effect=lambda: kept_at_close.append(worker._lock_keeper.kept))

    worker._request_shutdown()
    await worker._execute_shutdown()

    worker._lock_manager.release.assert_awaited_once_with(token)
    assert kept_at_close == [0]


async def test_gate_without_keep_lock_acquires_as_before(broker, backend, config) -> None:
    import uuid

    from swarmq.worker.gates import GateContext
    from swarmq.worker.gates.lock import LockGate
    from swarmq.worker.outcome import Continue

    worker = _worker(broker, backend, config)
    worker._gate_deps.keep_lock = None
    gate = LockGate(worker._gate_deps, dead_letter=AsyncMock())
    ctx = GateContext(
        task_id="t1",
        task_name="kept",
        queue="default",
        task_info=_Locked.task_info,
        task_class=_Locked,
        handler_id=uuid.uuid4(),
    )

    assert await gate.apply(make_message(_default_payload(name="kept", args=[], kwargs={})), ctx) is Continue
    assert len(ctx.acquired_locks) == 1
    assert worker._lock_keeper.kept == 0


async def test_abort_teardown_closes_the_keeper(broker, backend, config) -> None:
    """Review MR 13 (Hinweis): ein abgebrochener ``start_worker()`` darf keine
    Keeper-Schleife hinterlassen."""
    worker = _worker(broker, backend, config)
    worker._lock_keeper.keep(LockToken("resource", "o-1", 1), 90_000, task_id="t", task_name="kept")
    loop = worker._lock_keeper._loop
    assert loop is not None

    await worker._abort_teardown()

    assert worker._lock_keeper.kept == 0
    assert loop.cancelling() or loop.cancelled()
