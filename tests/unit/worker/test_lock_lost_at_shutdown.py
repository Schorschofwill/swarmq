"""BZ-04 (Laufzeit, Plan 0.12.0, MR 13): auch die Freigabe liegengebliebener
Locks im Shutdown meldet einen verlorenen Lock laut.

Test-Liste:
- [x] ``release`` liefert ``False`` fuer einen liegengebliebenen Handler-Lock
      -> WARNING ``task_lock_lost`` mit ``lock_name`` und ``stage=release``
- [x] Review MR 13 (Hinweis): haelt der Keeper die Lease noch, nennt die
      Meldung ``task_id`` und ``task_name`` (im Shutdown fehlt der
      Log-Kontext des Handlers); ebenso ``release_acquired_locks`` mit der
      vom Keeper zurueckgegebenen Lease
"""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.contracts.coordination import LockToken
from swarmq.worker.lifecycle import WorkerState

pytestmark = pytest.mark.timeout(5)


async def test_lost_leftover_lock_is_reported_at_shutdown(worker, swarmq_logs) -> None:
    worker._state = WorkerState.RUNNING
    worker._lock_manager = MagicMock()
    worker._lock_manager.release = AsyncMock(return_value=False)
    worker._active_handler_locks[uuid.uuid4()] = [LockToken(name="res-lost", owner="tok", max_concurrency=1)]

    worker._request_shutdown()
    await worker._execute_shutdown()

    [lost] = [e for e in swarmq_logs if e["event"] == "task_lock_lost"]
    assert lost["log_level"] == "warning"
    assert lost["lock_name"] == "res-lost"
    assert lost["stage"] == "release"
    assert worker._active_handler_locks == {}


async def test_lost_leftover_lock_names_the_task(worker, swarmq_logs) -> None:
    token = LockToken(name="res-lost", owner="tok", max_concurrency=1)
    worker._state = WorkerState.RUNNING
    worker._lock_manager = MagicMock()
    worker._lock_manager.release = AsyncMock(return_value=False)
    worker._active_handler_locks[uuid.uuid4()] = [token]
    worker._lock_keeper.keep(token, 90_000, task_id="sd-1", task_name="locked")

    worker._request_shutdown()
    await worker._execute_shutdown()

    [lost] = [e for e in swarmq_logs if e["event"] == "task_lock_lost"]
    assert lost["task_id"] == "sd-1"
    assert lost["task_name"] == "locked"


async def test_release_acquired_locks_names_the_task_from_the_lease(swarmq_logs) -> None:
    from swarmq.logging import get_logger
    from swarmq.worker.gates.lock import release_acquired_locks
    from swarmq.worker.lock_keeper import KeptLock

    token = LockToken(name="res-lost", owner="tok", max_concurrency=1)
    store = MagicMock()
    store.release = AsyncMock(return_value=False)
    handler_id = uuid.uuid4()

    await release_acquired_locks(
        store,
        {handler_id: [token]},
        handler_id,
        [token],
        log=get_logger("swarmq.worker.pipeline"),
        drop=lambda t: KeptLock(t, 90_000, "rl-1", "locked", 0.0, 0.0),
    )

    [lost] = [e for e in swarmq_logs if e["event"] == "task_lock_lost"]
    assert lost["task_id"] == "rl-1"
    assert lost["task_name"] == "locked"
    assert lost["stage"] == "release"
