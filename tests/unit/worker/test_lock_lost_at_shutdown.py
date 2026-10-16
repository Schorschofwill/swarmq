"""BZ-04 (Laufzeit, Plan 0.12.0, MR 13): auch die Freigabe liegengebliebener
Locks im Shutdown meldet einen verlorenen Lock laut.

Test-Liste:
- [x] ``release`` liefert ``False`` fuer einen liegengebliebenen Handler-Lock
      -> WARNING ``task_lock_lost`` mit ``lock_name`` und ``stage=release``
- [x] Review MR 13 (Hinweis): haelt der Keeper die Lease noch, nennt die
      Meldung ``task_id`` und ``task_name`` (im Shutdown fehlt der
      Log-Kontext des Handlers); ebenso ``release_acquired_locks`` mit der
      vom Keeper zurueckgegebenen Lease
- [x] F3-12: hat der Shutdown den Registry-Eintrag schon entnommen, gibt
      ``release_acquired_locks`` nichts frei (kein ``release``, kein
      ``drop``, kein Log)
- [x] F3-12: Semaphor-Lock mit zwei Tokens desselben Handlers -- Shutdown
      raeumt ab, danach der ``finally`` des Handlers -> je Token eine
      Freigabe, kein ``task_lock_lost``
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


async def test_release_acquired_locks_skips_tokens_the_shutdown_already_took(swarmq_logs) -> None:
    from swarmq.logging import get_logger
    from swarmq.worker.gates.lock import release_acquired_locks

    token = LockToken(name="res-gone", owner="tok", max_concurrency=1)
    store = MagicMock()
    store.release = AsyncMock(return_value=False)
    dropped: list[LockToken] = []

    await release_acquired_locks(
        store,
        {},
        uuid.uuid4(),
        [token],
        log=get_logger("swarmq.worker.pipeline"),
        drop=dropped.append,
    )

    store.release.assert_not_called()
    assert dropped == []
    assert [e for e in swarmq_logs if e["event"] in {"task_lock_lost", "lock_release_failed"}] == []


async def test_semaphore_tokens_taken_by_shutdown_are_released_once(worker, swarmq_logs) -> None:
    from collections import Counter

    from tests.support.inmemory.backend import InMemoryLockStore
    from tests.support.inmemory.clock import FakeClock

    store = InMemoryLockStore(FakeClock())
    first = await store.acquire("res-sem", owner="tok-1", ttl_ms=60_000, max_concurrency=2)
    second = await store.acquire("res-sem", owner="tok-2", ttl_ms=60_000, max_concurrency=2)
    assert first is not None
    assert second is not None
    releases: list[str] = []
    original_release = store.release

    async def counting_release(token: LockToken) -> bool:
        releases.append(token.owner)
        return await original_release(token)

    store.release = counting_release  # type: ignore[method-assign]  # Zaehler um den echten In-Memory-Store
    handler_id = uuid.uuid4()
    worker._state = WorkerState.RUNNING
    worker._lock_manager = store
    worker._active_handler_locks[handler_id] = [first, second]

    worker._request_shutdown()
    await worker._execute_shutdown()
    await worker._release_acquired_locks(handler_id, [first, second])

    assert Counter(releases) == {"tok-1": 1, "tok-2": 1}
    assert [e for e in swarmq_logs if e["event"] == "task_lock_lost"] == []
