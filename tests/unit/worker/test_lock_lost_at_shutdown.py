"""BZ-04 (Laufzeit, Plan 0.12.0, MR 13): auch die Freigabe liegengebliebener
Locks im Shutdown meldet einen verlorenen Lock laut.

Test-Liste:
- [x] ``release`` liefert ``False`` fuer einen liegengebliebenen Handler-Lock
      -> WARNING ``task_lock_lost`` mit ``lock_name`` und ``stage=release``
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
    assert lost["lock_name"] == "res-lost" and lost["stage"] == "release"
    assert worker._active_handler_locks == {}
