"""Q6 fuer die Lock-Wartezeit (Plan 0.12.0, MR 13, BZ-05): die Zustands-Header
``x-swarmq-lock-conflicts``, ``-lock-wait-since`` und ``-lock-holder*``
bleiben in den Kopien der Gates (sonst setzte jede fremde Rueckstellung die
Frist zurueck) und entfallen nur in der fachlichen Retry-Kopie, im
DLQ-Stempel und durch ``dlq retry``.

Test-Liste:
- [x] die fachliche Retry-Kopie entfernt die Lock-Zustands-Header
- [x] Rate-Limit-Kopie eines Tasks mit Lock **und** Rate-Limit behaelt sie;
      die Obergrenze greift danach trotzdem (Q6, Obergrenze mit Gate-Kopien)
- [x] der DLQ-Stempel (Test in tests/unit/broker/test_lock_wait_state_dlq.py) entfernt sie
- [x] ``dlq retry`` entfernt sie aus einer DLX-Kopie
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.headers import (
    HEADER_LOCK_CONFLICTS,
    HEADER_LOCK_HOLDER,
    HEADER_LOCK_HOLDER_SINCE,
    HEADER_LOCK_HOLDER_TTL_MS,
    HEADER_LOCK_WAIT_SINCE,
    HEADER_RETRY_COUNT,
)
from swarmq.registry import TaskRegistry
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker
from tests.support.messages import make_message
from tests.unit.worker._helpers import _default_payload

pytestmark = pytest.mark.timeout(5)

_STATE = {
    HEADER_LOCK_CONFLICTS: 7,
    HEADER_LOCK_WAIT_SINCE: 1_700_000_000_000,
    HEADER_LOCK_HOLDER: "abc",
    HEADER_LOCK_HOLDER_SINCE: 1_700_000_000_000,
    HEADER_LOCK_HOLDER_TTL_MS: 90_000,
}


class FailsOnce(Task):
    task_info = TaskInfo(name="lockwait_fails_once", queues=["default"], max_retries=3)

    async def run(self) -> None:
        raise ValueError("boom")


async def test_retry_copy_drops_the_lock_wait_state(broker, backend, config) -> None:
    registry = TaskRegistry()
    registry.register(FailsOnce)
    worker = Worker(broker=broker, backend=backend, registry=registry, config=config)
    message = make_message(
        _default_payload(task_id="r-1", name="lockwait_fails_once", args=[], kwargs={}), headers=dict(_STATE)
    )
    message.queue = "default"

    await worker._handle_message(message)

    headers: dict[str, Any] = broker.retry_later.await_args.kwargs["headers"]
    assert headers[HEADER_RETRY_COUNT] == 1
    assert not set(_STATE) & set(headers)


class LockedAndLimited(Task):
    task_info = TaskInfo(
        name="lockwait_limited", queues=["default"], lock="resource", hard_timeout=60, rate_limit="1/m"
    )

    async def run(self) -> None:
        raise AssertionError("darf nicht laufen")


async def test_rate_limit_copy_keeps_the_wait_and_the_limit_still_applies(broker, backend, config) -> None:
    import time

    from swarmq.contracts.coordination import RateLimitDecision

    registry = TaskRegistry()
    registry.register(LockedAndLimited)
    worker = Worker(broker=broker, backend=backend, registry=registry, config=config)
    worker._rate_limiter = MagicMock()
    worker._rate_limiter.check = AsyncMock(return_value=RateLimitDecision(False, 0, 5_000))
    worker._lock_manager = MagicMock()
    worker._lock_manager.acquire = AsyncMock(return_value=None)
    over_limit = int(time.time() * 1000) - int(config.lock_wait_max_s * 1000) - 1_000
    message = make_message(
        _default_payload(task_id="rl-1", name="lockwait_limited", args=[], kwargs={}),
        headers={**_STATE, HEADER_LOCK_WAIT_SINCE: over_limit},
    )
    message.queue = "default"

    await worker._handle_message(message)  # Rate-Limit stellt zurueck
    copy = dict(broker.retry_later.await_args.kwargs["headers"])
    assert {k: copy[k] for k in _STATE} == {**_STATE, HEADER_LOCK_WAIT_SINCE: over_limit}

    worker._rate_limiter.check = AsyncMock(return_value=RateLimitDecision(True, 0, 0))
    again = make_message(_default_payload(task_id="rl-1", name="lockwait_limited", args=[], kwargs={}), headers=copy)
    again.queue = "default"
    await worker._handle_message(again)  # Lock-Konflikt auf der Kopie
    assert broker.dead_letter.await_count == 1, "die Obergrenze gilt auch nach einer Rate-Limit-Kopie"
