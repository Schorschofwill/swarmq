"""Q6 fuer die Lock-Wartezeit (Plan 0.12.0, MR 13, BZ-05): die Zustands-Header
``x-swarmq-lock-conflicts``, ``-lock-wait-since`` und ``-lock-holder*``
bleiben in den Kopien der Gates (sonst setzte jede fremde Rueckstellung die
Frist zurueck) und entfallen nur in der fachlichen Retry-Kopie, im
DLQ-Stempel und durch ``dlq retry``.

Test-Liste:
- [x] die fachliche Retry-Kopie entfernt die Lock-Zustands-Header
- [x] Review MR 13, P2-4: auch die Generation ``x-swarmq-lock-wait-gen``
      entfaellt in der Retry-Kopie (Zustands-Header wie die uebrigen)
- [x] Review MR 13, P2-4: eine Gate-Kopie (Ausfuehrungsschutz) einer vom
      Broker neu zugestellten Nachricht traegt die alte Wartezeit nicht weiter
      (sonst waere sie auf der Kopie wieder "aktuell")
- [x] Rate-Limit-Kopie eines Tasks mit Lock **und** Rate-Limit behaelt sie;
      die Obergrenze greift danach trotzdem (Q6, Obergrenze mit Gate-Kopien)
- [x] Review MR 13, P2-6: Backend-Defer-Kopie (``deferral_copy``) und
      Guard-Defer-Kopie (``ExecutionGuard._defer``) behalten sie ebenfalls;
      die Obergrenze greift danach trotzdem
- [x] der DLQ-Stempel (Test in tests/unit/broker/test_lock_wait_state_dlq.py) entfernt sie
- [x] ``dlq retry`` entfernt sie aus einer DLX-Kopie
"""

from __future__ import annotations

from typing import Any, override
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

    @override
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


async def test_retry_copy_drops_the_wait_generation(broker, backend, config) -> None:
    from swarmq.headers import HEADER_LOCK_WAIT_GEN, LOOP_STATE_HEADERS

    registry = TaskRegistry()
    registry.register(FailsOnce)
    worker = Worker(broker=broker, backend=backend, registry=registry, config=config)
    message = make_message(
        _default_payload(task_id="r-2", name="lockwait_fails_once", args=[], kwargs={}),
        headers={**_STATE, HEADER_LOCK_WAIT_GEN: "0:"},
    )
    message.queue = "default"

    await worker._handle_message(message)

    assert HEADER_LOCK_WAIT_GEN in LOOP_STATE_HEADERS  # DLQ-Stempel und dlq retry nutzen dieselbe Liste
    assert HEADER_LOCK_WAIT_GEN not in broker.retry_later.await_args.kwargs["headers"]


class LockedAndLimited(Task):
    task_info = TaskInfo(
        name="lockwait_limited", queues=["default"], lock="resource", hard_timeout=60, rate_limit="1/m"
    )

    @override
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


async def test_guard_copy_of_a_broker_redelivery_drops_the_old_wait() -> None:
    """Review MR 13, P2-4: SIGKILL nach dem Acquire -> der Broker stellt neu zu
    (``x-delivery-count``), der Ausfuehrungsschutz sieht den Running-Marker des
    toten Workers und stellt zurueck. Die Kopie hat keine Broker-Zaehler mehr;
    truege sie die alte Wartezeit, endete der naechste Konflikt sofort."""
    from swarmq.config import SwarmQConfig
    from swarmq.worker.gates import GateContext
    from swarmq.worker.gates.execution_guard import ExecutionGuard
    from tests.support.inmemory import FakeClock, InMemoryBackend

    store = InMemoryBackend(FakeClock())
    await store.connect()
    await store.execution_guard.claim(
        "g-1", owner="dead-worker", retry_count=0, ttl_ms=60_000, state_ttl_ms=600_000, max_orphans=3
    )
    deps = MagicMock()
    deps.config = SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x")
    deps.broker.retry_later = AsyncMock()
    guard = ExecutionGuard(deps, store=lambda: store.execution_guard, dead_letter=AsyncMock())
    ctx = GateContext(task_id="g-1", task_name="t", queue="q", task_info=TaskInfo(name="t", queues=["q"]))

    await guard.apply(make_message(headers={**_STATE, "x-delivery-count": 1}), ctx)

    copy = deps.broker.retry_later.await_args.kwargs["headers"]
    assert not set(_STATE) & set(copy)
    await store.close()


def _over_limit_state(config: Any) -> dict[str, Any]:
    import time

    return {**_STATE, HEADER_LOCK_WAIT_SINCE: int(time.time() * 1000) - int(config.lock_wait_max_s * 1000) - 1_000}


def _locked_worker(broker: Any, backend: Any, config: Any) -> Worker:
    registry = TaskRegistry()
    registry.register(LockedAndLimited)
    worker = Worker(broker=broker, backend=backend, registry=registry, config=config)
    worker._rate_limiter = None
    worker._lock_manager = MagicMock()
    worker._lock_manager.acquire = AsyncMock(return_value=None)
    return worker


def _locked_message(task_id: str, headers: dict[str, Any]) -> Any:
    message = make_message(
        _default_payload(task_id=task_id, name="lockwait_limited", args=[], kwargs={}), headers=headers
    )
    message.queue = "default"
    return message


async def test_backend_defer_copy_keeps_the_wait_and_the_limit_still_applies(broker, backend, config) -> None:
    """Review MR 13, P2-6 (Q6): Backend-Ausfall beim Acquire -> Zurueckstellung
    ueber ``deferral_copy``; die Kopie behaelt die Wartezeit."""
    from swarmq.exceptions import BackendConnectionError

    worker = _locked_worker(broker, backend, config)
    worker._lock_manager.acquire = AsyncMock(side_effect=BackendConnectionError("valkey away"))
    state = _over_limit_state(config)

    await worker._handle_message(_locked_message("bd-1", state))  # Backend-Defer
    copy = dict(broker.retry_later.await_args.kwargs["headers"])
    assert copy["x-swarmq-backend-defer-count"] == 1
    assert {k: copy[k] for k in _STATE} == state
    broker.dead_letter.assert_not_awaited()

    worker._lock_manager.acquire = AsyncMock(return_value=None)
    await worker._handle_message(_locked_message("bd-1", copy))  # Konflikt auf der Kopie
    assert broker.dead_letter.await_count == 1, "die Obergrenze gilt auch nach einer Backend-Defer-Kopie"


async def test_guard_defer_copy_keeps_the_wait_and_the_limit_still_applies(broker, backend, config) -> None:
    """Review MR 13, P2-6 (Q6): dieselbe Ausfuehrung laeuft anderswo ->
    Zurueckstellung durch den Ausfuehrungsschutz; die Kopie behaelt die
    Wartezeit, und der Lock-Konflikt danach endet an der Obergrenze."""
    from swarmq.config import SwarmQConfig
    from swarmq.worker.gates import GateContext
    from swarmq.worker.gates.execution_guard import ExecutionGuard
    from tests.support.inmemory import FakeClock, InMemoryBackend

    store = InMemoryBackend(FakeClock())
    await store.connect()
    await store.execution_guard.claim(
        "gd-1", owner="other", retry_count=0, ttl_ms=60_000, state_ttl_ms=600_000, max_orphans=3
    )
    deps = MagicMock()
    deps.config = SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x")
    deps.broker.retry_later = AsyncMock()
    guard = ExecutionGuard(deps, store=lambda: store.execution_guard, dead_letter=AsyncMock())
    ctx = GateContext(task_id="gd-1", task_name="t", queue="q", task_info=TaskInfo(name="t", queues=["q"]))
    state = _over_limit_state(config)

    await guard.apply(make_message(headers=dict(state)), ctx)  # Guard-Defer
    copy = dict(deps.broker.retry_later.await_args.kwargs["headers"])
    assert copy["x-swarmq-guard-defer-count"] == 1
    assert {k: copy[k] for k in _STATE} == state
    await store.close()

    worker = _locked_worker(broker, backend, config)
    await worker._handle_message(_locked_message("gd-1", copy))  # Konflikt auf der Kopie
    assert broker.dead_letter.await_count == 1, "die Obergrenze gilt auch nach einer Guard-Defer-Kopie"
