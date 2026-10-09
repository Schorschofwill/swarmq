"""Ausführungsschutz im Runner (Plan 2026-10-08-1541, U8, KTD5): Position
des Gates, Marker nach dem tatsächlich ausgeführten Verb, ``finally``.

Test-Liste:
1. Reihenfolge: Claim nach Pre-Pipeline-Gates und Registry-Lookup, vor
   Rate-Limit und Lock (und damit vor dem Expiry-Gate).
2. Erfolg → Done-Marker nach dem Ack; eine spätere Zustellung wird bestätigt
   und übersprungen, der Task läuft nicht erneut.
3. Unbehandelte Ausnahme im Hauptteil → kein Done-Marker, Running frei;
   die Neuzustellung läuft.
4. Abbruch (``CancelledError``, Shutdown-Drain) → ebenso.
5. ``max_age`` überschritten, Original läuft → Kopie zurückgestellt, nicht
   dead-gelettert.
6. Retry-Ausgang → Superseded-Marker; eine Kopie des alten Versuchs wird
   übersprungen, nur die Retry-Kopie läuft.
7. DLQ-Ausgang → Done-Marker; eine Kopie wird übersprungen.
8. Shutdown vor dem Gate → ``nack`` ohne Claim.
9. Die Gates sprechen den protokollierenden Wrapper: ein Rate-Limit-Requeue
   gibt Running vor dem Publish frei.
10. Abgeschaltetes Dead-Lettering (U9, KTD7): das Ack des Ergebnis-Tails
    laeuft ueber den Wrapper -> Done-Marker; eine Kopie wird übersprungen.
11. Review Phase 2: ein Abbruch waehrend des Finalisierens im ``finally``
    (Schedfire/Unique) laesst die Freigabe des Running-Markers nicht aus.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import AsyncIterator
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.config import SwarmQConfig
from swarmq.contracts import GuardClaim
from swarmq.contracts.coordination import LockToken, RateLimitDecision
from swarmq.registry import TaskRegistry
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker
from tests.support.inmemory import FakeClock, InMemoryBackend
from tests.support.messages import make_message

pytestmark = pytest.mark.timeout(5)

RUNS: list[str] = []


class CountedTask(Task):
    task_info = TaskInfo(name="guard-counted", queues=["default"], rate_limit="10/m", lock="res", hard_timeout=60)

    async def run(self) -> str:
        RUNS.append("run")
        return "ok"


def _payload(name: str, task_id: str = "t-1") -> dict:
    return {"task": {"id": task_id, "queue": "default", "name": name}, "args": [], "kwargs": {}}


class Recorder:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def spy(self, name: str, result: object = None) -> AsyncMock:
        async def _side_effect(*_args: object, **_kwargs: object) -> object:
            self.calls.append(name)
            return result

        return AsyncMock(side_effect=_side_effect)


@pytest.fixture(autouse=True)
def _reset_runs() -> None:
    RUNS.clear()


@pytest.fixture
def rec() -> Recorder:
    return Recorder()


@pytest.fixture
def broker(rec: Recorder) -> MagicMock:
    b = MagicMock(spec=RabbitMQBroker)
    b.ack = rec.spy("broker.ack")
    b.redeliver = rec.spy("broker.redeliver")
    b.retry_later = rec.spy("broker.retry_later")
    b.dead_letter = rec.spy("broker.dead_letter")
    b.route_via_dlx = rec.spy("broker.route_via_dlx")
    return b


@pytest.fixture
def backend(rec: Recorder) -> MagicMock:
    be = MagicMock(spec=ValkeyBackend)
    be.store_result = rec.spy("backend.store_result")
    be.notify_result = rec.spy("backend.notify_result")
    be.is_cancelled = rec.spy("backend.is_cancelled", False)
    be.is_workflow_cancelled = rec.spy("backend.is_workflow_cancelled", False)
    be.clear_cancel_flag = rec.spy("backend.clear_cancel_flag")
    be.release_unique = rec.spy("backend.release_unique", True)
    return be


@pytest.fixture
async def guard_backend() -> AsyncIterator[InMemoryBackend]:
    impl = InMemoryBackend(FakeClock())
    await impl.connect()
    yield impl
    await impl.close()


def _worker(broker: MagicMock, backend: MagicMock, rec: Recorder, store: object, *tasks: type[Task]) -> Worker:
    registry = TaskRegistry()
    for task in tasks:
        registry.register(task)
    config = SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x", concurrency=4, queues=["default"])
    worker = Worker(broker=broker, backend=backend, registry=registry, config=config)
    limiter = MagicMock()
    limiter.check = rec.spy("rate_limiter.check", RateLimitDecision(allowed=True, remaining=9, retry_after_ms=0))
    worker._rate_limiter = limiter
    locks = MagicMock()
    locks.acquire = rec.spy("lock_manager.acquire", LockToken(name="res", owner="tok", max_concurrency=1))
    locks.release = rec.spy("lock_manager.release", True)
    worker._lock_manager = locks
    worker._guard_store = store
    return worker


async def test_claim_follows_the_pre_pipeline_gates_and_precedes_rate_limit_and_lock(broker, backend, rec) -> None:
    store = MagicMock()
    store.claim = rec.spy("guard.claim", GuardClaim("claimed"))
    store.mark_done = rec.spy("guard.mark_done")
    worker = _worker(broker, backend, rec, store, CountedTask)

    await worker._handle_message(make_message(_payload("guard-counted")))

    order = [c for c in rec.calls if c in ("backend.is_cancelled", "guard.claim", "rate_limiter.check",
                                           "lock_manager.acquire", "broker.ack", "guard.mark_done")]
    assert order == [
        "backend.is_cancelled", "guard.claim", "rate_limiter.check", "lock_manager.acquire",
        "broker.ack", "guard.mark_done",
    ]


async def _status(guard_backend: InMemoryBackend, identity: str = "t-1", retry_count: int = 0) -> str:
    claim = await guard_backend.execution_guard.claim(
        identity, owner="probe", retry_count=retry_count, ttl_ms=60_000, state_ttl_ms=600_000, max_orphans=99,
    )
    return claim.status


async def test_unhandled_exception_frees_running_without_done(broker, backend, rec, guard_backend) -> None:
    chain = MagicMock()
    chain.run_pre_execute = AsyncMock(side_effect=RuntimeError("hook exploded"))
    worker = _worker(broker, backend, rec, guard_backend.execution_guard, CountedTask)
    worker._middleware_chain_factory = lambda: chain

    await worker._handle_message(make_message(_payload("guard-counted")))

    assert await _status(guard_backend) == "claimed"



async def test_cancelled_delivery_frees_running_without_done(broker, backend, rec, guard_backend) -> None:
    """Ein Abbruch im Hauptteil (Shutdown, hier während des Rate-Limit-Gates)
    setzt keinen Done-Marker; die Neuzustellung läuft sofort."""
    worker = _worker(broker, backend, rec, guard_backend.execution_guard, CountedTask)
    entered = asyncio.Event()

    async def _blocking_check(*_args: object, **_kwargs: object) -> RateLimitDecision:
        entered.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    worker._rate_limiter.check = _blocking_check
    handler = asyncio.create_task(worker._handle_message(make_message(_payload("guard-counted"))))
    await asyncio.wait_for(entered.wait(), 1)

    handler.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await handler

    assert await _status(guard_backend) == "claimed"
    assert RUNS == []


async def test_cancel_during_finalize_still_frees_running(broker, backend, rec, guard_backend) -> None:
    chain = MagicMock()
    chain.run_pre_execute = AsyncMock(side_effect=RuntimeError("hook exploded"))
    worker = _worker(broker, backend, rec, guard_backend.execution_guard, CountedTask)
    worker._middleware_chain_factory = lambda: chain
    entered = asyncio.Event()

    async def _blocking_finalize(*_args: object, **_kwargs: object) -> bool:
        entered.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    worker._finalize_schedfire = _blocking_finalize  # type: ignore[method-assign]
    handler = asyncio.create_task(worker._handle_message(make_message(_payload("guard-counted"))))
    await asyncio.wait_for(entered.wait(), 1)

    handler.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await handler
    await asyncio.gather(*worker._detached_releases)

    assert await _status(guard_backend) == "claimed"


async def test_redelivery_after_completion_is_acked_and_skipped(broker, backend, rec, guard_backend) -> None:
    worker = _worker(broker, backend, rec, guard_backend.execution_guard, CountedTask)

    await worker._handle_message(make_message(_payload("guard-counted")))
    rec.calls.clear()
    await worker._handle_message(make_message(_payload("guard-counted"), headers={"x-delivery-count": 1}))

    assert RUNS == ["run"]
    assert rec.calls.count("broker.ack") == 1
    assert "backend.store_result" not in rec.calls


class ExpiringTask(Task):
    task_info = TaskInfo(name="guard-expiring", queues=["default"], max_age=1)

    async def run(self) -> str:
        RUNS.append("run")
        return "ok"


async def test_expired_copy_of_a_running_original_is_deferred_not_dead_lettered(
    broker, backend, rec, guard_backend,
) -> None:
    """Sonst läge eine Kopie in der DLQ, während das Original noch läuft und
    gleich erfolgreich abschließt."""
    await guard_backend.execution_guard.claim(
        "t-1", owner="original", retry_count=0, ttl_ms=60_000, state_ttl_ms=600_000, max_orphans=3,
    )
    worker = _worker(broker, backend, rec, guard_backend.execution_guard, ExpiringTask)

    await worker._handle_message(
        make_message(_payload("guard-expiring"), headers={"x-swarmq-enqueued-at": int(time.time()) - 3600}),
    )

    assert "broker.retry_later" in rec.calls
    assert "broker.dead_letter" not in rec.calls
    assert RUNS == []


class FlakyTask(Task):
    task_info = TaskInfo(name="guard-flaky", queues=["default"], max_retries=1)

    async def run(self) -> str:
        RUNS.append("run")
        if len(RUNS) == 1:
            raise RuntimeError("first attempt fails")
        return "ok"


async def test_retry_supersedes_the_old_attempt_only_the_retry_copy_runs(broker, backend, rec, guard_backend) -> None:
    worker = _worker(broker, backend, rec, guard_backend.execution_guard, FlakyTask)

    await worker._handle_message(make_message(_payload("guard-flaky")))
    assert broker.retry_later.await_args.kwargs["headers"]["x-swarmq-retry-count"] == 1

    # Eine liegengebliebene Kopie des ersten Versuchs (z. B. zurückgestellt,
    # während er lief) wird übersprungen ...
    await worker._handle_message(make_message(_payload("guard-flaky"), headers={"x-swarmq-guard-defer-count": 1}))
    assert RUNS == ["run"]

    # ... die Retry-Kopie läuft.
    await worker._handle_message(make_message(_payload("guard-flaky"), headers={"x-swarmq-retry-count": 1}))
    assert RUNS == ["run", "run"]


async def test_dead_lettered_execution_skips_a_leftover_copy(broker, backend, rec, guard_backend) -> None:
    class FailingTask(Task):
        task_info = TaskInfo(name="guard-failing", queues=["default"], max_retries=0)

        async def run(self) -> str:
            RUNS.append("run")
            raise RuntimeError("boom")

    worker = _worker(broker, backend, rec, guard_backend.execution_guard, FailingTask)

    await worker._handle_message(make_message(_payload("guard-failing")))
    await worker._handle_message(make_message(_payload("guard-failing"), headers={"x-swarmq-guard-defer-count": 1}))

    assert RUNS == ["run"]
    assert rec.calls.count("broker.dead_letter") == 1


async def test_failure_without_dead_lettering_marks_done_via_the_ack(broker, backend, rec, guard_backend) -> None:
    class NoDlqTask(Task):
        task_info = TaskInfo(name="guard-no-dlq", queues=["default"], max_retries=0, dead_letter=False)

        async def run(self) -> str:
            RUNS.append("run")
            raise RuntimeError("boom")

    worker = _worker(broker, backend, rec, guard_backend.execution_guard, NoDlqTask)

    await worker._handle_message(make_message(_payload("guard-no-dlq")))
    await worker._handle_message(make_message(_payload("guard-no-dlq"), headers={"x-swarmq-guard-defer-count": 1}))

    assert RUNS == ["run"]
    assert "broker.dead_letter" not in rec.calls
    assert rec.calls.count("broker.ack") == 2  # Fehlerergebnis, dann die übersprungene Kopie


async def test_shutdown_nack_comes_before_any_claim(broker, backend, rec, guard_backend) -> None:
    worker = _worker(broker, backend, rec, guard_backend.execution_guard, CountedTask)
    worker._shutdown_event.set()

    await worker._handle_message(make_message(_payload("guard-counted")))

    assert rec.calls[-1] == "broker.redeliver"
    assert await _status(guard_backend) == "claimed"


async def test_rate_limit_requeue_frees_running_before_the_copy(broker, backend, rec, guard_backend) -> None:
    worker = _worker(broker, backend, rec, guard_backend.execution_guard, CountedTask)
    worker._rate_limiter.check = rec.spy(
        "rate_limiter.check", RateLimitDecision(allowed=False, remaining=0, retry_after_ms=500),
    )
    seen: list[str] = []

    async def _publish(*_args: object, **_kwargs: object) -> None:
        seen.append(await _status(guard_backend))

    broker.retry_later = AsyncMock(side_effect=_publish)
    await worker._handle_message(make_message(_payload("guard-counted")))

    assert seen == ["claimed"]


async def test_guard_events_reach_the_worker_metrics(broker, backend, rec, guard_backend) -> None:
    from swarmq.metrics import Metrics

    metrics = Metrics()
    if not metrics.enabled:
        pytest.skip("prometheus_client missing")
    worker = _worker(broker, backend, rec, guard_backend.execution_guard, CountedTask)
    worker._metrics = metrics
    await guard_backend.execution_guard.mark_done("t-1", owner="x", state_ttl_ms=600_000)

    await worker._handle_message(make_message(_payload("guard-counted")))

    sample = "swarmq_execution_guard_events_total"
    assert metrics.registry.get_sample_value(sample, {"event": "duplicate_skipped"}) == 1.0
