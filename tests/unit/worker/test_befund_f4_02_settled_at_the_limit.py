"""F4-02 über die ganze Pipeline (Plan 0.13.0, U12, KTD5): Das
Delivery-Count-Gate liest am Zustelllimit den Ausführungsschutz
(``ExecutionGuard.skip_if_settled``), bevor es dead-lettert. Ergänzt die
Nachweise in ``test_befund_f4_02_delivery_limit_before_guard.py``.

Subjekt ist ``Worker._handle_message`` mit dem Ausführungsschutz des
``InMemoryBackend``; Broker und Ergebnis-Backend sind Mocks.

Testliste:
# [x] Done-Marker am Limit: ``duplicate_delivery_skipped``, kein
#     ``delivery_limit_reached_dead_letter``, kein ``TASK_DLQ``, keine Unique-Freigabe
# [x] Superseded-Marker am Limit (Workflow-Mitglied): Ack, der Workflow
#     bleibt unberührt (keine Fehlmarkierung)
# [x] Kein Marker am Limit: Dead-Letter wie bisher
# [x] Backend-Ausfall beim Lesen: Dead-Letter wie bisher plus WARNING
# [x] FIRE_AND_FORGET am Limit: kein Lesen, Dead-Letter wie bisher
# [x] Gehaltener Schedfire-Claim am Limit, Done-Marker: der Claim wird
#     freigegeben (nicht-terminal, wie beim Duplikat im Gate), kein Completed-Marker
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any, override
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.config import SwarmQConfig
from swarmq.contracts.coordination import RateLimitDecision
from swarmq.exceptions import BackendConnectionError
from swarmq.headers import HEADER_WORKFLOW_ID, HEADER_WORKFLOW_STEP
from swarmq.registry import TaskRegistry
from swarmq.signals import Signal
from swarmq.task import DeliveryMode, Task, TaskInfo
from swarmq.worker import Worker
from tests.support.inmemory import FakeClock, InMemoryBackend
from tests.support.messages import make_message
from tests.unit._log_capture import capture_swarmq_logs

pytestmark = pytest.mark.timeout(5)

_DELIVERY_LIMIT = 20
_AT_THE_LIMIT = {"x-delivery-count": _DELIVERY_LIMIT - 1}


class F402Job(Task):
    task_info = TaskInfo(name="f4_02_job", queues=["default"], max_retries=3)

    @override
    async def run(self) -> str:
        return "ok"


class F402Faf(Task):
    task_info = TaskInfo(name="f4_02_faf", queues=["default"], delivery=DeliveryMode.FIRE_AND_FORGET)

    @override
    async def run(self) -> str:
        return "ok"


@pytest.fixture
async def guard_backend() -> AsyncIterator[InMemoryBackend]:
    impl = InMemoryBackend(FakeClock())
    await impl.connect()
    yield impl
    await impl.close()


def _payload(name: str = "f4_02_job") -> dict[str, Any]:
    return {"task": {"id": "t-1", "queue": "default", "name": name}, "args": [], "kwargs": {}}


def _worker(guard_backend: InMemoryBackend, task: type[Task] = F402Job) -> tuple[Worker, MagicMock, MagicMock]:
    broker = MagicMock(spec=RabbitMQBroker)
    for verb in ("ack", "redeliver", "retry_later", "dead_letter", "route_via_dlx"):
        setattr(broker, verb, AsyncMock())
    backend = MagicMock(spec=ValkeyBackend)
    backend.store_result = AsyncMock()
    backend.notify_result = AsyncMock()
    backend.is_cancelled = AsyncMock(return_value=False)
    backend.is_workflow_cancelled = AsyncMock(return_value=False)
    backend.clear_cancel_flag = AsyncMock()
    backend.release_unique = AsyncMock(return_value=True)
    registry = TaskRegistry()
    registry.register(task)
    config = SwarmQConfig(
        broker_url="amqp://x", backend_url="valkey://x", queues=["default"], delivery_limit=_DELIVERY_LIMIT
    )
    worker = Worker(broker=broker, backend=backend, registry=registry, config=config)
    limiter = MagicMock()
    limiter.check = AsyncMock(return_value=RateLimitDecision(allowed=True, remaining=9, retry_after_ms=0))
    worker._rate_limiter = limiter
    worker._lock_manager = MagicMock()
    worker._guard_store = guard_backend.execution_guard
    return worker, broker, backend


async def _settle(guard_backend: InMemoryBackend, marker: str) -> None:
    store = guard_backend.execution_guard
    await store.claim("t-1", owner="o", retry_count=0, ttl_ms=60_000, state_ttl_ms=600_000, max_orphans=3)
    if marker == "done":
        await store.mark_done("t-1", owner="o", state_ttl_ms=600_000)
    else:
        await store.mark_superseded("t-1", owner="o", retry_count=1, state_ttl_ms=600_000)


async def test_done_marker_at_the_limit_skips_without_dlq_side_effects(guard_backend: InMemoryBackend) -> None:
    worker, broker, backend = _worker(guard_backend)
    await _settle(guard_backend, "done")
    signals: list[Signal] = []

    async def on_dlq(**_kw: Any) -> None:
        signals.append(Signal.TASK_DLQ)

    worker._signal_dispatcher.connect(Signal.TASK_DLQ, on_dlq)
    message = make_message(_payload(), headers={**_AT_THE_LIMIT, "x-swarmq-unique-hash": "h"})

    with capture_swarmq_logs() as logs:
        await worker._handle_message(message)

    events = [e["event"] for e in logs]
    assert "duplicate_delivery_skipped" in events
    assert "delivery_limit_reached_dead_letter" not in events
    broker.ack.assert_awaited_once()
    broker.dead_letter.assert_not_awaited()
    backend.store_result.assert_not_awaited()
    backend.release_unique.assert_not_awaited()
    assert signals == []


async def test_superseded_workflow_member_at_the_limit_leaves_the_workflow_alone(
    guard_backend: InMemoryBackend,
) -> None:
    worker, broker, backend = _worker(guard_backend)
    await _settle(guard_backend, "superseded")
    worker._workflow_advancer = MagicMock()
    worker._workflow_advancer.fail = AsyncMock()
    headers = {**_AT_THE_LIMIT, HEADER_WORKFLOW_ID: "01890a5d-ac96-774b-bcce-b302099a8057", HEADER_WORKFLOW_STEP: "/1"}

    await worker._handle_message(make_message(_payload(), headers=headers))

    broker.ack.assert_awaited_once()
    broker.dead_letter.assert_not_awaited()
    worker._workflow_advancer.fail.assert_not_awaited()
    backend.store_result.assert_not_awaited()


async def test_without_a_marker_the_limit_still_dead_letters(guard_backend: InMemoryBackend) -> None:
    worker, broker, _backend = _worker(guard_backend)

    await worker._handle_message(make_message(_payload(), headers=_AT_THE_LIMIT))

    broker.dead_letter.assert_awaited_once()
    assert "WorkerHungError" in str(broker.dead_letter.await_args)


async def test_backend_outage_while_reading_dead_letters_with_a_warning(guard_backend: InMemoryBackend) -> None:
    worker, broker, _backend = _worker(guard_backend)
    await _settle(guard_backend, "done")
    store = MagicMock(wraps=guard_backend.execution_guard)
    store.peek = AsyncMock(side_effect=BackendConnectionError("valkey down"))
    worker._guard_store = store

    with capture_swarmq_logs() as logs:
        await worker._handle_message(make_message(_payload(), headers=_AT_THE_LIMIT))

    broker.dead_letter.assert_awaited_once()
    assert [e["log_level"] for e in logs if e["event"] == "execution_guard_peek_failed"] == ["warning"]


async def test_fire_and_forget_at_the_limit_is_not_read(guard_backend: InMemoryBackend) -> None:
    worker, broker, _backend = _worker(guard_backend, F402Faf)
    await _settle(guard_backend, "done")
    store = MagicMock(wraps=guard_backend.execution_guard)
    store.peek = AsyncMock(wraps=guard_backend.execution_guard.peek)
    worker._guard_store = store

    await worker._handle_message(make_message(_payload("f4_02_faf"), headers=_AT_THE_LIMIT))

    store.peek.assert_not_awaited()
    broker.dead_letter.assert_awaited_once()


async def test_held_schedfire_claim_is_released_when_the_limit_skips_a_duplicate(
    guard_backend: InMemoryBackend,
) -> None:
    worker, broker, backend = _worker(guard_backend)
    await _settle(guard_backend, "done")
    backend.schedules.is_fire_completed = AsyncMock(return_value=False)
    backend.schedules.claim_fire = AsyncMock(return_value=True)
    backend.schedules.release_fire_claim = AsyncMock()
    backend.schedules.mark_fire_completed = AsyncMock()
    message = make_message(_payload(), headers={**_AT_THE_LIMIT, "x-swarmq-schedfire": "cron:f402:1"})

    await worker._handle_message(message)

    broker.ack.assert_awaited_once()
    broker.dead_letter.assert_not_awaited()
    backend.schedules.release_fire_claim.assert_awaited_once()
    assert backend.schedules.release_fire_claim.await_args.args == ("cron:f402:1",)
    backend.schedules.mark_fire_completed.assert_not_awaited()
