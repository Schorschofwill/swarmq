"""Schedfire-Claim vor dem Requeue an derselben Stelle freigeben (0.9.0).

Ein ``nack(requeue=True)`` gibt dem Broker das Original zurueck -- ohne
``x-swarmq-schedfire-claim``. Gab der Worker den Claim erst danach (im
finally) frei, konnte die Redelivery den noch stehenden Claim sehen und sich
als Duplikat bestaetigen: der Fire ging verloren. Jetzt wird der Claim
(owner-geprueft, begrenzt) VOR dem nack freigegeben.

Test-Liste (jeder Requeue-Pfad nach gewonnenem Claim):
1. Shutdown-Requeue (Pipeline 3b)
2. Rate-Limit: anderer Fehler als Backend-Ausfall beim Check
3. Rate-Limit: Publish der Rueckstellung scheitert
4. Lock: anderer Fehler als Backend-Ausfall beim Acquire
5. Lock: Publish der Konflikt-Rueckstellung scheitert
6. Zurueckstellung bei Backend-Ausfall: Publish der Kopie scheitert
7. Retry: Publish der Retry-Kopie scheitert
8. Ergebnis-Tail: Speichern des Ergebnisses scheitert (AT_LEAST_ONCE)
9. Ergebnis-Tail: Workflow-Advance scheitert am Backend / Compare-and-Set
10. Ergebnis-Tail: Workflow-Advance scheitert am Broker
11. Ergebnis-Tail: Fehlermarkierung des Workflows scheitert am Backend
12. Retry: Rechenslot nicht verfuegbar (Redelivery ohne Retry-Zaehler)
13. Dead-Letter: Fehlermarkierung des Workflows scheitert am Backend
14. Dead-Letter: Publish ans Dead-Letter-Ziel scheitert
15. Expiry: Publish ans Dead-Letter-Ziel scheitert
16. Der Claim wird nur einmal freigegeben (der finally gibt nicht erneut frei)
"""

from __future__ import annotations

from typing import Any, override
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.contracts.coordination import RateLimitDecision
from swarmq.exceptions import BackendConnectionError, BrokerConnectionError
from swarmq.headers import HEADER_WORKFLOW_ID, HEADER_WORKFLOW_STEP
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker
from swarmq.worker.execution import ExecutionResult
from swarmq.worker.gates import GateContext, SchedfireClaim
from tests.support.messages import make_message
from tests.unit.worker._helpers import _default_payload

pytestmark = pytest.mark.timeout(5)

FIRE = "cron_a:1700000000000"
WF_ID = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b"


class LimitedTask(Task):
    task_info = TaskInfo(name="sfrq-limited", queues=["default"], rate_limit="10/m")

    @override
    async def run(self) -> None:
        raise AssertionError("must not run")


class LockedTask(Task):
    task_info = TaskInfo(name="sfrq-locked", queues=["default"], lock="the-lock", hard_timeout=30)

    @override
    async def run(self) -> None:
        raise AssertionError("must not run")


class FailingTask(Task):
    task_info = TaskInfo(name="sfrq-failing", queues=["default"], max_retries=3)

    @override
    async def run(self) -> None:
        raise RuntimeError("task boom")


def _record(broker: MagicMock, backend: MagicMock) -> list[str]:
    calls: list[str] = []
    backend.schedules.release_fire_claim = AsyncMock(side_effect=lambda *a, **k: calls.append("release"))
    broker.redeliver = AsyncMock(side_effect=lambda *a, **k: calls.append("redeliver"))
    return calls


def _message(name: str = "add") -> Any:
    payload = _default_payload() if name == "add" else _default_payload(name=name, args=[], kwargs={})
    return make_message(payload, headers={"x-swarmq-schedfire": FIRE})


async def test_shutdown_requeue_releases_the_claim_first(worker: Worker, broker, backend) -> None:
    """Liste 1."""
    calls = _record(broker, backend)
    worker._shutdown_event.set()

    await worker._handle_message(_message())

    assert calls == ["release", "redeliver"]


def _limited(worker: Worker, **check: Any) -> None:
    worker._registry.register(LimitedTask)
    worker._rate_limiter = MagicMock()
    worker._rate_limiter.check = AsyncMock(**check)


async def test_rate_limit_check_error_releases_the_claim_first(worker: Worker, broker, backend) -> None:
    """Liste 2."""
    calls = _record(broker, backend)
    _limited(worker, side_effect=RuntimeError("script error"))

    await worker._handle_message(_message("sfrq-limited"))

    assert calls == ["release", "redeliver"]


async def test_rate_limit_publish_failure_releases_the_claim_first(worker: Worker, broker, backend) -> None:
    """Liste 3."""
    calls = _record(broker, backend)
    _limited(worker, return_value=RateLimitDecision(allowed=False, remaining=0, retry_after_ms=100))
    broker.retry_later.side_effect = RuntimeError("broker down")

    await worker._handle_message(_message("sfrq-limited"))

    assert calls == ["release", "redeliver"]


def _locked(worker: Worker, **acquire: Any) -> None:
    worker._registry.register(LockedTask)
    worker._lock_manager = MagicMock()
    worker._lock_manager.acquire = AsyncMock(**acquire)
    worker._lock_manager.release = AsyncMock(return_value=True)


async def test_lock_acquire_error_releases_the_claim_first(worker: Worker, broker, backend) -> None:
    """Liste 4."""
    calls = _record(broker, backend)
    _locked(worker, side_effect=RuntimeError("WRONGTYPE"))

    await worker._handle_message(_message("sfrq-locked"))

    assert calls == ["release", "redeliver"]


async def test_lock_conflict_publish_failure_releases_the_claim_first(worker: Worker, broker, backend) -> None:
    """Liste 5."""
    calls = _record(broker, backend)
    _locked(worker, return_value=None)
    broker.retry_later.side_effect = RuntimeError("broker down")

    await worker._handle_message(_message("sfrq-locked"))

    assert calls == ["release", "redeliver"]


async def test_deferral_publish_failure_releases_the_claim_first(worker: Worker, broker, backend) -> None:
    """Liste 6: Cancel-Check scheitert am Backend, die Kopie am Broker."""
    calls = _record(broker, backend)
    backend.is_cancelled.side_effect = BackendConnectionError("valkey down")
    broker.retry_later.side_effect = RuntimeError("broker down")

    await worker._handle_message(_message())

    assert calls == ["release", "redeliver"]


async def test_retry_publish_failure_releases_the_claim_first(worker: Worker, broker, backend) -> None:
    """Liste 7."""
    calls = _record(broker, backend)
    worker._registry.register(FailingTask)
    broker.retry_later.side_effect = RuntimeError("broker down")

    await worker._handle_message(_message("sfrq-failing"))

    assert calls == ["release", "redeliver"]


async def test_result_store_failure_releases_the_claim_first(worker: Worker, broker, backend) -> None:
    """Liste 8: der Task lief, das Ergebnis liess sich nicht speichern."""
    calls = _record(broker, backend)
    backend.store_result.side_effect = BackendConnectionError("valkey down")

    await worker._handle_message(_message())

    assert calls == ["release", "redeliver"]


class StepTask(Task):
    task_info = TaskInfo(name="sfrq-step", queues=["default"])

    @override
    async def run(self) -> str:
        return "ok"


def _claimed_ctx(task: type[Task] = StepTask) -> GateContext:
    ctx = GateContext(
        task_id="abc-123",
        task_name=task.task_info.name,
        queue="default",
        task_info=task.task_info,
        task_class=task,
    )
    ctx.schedfire_claim = SchedfireClaim(key=FIRE, owner="tok-1")
    return ctx


def _step_message() -> Any:
    return make_message(
        _default_payload(name="sfrq-step", args=[], kwargs={}),
        headers={"x-swarmq-schedfire": FIRE, HEADER_WORKFLOW_ID: WF_ID, HEADER_WORKFLOW_STEP: "/1"},
    )


def _ok(**overrides: Any) -> ExecutionResult:
    base: dict[str, Any] = {
        "result_value": "ok",
        "is_error": False,
        "is_cancelled": False,
        "error_message": "",
        "task_exception": None,
    }
    base.update(overrides)
    return ExecutionResult(**base)


def _advancer(worker: Worker, **behaviour: Any) -> MagicMock:
    worker._registry.register(StepTask)
    advancer = worker._workflow_advancer = MagicMock()
    advancer.advance = AsyncMock(**behaviour.get("advance", {}))
    advancer.fail = AsyncMock(**behaviour.get("fail", {}))
    return advancer


async def test_workflow_advance_backend_error_releases_the_claim_first(worker: Worker, broker, backend) -> None:
    """Liste 9."""
    calls = _record(broker, backend)
    _advancer(worker, advance={"side_effect": BackendConnectionError("down")})

    await worker._result_tail.finish(_step_message(), _claimed_ctx(), _ok())

    assert calls == ["release", "redeliver"]


async def test_workflow_advance_broker_error_releases_the_claim_first(worker: Worker, broker, backend) -> None:
    """Liste 10."""
    calls = _record(broker, backend)
    _advancer(worker, advance={"side_effect": BrokerConnectionError("publish nacked")})

    await worker._result_tail.finish(_step_message(), _claimed_ctx(), _ok())

    assert calls == ["release", "redeliver"]


async def test_workflow_fail_mark_backend_error_releases_the_claim_first(worker: Worker, broker, backend) -> None:
    """Liste 11: Cancel-Schritt, die Fehlermarkierung scheitert am Backend."""
    calls = _record(broker, backend)
    _advancer(worker, fail={"side_effect": BackendConnectionError("down")})

    await worker._result_tail.finish(_step_message(), _claimed_ctx(), _ok(is_cancelled=True, result_value=None))

    assert calls == ["release", "redeliver"]


async def test_slot_unavailable_releases_the_claim_first(worker: Worker, broker, backend) -> None:
    """Liste 12."""
    from swarmq.middleware import MiddlewareChain
    from swarmq.worker.compute.pool import SlotUnavailable

    calls = _record(broker, backend)
    worker._registry.register(StepTask)

    await worker._retry_handler.evaluate(
        _step_message(),
        _claimed_ctx(),
        SlotUnavailable("no ready"),
        "no ready",
        {"retry_count": 0},
        MiddlewareChain([]),
    )

    assert calls == ["release", "redeliver"]


async def test_dead_letter_fail_mark_error_releases_the_claim_first(worker: Worker, broker, backend) -> None:
    """Liste 13."""
    calls = _record(broker, backend)
    _advancer(worker, fail={"side_effect": BackendConnectionError("down")})

    await worker._retry_handler.dead_letter(_step_message(), _claimed_ctx(), "boom", "RuntimeError")

    assert calls == ["release", "redeliver"]


async def test_dead_letter_publish_failure_releases_the_claim_first(worker: Worker, broker, backend) -> None:
    """Liste 14."""
    calls = _record(broker, backend)
    worker._registry.register(StepTask)
    broker.dead_letter.side_effect = RuntimeError("broker down")
    msg = make_message(_default_payload(name="sfrq-step", args=[], kwargs={}), headers={"x-swarmq-schedfire": FIRE})

    await worker._retry_handler.dead_letter(msg, _claimed_ctx(), "boom", "RuntimeError")

    assert calls == ["release", "redeliver"]


class ExpiringTask(Task):
    task_info = TaskInfo(name="sfrq-expiring", queues=["default"], max_age=1)

    @override
    async def run(self) -> None:
        raise AssertionError("must not run")


async def test_expiry_dead_letter_failure_releases_the_claim_first(worker: Worker, broker, backend) -> None:
    """Liste 15."""
    calls = _record(broker, backend)
    worker._registry.register(ExpiringTask)
    broker.dead_letter.side_effect = BrokerConnectionError("broker down")
    msg = make_message(
        _default_payload(name="sfrq-expiring", args=[], kwargs={}),
        headers={"x-swarmq-schedfire": FIRE},
        enqueued_at=1_000,
    )

    await worker._handle_message(msg)

    assert calls == ["release", "redeliver"]


async def test_claim_released_before_requeue_is_not_released_again(worker: Worker, broker, backend) -> None:
    """Liste 16."""
    worker._shutdown_event.set()

    await worker._handle_message(_message())

    backend.schedules.release_fire_claim.assert_awaited_once()
