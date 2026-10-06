"""KTD4: SlotUnavailable → Redelivery ohne Retry-Zaehler (Plan 2026-09-03-001)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.config import SwarmQConfig
from swarmq.task import TaskInfo
from swarmq.worker.compute.pool import SlotUnavailable
from swarmq.worker.gates import GateContext, GateDeps
from swarmq.worker.outcome import Redelivered
from swarmq.worker.retry import RetryHandler


def _handler() -> tuple[RetryHandler, GateDeps]:
    broker = MagicMock()
    broker.redeliver = AsyncMock()
    broker.publish = AsyncMock()
    deps = GateDeps(
        broker=broker, backend=MagicMock(),
        config=SwarmQConfig(broker_url="amqp://x", backend_url="redis://x"),
        signals=MagicMock(dispatch=AsyncMock()), registry=MagicMock(),
        rate_limiter=lambda: None, lock_manager=lambda: None, finalize_schedfire=AsyncMock(),
        maybe_release_unique=AsyncMock(), extract_valid_workflow_id=lambda _m: None,
        claim_schedfire=lambda _m, _t: None, register_lock=lambda _h, _e: None,
    )
    return RetryHandler(deps, workflow_advancer=lambda: None), deps


def _ctx() -> GateContext:
    return GateContext(task_id="t1", task_name="t", queue="q", task_info=TaskInfo(name="t", queues=["q"]))


@pytest.mark.timeout(5)
async def test_slot_unavailable_redelivers_without_counting_a_retry():
    handler, deps = _handler()
    delivery = MagicMock()
    outcome = await handler.evaluate(
        delivery, _ctx(), SlotUnavailable("no ready"), "no ready", {"retry_count": 0}, MagicMock()
    )
    assert outcome is Redelivered
    deps.broker.redeliver.assert_awaited_once_with(delivery)
    deps.broker.publish.assert_not_awaited()  # keine Retry-Kopie, kein Zaehler
    deps.signals.dispatch.assert_not_awaited()


@pytest.mark.timeout(5)
async def test_slot_unavailable_logs_warning_with_error_fields(swarmq_logs):
    """Plan 2026-10-06-0831, U9 (R17)."""
    handler, _ = _handler()
    await handler.evaluate(
        MagicMock(), _ctx(), SlotUnavailable("no ready"), "no ready", {"retry_count": 0}, MagicMock()
    )
    [entry] = [e for e in swarmq_logs if e["event"] == "compute_slot_unavailable_redeliver"]
    assert entry["log_level"] == "warning" and entry["task_id"] == "t1"
    assert entry["error_type"] == "SlotUnavailable" and entry["error"] == "no ready"
