"""KTD4: SlotUnavailable → Redelivery ohne Retry-Zaehler (Plan 2026-09-03-001)."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.config import SwarmQConfig
from swarmq.exceptions import BrokerConnectionError
from swarmq.task import TaskInfo
from swarmq.worker.compute.pool import SlotUnavailable
from swarmq.worker.gates import GateContext, GateDeps
from swarmq.worker.outcome import Redelivered
from swarmq.worker.retry import RetryHandler
from tests.unit.worker._helpers import partial_message


def _handler() -> tuple[RetryHandler, Any]:
    # ``Any``: die Tests pruefen die Mock-Attribute der ``GateDeps``.
    broker = MagicMock()
    broker.redeliver = AsyncMock()
    broker.publish = AsyncMock()
    deps = GateDeps(
        broker=broker,
        backend=MagicMock(),
        config=SwarmQConfig(broker_url="amqp://x", backend_url="redis://x"),
        signals=MagicMock(dispatch=AsyncMock()),
        registry=MagicMock(),
        rate_limiter=lambda: None,
        lock_manager=lambda: None,
        finalize_schedfire=AsyncMock(),
        maybe_release_unique=AsyncMock(),
        extract_valid_workflow_id=lambda _m: None,
        register_lock=lambda _h, _e: None,
    )
    return RetryHandler(deps, workflow_advancer=lambda: None, finish=AsyncMock()), deps


def _ctx() -> GateContext:
    return GateContext(task_id="t1", task_name="t", queue="q", task_info=TaskInfo(name="t", queues=["q"]))


@pytest.mark.timeout(5)
async def test_slot_unavailable_redelivers_without_counting_a_retry():
    handler, deps = _handler()
    delivery = MagicMock()
    outcome = await handler.evaluate(
        delivery, _ctx(), SlotUnavailable("no ready"), "no ready", partial_message(retry_count=0), MagicMock()
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
        MagicMock(), _ctx(), SlotUnavailable("no ready"), "no ready", partial_message(retry_count=0), MagicMock()
    )
    [entry] = [e for e in swarmq_logs if e["event"] == "compute_slot_unavailable_redeliver"]
    assert entry["log_level"] == "warning"
    assert entry["task_id"] == "t1"
    assert entry["error_type"] == "SlotUnavailable"
    assert entry["error"] == "no ready"


@pytest.mark.befund("F4-18")
@pytest.mark.timeout(5)
async def test_slot_unavailable_with_failed_nack_stays_redelivered():
    """F4-18 (0.13.0rc2): scheitert das ``nack`` am toten Kanal, bleibt es bei
    ``Redelivered`` -- der Broker stellt die Zustellung ohnehin neu zu, das
    Unique-Flag bleibt."""
    handler, deps = _handler()
    deps.broker.redeliver = AsyncMock(side_effect=BrokerConnectionError("redeliver failed, channel lost"))

    outcome = await handler.evaluate(
        MagicMock(), _ctx(), SlotUnavailable("no ready"), "no ready", partial_message(retry_count=0), MagicMock()
    )

    assert outcome is Redelivered
