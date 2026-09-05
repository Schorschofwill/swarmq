"""DeliveryCountGate (Plan 2026-09-03-001, KTD10): Giftnachricht in den Anwendungs-DLQ."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.config import SwarmQConfig
from swarmq.task import TaskInfo
from swarmq.worker.gates import GateContext
from swarmq.worker.gates.delivery_count import DeliveryCountGate
from swarmq.worker.outcome import Continue, Terminal


def _gate(limit: int = 20):
    deps = MagicMock()
    deps.config = SwarmQConfig(broker_url="amqp://x", backend_url="redis://x", delivery_limit=limit)
    dead_letter = AsyncMock()
    return DeliveryCountGate(deps, dead_letter=dead_letter), dead_letter


def _ctx() -> GateContext:
    return GateContext(task_id="t1", task_name="t", queue="q", task_info=TaskInfo(name="t", queues=["q"]))


@pytest.mark.timeout(5)
@pytest.mark.parametrize("headers", [None, {}, {"x-delivery-count": "abc"}, {"x-delivery-count": 3}])
async def test_below_limit_or_unparseable_continues(headers):
    gate, dead_letter = _gate(limit=20)
    assert await gate.apply(SimpleNamespace(headers=headers), _ctx()) is Continue
    dead_letter.assert_not_awaited()


@pytest.mark.timeout(5)
@pytest.mark.parametrize("count", [19, 20, 25])
async def test_at_penultimate_delivery_goes_to_application_dead_letter(count):
    gate, dead_letter = _gate(limit=20)
    delivery = SimpleNamespace(headers={"x-delivery-count": count})
    outcome = await gate.apply(delivery, _ctx())
    assert isinstance(outcome, Terminal) and outcome.release_unique is False
    dead_letter.assert_awaited_once()
    args = dead_letter.await_args.args
    assert args[0] is delivery and args[1].task_id == "t1"
    assert "WorkerHungError" in args[2] and f"delivered {count} times" in args[2]


@pytest.mark.timeout(5)
async def test_limit_two_dead_letters_on_first_redelivery():
    gate, _dead_letter = _gate(limit=2)
    assert await gate.apply(SimpleNamespace(headers={"x-delivery-count": 0}), _ctx()) is Continue
    assert isinstance(await gate.apply(SimpleNamespace(headers={"x-delivery-count": 1}), _ctx()), Terminal)
