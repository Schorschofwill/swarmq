"""Ergebnis-Tail (Plan U20): direkte ``ResultTail``-Tests — ``result_ttl``-
Kaskade, Backend-Fehler, Workflow-Advance-Seam."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

from swarmq.exceptions import BackendConnectionError, BrokerConnectionError
from swarmq.headers import HEADER_WORKFLOW_ID, HEADER_WORKFLOW_STEP
from swarmq.registry import TaskRegistry
from swarmq.task import DeliveryMode, Task, TaskInfo
from swarmq.worker import Worker
from swarmq.worker.execution import ExecutionResult
from swarmq.worker.gates import GateContext
from swarmq.worker.outcome import Redelivered, Terminal
from tests.support.messages import make_message
from tests.unit.worker._helpers import _default_payload

_WF_ID = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b"


class TailTask(Task):
    task_info = TaskInfo(name="tail-task", queues=["default"])

    async def run(self) -> str:
        return "ok"


class TailFafTask(Task):
    task_info = TaskInfo(name="tail-faf", queues=["default"], delivery=DeliveryMode.FIRE_AND_FORGET)

    async def run(self) -> None:
        return None


def _tail(broker, backend, config, task=TailTask):
    registry = TaskRegistry()
    registry.register(task)
    worker = Worker(broker=broker, backend=backend, registry=registry, config=config)
    worker._workflow_advancer = MagicMock()
    worker._workflow_advancer.advance = AsyncMock()
    worker._workflow_advancer.fail = AsyncMock()
    return worker._result_tail, worker


def _execution(**overrides) -> ExecutionResult:
    base = dict(result_value="ok", is_error=False, is_cancelled=False, error_message="", task_exception=None)
    base.update(overrides)
    return ExecutionResult(**base)


def _ctx(task=TailTask) -> GateContext:
    return GateContext(
        task_id="abc-123", task_name=task.task_info.name, queue="default", task_info=task.task_info, task_class=task
    )


async def test_happy_path_stores_notifies_and_acks(broker, backend, config):
    tail, _ = _tail(broker, backend, config)
    msg = make_message(_default_payload(name="tail-task"))
    outcome = await tail.finish(msg, _ctx(), _execution())
    assert outcome == Terminal.release_in_finally()
    backend.store_result.assert_awaited_once()
    backend.publish_notification.assert_awaited_once()
    broker.ack.assert_awaited_once_with(msg)


async def test_result_ttl_zero_stores_nothing_and_fails_faf_workflow_step(broker, backend, config):
    tail, worker = _tail(broker, backend, config, TailFafTask)
    msg = make_message(
        _default_payload(name="tail-faf"), headers={HEADER_WORKFLOW_ID: _WF_ID, HEADER_WORKFLOW_STEP: "/1"}
    )
    outcome = await tail.finish(msg, _ctx(TailFafTask), _execution(result_value=None))
    assert outcome == Terminal.release_in_finally()
    backend.store_result.assert_not_awaited()
    worker._workflow_advancer.fail.assert_awaited_once()
    assert "FIRE_AND_FORGET" in worker._workflow_advancer.fail.await_args.args[2]


async def test_backend_store_error_is_redelivered(broker, backend, config):
    backend.store_result = AsyncMock(side_effect=BackendConnectionError("down"))
    tail, _ = _tail(broker, backend, config)
    msg = make_message(_default_payload(name="tail-task"))
    outcome = await tail.finish(msg, _ctx(), _execution())
    assert outcome is Redelivered
    broker.redeliver.assert_awaited_once_with(msg)
    broker.ack.assert_not_awaited()


async def test_workflow_advance_broker_error_is_redelivered_without_fail_mark(broker, backend, config):
    tail, worker = _tail(broker, backend, config)
    worker._workflow_advancer.advance = AsyncMock(side_effect=BrokerConnectionError("publish nacked"))
    msg = make_message(
        _default_payload(name="tail-task"), headers={HEADER_WORKFLOW_ID: _WF_ID, HEADER_WORKFLOW_STEP: "/1"}
    )
    outcome = await tail.finish(msg, _ctx(), _execution())
    assert outcome is Redelivered
    worker._workflow_advancer.fail.assert_not_awaited()
    broker.redeliver.assert_awaited_once_with(msg)


async def test_workflow_advance_unexpected_error_fail_marks_and_acks(broker, backend, config):
    tail, worker = _tail(broker, backend, config)
    worker._workflow_advancer.advance = AsyncMock(side_effect=RuntimeError("advance bug"))
    msg = make_message(
        _default_payload(name="tail-task"), headers={HEADER_WORKFLOW_ID: _WF_ID, HEADER_WORKFLOW_STEP: "/1"}
    )
    outcome = await tail.finish(msg, _ctx(), _execution())
    assert outcome == Terminal.release_in_finally()
    worker._workflow_advancer.fail.assert_awaited_once()
    broker.ack.assert_awaited_once_with(msg)
