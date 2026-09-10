"""Dead-Letter-Pfad des ``RetryHandler`` (Plan 2026-09-08-001, U10).

Test-Liste U10 (R25/KTD12) — Fail-Mark-Regel im Dead-Letter:

1. Fail-Mark wirft ``BackendConnectionError`` → Redelivery statt Dead-Letter,
   kein ``TASK_DLQ``, keine Unique-Freigabe (AE11).
2. Fail-Mark wirft ``WorkflowStateConflict`` → dieselbe Behandlung.
3. Fail-Mark wirft ``ValueError`` → Dead-Letter wie bisher, Log mit Kennung.
4. FIRE_AND_FORGET-Schritt → kein Broker-Verb aus dem Fail-Mark-Zweig.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

from swarmq.exceptions import BackendConnectionError, WorkflowStateConflict
from swarmq.headers import HEADER_WORKFLOW_ID
from swarmq.registry import TaskRegistry
from swarmq.signals import Signal
from swarmq.task import DeliveryMode, Task, TaskInfo
from swarmq.worker import Worker
from swarmq.worker.gates import GateContext
from tests.support.messages import make_message
from tests.unit.worker._helpers import _default_payload

_WF_ID = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b"


class DlqTask(Task):
    task_info = TaskInfo(name="dlq-task", queues=["default"])

    async def run(self) -> str:
        return "ok"


class DlqFafTask(Task):
    task_info = TaskInfo(
        name="dlq-faf", queues=["default"], delivery=DeliveryMode.FIRE_AND_FORGET
    )

    async def run(self) -> None:
        return None


def _handler(broker, backend, config, task=DlqTask):
    registry = TaskRegistry()
    registry.register(task)
    worker = Worker(broker=broker, backend=backend, registry=registry, config=config)
    worker._workflow_advancer = MagicMock()
    worker._workflow_advancer.fail = AsyncMock()
    # Der Dispatcher steckt bereits in ``GateDeps`` — die Methode am Objekt
    # ersetzen, nicht das Attribut am Worker.
    worker._signal_dispatcher.dispatch = AsyncMock()
    return worker._retry_handler, worker


def _ctx(task=DlqTask) -> GateContext:
    return GateContext(
        task_id="abc-123", task_name=task.task_info.name, queue="default",
        task_info=task.task_info, task_class=task,
    )


async def test_fail_mark_backend_error_redelivers_instead_of_dead_lettering(broker, backend, config):
    """U10 Fall 1 (AE11): ``dead_letter`` bestätigt das Original — solange der
    Workflow nicht als gescheitert markiert ist, darf das nicht passieren,
    sonst wartet ``handle.get()`` bis zum TTL auf einen Zustand, der nie
    terminal wird."""
    handler, worker = _handler(broker, backend, config)
    worker._workflow_advancer.fail = AsyncMock(side_effect=BackendConnectionError("down"))
    msg = make_message(_default_payload(name="dlq-task"), headers={HEADER_WORKFLOW_ID: _WF_ID})
    await handler.dead_letter(msg, _ctx(), "boom")
    broker.redeliver.assert_awaited_once_with(msg)
    broker.dead_letter.assert_not_awaited()
    for call in worker._signal_dispatcher.dispatch.await_args_list:
        assert call.args[0] is not Signal.TASK_DLQ


async def test_fail_mark_state_conflict_redelivers_instead_of_dead_lettering(broker, backend, config):
    """U10 Fall 2: Die Konfliktgrenze des Compare-and-Set (U9) ist derselbe
    Fall — der Zustand ist nicht terminal geschrieben."""
    handler, worker = _handler(broker, backend, config)
    worker._workflow_advancer.fail = AsyncMock(side_effect=WorkflowStateConflict("25 rounds lost"))
    msg = make_message(_default_payload(name="dlq-task"), headers={HEADER_WORKFLOW_ID: _WF_ID})
    await handler.dead_letter(msg, _ctx(), "boom")
    broker.redeliver.assert_awaited_once_with(msg)
    broker.dead_letter.assert_not_awaited()


async def test_fail_mark_deterministic_error_still_dead_letters(broker, backend, config):
    """U10 Fall 3: Ein deterministischer Fail-Mark-Fehler haelt den
    Dead-Letter nicht auf — sonst drehte die Nachricht bis zum Delivery-Limit,
    ohne dass die Markierung je gelingt."""
    handler, worker = _handler(broker, backend, config)
    worker._workflow_advancer.fail = AsyncMock(side_effect=ValueError("bad state doc"))
    msg = make_message(_default_payload(name="dlq-task"), headers={HEADER_WORKFLOW_ID: _WF_ID})
    await handler.dead_letter(msg, _ctx(), "boom")
    broker.dead_letter.assert_awaited_once()
    broker.redeliver.assert_not_awaited()
    dispatched = [c.args[0] for c in worker._signal_dispatcher.dispatch.await_args_list]
    assert Signal.TASK_DLQ in dispatched


async def test_fail_mark_backend_error_on_faf_step_emits_no_redeliver(broker, backend, config):
    """U10 Fall 4: FIRE_AND_FORGET hat kein Ack zurueckzuhalten — der
    Dead-Letter laeuft weiter, ohne Redelivery."""
    handler, worker = _handler(broker, backend, config, DlqFafTask)
    worker._workflow_advancer.fail = AsyncMock(side_effect=BackendConnectionError("down"))
    msg = make_message(_default_payload(name="dlq-faf"), headers={HEADER_WORKFLOW_ID: _WF_ID})
    await handler.dead_letter(msg, _ctx(DlqFafTask), "boom")
    broker.redeliver.assert_not_awaited()
    broker.dead_letter.assert_awaited_once()
