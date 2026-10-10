"""Ergebnis-Tail (Plan U20): direkte ``ResultTail``-Tests — ``result_ttl``-
Kaskade, Backend-Fehler, Workflow-Advance-Seam.

Test-Liste U10 (Plan 2026-09-08-001, R25/KTD12) — Fail-Mark-Regel:

1. Fail-Mark wirft ``BackendConnectionError`` nach ``workflow_advance_failed``
   → ``Redelivered``, kein Ack (AE11).
2. Derselbe Fall bei FIRE_AND_FORGET → nur Log, kein Broker-Verb.
3. Fail-Mark wirft ``ValueError`` → Ack wie bisher, Log mit Workflow-Kennung.
4. Fehler-/Cancel-Zweig: dieselbe Regel.
5. FAF-Guard (``result_ttl=0``): dieselbe Regel.
6. ``WorkflowStateConflict`` aus ``advance`` → ``Redelivered`` ohne Fail-Mark.

Test-Liste U9 (Plan 2026-10-06-0831, R17/R18) — Logs ueber ``swarmq_logs``:

7. Advance-Bug, Fail-Mark scheitert deterministisch: ``workflow_advance_failed``
   und ``workflow_fail_mark_error`` mit ``workflow_id``, ``error_type``, ``error``.
8. Cancel-Zweig: ``workflow_fail_mark_failed`` mit ``redelivered``.
9. FAF-Guard: ``workflow_faf_fail_mark_error``.
10. ``result_serialization_failed`` und ``publish_notification_failed`` mit
    ``error_type``/``error``.

Test-Liste B23 (0.9.1, Level-Regel KTD11: selbstheilend → WARNING):

11. [x] ``backend_connection_error`` bei AT_LEAST_ONCE: WARNING mit
    ``error_type``/``error``; im Worker traegt er task_id/task_name aus dem
    gebundenen Kontext.
12. [x] Derselbe Fehler bei FIRE_AND_FORGET: ERROR (Ergebnis verloren).
13. [x] ``workflow_advance_backend_error`` mit Fehlerfeldern, WARNING bei
    AT_LEAST_ONCE (Neuzustellung).
14. [x] ... ERROR bei FIRE_AND_FORGET (kein Advance, keine Neuzustellung).
15. [x] ``workflow_advance_broker_error`` mit Fehlerfeldern, WARNING bei
    AT_LEAST_ONCE.
16. [x] ... ERROR bei FIRE_AND_FORGET.

Test-Liste Plan 0.12.0, MR 6 (BZ-03, Q14):

17. [x] ``UnroutableMessage`` aus ``advance``: Workflow sofort fail-markiert,
    Ack, kein Redeliver (die Queue des naechsten Schritts fehlt).
18. [x] Fail-Mark scheitert transient: Redeliver wie bei jedem Fail-Mark.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

from swarmq.exceptions import (
    BackendConnectionError,
    BrokerConnectionError,
    UnroutableMessage,
    WorkflowStateConflict,
)
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


class TailFafStoreTask(Task):
    """FIRE_AND_FORGET mit explizitem ``result_ttl`` — erreicht den
    Advance-Seam, hat aber nichts zu bestätigen."""

    task_info = TaskInfo(
        name="tail-faf-store", queues=["default"],
        delivery=DeliveryMode.FIRE_AND_FORGET, result_ttl=60,
    )

    async def run(self) -> str:
        return "ok"


class TailNoStoreTask(Task):
    """AT_LEAST_ONCE mit ``result_ttl=0`` — erreicht den ``_finish_without_store``-
    Pfad, ohne FIRE_AND_FORGET zu sein (dort gibt es ein Ack zu unterlassen)."""

    task_info = TaskInfo(name="tail-nostore", queues=["default"], result_ttl=0)

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
    backend.notify_result.assert_awaited_once()
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


async def test_fail_mark_backend_error_after_advance_failure_is_redelivered(broker, backend, config):
    """U10 Fall 1 (AE11): Der Advance ist an einem Bug gescheitert, aber die
    Fehlermarkierung erreicht das Backend nicht — ein Ack liesse den Workflow
    bis zum TTL ``RUNNING`` und ``handle.get()`` in den Timeout laufen."""
    tail, worker = _tail(broker, backend, config)
    worker._workflow_advancer.advance = AsyncMock(side_effect=RuntimeError("advance bug"))
    worker._workflow_advancer.fail = AsyncMock(side_effect=BackendConnectionError("down"))
    msg = make_message(
        _default_payload(name="tail-task"), headers={HEADER_WORKFLOW_ID: _WF_ID, HEADER_WORKFLOW_STEP: "/1"}
    )
    outcome = await tail.finish(msg, _ctx(), _execution())
    assert outcome is Redelivered
    broker.redeliver.assert_awaited_once_with(msg)
    broker.ack.assert_not_awaited()


async def test_workflow_state_conflict_from_advance_is_redelivered(broker, backend, config):
    """U10 Fall 6: Die Konfliktgrenze des Compare-and-Set (U9) bedeutet, dass
    der Advance nicht fertig ist — dieselbe Behandlung wie ein Backend-Ausfall,
    kein Fail-Mark (der Workflow ist nicht gescheitert)."""
    tail, worker = _tail(broker, backend, config)
    worker._workflow_advancer.advance = AsyncMock(side_effect=WorkflowStateConflict("25 rounds lost"))
    msg = make_message(
        _default_payload(name="tail-task"), headers={HEADER_WORKFLOW_ID: _WF_ID, HEADER_WORKFLOW_STEP: "/1"}
    )
    outcome = await tail.finish(msg, _ctx(), _execution())
    assert outcome is Redelivered
    worker._workflow_advancer.fail.assert_not_awaited()
    broker.redeliver.assert_awaited_once_with(msg)
    broker.ack.assert_not_awaited()


async def test_cancel_branch_fail_mark_backend_error_is_redelivered(broker, backend, config):
    """U10 Fall 4: Der Fehler-/Cancel-Zweig markiert synchron fail, damit
    ``handle.get()`` in endlicher Zeit wirft — scheitert das transient, gilt
    dieselbe Regel wie nach einem Advance-Fehler."""
    tail, worker = _tail(broker, backend, config)
    worker._workflow_advancer.fail = AsyncMock(side_effect=BackendConnectionError("down"))
    msg = make_message(
        _default_payload(name="tail-task"), headers={HEADER_WORKFLOW_ID: _WF_ID, HEADER_WORKFLOW_STEP: "/1"}
    )
    outcome = await tail.finish(msg, _ctx(), _execution(is_cancelled=True, result_value=None))
    assert outcome is Redelivered
    worker._workflow_advancer.advance.assert_not_awaited()
    broker.redeliver.assert_awaited_once_with(msg)
    broker.ack.assert_not_awaited()


async def test_faf_guard_fail_mark_backend_error_is_redelivered(broker, backend, config):
    """U10 Fall 5: Der ``result_ttl=0``-Guard markiert den Workflow fail, weil
    dieser Schritt den Advance-Seam nie erreicht — scheitert die Markierung
    transient, wird nicht bestätigt."""
    tail, worker = _tail(broker, backend, config, TailNoStoreTask)
    worker._workflow_advancer.fail = AsyncMock(side_effect=BackendConnectionError("down"))
    msg = make_message(
        _default_payload(name="tail-nostore"), headers={HEADER_WORKFLOW_ID: _WF_ID, HEADER_WORKFLOW_STEP: "/1"}
    )
    outcome = await tail.finish(msg, _ctx(TailNoStoreTask), _execution(result_value=None))
    assert outcome is Redelivered
    backend.store_result.assert_not_awaited()
    broker.redeliver.assert_awaited_once_with(msg)
    broker.ack.assert_not_awaited()


async def test_fail_mark_backend_error_on_faf_step_emits_no_broker_verb(broker, backend, config):
    """U10 Fall 2: FIRE_AND_FORGET hat kein Ack zurückzuhalten — der transiente
    Fail-Mark-Fehler wird geloggt, aber es folgt kein Broker-Verb (nach dem
    Früh-Ack der Pipeline wäre jedes Verb ein Protokollfehler)."""
    tail, worker = _tail(broker, backend, config, TailFafStoreTask)
    worker._workflow_advancer.advance = AsyncMock(side_effect=RuntimeError("advance bug"))
    worker._workflow_advancer.fail = AsyncMock(side_effect=BackendConnectionError("down"))
    msg = make_message(
        _default_payload(name="tail-faf-store"), headers={HEADER_WORKFLOW_ID: _WF_ID, HEADER_WORKFLOW_STEP: "/1"}
    )
    outcome = await tail.finish(msg, _ctx(TailFafStoreTask), _execution())
    assert outcome == Terminal.release_in_finally()
    broker.redeliver.assert_not_awaited()
    broker.ack.assert_not_awaited()


async def test_fail_mark_deterministic_error_acks(broker, backend, config):
    """U10 Fall 3: Ein deterministischer Fail-Mark-Fehler wird bestätigt — eine
    Redelivery führte den Task-Körper erneut aus und drehte bis zum
    Delivery-Limit."""
    tail, worker = _tail(broker, backend, config)
    worker._workflow_advancer.advance = AsyncMock(side_effect=RuntimeError("advance bug"))
    worker._workflow_advancer.fail = AsyncMock(side_effect=ValueError("bad state doc"))
    msg = make_message(
        _default_payload(name="tail-task"), headers={HEADER_WORKFLOW_ID: _WF_ID, HEADER_WORKFLOW_STEP: "/1"}
    )
    outcome = await tail.finish(msg, _ctx(), _execution())
    assert outcome == Terminal.release_in_finally()
    broker.ack.assert_awaited_once_with(msg)
    broker.redeliver.assert_not_awaited()


def _events(logs, event):
    return [e for e in logs if e["logger"] == "swarmq.worker.results" and e["event"] == event]


async def test_advance_bug_and_failed_fail_mark_log_fixed_events_with_error_fields(
    broker, backend, config, swarmq_logs
):
    tail, worker = _tail(broker, backend, config)
    worker._workflow_advancer.advance = AsyncMock(side_effect=RuntimeError("advance bug"))
    worker._workflow_advancer.fail = AsyncMock(side_effect=ValueError("bad state doc"))
    msg = make_message(
        _default_payload(name="tail-task"), headers={HEADER_WORKFLOW_ID: _WF_ID, HEADER_WORKFLOW_STEP: "/1"}
    )
    await tail.finish(msg, _ctx(), _execution())

    [advance] = _events(swarmq_logs, "workflow_advance_failed")
    assert advance["log_level"] == "error" and advance["workflow_id"] == _WF_ID
    assert advance["error_type"] == "RuntimeError" and advance["error"] == "advance bug"
    [mark] = _events(swarmq_logs, "workflow_fail_mark_error")
    assert mark["log_level"] == "error" and mark["workflow_id"] == _WF_ID
    assert mark["error_type"] == "ValueError" and mark["error"] == "bad state doc"
    assert mark["redelivered"] is False


async def test_cancel_branch_fail_mark_error_logs_workflow_fail_mark_failed(broker, backend, config, swarmq_logs):
    tail, worker = _tail(broker, backend, config)
    worker._workflow_advancer.fail = AsyncMock(side_effect=BackendConnectionError("down"))
    msg = make_message(
        _default_payload(name="tail-task"), headers={HEADER_WORKFLOW_ID: _WF_ID, HEADER_WORKFLOW_STEP: "/1"}
    )
    await tail.finish(msg, _ctx(), _execution(is_cancelled=True, result_value=None))

    [entry] = _events(swarmq_logs, "workflow_fail_mark_failed")
    assert entry["log_level"] == "error" and entry["redelivered"] is True
    assert entry["error_type"] == "BackendConnectionError" and entry["error"] == "down"


async def test_faf_guard_fail_mark_error_logs_workflow_faf_fail_mark_error(broker, backend, config, swarmq_logs):
    tail, worker = _tail(broker, backend, config, TailNoStoreTask)
    worker._workflow_advancer.fail = AsyncMock(side_effect=ValueError("bad state doc"))
    msg = make_message(
        _default_payload(name="tail-nostore"), headers={HEADER_WORKFLOW_ID: _WF_ID, HEADER_WORKFLOW_STEP: "/1"}
    )
    await tail.finish(msg, _ctx(TailNoStoreTask), _execution(result_value=None))

    [entry] = _events(swarmq_logs, "workflow_faf_fail_mark_error")
    assert entry["log_level"] == "error" and entry["redelivered"] is False
    assert entry["workflow_id"] == _WF_ID


async def test_notify_failure_logs_warning_with_error_fields(broker, backend, config, swarmq_logs):
    backend.notify_result = AsyncMock(side_effect=ConnectionError("pubsub down"))
    tail, _ = _tail(broker, backend, config)
    await tail.finish(make_message(_default_payload(name="tail-task")), _ctx(), _execution())

    [entry] = _events(swarmq_logs, "publish_notification_failed")
    assert entry["log_level"] == "warning" and entry["task_id"] == "abc-123"
    assert entry["error_type"] == "ConnectionError" and entry["error"] == "pubsub down"


async def test_unserializable_result_logs_error_with_error_fields(broker, backend, config, swarmq_logs):
    tail, _ = _tail(broker, backend, config)
    await tail.finish(make_message(_default_payload(name="tail-task")), _ctx(), _execution(result_value=object()))

    [entry] = _events(swarmq_logs, "result_serialization_failed")
    assert entry["log_level"] == "error"
    assert entry["error_type"] == "ResultSerializationError" and entry["error"]


async def test_backend_store_error_at_least_once_logs_warning_with_error_fields_and_task_context(
    broker, backend, config, swarmq_logs
):
    """B23: im Worker, damit der gebundene Task-Kontext mitzaehlt."""
    backend.store_result = AsyncMock(side_effect=BackendConnectionError("valkey down"))
    _, worker = _tail(broker, backend, config)

    await worker._handle_message(make_message(_default_payload(task_id="t-store", name="tail-task", args=[])))

    [entry] = _events(swarmq_logs, "backend_connection_error")
    assert entry["log_level"] == "warning"
    assert entry["error_type"] == "BackendConnectionError" and entry["error"] == "valkey down"
    assert entry["task_id"] == "t-store" and entry["task_name"] == "tail-task"


async def test_backend_store_error_fire_and_forget_logs_error_because_the_result_is_lost(
    broker, backend, config, swarmq_logs
):
    backend.store_result = AsyncMock(side_effect=BackendConnectionError("valkey down"))
    tail, _ = _tail(broker, backend, config, TailFafStoreTask)

    await tail.finish(make_message(_default_payload(name="tail-faf-store")), _ctx(TailFafStoreTask), _execution())

    [entry] = _events(swarmq_logs, "backend_connection_error")
    assert entry["log_level"] == "error"
    assert entry["error_type"] == "BackendConnectionError" and entry["error"] == "valkey down"


def _wf_message(name: str = "tail-task"):
    return make_message(_default_payload(name=name), headers={HEADER_WORKFLOW_ID: _WF_ID, HEADER_WORKFLOW_STEP: "/1"})


async def test_advance_backend_error_at_least_once_logs_warning_with_error_fields(
    broker, backend, config, swarmq_logs
):
    tail, worker = _tail(broker, backend, config)
    worker._workflow_advancer.advance = AsyncMock(side_effect=WorkflowStateConflict("25 rounds lost"))

    await tail.finish(_wf_message(), _ctx(), _execution())

    [entry] = _events(swarmq_logs, "workflow_advance_backend_error")
    assert entry["log_level"] == "warning" and entry["workflow_id"] == _WF_ID
    assert entry["error_type"] == "WorkflowStateConflict" and entry["error"] == "25 rounds lost"


async def test_advance_backend_error_fire_and_forget_logs_error(broker, backend, config, swarmq_logs):
    """Ohne Neuzustellung bleibt der Workflow haengen -- nicht selbstheilend."""
    tail, worker = _tail(broker, backend, config, TailFafStoreTask)
    worker._workflow_advancer.advance = AsyncMock(side_effect=BackendConnectionError("valkey down"))

    await tail.finish(_wf_message("tail-faf-store"), _ctx(TailFafStoreTask), _execution())

    [entry] = _events(swarmq_logs, "workflow_advance_backend_error")
    assert entry["log_level"] == "error"
    assert entry["error_type"] == "BackendConnectionError" and entry["error"] == "valkey down"


async def test_advance_broker_error_at_least_once_logs_warning_with_error_fields(
    broker, backend, config, swarmq_logs
):
    tail, worker = _tail(broker, backend, config)
    worker._workflow_advancer.advance = AsyncMock(side_effect=BrokerConnectionError("publish nacked"))

    await tail.finish(_wf_message(), _ctx(), _execution())

    [entry] = _events(swarmq_logs, "workflow_advance_broker_error")
    assert entry["log_level"] == "warning" and entry["workflow_id"] == _WF_ID
    assert entry["error_type"] == "BrokerConnectionError" and entry["error"] == "publish nacked"


async def test_advance_broker_error_fire_and_forget_logs_error(broker, backend, config, swarmq_logs):
    tail, worker = _tail(broker, backend, config, TailFafStoreTask)
    worker._workflow_advancer.advance = AsyncMock(side_effect=BrokerConnectionError("publish nacked"))

    await tail.finish(_wf_message("tail-faf-store"), _ctx(TailFafStoreTask), _execution())

    [entry] = _events(swarmq_logs, "workflow_advance_broker_error")
    assert entry["log_level"] == "error"
    assert entry["error_type"] == "BrokerConnectionError" and entry["error"] == "publish nacked"


async def test_unroutable_advance_fail_marks_workflow_and_acks_without_redeliver(broker, backend, config):
    tail, worker = _tail(broker, backend, config)
    worker._workflow_advancer.advance = AsyncMock(side_effect=UnroutableMessage("no queue 'next-step'"))
    msg = make_message(
        _default_payload(name="tail-task"), headers={HEADER_WORKFLOW_ID: _WF_ID, HEADER_WORKFLOW_STEP: "/1"}
    )
    outcome = await tail.finish(msg, _ctx(), _execution())
    assert outcome == Terminal.release_in_finally()
    worker._workflow_advancer.fail.assert_awaited_once()
    assert worker._workflow_advancer.fail.await_args.args[1] == "UnroutableMessage"
    broker.redeliver.assert_not_awaited()
    broker.ack.assert_awaited_once_with(msg)


async def test_unroutable_advance_with_transient_fail_mark_error_is_redelivered(broker, backend, config):
    tail, worker = _tail(broker, backend, config)
    worker._workflow_advancer.advance = AsyncMock(side_effect=UnroutableMessage("no queue 'next-step'"))
    worker._workflow_advancer.fail = AsyncMock(side_effect=BackendConnectionError("down"))
    msg = make_message(
        _default_payload(name="tail-task"), headers={HEADER_WORKFLOW_ID: _WF_ID, HEADER_WORKFLOW_STEP: "/1"}
    )
    outcome = await tail.finish(msg, _ctx(), _execution())
    assert outcome is Redelivered
    broker.redeliver.assert_awaited_once_with(msg)
    broker.ack.assert_not_awaited()
