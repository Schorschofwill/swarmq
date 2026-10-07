"""Dead-Letter-Pfad des ``RetryHandler`` (Plan 2026-09-08-001, U10).

Test-Liste U10 (R25/KTD12) — Fail-Mark-Regel im Dead-Letter:

1. Fail-Mark wirft ``BackendConnectionError`` → Redelivery statt Dead-Letter,
   kein ``TASK_DLQ``, keine Unique-Freigabe (AE11).
2. Fail-Mark wirft ``WorkflowStateConflict`` → dieselbe Behandlung.
3. Fail-Mark wirft ``ValueError`` → Dead-Letter wie bisher, Log mit Kennung.
4. FIRE_AND_FORGET-Schritt → kein Broker-Verb aus dem Fail-Mark-Zweig.

Test-Liste U9 (Plan 2026-10-06-0831, R17/KTD11) — Logs ueber ``swarmq_logs``:

5. ``workflow_fail_mark_failed`` mit ``workflow_id``, ``error_type``, ``error``.
6. ``dlq_result_store_failed``/``publish_notification_failed`` mit Fehlerfeldern.
7. ``dlq_publish_failed`` und ``retry_publish_failed`` mit Fehlerfeldern.
8. ``task_sent_to_dlq`` auf ERROR: der Task ist endgueltig fehlgeschlagen.
9. ``task_sent_to_dlq`` traegt ``error_type`` (Klassenname der ausloesenden
   Ausnahme) neben ``error`` — auf allen Pfaden der Retry-Bewertung
   (``NoRetry``, Retry-Filter, Erschoepfung inkl. ``max_retries=0``).
10. Delivery-Count-Gate ohne Ausnahme: ``error_type`` ist fest
    ``WorkerHungError`` (siehe ``test_delivery_count.py``).

Test-Liste 0.9.0 (Retry-Kopie und Broker-Zaehler):

11. Die Retry-Kopie traegt weder ``x-delivery-count`` noch
    ``x-acquired-count`` des Originals (neue Nachricht, eigenes
    Zustellbudget); alle anderen Header bleiben.
12. Haelt die Zustellung einen Schedfire-Claim, traegt die Retry-Kopie das
    Claim-Token (``x-swarmq-schedfire-claim``).
13. Die Retry-Kopie traegt keinen ``x-swarmq-backend-defer-count``: der Task
    lief, die Backend-Stoerung ist ueberstanden; eine naechste Stoerung
    beginnt wieder mit der Basiswartezeit.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.exceptions import BackendConnectionError, NoRetry, WorkflowStateConflict
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
    await handler.dead_letter(msg, _ctx(), "boom", "RuntimeError")
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
    await handler.dead_letter(msg, _ctx(), "boom", "RuntimeError")
    broker.redeliver.assert_awaited_once_with(msg)
    broker.dead_letter.assert_not_awaited()


async def test_fail_mark_deterministic_error_still_dead_letters(broker, backend, config):
    """U10 Fall 3: Ein deterministischer Fail-Mark-Fehler haelt den
    Dead-Letter nicht auf — sonst drehte die Nachricht bis zum Delivery-Limit,
    ohne dass die Markierung je gelingt."""
    handler, worker = _handler(broker, backend, config)
    worker._workflow_advancer.fail = AsyncMock(side_effect=ValueError("bad state doc"))
    msg = make_message(_default_payload(name="dlq-task"), headers={HEADER_WORKFLOW_ID: _WF_ID})
    await handler.dead_letter(msg, _ctx(), "boom", "RuntimeError")
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
    await handler.dead_letter(msg, _ctx(DlqFafTask), "boom", "RuntimeError")
    broker.redeliver.assert_not_awaited()
    broker.dead_letter.assert_awaited_once()


def _events(logs, event):
    return [e for e in logs if e["logger"] == "swarmq.worker.retry" and e["event"] == event]


async def test_fail_mark_error_logs_workflow_id_and_error_fields(broker, backend, config, swarmq_logs):
    handler, worker = _handler(broker, backend, config)
    worker._workflow_advancer.fail = AsyncMock(side_effect=ValueError("bad state doc"))
    msg = make_message(_default_payload(name="dlq-task"), headers={HEADER_WORKFLOW_ID: _WF_ID})
    await handler.dead_letter(msg, _ctx(), "boom", "RuntimeError")

    [entry] = _events(swarmq_logs, "workflow_fail_mark_failed")
    assert entry["log_level"] == "error" and entry["workflow_id"] == _WF_ID
    assert entry["redelivered"] is False
    assert entry["error_type"] == "ValueError" and entry["error"] == "bad state doc"


async def test_result_store_failure_logs_warning_with_error_fields(broker, backend, config, swarmq_logs):
    handler, _ = _handler(broker, backend, config)
    backend.store_result = AsyncMock(side_effect=ConnectionError("valkey down"))
    await handler.dead_letter(make_message(_default_payload(name="dlq-task")), _ctx(), "boom", "RuntimeError")

    [entry] = _events(swarmq_logs, "dlq_result_store_failed")
    assert entry["log_level"] == "warning"
    assert entry["error_type"] == "ConnectionError" and entry["error"] == "valkey down"


async def test_notify_failure_in_dead_letter_logs_warning_with_error_fields(broker, backend, config, swarmq_logs):
    handler, _ = _handler(broker, backend, config)
    backend.notify_result = AsyncMock(side_effect=ConnectionError("pubsub down"))
    await handler.dead_letter(make_message(_default_payload(name="dlq-task")), _ctx(), "boom", "RuntimeError")

    [entry] = _events(swarmq_logs, "publish_notification_failed")
    assert entry["log_level"] == "warning" and entry["task_id"] == "abc-123"
    assert entry["error_type"] == "ConnectionError" and entry["error"] == "pubsub down"


async def test_dlq_publish_failure_logs_error_with_error_fields(broker, backend, config, swarmq_logs):
    handler, _ = _handler(broker, backend, config)
    broker.dead_letter = AsyncMock(side_effect=ConnectionError("broker gone"))
    await handler.dead_letter(make_message(_default_payload(name="dlq-task")), _ctx(), "boom", "RuntimeError")

    [entry] = _events(swarmq_logs, "dlq_publish_failed")
    assert entry["log_level"] == "error"
    assert entry["error_type"] == "ConnectionError" and entry["error"] == "broker gone"


async def test_retry_publish_failure_logs_error_with_error_fields(broker, backend, config, swarmq_logs):
    from swarmq.middleware import MiddlewareChain

    class RetryingTask(Task):
        task_info = TaskInfo(name="retrying-task", queues=["default"], max_retries=1)

    handler, _ = _handler(broker, backend, config, RetryingTask)
    broker.retry_later = AsyncMock(side_effect=ConnectionError("broker gone"))
    msg = make_message(_default_payload(name="retrying-task"))
    await handler.evaluate(
        msg, _ctx(RetryingTask), RuntimeError("task boom"), "task boom", {"retry_count": 0}, MiddlewareChain([])
    )

    [entry] = _events(swarmq_logs, "retry_publish_failed")
    assert entry["log_level"] == "error"
    assert entry["error_type"] == "ConnectionError" and entry["error"] == "broker gone"


async def test_task_sent_to_dlq_is_an_error(broker, backend, config, swarmq_logs):
    """KTD11: der Task ist endgueltig fehlgeschlagen -> ERROR (bisher WARNING)."""
    handler, _ = _handler(broker, backend, config)
    await handler.dead_letter(make_message(_default_payload(name="dlq-task")), _ctx(), "boom", "RuntimeError")

    [entry] = _events(swarmq_logs, "task_sent_to_dlq")
    assert entry["log_level"] == "error"
    assert entry["error"] == "boom"


class _FilteredTask(Task):
    task_info = TaskInfo(
        name="filtered-task", queues=["default"], max_retries=3, retry_exceptions=(KeyError,)
    )


class _ZeroRetryTask(Task):
    task_info = TaskInfo(name="zero-retry-task", queues=["default"], max_retries=0)


@pytest.mark.timeout(5)
@pytest.mark.parametrize(
    ("task", "exc", "retry_count"),
    [
        (DlqTask, NoRetry("give up"), 0),  # NoRetry -> direkt DLQ
        (_FilteredTask, ValueError("not listed"), 0),  # nicht in retry_exceptions
        (_ZeroRetryTask, RuntimeError("boom"), 0),  # max_retries=0
        (_FilteredTask, KeyError("gone"), 3),  # Retries erschoepft (3 von 3)
    ],
)
async def test_task_sent_to_dlq_carries_error_type_of_triggering_exception(
    broker, backend, config, swarmq_logs, task, exc, retry_count
):
    """U9 (R17): ``task_sent_to_dlq`` folgt dem Fehlerschema der anderen
    Fehler-Events — ``error_type`` ist der Klassenname der ausloesenden
    Ausnahme, ``error`` bleibt der Fehlertext der Retry-Bewertung."""
    from swarmq.middleware import MiddlewareChain

    handler, _ = _handler(broker, backend, config, task)
    msg = make_message(
        _default_payload(name=task.task_info.name), headers={"x-swarmq-retry-count": retry_count}
    )
    await handler.evaluate(msg, _ctx(task), exc, "Traceback: boom", {"retry_count": 0}, MiddlewareChain([]))

    [entry] = _events(swarmq_logs, "task_sent_to_dlq")
    assert entry["error_type"] == type(exc).__name__
    assert entry["error"] == "Traceback: boom"


@pytest.mark.timeout(5)
async def test_retry_copy_drops_broker_delivery_counters(broker, backend, config):
    """Liste 11: Die Retry-Kopie ist eine neue Nachricht. Ein mitkopierter
    ``x-delivery-count`` (RabbitMQ 4 behaelt einen Publisher-Wert bei, 3.13
    zaehlt Requeues) liesse das Delivery-Count-Gate sie als haengend in den
    Dead-Letter schicken."""
    from swarmq.middleware import MiddlewareChain

    class RetryingTask(Task):
        task_info = TaskInfo(name="retrying-task", queues=["default"], max_retries=3)

    handler, _ = _handler(broker, backend, config, RetryingTask)
    msg = make_message(
        _default_payload(name="retrying-task"),
        headers={"x-delivery-count": 18, "x-acquired-count": 30, "x-swarmq-unique-hash": "h1"},
    )
    await handler.evaluate(
        msg, _ctx(RetryingTask), RuntimeError("task boom"), "task boom", {"retry_count": 0}, MiddlewareChain([])
    )

    headers = broker.retry_later.await_args.kwargs["headers"]
    assert "x-delivery-count" not in headers
    assert "x-acquired-count" not in headers
    assert headers["x-swarmq-unique-hash"] == "h1"
    assert headers["x-swarmq-retry-count"] == 1


@pytest.mark.timeout(5)
async def test_retry_copy_carries_schedfire_claim_token(broker, backend, config):
    """Liste 12: scheitert die Freigabe des Claims im finally, uebernimmt die
    Retry-Kopie ihn ueber das Token, statt als Duplikat zu enden."""
    from swarmq.middleware import MiddlewareChain

    class RetryingTask(Task):
        task_info = TaskInfo(name="retrying-task", queues=["default"], max_retries=3)

    handler, _ = _handler(broker, backend, config, RetryingTask)
    msg = make_message(_default_payload(name="retrying-task"), headers={"x-swarmq-schedfire": "s:1"})
    ctx = _ctx(RetryingTask)
    ctx.schedfire_claim = "tok-1"
    await handler.evaluate(msg, ctx, RuntimeError("task boom"), "task boom", {"retry_count": 0}, MiddlewareChain([]))

    headers = broker.retry_later.await_args.kwargs["headers"]
    assert headers["x-swarmq-schedfire-claim"] == "tok-1"


@pytest.mark.timeout(5)
async def test_retry_copy_drops_backend_defer_count(broker, backend, config):
    """Liste 13."""
    from swarmq.middleware import MiddlewareChain

    class RetryingTask(Task):
        task_info = TaskInfo(name="retrying-task", queues=["default"], max_retries=3)

    handler, _ = _handler(broker, backend, config, RetryingTask)
    msg = make_message(_default_payload(name="retrying-task"), headers={"x-swarmq-backend-defer-count": 6})
    await handler.evaluate(
        msg, _ctx(RetryingTask), RuntimeError("task boom"), "task boom", {"retry_count": 0}, MiddlewareChain([])
    )

    assert "x-swarmq-backend-defer-count" not in broker.retry_later.await_args.kwargs["headers"]
