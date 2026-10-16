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

Test-Liste U9 (Plan 2026-10-08-1541, R15/KTD7) — abschaltbares Dead-Lettering:

14. ``dead_letter=False``, ``max_retries=0``, Fehler -> Ergebnis mit
    ``task_info.result_ttl``, Ack, kein XQ-Publish, kein ``TASK_DLQ``.
15. Dasselbe fuer ``NoRetry`` und eine nicht wiederholbare Ausnahme.
16. Workflow-Schritt: der Fehler propagiert (Fail-Mark ``TaskError``).
17. Global aus (``dead_letter_terminal_failures=False``), Task ohne Angabe
    -> kein XQ; Task-Feld ``True`` gewinnt -> XQ.
18. Das Ack laeuft ueber den ``GuardedBroker``: der Done-Marker wird gesetzt.
19. Der Dead-Letter-Pfad speichert das Fehlerergebnis mit
    ``task_info.result_ttl`` statt ``config.result_ttl``.
20. Delivery-Count-Gate und undecodierbare Nachricht bei ausgeschaltetem
    Dead-Lettering -> weiterhin XQ.

Review Phase 2 (2026-10-09):

21. ``dead_letter`` wirft ``SettleFailedAfterPublish`` (Kopie liegt in der
    XQ, nur das Ack scheiterte): terminal wie ein Erfolg -- ``TASK_DLQ``,
    Unique-Freigabe, kein ``dlq_publish_failed``, kein Requeue.

BZ-17 (Plan 0.12.0, MR 2) -- begrenzter Fehlertext-Header:

22. Die Dead-Letter-Kopie traegt ``x-swarmq-traceback`` hoechstens 8 KiB lang.
23. Die Retry-Kopie ebenso.

P7b (Plan 2026-10-12-1105, MR 1, T9) -- ``task_sent_to_dlq`` nur, wenn die
Kopie liegt oder der DLX-Rueckfall lief:

24. DLQ-Publish scheitert ohne ``on_publish_failure`` -> ``dlq_publish_failed``
    einmal, kein ``task_sent_to_dlq``, ``redeliver`` einmal.
25. DLQ-Publish scheitert mit ``on_publish_failure`` (DLX-Rueckfall) ->
    ``task_sent_to_dlq`` einmal.
26. ``SettleFailedAfterPublish`` (Kopie liegt) -> ``task_sent_to_dlq`` einmal.
27. Workflow-Fehlmarkierung scheitert voruebergehend -> kein
    ``task_sent_to_dlq``.
28. Erfolg -> ``task_sent_to_dlq`` einmal mit ``error_type`` (Liste 8/9).
29. Kopie liegt, ``maybe_release_unique`` wirft (ohne ``on_publish_failure``)
    -> ``task_sent_to_dlq`` einmal (exakte Bedingung aus T9).

P8 (Plan 2026-10-12-1105, MR 2, T10, E4 A) -- ``Retry`` gilt unabhaengig von
``retry_exceptions``, verbraucht aber das Budget:

30. ``retry_exceptions=[KeyError]``, ``max_retries=3``, ``raise Retry()`` ->
    ``retry_later`` mit Backoff-Verzoegerung, kein ``dead_letter``.
31. Wie 30 mit ``Retry(delay=7)`` -> ``delay_ms == 7000``.
32. Wie 30, Budget erschoepft (``x-swarmq-retry-count == 3``) -> terminal.
33. ``max_retries=0``, ``raise Retry(delay=5)`` -> terminal.
34. ``ValueError`` nicht im Filter -> weiter terminal (Liste 9,
    ``test_task_sent_to_dlq_carries_error_type_of_triggering_exception``).
35. CPU-Modus: aus dem Envelope rekonstruierte ``Retry(delay=5)`` mit
    Filter -> ``retry_later`` mit 5000 ms.

F3-03 (Plan 2026-10-15-1138, U5, E-060) -- Grenzen der ``Retry``-Wartezeit,
Zusatzfaelle zum Nachweis ``test_befund_f3_03_retry_negative_delay.py``
(Klasse ``TestRetryDelayBounds``):

36. ``Retry(delay=-inf)`` -> ``ValueError`` beim Erzeugen.
37. ``Retry(delay=-0.0)`` und ``Retry(delay=-0.0005)`` -> ``delay_ms == 0``.
38. ``Retry(delay=0)`` -> ``delay_ms == 0``, Ziel ist die Stufe ``{queue}.DQ.1s``.
39. ``max_retries=2``, Header ``retry_count=2``, ``Retry(delay=-5)`` -> terminal.
40. ``on_retry`` und ``TASK_RETRYING`` sehen die geklemmte Wartezeit 0.
41. Ein gepickeltes ``Retry`` aus dem Rechenkind behaelt den geklemmten Wert.
42. Ein Task wirft ``Retry(delay=math.inf)`` mit
    ``retry_exceptions=[ConnectionError]``: der Konstruktor wirft
    ``ValueError``, der Filter greift nicht -> terminal (Dead-Letter,
    Fehlerergebnis ``builtins.ValueError``), kein Retry, nichts an den Broker.
"""

from __future__ import annotations

import math
from typing import override
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.exceptions import BackendConnectionError, NoRetry, SettleFailedAfterPublish, WorkflowStateConflict
from swarmq.headers import HEADER_WORKFLOW_ID
from swarmq.registry import TaskRegistry
from swarmq.signals import Signal
from swarmq.task import DeliveryMode, Task, TaskInfo
from swarmq.worker import Worker
from swarmq.worker.gates import GateContext, SchedfireClaim
from tests.support.messages import make_message
from tests.unit.worker._helpers import _default_payload

_WF_ID = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b"


class DlqTask(Task):
    task_info = TaskInfo(name="dlq-task", queues=["default"])

    @override
    async def run(self) -> str:
        return "ok"


class DlqFafTask(Task):
    task_info = TaskInfo(name="dlq-faf", queues=["default"], delivery=DeliveryMode.FIRE_AND_FORGET)

    @override
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
    worker._signal_dispatcher.dispatch = AsyncMock()  # type: ignore[method-assign]  # Dispatch-Spy
    return worker._retry_handler, worker


def _ctx(task=DlqTask) -> GateContext:
    return GateContext(
        task_id="abc-123",
        task_name=task.task_info.name,
        queue="default",
        task_info=task.task_info,
        task_class=task,
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
    assert entry["log_level"] == "error"
    assert entry["workflow_id"] == _WF_ID
    assert entry["redelivered"] is False
    assert entry["error_type"] == "ValueError"
    assert entry["error"] == "bad state doc"


async def test_result_store_failure_logs_warning_with_error_fields(broker, backend, config, swarmq_logs):
    handler, _ = _handler(broker, backend, config)
    backend.store_result = AsyncMock(side_effect=ConnectionError("valkey down"))
    await handler.dead_letter(make_message(_default_payload(name="dlq-task")), _ctx(), "boom", "RuntimeError")

    [entry] = _events(swarmq_logs, "dlq_result_store_failed")
    assert entry["log_level"] == "warning"
    assert entry["error_type"] == "ConnectionError"
    assert entry["error"] == "valkey down"


async def test_notify_failure_in_dead_letter_logs_warning_with_error_fields(broker, backend, config, swarmq_logs):
    handler, _ = _handler(broker, backend, config)
    backend.notify_result = AsyncMock(side_effect=ConnectionError("pubsub down"))
    await handler.dead_letter(make_message(_default_payload(name="dlq-task")), _ctx(), "boom", "RuntimeError")

    [entry] = _events(swarmq_logs, "publish_notification_failed")
    assert entry["log_level"] == "warning"
    assert entry["task_id"] == "abc-123"
    assert entry["error_type"] == "ConnectionError"
    assert entry["error"] == "pubsub down"


async def test_dlq_publish_failure_logs_error_with_error_fields(broker, backend, config, swarmq_logs):
    handler, _ = _handler(broker, backend, config)
    broker.dead_letter = AsyncMock(side_effect=ConnectionError("broker gone"))
    await handler.dead_letter(make_message(_default_payload(name="dlq-task")), _ctx(), "boom", "RuntimeError")

    [entry] = _events(swarmq_logs, "dlq_publish_failed")
    assert entry["log_level"] == "error"
    assert entry["error_type"] == "ConnectionError"
    assert entry["error"] == "broker gone"


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
    assert entry["error_type"] == "ConnectionError"
    assert entry["error"] == "broker gone"


async def test_task_sent_to_dlq_is_an_error(broker, backend, config, swarmq_logs):
    """KTD11: der Task ist endgueltig fehlgeschlagen -> ERROR (bisher WARNING)."""
    handler, _ = _handler(broker, backend, config)
    await handler.dead_letter(make_message(_default_payload(name="dlq-task")), _ctx(), "boom", "RuntimeError")

    [entry] = _events(swarmq_logs, "task_sent_to_dlq")
    assert entry["log_level"] == "error"
    assert entry["error"] == "boom"


class _FilteredTask(Task):
    task_info = TaskInfo(name="filtered-task", queues=["default"], max_retries=3, retry_exceptions=[KeyError])


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
    msg = make_message(_default_payload(name=task.task_info.name), headers={"x-swarmq-retry-count": retry_count})
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
    ctx.schedfire_claim = SchedfireClaim(key="s:1", owner="tok-1")
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


@pytest.mark.timeout(5)
@pytest.mark.entscheidung("E-046")
async def test_retry_copy_drops_schedfire_defer_count(broker, backend, config):
    """Review U5 (P2): der Schedfire-Defer-Zaehler gehoert zur Zurueckstellung
    einer Neuzustellung. Die fachliche Retry-Kopie eines gelaufenen Fires ist
    keine Neuzustellung -- traegt sie den Zaehler, wird sie bei fremdem Claim
    zurueckgestellt statt als Duplikat bestaetigt, und der Deckel startet mit
    dem alten Stand."""
    from swarmq.headers import HEADER_SCHEDFIRE_DEFER_COUNT
    from swarmq.middleware import MiddlewareChain

    class RetryingTask(Task):
        task_info = TaskInfo(name="retrying-task", queues=["default"], max_retries=3)

    handler, _ = _handler(broker, backend, config, RetryingTask)
    msg = make_message(
        _default_payload(name="retrying-task"),
        headers={HEADER_SCHEDFIRE_DEFER_COUNT: 4, "x-swarmq-schedfire": "cron:a:1"},
    )
    await handler.evaluate(
        msg, _ctx(RetryingTask), RuntimeError("task boom"), "task boom", {"retry_count": 0}, MiddlewareChain([])
    )

    headers = broker.retry_later.await_args.kwargs["headers"]
    assert HEADER_SCHEDFIRE_DEFER_COUNT not in headers
    assert headers["x-swarmq-schedfire"] == "cron:a:1"


class _NoDlqTask(Task):
    task_info = TaskInfo(name="no-dlq-task", queues=["default"], max_retries=0, result_ttl=120, dead_letter=False)


@pytest.mark.timeout(5)
async def test_disabled_dead_lettering_stores_error_result_and_acks(broker, backend, config):
    """Liste 14 (R15): Verhalten vor MR !12 -- Fehlerergebnis speichern und
    bestaetigen, nichts in die DLQ."""
    from swarmq.middleware import MiddlewareChain
    from swarmq.worker.outcome import Terminal

    handler, worker = _handler(broker, backend, config, _NoDlqTask)
    msg = make_message(_default_payload(name="no-dlq-task"))
    outcome = await handler.evaluate(
        msg, _ctx(_NoDlqTask), RuntimeError("boom"), "Traceback: boom", {"retry_count": 0}, MiddlewareChain([])
    )

    broker.dead_letter.assert_not_awaited()
    broker.ack.assert_awaited_once_with(msg)
    assert outcome == Terminal.release_in_finally()
    task_id, result_bytes, ttl_ms = backend.store_result.await_args.args
    assert task_id == "abc-123"
    assert ttl_ms == 120_000
    assert b"Traceback: boom" in result_bytes
    dispatched = [c.args[0] for c in worker._signal_dispatcher.dispatch.await_args_list]
    assert Signal.TASK_DLQ not in dispatched


class _NoDlqFilteredTask(Task):
    task_info = TaskInfo(
        name="no-dlq-filtered",
        queues=["default"],
        max_retries=3,
        retry_exceptions=[KeyError],
        dead_letter=False,
    )


@pytest.mark.timeout(5)
@pytest.mark.parametrize(
    ("task", "exc"),
    [(_NoDlqTask, NoRetry("give up")), (_NoDlqFilteredTask, ValueError("not listed"))],
)
async def test_disabled_dead_lettering_covers_every_terminal_branch(broker, backend, config, task, exc):
    """Liste 15: die Weiche gilt fuer ``NoRetry`` und den Retry-Filter wie
    fuer erschoepfte Retries."""
    from swarmq.middleware import MiddlewareChain

    handler, _ = _handler(broker, backend, config, task)
    msg = make_message(_default_payload(name=task.task_info.name))
    await handler.evaluate(msg, _ctx(task), exc, "boom", {"retry_count": 0}, MiddlewareChain([]))

    broker.dead_letter.assert_not_awaited()
    broker.ack.assert_awaited_once_with(msg)


@pytest.mark.timeout(5)
async def test_disabled_dead_lettering_propagates_workflow_failure(broker, backend, config):
    """Liste 16: ein Workflow-Schritt markiert den Workflow als gescheitert,
    damit ``handle.get()`` wirft statt in den Timeout zu laufen."""
    from swarmq.middleware import MiddlewareChain

    handler, worker = _handler(broker, backend, config, _NoDlqTask)
    msg = make_message(_default_payload(name="no-dlq-task"), headers={HEADER_WORKFLOW_ID: _WF_ID})
    await handler.evaluate(msg, _ctx(_NoDlqTask), RuntimeError("boom"), "boom", {"retry_count": 0}, MiddlewareChain([]))

    # P9 (Plan 2026-10-12-1105, MR 3, T13, Abschnitt 6): die Klasse der
    # echten Task-Exception wird durchgereicht.
    worker._workflow_advancer.fail.assert_awaited_once_with(
        _WF_ID, "TaskError", "boom", error_class="builtins.RuntimeError"
    )
    broker.dead_letter.assert_not_awaited()


class _DefaultDlqTask(Task):
    task_info = TaskInfo(name="default-dlq-task", queues=["default"], max_retries=0)


class _ForcedDlqTask(Task):
    task_info = TaskInfo(name="forced-dlq-task", queues=["default"], max_retries=0, dead_letter=True)


@pytest.mark.timeout(5)
@pytest.mark.parametrize(("task", "dead_lettered"), [(_DefaultDlqTask, False), (_ForcedDlqTask, True)])
async def test_global_switch_applies_unless_the_task_field_is_set(broker, backend, task, dead_lettered):
    """Liste 17: global aus, ``TaskInfo.dead_letter`` unset -> kein XQ; das
    gesetzte Task-Feld gewinnt."""
    from swarmq.config import SwarmQConfig
    from swarmq.middleware import MiddlewareChain

    config = SwarmQConfig(
        broker_url="amqp://localhost",
        backend_url="valkey://localhost",
        queues=["default"],
        dead_letter_terminal_failures=False,
    )
    handler, _ = _handler(broker, backend, config, task)
    msg = make_message(_default_payload(name=task.task_info.name))
    await handler.evaluate(msg, _ctx(task), RuntimeError("boom"), "boom", {"retry_count": 0}, MiddlewareChain([]))

    assert broker.dead_letter.await_count == (1 if dead_lettered else 0)


@pytest.mark.timeout(5)
@pytest.mark.entscheidung("E-031")
async def test_dead_letter_stores_error_result_with_task_result_ttl(broker, backend, config):
    """Liste 19: das Fehlerergebnis im Dead-Letter-Pfad lebt so lange wie
    jedes andere Ergebnis des Tasks (``task_info.result_ttl``)."""

    class ShortLivedTask(Task):
        task_info = TaskInfo(name="short-lived", queues=["default"], result_ttl=120)

    handler, _ = _handler(broker, backend, config, ShortLivedTask)
    await handler.dead_letter(
        make_message(_default_payload(name="short-lived")), _ctx(ShortLivedTask), "boom", "RuntimeError"
    )

    assert backend.store_result.await_args.args[2] == 120_000
    broker.dead_letter.assert_awaited_once()


@pytest.mark.timeout(5)
async def test_delivery_count_gate_and_undecodable_messages_still_reach_the_dlq(broker, backend):
    """Liste 20: die Weiche gilt nur fuer terminale Task-Fehler. Haengende
    Worker und undecodierbare Nachrichten landen weiter in der XQ."""
    from swarmq.config import SwarmQConfig

    config = SwarmQConfig(
        broker_url="amqp://localhost",
        backend_url="valkey://localhost",
        queues=["default"],
        dead_letter_terminal_failures=False,
    )
    _, worker = _handler(broker, backend, config, _NoDlqTask)

    await worker._handle_message(make_message(_default_payload(name="no-dlq-task"), headers={"x-delivery-count": 19}))
    broker.dead_letter.assert_awaited_once()

    await worker._handle_message(make_message(body=b"not json"))
    # Plan 2026-10-12-1105, MR 8: die undekodierbare Nachricht als Kopie.
    assert broker.dead_letter.await_count == 2
    broker.route_via_dlx.assert_not_awaited()


@pytest.mark.entscheidung("E-043")
async def test_dead_letter_with_failed_ack_after_publish_is_terminal(broker, backend, config, swarmq_logs):
    """Die Kopie liegt in der XQ; die Neuzustellung des Originals ueberspringt
    der Ausfuehrungsschutz als ``done`` -- also gibt dieser Pfad frei."""
    handler, worker = _handler(broker, backend, config)
    broker.dead_letter = AsyncMock(side_effect=SettleFailedAfterPublish("channel closed"))
    broker.redeliver = AsyncMock()
    worker._gate_deps.maybe_release_unique = AsyncMock()
    msg = make_message(_default_payload(name="dlq-task"))

    await handler.dead_letter(msg, _ctx(), "boom", "RuntimeError")

    assert _events(swarmq_logs, "dlq_publish_failed") == []
    broker.redeliver.assert_not_awaited()
    worker._gate_deps.maybe_release_unique.assert_awaited_once()
    signals = [c.args[0] for c in worker._signal_dispatcher.dispatch.await_args_list]
    assert Signal.TASK_DLQ in signals


_HUGE_ERROR = "E" * (1024 * 1024)


async def test_dead_letter_copy_bounds_the_traceback_header(broker, backend, config):
    """Liste 22 (BZ-17): Ein unbegrenzter Header sprengt ``frame_max``, der
    Broker schliesst die Verbindung, und alle Nachbar-Zustellungen kommen
    zurueck."""
    handler, _ = _handler(broker, backend, config)

    await handler.dead_letter(make_message(_default_payload(name="dlq-task")), _ctx(), _HUGE_ERROR, "RuntimeError")

    traceback_header = broker.dead_letter.await_args.kwargs["headers"]["x-swarmq-traceback"]
    assert len(traceback_header.encode()) <= 8 * 1024


@pytest.mark.timeout(5)
async def test_retry_copy_bounds_the_traceback_header(broker, backend, config):
    """Liste 23 (BZ-17): dasselbe fuer die Retry-Kopie (Nachweis
    ``test_befund_bz17_traceback_header.py`` gegen den echten Broker)."""
    from swarmq.middleware import MiddlewareChain

    class RetryingTask(Task):
        task_info = TaskInfo(name="retrying-task", queues=["default"], max_retries=3)

    handler, _ = _handler(broker, backend, config, RetryingTask)
    await handler.evaluate(
        make_message(_default_payload(name="retrying-task")),
        _ctx(RetryingTask),
        RuntimeError(_HUGE_ERROR),
        _HUGE_ERROR,
        {"retry_count": 0},
        MiddlewareChain([]),
    )

    traceback_header = broker.retry_later.await_args.kwargs["headers"]["x-swarmq-traceback"]
    assert len(traceback_header.encode()) <= 8 * 1024


async def test_dead_letter_of_a_step_that_already_succeeded_does_not_fail_its_workflow(broker, backend, config):
    """Plan 0.12.0, MR 3 (BZ-13, Schutz): ein spaetes Duplikat eines
    erfolgreich erledigten Schritts (etwa am Zustelllimit) kippt den
    weiterlaufenden Workflow nicht -- auch nicht ueber das eigene
    Fehlerergebnis, das der Dead-Letter-Pfad speichert."""
    from swarmq.cancellation import build_result_dict
    from swarmq.serialization import encode_result

    handler, worker = _handler(broker, backend, config)
    results = {
        "abc-123": encode_result(
            build_result_dict(
                task_id="abc-123",
                queue="default",
                task_name="dlq-task",
                is_error=False,
                is_cancelled=False,
                result="ok",
            )
        )
    }

    async def store(task_id, raw, ttl_ms, **_kw):
        results[task_id] = raw

    async def get(task_id):
        return results.get(task_id)

    backend.store_result = AsyncMock(side_effect=store)
    backend.get_result = AsyncMock(side_effect=get)
    msg = make_message(_default_payload(name="dlq-task"), headers={HEADER_WORKFLOW_ID: _WF_ID})

    await handler.dead_letter(msg, _ctx(), "WorkerHungError: delivered 19 times", "WorkerHungError")

    worker._workflow_advancer.fail.assert_not_awaited()
    broker.dead_letter.assert_awaited_once()


@pytest.mark.timeout(5)
async def test_failed_dlq_publish_with_redelivery_does_not_log_task_sent_to_dlq(broker, backend, config, swarmq_logs):
    """Liste 24 (P7b, T9): Die Nachricht liegt nicht in der XQ, sondern wird
    neu zugestellt -- ``dlq_publish_failed`` beschreibt den Fall allein."""
    from swarmq.exceptions import BrokerConnectionError

    handler, _ = _handler(broker, backend, config)
    broker.dead_letter = AsyncMock(side_effect=BrokerConnectionError("broker gone"))
    msg = make_message(_default_payload(name="dlq-task"))

    placed = await handler.dead_letter(msg, _ctx(), "boom", "RuntimeError")

    assert placed is False
    assert len(_events(swarmq_logs, "dlq_publish_failed")) == 1
    assert _events(swarmq_logs, "task_sent_to_dlq") == []
    broker.redeliver.assert_awaited_once_with(msg)


@pytest.mark.timeout(5)
async def test_placed_copy_with_failing_release_still_logs_task_sent_to_dlq(broker, backend, config, swarmq_logs):
    """Liste 29 (P7b, T9, Review P1-10): ``placed`` gilt ab der liegenden
    Kopie. Wirft danach die Unique-Freigabe, liegt die Nachricht trotzdem in
    der XQ -- das Event bleibt. Die vorbestehende Doppelbehandlung
    (``dlq_publish_failed`` und Neuzustellung) aendert P7b nicht."""
    handler, worker = _handler(broker, backend, config)
    worker._gate_deps.maybe_release_unique = AsyncMock(side_effect=ConnectionError("valkey down"))

    placed = await handler.dead_letter(make_message(_default_payload(name="dlq-task")), _ctx(), "boom", "RuntimeError")

    assert placed is True
    broker.dead_letter.assert_awaited_once()
    [entry] = _events(swarmq_logs, "task_sent_to_dlq")
    assert entry["error_type"] == "RuntimeError"


@pytest.mark.timeout(5)
async def test_failed_dlq_publish_with_dlx_fallback_logs_task_sent_to_dlq(broker, backend, config, swarmq_logs):
    """Liste 25 (P7b, T9): Mit ``on_publish_failure`` (Lock- und
    Unknown-Pfade) liegt die Nachricht danach ueber den Broker-DLX in der
    XQ -- das Event bleibt."""
    from swarmq.exceptions import BrokerConnectionError

    handler, _ = _handler(broker, backend, config)
    broker.dead_letter = AsyncMock(side_effect=BrokerConnectionError("broker gone"))
    fallback = AsyncMock()

    placed = await handler.dead_letter(
        make_message(_default_payload(name="dlq-task")),
        _ctx(),
        "boom",
        "LockWaitExceeded",
        on_publish_failure=fallback,
    )

    assert placed is False
    fallback.assert_awaited_once()
    broker.redeliver.assert_not_awaited()
    [entry] = _events(swarmq_logs, "task_sent_to_dlq")
    assert entry["error_type"] == "LockWaitExceeded"


@pytest.mark.timeout(5)
async def test_settle_failed_after_publish_logs_task_sent_to_dlq_once(broker, backend, config, swarmq_logs):
    """Liste 26 (P7b): ``SettleFailedAfterPublish`` heisst, die Kopie liegt --
    ``placed`` und genau ein ``task_sent_to_dlq``."""
    handler, _ = _handler(broker, backend, config)
    broker.dead_letter = AsyncMock(side_effect=SettleFailedAfterPublish("channel closed"))

    placed = await handler.dead_letter(make_message(_default_payload(name="dlq-task")), _ctx(), "boom", "RuntimeError")

    assert placed is True
    assert len(_events(swarmq_logs, "task_sent_to_dlq")) == 1


@pytest.mark.timeout(5)
async def test_transient_workflow_fail_mark_error_does_not_log_task_sent_to_dlq(broker, backend, config, swarmq_logs):
    """Liste 27 (P7b): Scheitert die Workflow-Fehlmarkierung voruebergehend,
    kehrt ``dead_letter`` vor der Kopie zurueck -- kein Event."""
    handler, worker = _handler(broker, backend, config)
    worker._workflow_advancer.fail = AsyncMock(side_effect=BackendConnectionError("down"))
    msg = make_message(_default_payload(name="dlq-task"), headers={HEADER_WORKFLOW_ID: _WF_ID})

    placed = await handler.dead_letter(msg, _ctx(), "boom", "RuntimeError")

    assert placed is False
    broker.dead_letter.assert_not_awaited()
    assert _events(swarmq_logs, "task_sent_to_dlq") == []


class _FilteredFixedTask(Task):
    task_info = TaskInfo(
        name="filtered-fixed-task",
        queues=["default"],
        max_retries=3,
        retry_exceptions=[KeyError],
        retry_backoff="fixed",
        retry_base_delay=2.0,
    )


@pytest.mark.timeout(5)
@pytest.mark.entscheidung("E-060")
async def test_retry_retries_although_retry_exceptions_does_not_list_it(broker, backend, config):
    """Liste 30 (P8, T10): ``raise Retry()`` ist eine ausdrueckliche
    Anweisung des Tasks; der Filter ``retry_exceptions`` gilt fuer
    unerwartete Ausnahmen, nicht fuer ``Retry``."""
    from swarmq.exceptions import Retry
    from swarmq.middleware import MiddlewareChain

    handler, _ = _handler(broker, backend, config, _FilteredFixedTask)
    msg = make_message(_default_payload(name="filtered-fixed-task"))

    await handler.evaluate(msg, _ctx(_FilteredFixedTask), Retry(), "", {"retry_count": 0}, MiddlewareChain([]))

    broker.dead_letter.assert_not_awaited()
    broker.retry_later.assert_awaited_once()
    assert broker.retry_later.await_args.kwargs["delay_ms"] == 2000


@pytest.mark.timeout(5)
@pytest.mark.entscheidung("E-060")
async def test_retry_delay_wins_although_retry_exceptions_does_not_list_it(broker, backend, config):
    """Liste 31 (P8): die Verzoegerung aus ``Retry(delay=...)`` gilt auch mit
    Filter."""
    from swarmq.exceptions import Retry
    from swarmq.middleware import MiddlewareChain

    handler, _ = _handler(broker, backend, config, _FilteredFixedTask)
    msg = make_message(_default_payload(name="filtered-fixed-task"))

    await handler.evaluate(msg, _ctx(_FilteredFixedTask), Retry(delay=7), "", {"retry_count": 0}, MiddlewareChain([]))

    broker.dead_letter.assert_not_awaited()
    assert broker.retry_later.await_args.kwargs["delay_ms"] == 7000


@pytest.mark.timeout(5)
async def test_retry_with_filter_still_consumes_the_budget(broker, backend, config, swarmq_logs):
    """Liste 32 (P8, E4 A): ``Retry`` umgeht den Filter, nicht das Budget --
    nach drei von drei Versuchen endet es terminal."""
    from swarmq.exceptions import Retry
    from swarmq.middleware import MiddlewareChain

    handler, _ = _handler(broker, backend, config, _FilteredFixedTask)
    msg = make_message(_default_payload(name="filtered-fixed-task"), headers={"x-swarmq-retry-count": 3})

    await handler.evaluate(msg, _ctx(_FilteredFixedTask), Retry(delay=7), "", {"retry_count": 0}, MiddlewareChain([]))

    broker.retry_later.assert_not_awaited()
    broker.dead_letter.assert_awaited_once()
    [entry] = _events(swarmq_logs, "task_sent_to_dlq")
    assert entry["error_type"] == "Retry"


@pytest.mark.timeout(5)
@pytest.mark.entscheidung("E-060")
async def test_retry_with_zero_max_retries_ends_terminally(broker, backend, config):
    """Liste 33 (P8, E4 A): bei ``max_retries=0`` endet ``Retry`` terminal."""
    from swarmq.exceptions import Retry
    from swarmq.middleware import MiddlewareChain

    handler, _ = _handler(broker, backend, config, _ZeroRetryTask)
    msg = make_message(_default_payload(name="zero-retry-task"))

    await handler.evaluate(msg, _ctx(_ZeroRetryTask), Retry(delay=5), "", {"retry_count": 0}, MiddlewareChain([]))

    broker.retry_later.assert_not_awaited()
    broker.dead_letter.assert_awaited_once()


@pytest.mark.timeout(5)
async def test_retry_rebuilt_from_compute_envelope_retries_despite_filter(broker, backend, config):
    """Liste 35 (P8, T10): Im CPU-Modus kommt ``Retry`` ueber das Envelope
    aus dem Rechenkind; die rekonstruierte Instanz geht durch denselben
    ``RetryHandler`` und behaelt ``delay``."""
    import pickle

    from swarmq.exceptions import Retry
    from swarmq.middleware import MiddlewareChain
    from swarmq.worker.compute.envelope import error_envelope

    rebuilt = pickle.loads(pickle.dumps(error_envelope("abc-123", Retry(delay=5)))).error()
    assert rebuilt is not None
    handler, _ = _handler(broker, backend, config, _FilteredFixedTask)
    msg = make_message(_default_payload(name="filtered-fixed-task"))

    await handler.evaluate(msg, _ctx(_FilteredFixedTask), rebuilt, "", {"retry_count": 0}, MiddlewareChain([]))

    broker.dead_letter.assert_not_awaited()
    assert broker.retry_later.await_args.kwargs["delay_ms"] == 5000


class _BudgetTwoTask(Task):
    task_info = TaskInfo(name="budget-two-task", queues=["default"], max_retries=2)


class _InfiniteRetryTask(Task):
    task_info = TaskInfo(
        name="infinite-retry-task", queues=["default"], max_retries=3, retry_exceptions=[ConnectionError]
    )

    @override
    async def run(self) -> None:
        from swarmq.exceptions import Retry

        raise Retry(delay=math.inf)


class TestRetryDelayBounds:
    """F3-03 (E-060): Klemmen und Pruefen in ``Retry`` (KTD6)."""

    def test_negative_infinity_is_rejected(self) -> None:
        """Liste 36."""
        from swarmq.exceptions import Retry

        with pytest.raises(ValueError, match="delay"):
            Retry(delay=-math.inf)

    @pytest.mark.timeout(5)
    @pytest.mark.parametrize("delay", [-0.0, -0.0005], ids=["minus-zero", "sub-millisecond"])
    async def test_tiny_negative_delays_retry_immediately(self, broker, backend, config, delay) -> None:
        """Liste 37. ``delay_ms`` ist auch ohne Klemmen 0 (``int(-0.5) == 0``);
        das Klemmen belegt erst der Wert, den die Hooks sehen. ``-0.0`` ist
        reine Charakterisierung: ``-0.0 < 0`` greift nicht, ``-0.0 == 0``."""
        from swarmq.exceptions import Retry
        from swarmq.middleware import Middleware, MiddlewareChain

        seen: list[float] = []

        class _Spy(Middleware):
            @override
            async def on_retry(self, message, exception, retry_count, delay) -> None:
                seen.append(delay)

        handler, _ = _handler(broker, backend, config, _BudgetTwoTask)
        msg = make_message(_default_payload(name="budget-two-task"))

        await handler.evaluate(
            msg, _ctx(_BudgetTwoTask), Retry(delay=delay), "", {"retry_count": 0}, MiddlewareChain([_Spy()])
        )

        assert broker.retry_later.await_args.kwargs["delay_ms"] == 0
        assert seen == [0]
        assert Retry(delay=delay).delay == 0

    @pytest.mark.timeout(5)
    async def test_zero_delay_goes_to_the_one_second_tier(self, broker, backend, config) -> None:
        """Liste 38: ``delay_ms=0`` braucht keine eigene Queue; die kleinste
        Stufe nimmt es auf (RabbitMQ erlaubt ``expiration="0"``)."""
        from swarmq.exceptions import Retry
        from swarmq.middleware import MiddlewareChain
        from swarmq.queue_names import delay_tier_for

        handler, _ = _handler(broker, backend, config, _BudgetTwoTask)
        msg = make_message(_default_payload(name="budget-two-task"))

        await handler.evaluate(msg, _ctx(_BudgetTwoTask), Retry(delay=0), "", {"retry_count": 0}, MiddlewareChain([]))

        delay_ms = broker.retry_later.await_args.kwargs["delay_ms"]
        assert delay_ms == 0
        assert delay_tier_for("default", delay_ms) == "default.DQ.1s"

    @pytest.mark.timeout(5)
    @pytest.mark.entscheidung("E-060")
    async def test_clamped_retry_with_exhausted_budget_ends_terminally(self, broker, backend, config) -> None:
        """Liste 39: die geklemmte Wartezeit zaehlt normal gegen ``max_retries``."""
        from swarmq.exceptions import Retry
        from swarmq.middleware import MiddlewareChain

        handler, _ = _handler(broker, backend, config, _BudgetTwoTask)
        msg = make_message(_default_payload(name="budget-two-task"), headers={"x-swarmq-retry-count": 2})

        await handler.evaluate(msg, _ctx(_BudgetTwoTask), Retry(delay=-5), "", {"retry_count": 0}, MiddlewareChain([]))

        broker.retry_later.assert_not_awaited()
        broker.dead_letter.assert_awaited_once()

    @pytest.mark.timeout(5)
    async def test_hooks_see_the_clamped_delay(self, broker, backend, config) -> None:
        """Liste 40: Middleware und Signal bekommen nie ``-5``."""
        from swarmq.exceptions import Retry
        from swarmq.middleware import Middleware, MiddlewareChain

        seen: list[float] = []

        class _Spy(Middleware):
            @override
            async def on_retry(self, message, exception, retry_count, delay) -> None:
                seen.append(delay)

        handler, worker = _handler(broker, backend, config, _BudgetTwoTask)
        msg = make_message(_default_payload(name="budget-two-task"))

        await handler.evaluate(
            msg, _ctx(_BudgetTwoTask), Retry(delay=-5), "", {"retry_count": 0}, MiddlewareChain([_Spy()])
        )

        assert seen == [0]
        retrying = [c for c in worker._signal_dispatcher.dispatch.await_args_list if c.args[0] is Signal.TASK_RETRYING]
        assert [c.kwargs["delay"] for c in retrying] == [0]

    @pytest.mark.timeout(5)
    @pytest.mark.entscheidung("E-060")
    async def test_task_raising_infinite_retry_ends_terminally(self, broker, backend, config) -> None:
        """Liste 42: der ``ValueError`` aus ``Retry()`` ist ein gewoehnlicher
        Task-Fehler, kein ``Retry``; ``retry_exceptions`` nennt ihn nicht."""
        from swarmq.serialization import decode_result

        registry = TaskRegistry()
        registry.register(_InfiniteRetryTask)
        worker = Worker(broker=broker, backend=backend, registry=registry, config=config)

        await worker._handle_message(make_message(_default_payload(name="infinite-retry-task", args=[], kwargs={})))

        broker.retry_later.assert_not_awaited()
        broker.dead_letter.assert_awaited_once()
        stored = decode_result(backend.store_result.await_args.args[1])
        assert stored["is_error"] is True
        assert stored["error_class"] == "builtins.ValueError"

    def test_pickled_retry_from_the_compute_child_keeps_the_clamped_delay(self) -> None:
        """Liste 41: Entpickeln ruft ``Retry()`` ohne Wert und setzt danach
        ``__dict__``; der im Kind geklemmte Wert bleibt."""
        import pickle

        from swarmq.exceptions import Retry
        from swarmq.worker.compute.envelope import error_envelope

        rebuilt = pickle.loads(pickle.dumps(error_envelope("abc-123", Retry(delay=-5)))).error()

        assert isinstance(rebuilt, Retry)
        assert rebuilt.delay == 0
