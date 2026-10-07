"""Zurueckstellung bei Backend-Ausfall (Gate-Backoff).

Scheitert ein Gate vor der Ausfuehrung am Backend, geht die Nachricht nicht
per ``nack(requeue=True)`` zurueck (das verbraucht auf Quorum-Queues das
Zustellbudget, nach ``delivery_limit`` Zyklen landet sie still im ``.XQ``),
sondern als neue Nachricht ueber die Delay-Queue -- mit wachsender Wartezeit.

Test-Liste:
- [x] veroeffentlicht ueber ``retry_later`` mit der Basiswartezeit, Outcome
      ``Redelivered``, kein Requeue
- [x] uebernimmt alle Header (Retry-Zaehler, Unique-Hash, Workflow-Kontext,
      Enqueue-Zeit, Traceback ...) und setzt den Zurueckstellungszaehler auf 1
- [x] Brokerzaehler ``x-delivery-count``/``x-acquired-count`` wandern nicht mit
- [x] Folgeversuch: Zaehler +1, Wartezeit verdoppelt, gedeckelt
- [x] kaputter Zaehler-Header zaehlt wie keiner
- [x] WARNING ``task_deferred_backend_unavailable`` mit Feldern
- [x] kein Retry: Retry-Zaehler unveraendert (Header-Test), kein Signal
- [x] Publish scheitert -> ``redeliver`` (alter Weg), kein Ack, Log
- [x] 0.9.0: eine Zeile je Zurueckstellung -- das Event traegt die Stufe
      (``stage``) und den Kontext des Gates (z. B. ``workflow_id``), die
      bisher nur die zusaetzliche Gate-Warnung trug
- [x] 0.9.0: der Fallback-Log traegt Stufe, Kontext und den Backend-Fehler
      (``backend_error_type``/``backend_error``) neben dem Publish-Fehler
- [x] 0.9.0: Kopien ohne Backend-Fehler (Retry, Rate-Limit-/Lock-Requeue)
      tragen den Zaehler nicht (``republish_headers``; Tests in den Gates)
- [x] 0.9.0: gab der Broker die Zustellung zurueck (``x-acquired-count`` bzw.
      ``x-delivery-count`` > 0: Requeue nach bestandenen Gates, Absturz),
      beginnt die Zaehlung neu -- die Stoerung davor ist ueberstanden
"""

from __future__ import annotations

import pytest

from swarmq.exceptions import BackendConnectionError, BrokerConnectionError, PublishIndeterminate
from swarmq.headers import HEADER_BACKEND_DEFER_COUNT
from swarmq.worker.gates import GateContext
from swarmq.worker.gates.backend_defer import defer_backend_unavailable
from swarmq.worker.outcome import Redelivered
from tests.support.messages import make_message
from tests.unit.worker._helpers import _default_payload


def _ctx() -> GateContext:
    return GateContext(task_id="abc-123", task_name="add", queue="default")


async def test_first_deferral_publishes_to_delay_queue_with_base_delay(worker, broker):
    msg = make_message(_default_payload())

    outcome = await defer_backend_unavailable(
        worker._gate_deps, msg, _ctx(), BackendConnectionError("down"), gate="cancel",
    )

    assert outcome is Redelivered
    broker.retry_later.assert_awaited_once()
    assert broker.retry_later.await_args.kwargs["delay_ms"] == 1000
    broker.redeliver.assert_not_awaited()


async def test_deferral_keeps_all_headers_and_sets_defer_count(worker, broker):
    original = {
        "x-swarmq-retry-count": 2,
        "x-swarmq-unique-hash": "h1",
        "x-swarmq-workflow-id": "wf-1",
        "x-swarmq-enqueued-at": 1700000000,
        "x-swarmq-traceback": "boom",
    }
    msg = make_message(_default_payload(), headers=dict(original))

    await defer_backend_unavailable(
        worker._gate_deps, msg, _ctx(), BackendConnectionError("down"), gate="cancel",
    )

    headers = broker.retry_later.await_args.kwargs["headers"]
    assert headers == {**original, HEADER_BACKEND_DEFER_COUNT: 1}


async def test_deferral_drops_broker_delivery_counters(worker, broker):
    """Die Kopie ist eine neue Nachricht mit eigenem Zustellbudget: ein
    mitkopierter ``x-delivery-count`` liesse das Delivery-Count-Gate sie
    sofort als haengend in den Dead-Letter schicken."""
    msg = make_message(
        _default_payload(),
        headers={"x-delivery-count": 18, "x-acquired-count": 30, "x-swarmq-retry-count": 1},
    )

    await defer_backend_unavailable(
        worker._gate_deps, msg, _ctx(), BackendConnectionError("down"), gate="cancel",
    )

    headers = broker.retry_later.await_args.kwargs["headers"]
    assert "x-delivery-count" not in headers
    assert "x-acquired-count" not in headers
    assert headers["x-swarmq-retry-count"] == 1


async def test_followup_deferral_increments_count_and_doubles_delay(worker, broker):
    msg = make_message(_default_payload(), headers={HEADER_BACKEND_DEFER_COUNT: 3})

    await defer_backend_unavailable(
        worker._gate_deps, msg, _ctx(), BackendConnectionError("down"), gate="cancel",
    )

    kwargs = broker.retry_later.await_args.kwargs
    assert kwargs["headers"][HEADER_BACKEND_DEFER_COUNT] == 4
    assert kwargs["delay_ms"] == 8000



@pytest.mark.parametrize("counter", ["x-acquired-count", "x-delivery-count"])
async def test_broker_redelivered_message_restarts_the_defer_count(worker, broker, counter):
    """Eine Kopie mit Zaehler 5 bestand die Gates, scheiterte danach (z. B.
    Ergebnis-Schreiben -> Requeue) und geraet in eine neue Stoerung: sie
    beginnt wieder mit der Basiswartezeit statt mit 32 s."""
    msg = make_message(_default_payload(), headers={HEADER_BACKEND_DEFER_COUNT: 5, counter: 1})

    await defer_backend_unavailable(
        worker._gate_deps, msg, _ctx(), BackendConnectionError("down"), gate="cancel",
    )

    kwargs = broker.retry_later.await_args.kwargs
    assert kwargs["headers"][HEADER_BACKEND_DEFER_COUNT] == 1
    assert kwargs["delay_ms"] == 1000


async def test_followup_deferral_delay_is_capped_by_config(worker, broker):
    worker._gate_deps.config.backend_defer_max_delay = 5.0
    msg = make_message(_default_payload(), headers={HEADER_BACKEND_DEFER_COUNT: 9})

    await defer_backend_unavailable(
        worker._gate_deps, msg, _ctx(), BackendConnectionError("down"), gate="cancel",
    )

    assert broker.retry_later.await_args.kwargs["delay_ms"] == 5000


@pytest.mark.parametrize("raw", ["abc", None, -3, b"\xff"])
async def test_malformed_defer_count_counts_as_first_deferral(worker, broker, raw):
    msg = make_message(_default_payload(), headers={HEADER_BACKEND_DEFER_COUNT: raw})

    await defer_backend_unavailable(
        worker._gate_deps, msg, _ctx(), BackendConnectionError("down"), gate="cancel",
    )

    kwargs = broker.retry_later.await_args.kwargs
    assert kwargs["headers"][HEADER_BACKEND_DEFER_COUNT] == 1
    assert kwargs["delay_ms"] == 1000


async def test_deferral_logs_warning_with_attempt_delay_and_error(worker, broker, swarmq_logs):
    msg = make_message(_default_payload(), headers={HEADER_BACKEND_DEFER_COUNT: 1})

    await defer_backend_unavailable(
        worker._gate_deps, msg, _ctx(), BackendConnectionError("valkey down"), gate="cancel",
    )

    [entry] = [e for e in swarmq_logs if e["event"] == "task_deferred_backend_unavailable"]
    assert entry["log_level"] == "warning"
    assert entry["logger"] == "swarmq.worker.gates.backend_defer"
    assert entry["task_id"] == "abc-123"
    assert entry["task_name"] == "add"
    assert entry["gate"] == "cancel"
    assert entry["defer_count"] == 2
    assert entry["delay_ms"] == 2000
    assert entry["error_type"] == "BackendConnectionError"
    assert entry["error"] == "valkey down"


@pytest.mark.parametrize("publish_error", [BrokerConnectionError("rmq down"), PublishIndeterminate("deadline")])
async def test_failed_deferral_publish_falls_back_to_redeliver(worker, broker, swarmq_logs, publish_error):
    """Broker auch weg: der alte Weg (``nack(requeue=True)``) -- die
    Nachricht wird nie bestaetigt, ohne dass die Kopie sicher liegt."""
    broker.retry_later.side_effect = publish_error
    msg = make_message(_default_payload())

    outcome = await defer_backend_unavailable(
        worker._gate_deps, msg, _ctx(), BackendConnectionError("valkey down"), gate="cancel",
    )

    assert outcome is Redelivered
    broker.redeliver.assert_awaited_once_with(msg)
    broker.ack.assert_not_awaited()
    [entry] = [e for e in swarmq_logs if e["event"] == "task_defer_failed_redelivered"]
    assert entry["log_level"] == "warning"
    assert entry["task_id"] == "abc-123"
    assert entry["error_type"] == type(publish_error).__name__
    assert not [e for e in swarmq_logs if e["event"] == "task_deferred_backend_unavailable"]


async def test_deferral_event_carries_stage_and_gate_context(worker, broker, swarmq_logs):
    msg = make_message(_default_payload())

    await defer_backend_unavailable(
        worker._gate_deps, msg, _ctx(), BackendConnectionError("valkey down"),
        gate="cancel", stage="workflow_cancel_check", workflow_id="wf-1",
    )

    [entry] = [e for e in swarmq_logs if e["event"] == "task_deferred_backend_unavailable"]
    assert entry["stage"] == "workflow_cancel_check"
    assert entry["workflow_id"] == "wf-1"


async def test_failed_deferral_log_keeps_stage_context_and_backend_error(worker, broker, swarmq_logs):
    """Ohne die frueheren Gate-Warnungen traegt der Fallback-Log den
    Backend-Fehler selbst -- sonst stuende nur der Publish-Fehler da."""
    broker.retry_later.side_effect = BrokerConnectionError("rmq down")
    msg = make_message(_default_payload())

    await defer_backend_unavailable(
        worker._gate_deps, msg, _ctx(), BackendConnectionError("valkey down"),
        gate="schedfire", stage="completed_check", schedfire_key="s:1",
    )

    [entry] = [e for e in swarmq_logs if e["event"] == "task_defer_failed_redelivered"]
    assert entry["stage"] == "completed_check" and entry["schedfire_key"] == "s:1"
    assert entry["backend_error_type"] == "BackendConnectionError"
    assert entry["backend_error"] == "valkey down"
    assert entry["error_type"] == "BrokerConnectionError" and entry["error"] == "rmq down"
