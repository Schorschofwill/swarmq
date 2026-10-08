"""B22: Fehler beim Abschluss einer Zustellung entweichen nicht aus der Task.

Die Task jeder Zustellung startet ``_on_message`` per ``create_task``; eine
dort entweichende Exception meldet asyncio als „Task exception was never
retrieved“ -- auf ERROR und ohne Task-Kontext.

Test-Liste:
- [x] Ack scheitert mit ``BrokerConnectionError`` (Kanal weg): die Task endet
      ohne Exception, genau eine WARNING ``delivery_settle_failed`` mit
      task_id, task_name, error_type, error und Hinweis auf die Neuzustellung.
- [x] Eine unerwartete Exception aus der Pipeline endet ebenfalls nicht in der
      Task, sondern als ERROR ``delivery_handler_failed`` mit Task-Kontext und
      Fehlerfeldern.
- [x] ``BrokerConnectionError`` vor dem Task-Kontext (DLX-Route eines nicht
      dekodierbaren Bodys): WARNING ohne Entweichen.
- [x] ``CancelledError`` (Shutdown) geht weiter durch die Task.
"""

from __future__ import annotations

import asyncio

import pytest

from swarmq.exceptions import BrokerConnectionError
from tests.support.messages import make_message
from tests.unit.worker._helpers import _default_payload

pytestmark = pytest.mark.timeout(5)


async def _run_spawned(worker, msg) -> BaseException | None:
    """Wie im Betrieb: ``_on_message`` startet die Task; ihr Ausgang zaehlt."""
    await worker._on_message(msg)
    [task] = worker._active_tasks
    try:
        await task
    except BaseException as exc:  # der Ausgang ist das Pruefobjekt
        return exc
    return None


async def test_lost_channel_on_ack_logs_one_warning_and_does_not_escape(worker, broker, swarmq_logs):
    broker.ack.side_effect = BrokerConnectionError("ack failed, channel or connection lost: closed")
    msg = make_message(_default_payload(task_id="t-settle"))

    escaped = await _run_spawned(worker, msg)

    assert escaped is None
    [entry] = [e for e in swarmq_logs if e["event"] == "delivery_settle_failed"]
    assert entry["log_level"] == "warning"
    assert entry["task_id"] == "t-settle"
    assert entry["task_name"] == "add"
    assert entry["error_type"] == "BrokerConnectionError"
    assert "channel or connection lost" in entry["error"]
    assert "redeliver" in entry["hint"]


async def test_unexpected_error_is_logged_with_task_context_and_does_not_escape(worker, backend, swarmq_logs):
    backend.store_result.side_effect = RuntimeError("store exploded")
    msg = make_message(_default_payload(task_id="t-boom"))

    escaped = await _run_spawned(worker, msg)

    assert escaped is None
    [entry] = [e for e in swarmq_logs if e["event"] == "delivery_handler_failed"]
    assert entry["log_level"] == "error"
    assert entry["task_id"] == "t-boom"
    assert entry["task_name"] == "add"
    assert entry["error_type"] == "RuntimeError"
    assert entry["error"] == "store exploded"


async def test_lost_channel_before_task_context_logs_one_warning(worker, broker, swarmq_logs):
    """Ein nicht dekodierbarer Body geht per DLX-Route; scheitert sie am
    Kanal, ist noch kein Task-Kontext gebunden (task_id unbekannt)."""
    broker.route_via_dlx.side_effect = BrokerConnectionError("route_via_dlx failed, channel or connection lost")
    msg = make_message(body=b"not json")

    escaped = await _run_spawned(worker, msg)

    assert escaped is None
    [entry] = [e for e in swarmq_logs if e["event"] == "delivery_settle_failed"]
    assert entry["log_level"] == "warning"
    assert entry["error_type"] == "BrokerConnectionError"
    assert "task_id" not in entry


async def test_cancelled_error_still_ends_the_task_as_cancelled(worker, backend, swarmq_logs):
    """Shutdown-Pfade (``_abort_teardown``) brechen die Task ab; das muss sie
    weiterhin als abgebrochen beenden, nicht als geloggten Fehler."""
    backend.store_result.side_effect = asyncio.CancelledError()
    msg = make_message(_default_payload(task_id="t-cancel"))

    escaped = await _run_spawned(worker, msg)

    assert isinstance(escaped, asyncio.CancelledError)
    assert not [e for e in swarmq_logs if e["event"] in ("delivery_settle_failed", "delivery_handler_failed")]
