"""Unit tests for R4.4 Task Expiry (max_age).

Covers TaskInfo validation + the Expiry-Gate (``worker/gates/expiry.py``,
Plan U7) in isolation. Full E2E (schedule expired-age task, verify DLQ + signal
fire) lives in tests/e2e/test_task_expiry.py.
"""

from __future__ import annotations

import time
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.config import SwarmQConfig
from swarmq.headers import HEADER_ENQUEUED_AT
from swarmq.serialization import decode_result
from swarmq.signals import Signal
from swarmq.task import TaskInfo
from swarmq.worker import Worker, WorkerState
from swarmq.worker.gates import GateContext
from swarmq.worker.outcome import Continue, Terminal
from tests.support.messages import make_message
from tests.unit.worker._helpers import as_mock


class TestTaskInfoMaxAge:
    def test_default_is_none(self):
        ti = TaskInfo(name="t", queues=["q"])
        assert ti.max_age is None

    def test_valid_positive_value(self):
        ti = TaskInfo(name="t", queues=["q"], max_age=60)
        assert ti.max_age == 60

    def test_zero_rejected(self):
        with pytest.raises(ValueError, match="max_age must be > 0"):
            TaskInfo(name="t", queues=["q"], max_age=0)

    def test_negative_rejected(self):
        with pytest.raises(ValueError, match="max_age must be > 0"):
            TaskInfo(name="t", queues=["q"], max_age=-5)


def _make_worker() -> Worker:
    cfg = SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x")
    broker = MagicMock()
    broker.dead_letter = AsyncMock()
    broker.ack = AsyncMock()
    backend = MagicMock()
    backend.store_result = AsyncMock()
    registry = MagicMock()
    registry.get_all = MagicMock(return_value=[])
    w = Worker(broker, backend, registry, cfg)
    w._state = WorkerState.RUNNING
    return w


_EXPIRY_BODY = b'{"task": {"id": "t1", "name": "t", "queue": "q"}, "args": [], "kwargs": {}}'


def _expiry_message(enqueued_at: int | None) -> MagicMock:
    return make_message(body=_EXPIRY_BODY, enqueued_at=enqueued_at)


async def _apply(w: Worker, msg: MagicMock, ti: TaskInfo, *, task_id: str):
    ctx = GateContext(task_id=task_id, task_name="t", queue="q", task_info=ti)
    return await w._expiry_gate.apply(msg, ctx)


class TestCheckMaxAgeExpired:
    async def test_returns_false_when_max_age_not_set(self):
        w = _make_worker()
        ti = TaskInfo(name="t", queues=["q"])
        msg = _expiry_message(int(time.time()) - 99999)
        handled = await _apply(w, msg, ti, task_id="id")
        assert handled is Continue
        as_mock(w._broker).dead_letter.assert_not_awaited()

    async def test_returns_false_when_header_missing(self):
        """Pre-R4.4 producers (or external) omit the header — treat as
        non-expiring, let normal retry/delivery-limit safeguard handle
        pathological loops."""
        w = _make_worker()
        ti = TaskInfo(name="t", queues=["q"], max_age=60)
        msg = _expiry_message(enqueued_at=None)
        handled = await _apply(w, msg, ti, task_id="id")
        assert handled is Continue

    async def test_returns_false_when_header_corrupt(self):
        w = _make_worker()
        ti = TaskInfo(name="t", queues=["q"], max_age=60)
        msg = _expiry_message(0)
        msg.headers[HEADER_ENQUEUED_AT] = "not-a-number"
        handled = await _apply(w, msg, ti, task_id="id")
        assert handled is Continue

    async def test_fresh_message_not_expired(self):
        w = _make_worker()
        ti = TaskInfo(name="t", queues=["q"], max_age=60)
        msg = _expiry_message(int(time.time()) - 5)  # 5s old, limit 60
        handled = await _apply(w, msg, ti, task_id="id")
        assert handled is Continue

    async def test_expired_message_routes_to_dlq_and_fires_signal(self):
        w = _make_worker()
        # Capture TASK_EXPIRED payload.
        captured: list[dict[str, Any]] = []

        @w._signal_dispatcher.on_signal(Signal.TASK_EXPIRED)
        async def _cap(**kw):
            captured.append(kw)

        ti = TaskInfo(name="t", queues=["q"], max_age=60)
        msg = _expiry_message(int(time.time()) - 120)  # 120s old, limit 60
        handled = await _apply(w, msg, ti, task_id="id-xyz")
        assert handled == Terminal.release_only()  # BZ-22 (MR 3)
        as_mock(w._broker).dead_letter.assert_awaited_once()
        # U8: retry_later/dead_letter bestätigen das Original selbst
        as_mock(w._broker).ack.assert_not_awaited()
        assert len(captured) == 1
        assert captured[0]["task_id"] == "id-xyz"
        assert captured[0]["max_age_s"] == 60
        assert captured[0]["age_s"] >= 120

    @pytest.mark.entscheidung("E-031")
    async def test_expired_stores_error_result_for_callers(self):
        w = _make_worker()
        ti = TaskInfo(name="t", queues=["q"], max_age=10)
        msg = _expiry_message(int(time.time()) - 30)
        await _apply(w, msg, ti, task_id="id")
        as_mock(w._backend).store_result.assert_awaited_once()
        call = as_mock(w._backend).store_result.await_args
        # Seit dem U15-Bugfix gehen kodierte Bytes in den Store (vorher ein
        # dict, das gegen echtes Valkey still scheiterte).
        raw = call.args[1]
        assert isinstance(raw, bytes)
        result = decode_result(raw)
        assert result["is_error"] is True
        assert "expired" in result["error_message"].lower()

    async def test_expired_skips_result_store_when_ttl_zero(self):
        """Härtung R2 P2: ``result_ttl=0`` (fire-and-forget) auf dem
        Expired-Pfad erzeugte ``SET ... PX 0`` — Valkey lehnt das mit
        'invalid expire time' ab und das Error-Result ging als
        scheinbarer Backend-Fehler verloren. FAF heißt: kein Result-
        Store, wie auf dem Haupt-Pfad."""
        w = _make_worker()
        w._config.result_ttl = 0
        ti = TaskInfo(name="t", queues=["q"], max_age=10)
        msg = _expiry_message(int(time.time()) - 30)

        handled = await _apply(w, msg, ti, task_id="id")

        assert handled == Terminal.release_only()  # BZ-22 (MR 3)
        as_mock(w._backend).store_result.assert_not_awaited()
        # DLQ + ack continue regardless.
        as_mock(w._broker).dead_letter.assert_awaited_once()
        # U8: retry_later/dead_letter bestätigen das Original selbst
        as_mock(w._broker).ack.assert_not_awaited()

    async def test_expired_dlq_route_tolerates_backend_failure(self):
        w = _make_worker()
        from swarmq.exceptions import BackendConnectionError

        as_mock(w._backend).store_result = AsyncMock(
            side_effect=BackendConnectionError("valkey down"),
        )
        ti = TaskInfo(name="t", queues=["q"], max_age=10)
        msg = _expiry_message(int(time.time()) - 30)
        handled = await _apply(w, msg, ti, task_id="id")
        assert handled == Terminal.release_only()  # BZ-22 (MR 3)
        # DLQ + ack still happen even though store_result failed.
        as_mock(w._broker).dead_letter.assert_awaited_once()
        # U8: retry_later/dead_letter bestätigen das Original selbst
        as_mock(w._broker).ack.assert_not_awaited()


class TestExpiryErrorFields:
    """B23 (0.9.1): Fehler-Logs des Expiry-Gates tragen ``error_type``/``error``.

    Test-Liste:
    - [x] ``task_expired_result_store_failed`` (WARNING) mit Fehlerfeldern.
    - [x] ``task_expired_dlq_publish_failed`` (WARNING) mit Fehlerfeldern.
    - [x] Doppelfehler: ``delivery_settle_failed`` (WARNING, seit F4-18 statt ERROR
      ``task_expired_nack_also_failed``) mit den Fehlerfeldern des Nack.
    """

    async def test_result_store_failure_carries_error_fields(self, swarmq_logs):
        from swarmq.exceptions import BackendConnectionError

        w = _make_worker()
        as_mock(w._backend).store_result = AsyncMock(side_effect=BackendConnectionError("valkey down"))
        ti = TaskInfo(name="t", queues=["q"], max_age=10)

        await _apply(w, _expiry_message(int(time.time()) - 30), ti, task_id="id")

        [entry] = [e for e in swarmq_logs if e["event"] == "task_expired_result_store_failed"]
        assert entry["log_level"] == "warning"
        assert entry["error_type"] == "BackendConnectionError"
        assert entry["error"] == "valkey down"

    async def test_dlq_publish_failure_carries_error_fields(self, swarmq_logs):
        from swarmq.exceptions import BrokerConnectionError

        w = _make_worker()
        as_mock(w._broker).dead_letter = AsyncMock(side_effect=BrokerConnectionError("publish nacked"))
        as_mock(w._broker).redeliver = AsyncMock()
        ti = TaskInfo(name="t", queues=["q"], max_age=10)

        await _apply(w, _expiry_message(int(time.time()) - 30), ti, task_id="id")

        [entry] = [e for e in swarmq_logs if e["event"] == "task_expired_dlq_publish_failed"]
        assert entry["log_level"] == "warning"
        assert entry["error_type"] == "BrokerConnectionError"
        assert entry["error"] == "publish nacked"

    @pytest.mark.entscheidung("E-041")
    async def test_indeterminate_dlq_publish_redelivers_the_expired_original(self):
        """E-041 (Nachtrag 2026-10-17, Pfad Ablauf-DLQ): endet der Publish der
        Ablauf-Kopie unbestimmt, bleibt das Original unbestätigt und wird neu
        zugestellt (``task_expired_dlq_publish_failed``)."""
        from swarmq.exceptions import PublishIndeterminate

        w = _make_worker()
        as_mock(w._broker).dead_letter = AsyncMock(side_effect=PublishIndeterminate("deadline"))
        as_mock(w._broker).redeliver = AsyncMock()
        ti = TaskInfo(name="t", queues=["q"], max_age=10)
        msg = _expiry_message(int(time.time()) - 30)

        await _apply(w, msg, ti, task_id="id")

        as_mock(w._broker).redeliver.assert_awaited_once_with(msg)
        as_mock(w._broker).ack.assert_not_awaited()

    @pytest.mark.befund("F4-18")
    async def test_double_failure_is_a_warning_with_the_error_fields_of_the_nack(self, swarmq_logs):
        """F4-18 (0.13.0rc2): scheitert auch das ``nack`` am toten Kanal, stellt
        der Broker die Zustellung neu zu -- selbstheilend, also WARNING
        ``delivery_settle_failed`` statt ERROR ``task_expired_nack_also_failed``.
        Das Flag bleibt (``never_release``)."""
        from swarmq.exceptions import BrokerConnectionError

        w = _make_worker()
        as_mock(w._broker).dead_letter = AsyncMock(side_effect=BrokerConnectionError("publish nacked"))
        as_mock(w._broker).redeliver = AsyncMock(side_effect=BrokerConnectionError("redeliver failed, channel lost"))
        ti = TaskInfo(name="t", queues=["q"], max_age=10)

        handled = await _apply(w, _expiry_message(int(time.time()) - 30), ti, task_id="id")

        assert handled == Terminal.never_release()
        [entry] = [e for e in swarmq_logs if e["event"] == "delivery_settle_failed"]
        assert entry["log_level"] == "warning"
        assert entry["error_type"] == "BrokerConnectionError"
        assert entry["error"] == "redeliver failed, channel lost"
        assert [e["event"] for e in swarmq_logs if e["log_level"] == "error"] == []


class TestExpiredWorkflowStep:
    """BZ-13 (Plan 0.12.0, MR 3): ein abgelaufener Workflow-Schritt markiert
    seinen Workflow als fehlgeschlagen.

    Test-Liste:
    - [x] Markierung scheitert voruebergehend (Valkey weg) -> neu zugestellt,
          keine DLQ, kein ``TASK_EXPIRED`` (das Zustelllimit begrenzt das)
    - [x] F4-01 (U14): Erfolg schon gespeichert -> Advance nachgeholt, Ack
          ohne DLQ-Kopie, kein Fehlerergebnis, kein ``TASK_EXPIRED``,
          Freigabe wie nach der DLQ-Kopie (``release_only``)
    - [x] F4-01: ``StepDone.DEAD_LETTER`` -> DLQ-Kopie ohne Fehlerergebnis
    """

    _WF_ID = "01890a5d-ac96-774b-bcce-b302099a8057"

    def _step(self, w: Worker) -> MagicMock:
        from swarmq.headers import HEADER_WORKFLOW_ID, HEADER_WORKFLOW_STEP

        msg = _expiry_message(int(time.time()) - 120)
        msg.headers[HEADER_WORKFLOW_ID] = self._WF_ID
        msg.headers[HEADER_WORKFLOW_STEP] = "/1"
        return msg

    async def test_stored_success_catches_up_the_advance_and_acks_without_dead_letter(self):
        from swarmq.cancellation import build_result_dict
        from swarmq.exceptions import TaskNotFoundError
        from swarmq.serialization import encode_result

        w = _make_worker()
        as_mock(w._registry).get = MagicMock(side_effect=TaskNotFoundError("t"))
        success = build_result_dict(
            task_id="id", queue="q", task_name="t", is_error=False, is_cancelled=False, result=7
        )
        as_mock(w._backend).get_result = AsyncMock(return_value=encode_result(success))
        w._workflow_advancer = MagicMock(advance=AsyncMock(), fail=AsyncMock())
        expired: list[dict[str, Any]] = []

        @w._signal_dispatcher.on_signal(Signal.TASK_EXPIRED)
        async def _cap(**kw):
            expired.append(kw)

        msg = self._step(w)
        handled = await _apply(w, msg, TaskInfo(name="t", queues=["q"], max_age=60), task_id="id")

        assert handled == Terminal.release_only()
        w._workflow_advancer.advance.assert_awaited_once_with(self._WF_ID, "/1", "id", 7)
        w._workflow_advancer.fail.assert_not_awaited()
        as_mock(w._broker).ack.assert_awaited_once_with(msg)
        as_mock(w._broker).dead_letter.assert_not_awaited()
        as_mock(w._backend).store_result.assert_not_awaited()
        assert expired == []

    async def test_fail_mark_outage_redelivers_instead_of_dead_lettering(self):
        from swarmq.exceptions import BackendConnectionError, TaskNotFoundError
        from swarmq.headers import HEADER_WORKFLOW_ID, HEADER_WORKFLOW_STEP
        from swarmq.worker.outcome import Redelivered

        w = _make_worker()
        as_mock(w._broker).redeliver = AsyncMock()
        as_mock(w._registry).get = MagicMock(side_effect=TaskNotFoundError("t"))
        w._workflow_advancer = MagicMock()
        w._workflow_advancer.fail = AsyncMock(side_effect=BackendConnectionError("valkey down"))
        expired: list[dict[str, Any]] = []

        @w._signal_dispatcher.on_signal(Signal.TASK_EXPIRED)
        async def _cap(**kw):
            expired.append(kw)

        msg = _expiry_message(int(time.time()) - 120)
        msg.headers[HEADER_WORKFLOW_ID] = "01890a5d-ac96-774b-bcce-b302099a8057"
        msg.headers[HEADER_WORKFLOW_STEP] = "/1"

        handled = await _apply(w, msg, TaskInfo(name="t", queues=["q"], max_age=60), task_id="id")

        assert handled is Redelivered
        as_mock(w._broker).redeliver.assert_awaited_once_with(msg)
        as_mock(w._broker).dead_letter.assert_not_awaited()
        assert expired == []

    async def test_unadvanced_success_dead_letters_without_an_error_result(self):
        from swarmq.worker.gates import StepDone

        w = _make_worker()
        w._gate_deps.fail_workflow = AsyncMock(return_value=StepDone.DEAD_LETTER)
        msg = self._step(w)

        handled = await _apply(w, msg, TaskInfo(name="t", queues=["q"], max_age=60), task_id="id")

        assert handled == Terminal.release_only()
        as_mock(w._broker).dead_letter.assert_awaited_once()
        as_mock(w._backend).store_result.assert_not_awaited()


class TestExpiredUniqueRelease:
    """BZ-22 (Plan 0.12.0, MR 3): Freigabe des Unique-Flags nur nach
    erfolgreichem Dead-Letter.

    Test-Liste:
    - [x] Dead-Letter scheitert -> neu zugestellt, Flag bleibt stehen
          (``never_release``); die Neuzustellung gibt es frei
    """

    async def test_failed_dead_letter_keeps_the_flag(self):
        from swarmq.exceptions import BrokerConnectionError

        w = _make_worker()
        as_mock(w._broker).dead_letter = AsyncMock(side_effect=BrokerConnectionError("down"))
        as_mock(w._broker).redeliver = AsyncMock()
        msg = _expiry_message(int(time.time()) - 120)

        handled = await _apply(w, msg, TaskInfo(name="t", queues=["q"], max_age=60), task_id="id")

        assert handled == Terminal.never_release()
        as_mock(w._broker).redeliver.assert_awaited_once_with(msg)
