"""Unit tests for R4.4 Task Expiry (max_age).

Covers TaskInfo validation + the Expiry-Gate (``worker/gates/expiry.py``,
Plan U7) in isolation. Full E2E (schedule expired-age task, verify DLQ + signal
fire) lives in tests/e2e/test_task_expiry.py.
"""

from __future__ import annotations

import time
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
        w._broker.dead_letter.assert_not_awaited()

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
        captured: list[dict] = []

        @w._signal_dispatcher.on_signal(Signal.TASK_EXPIRED)
        async def _cap(**kw):
            captured.append(kw)

        ti = TaskInfo(name="t", queues=["q"], max_age=60)
        msg = _expiry_message(int(time.time()) - 120)  # 120s old, limit 60
        handled = await _apply(w, msg, ti, task_id="id-xyz")
        assert handled == Terminal.release_only()  # BZ-22 (MR 3)
        w._broker.dead_letter.assert_awaited_once()
        # U8: retry_later/dead_letter bestätigen das Original selbst
        w._broker.ack.assert_not_awaited()
        assert len(captured) == 1
        assert captured[0]["task_id"] == "id-xyz"
        assert captured[0]["max_age_s"] == 60
        assert captured[0]["age_s"] >= 120

    async def test_expired_stores_error_result_for_callers(self):
        w = _make_worker()
        ti = TaskInfo(name="t", queues=["q"], max_age=10)
        msg = _expiry_message(int(time.time()) - 30)
        await _apply(w, msg, ti, task_id="id")
        w._backend.store_result.assert_awaited_once()
        call = w._backend.store_result.await_args
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
        w._backend.store_result.assert_not_awaited()
        # DLQ + ack continue regardless.
        w._broker.dead_letter.assert_awaited_once()
        # U8: retry_later/dead_letter bestätigen das Original selbst
        w._broker.ack.assert_not_awaited()

    async def test_expired_dlq_route_tolerates_backend_failure(self):
        w = _make_worker()
        from swarmq.exceptions import BackendConnectionError

        w._backend.store_result = AsyncMock(
            side_effect=BackendConnectionError("valkey down"),
        )
        ti = TaskInfo(name="t", queues=["q"], max_age=10)
        msg = _expiry_message(int(time.time()) - 30)
        handled = await _apply(w, msg, ti, task_id="id")
        assert handled == Terminal.release_only()  # BZ-22 (MR 3)
        # DLQ + ack still happen even though store_result failed.
        w._broker.dead_letter.assert_awaited_once()
        # U8: retry_later/dead_letter bestätigen das Original selbst
        w._broker.ack.assert_not_awaited()


class TestExpiryErrorFields:
    """B23 (0.9.1): Fehler-Logs des Expiry-Gates tragen ``error_type``/``error``.

    Test-Liste:
    - [x] ``task_expired_result_store_failed`` (WARNING) mit Fehlerfeldern.
    - [x] ``task_expired_dlq_publish_failed`` (WARNING) mit Fehlerfeldern.
    - [x] ``task_expired_nack_also_failed`` (ERROR) mit den Fehlerfeldern des Nack.
    """

    async def test_result_store_failure_carries_error_fields(self, swarmq_logs):
        from swarmq.exceptions import BackendConnectionError

        w = _make_worker()
        w._backend.store_result = AsyncMock(side_effect=BackendConnectionError("valkey down"))
        ti = TaskInfo(name="t", queues=["q"], max_age=10)

        await _apply(w, _expiry_message(int(time.time()) - 30), ti, task_id="id")

        [entry] = [e for e in swarmq_logs if e["event"] == "task_expired_result_store_failed"]
        assert entry["log_level"] == "warning"
        assert entry["error_type"] == "BackendConnectionError" and entry["error"] == "valkey down"

    async def test_dlq_publish_failure_carries_error_fields(self, swarmq_logs):
        from swarmq.exceptions import BrokerConnectionError

        w = _make_worker()
        w._broker.dead_letter = AsyncMock(side_effect=BrokerConnectionError("publish nacked"))
        w._broker.redeliver = AsyncMock()
        ti = TaskInfo(name="t", queues=["q"], max_age=10)

        await _apply(w, _expiry_message(int(time.time()) - 30), ti, task_id="id")

        [entry] = [e for e in swarmq_logs if e["event"] == "task_expired_dlq_publish_failed"]
        assert entry["log_level"] == "warning"
        assert entry["error_type"] == "BrokerConnectionError" and entry["error"] == "publish nacked"

    async def test_double_failure_carries_error_fields_of_the_nack(self, swarmq_logs):
        from swarmq.exceptions import BrokerConnectionError

        w = _make_worker()
        w._broker.dead_letter = AsyncMock(side_effect=BrokerConnectionError("publish nacked"))
        w._broker.redeliver = AsyncMock(side_effect=BrokerConnectionError("redeliver failed, channel lost"))
        ti = TaskInfo(name="t", queues=["q"], max_age=10)

        await _apply(w, _expiry_message(int(time.time()) - 30), ti, task_id="id")

        [entry] = [e for e in swarmq_logs if e["event"] == "task_expired_nack_also_failed"]
        assert entry["log_level"] == "error"
        assert entry["error_type"] == "BrokerConnectionError"
        assert entry["error"] == "redeliver failed, channel lost"


class TestExpiredWorkflowStep:
    """BZ-13 (Plan 0.12.0, MR 3): ein abgelaufener Workflow-Schritt markiert
    seinen Workflow als fehlgeschlagen.

    Test-Liste:
    - [x] Markierung scheitert voruebergehend (Valkey weg) -> neu zugestellt,
          keine DLQ, kein ``TASK_EXPIRED`` (das Zustelllimit begrenzt das)
    """

    async def test_fail_mark_outage_redelivers_instead_of_dead_lettering(self):
        from swarmq.exceptions import BackendConnectionError, TaskNotFoundError
        from swarmq.headers import HEADER_WORKFLOW_ID, HEADER_WORKFLOW_STEP
        from swarmq.worker.outcome import Redelivered

        w = _make_worker()
        w._broker.redeliver = AsyncMock()
        w._registry.get = MagicMock(side_effect=TaskNotFoundError("t"))
        w._workflow_advancer = MagicMock()
        w._workflow_advancer.fail = AsyncMock(side_effect=BackendConnectionError("valkey down"))
        expired: list[dict] = []

        @w._signal_dispatcher.on_signal(Signal.TASK_EXPIRED)
        async def _cap(**kw):
            expired.append(kw)

        msg = _expiry_message(int(time.time()) - 120)
        msg.headers[HEADER_WORKFLOW_ID] = "01890a5d-ac96-774b-bcce-b302099a8057"
        msg.headers[HEADER_WORKFLOW_STEP] = "/1"

        handled = await _apply(w, msg, TaskInfo(name="t", queues=["q"], max_age=60), task_id="id")

        assert handled is Redelivered
        w._broker.redeliver.assert_awaited_once_with(msg)
        w._broker.dead_letter.assert_not_awaited()
        assert expired == []


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
        w._broker.dead_letter = AsyncMock(side_effect=BrokerConnectionError("down"))
        w._broker.redeliver = AsyncMock()
        msg = _expiry_message(int(time.time()) - 120)

        handled = await _apply(w, msg, TaskInfo(name="t", queues=["q"], max_age=60), task_id="id")

        assert handled == Terminal.never_release()
        w._broker.redeliver.assert_awaited_once_with(msg)
