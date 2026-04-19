"""Unit tests for R4.4 Task Expiry (max_age).

Covers TaskInfo validation + the _check_max_age_expired worker helper
in isolation. Full E2E (schedule expired-age task, verify DLQ + signal
fire) lives in tests/e2e/test_task_expiry.py.
"""

from __future__ import annotations

import time
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.config import SwarmQConfig
from swarmq.headers import HEADER_ENQUEUED_AT
from swarmq.signals import Signal, SignalDispatcher
from swarmq.task import TaskInfo
from swarmq.worker import Worker, WorkerState


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
    broker.publish_to_dead_letter_queue = AsyncMock()
    broker.ack = AsyncMock()
    backend = MagicMock()
    backend.store_result = AsyncMock()
    registry = MagicMock()
    registry.get_all = MagicMock(return_value=[])
    w = Worker(broker, backend, registry, cfg)
    w._state = WorkerState.RUNNING
    return w


def _make_message(enqueued_at: int | None) -> MagicMock:
    msg = MagicMock()
    msg.body = b'{"task": {"id": "t1", "name": "t", "queue": "q"}, "args": [], "kwargs": {}}'
    msg.headers = {}
    if enqueued_at is not None:
        msg.headers[HEADER_ENQUEUED_AT] = enqueued_at
    return msg


class TestCheckMaxAgeExpired:
    async def test_returns_false_when_max_age_not_set(self):
        w = _make_worker()
        ti = TaskInfo(name="t", queues=["q"])
        msg = _make_message(int(time.time()) - 99999)
        handled = await w._check_max_age_expired(msg, ti, "q", "id", "t")
        assert handled is False
        w._broker.publish_to_dead_letter_queue.assert_not_awaited()

    async def test_returns_false_when_header_missing(self):
        """Pre-R4.4 producers (or external) omit the header — treat as
        non-expiring, let normal retry/delivery-limit safeguard handle
        pathological loops."""
        w = _make_worker()
        ti = TaskInfo(name="t", queues=["q"], max_age=60)
        msg = _make_message(enqueued_at=None)
        handled = await w._check_max_age_expired(msg, ti, "q", "id", "t")
        assert handled is False

    async def test_returns_false_when_header_corrupt(self):
        w = _make_worker()
        ti = TaskInfo(name="t", queues=["q"], max_age=60)
        msg = _make_message(0)
        msg.headers[HEADER_ENQUEUED_AT] = "not-a-number"
        handled = await w._check_max_age_expired(msg, ti, "q", "id", "t")
        assert handled is False

    async def test_fresh_message_not_expired(self):
        w = _make_worker()
        ti = TaskInfo(name="t", queues=["q"], max_age=60)
        msg = _make_message(int(time.time()) - 5)  # 5s old, limit 60
        handled = await w._check_max_age_expired(msg, ti, "q", "id", "t")
        assert handled is False

    async def test_expired_message_routes_to_dlq_and_fires_signal(self):
        w = _make_worker()
        # Capture TASK_EXPIRED payload.
        captured: list[dict] = []

        @w._signal_dispatcher.on_signal(Signal.TASK_EXPIRED)
        async def _cap(**kw):
            captured.append(kw)

        ti = TaskInfo(name="t", queues=["q"], max_age=60)
        msg = _make_message(int(time.time()) - 120)  # 120s old, limit 60
        handled = await w._check_max_age_expired(msg, ti, "q", "id-xyz", "t")
        assert handled is True
        w._broker.publish_to_dead_letter_queue.assert_awaited_once()
        w._broker.ack.assert_awaited_once()
        assert len(captured) == 1
        assert captured[0]["task_id"] == "id-xyz"
        assert captured[0]["max_age_s"] == 60
        assert captured[0]["age_s"] >= 120

    async def test_expired_stores_error_result_for_callers(self):
        w = _make_worker()
        ti = TaskInfo(name="t", queues=["q"], max_age=10)
        msg = _make_message(int(time.time()) - 30)
        await w._check_max_age_expired(msg, ti, "q", "id", "t")
        w._backend.store_result.assert_awaited_once()
        call = w._backend.store_result.await_args
        # store_result is called as store_result(task_id, result_dict, ...)
        # via kwargs or positional — support both shapes.
        if call.args and len(call.args) >= 2:
            result = call.args[1]
        else:
            result = call.kwargs.get("result_dict") or call.args[0]
        assert result["is_error"] is True
        assert "expired" in result["error_message"].lower()

    async def test_expired_dlq_route_tolerates_backend_failure(self):
        w = _make_worker()
        from swarmq.exceptions import BackendConnectionError
        w._backend.store_result = AsyncMock(
            side_effect=BackendConnectionError("valkey down"),
        )
        ti = TaskInfo(name="t", queues=["q"], max_age=10)
        msg = _make_message(int(time.time()) - 30)
        handled = await w._check_max_age_expired(msg, ti, "q", "id", "t")
        assert handled is True
        # DLQ + ack still happen even though store_result failed.
        w._broker.publish_to_dead_letter_queue.assert_awaited_once()
        w._broker.ack.assert_awaited_once()
