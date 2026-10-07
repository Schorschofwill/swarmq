"""Fehler-Logs des ``TaskExecutor`` (Plan 2026-10-06-0831, U9, R2/R17).

Fehler kommen mit ``error_type`` (Klassenname) und ``error`` (Text) ueber den
Logger ``swarmq.worker.execution``.
"""

from __future__ import annotations

# Test list:
# 1. Ein fehlschlagender Fire-and-forget-Task loggt ``fire_and_forget_error``
#    auf ERROR mit ``error_type`` und ``error``.
# 2. Ein Backend-Fehler im Cancel-Monitor loggt ``cancel_monitor_error`` auf
#    WARNING mit ``task_id``, ``error_type`` und ``error``.
import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.config import SwarmQConfig
from swarmq.middleware import MiddlewareChain
from swarmq.task import DeliveryMode, Task, TaskInfo
from swarmq.worker.execution import TaskExecutor
from swarmq.worker.gates import GateContext, GateDeps

pytestmark = [pytest.mark.timeout(5)]


class FailingFireAndForget(Task):
    task_info = TaskInfo(name="faf_fail_exec_log", queues=["q"], delivery=DeliveryMode.FIRE_AND_FORGET)

    async def run(self) -> None:
        raise ValueError("boom")


_monitor_failed = asyncio.Event()


class WaitsForMonitorFailure(Task):
    task_info = TaskInfo(name="waits_for_monitor_failure_exec_log", queues=["q"])

    async def run(self) -> None:
        await _monitor_failed.wait()


def _deps(backend: Any = None) -> GateDeps:
    config = SwarmQConfig(broker_url="amqp://x", backend_url="redis://x", cancel_poll_interval=0.01)
    if backend is None:
        backend = MagicMock()
        backend.is_cancelled = AsyncMock(return_value=False)
    return GateDeps(
        broker=MagicMock(), backend=backend, config=config, signals=MagicMock(dispatch=AsyncMock()),
        registry=MagicMock(), rate_limiter=lambda: None, lock_manager=lambda: None,
        finalize_schedfire=AsyncMock(), maybe_release_unique=AsyncMock(),
        extract_valid_workflow_id=lambda _m: None,
        register_lock=lambda _h, _e: None,
    )


def _ctx(task_class: type) -> GateContext:
    return GateContext(
        task_id="t1", task_name=task_class.task_info.name, queue="q", task_info=task_class.task_info,
        task_class=task_class, args=[], kwargs={},
    )


def _entries(logs: list[dict[str, Any]], event: str) -> list[dict[str, Any]]:
    return [e for e in logs if e["logger"] == "swarmq.worker.execution" and e["event"] == event]


async def test_fire_and_forget_error_logs_error_type_and_error(swarmq_logs):
    await TaskExecutor(_deps()).execute(_ctx(FailingFireAndForget), {"retry_count": 0}, MiddlewareChain([]))

    [entry] = _entries(swarmq_logs, "fire_and_forget_error")
    assert entry["log_level"] == "error"
    assert entry["error_type"] == "ValueError"
    assert entry["error"] == "boom"


async def test_cancel_monitor_backend_failure_logs_warning_with_error_fields(swarmq_logs):
    _monitor_failed.clear()

    async def failing_is_cancelled(*_a: Any, **_k: Any) -> bool:
        _monitor_failed.set()
        raise ConnectionError("valkey down")

    backend = MagicMock()
    backend.is_cancelled = failing_is_cancelled
    await TaskExecutor(_deps(backend)).execute(_ctx(WaitsForMonitorFailure), {"retry_count": 0}, MiddlewareChain([]))

    entry = _entries(swarmq_logs, "cancel_monitor_error")[0]
    assert entry["log_level"] == "warning"
    assert entry["task_id"] == "t1"
    assert entry["error_type"] == "ConnectionError"
    assert entry["error"] == "valkey down"
