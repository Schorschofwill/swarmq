"""Lebenszyklus-Events je Task (Plan 2026-10-06-0831, U11: R16, KTD8, KTD12).

Alle Events kommen vom Logger ``swarmq.task``: ``task_received`` und
``task_started`` auf dem Lebenszyklus-Level (Default DEBUG), ``task_succeeded``,
``task_failed`` und ``task_cancelled`` auf INFO.

Test-Liste:
- [x] Ein erfolgreicher Task ergibt ``task_started`` (DEBUG) und danach
      ``task_succeeded`` (INFO) mit ``duration_ms`` >= 0, beide von ``swarmq.task``.
- [x] Ein fehlschlagender Task ergibt ``task_failed`` (INFO) mit ``error_type``,
      ``error`` und ``duration_ms``.
- [x] Ein abgebrochener Task ergibt ``task_cancelled`` (INFO).
- [x] Mit Lebenszyklus-Level INFO erscheint ``task_started`` auf INFO.
- [x] Die Pipeline meldet ``task_received`` (DEBUG) vor ``task_started`` und
      ``task_succeeded``, mit Task-Kontext.
- [x] Mit Lebenszyklus-Level INFO erscheint ``task_received`` auf INFO.
- [x] Ein unbekannter Task (``task_not_found``) ergibt ``task_received``, aber
      kein ``task_started``.
- [x] Ein fehlschlagender Task mit Retry ergibt ``task_failed`` und danach
      ``task_retrying``.
- [x] ``--logger-level swarmq.task=WARNING`` unterdrueckt die Lebenszyklus-Events,
      ``fire_and_forget_error`` aus ``swarmq.worker.execution`` bleibt sichtbar.
- Compute-Pool: in ``compute/test_executor.py`` und ``compute/test_child.py``.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.config import SwarmQConfig
from swarmq.logging import configure_logging
from swarmq.logging_settings import LoggingSettings
from swarmq.middleware import MiddlewareChain
from swarmq.task import DeliveryMode, Task, TaskInfo
from swarmq.worker import Worker
from swarmq.worker.execution import TaskExecutor
from swarmq.worker.gates import GateContext, GateDeps
from tests.support.messages import make_message
from tests.unit.worker._helpers import AddTask, _default_payload

pytestmark = [pytest.mark.timeout(5)]

_LIFECYCLE = ("task_received", "task_started", "task_succeeded", "task_failed", "task_cancelled")


class Succeeds(Task):
    task_info = TaskInfo(name="lifecycle_succeeds", queues=["q"])

    async def run(self) -> str:
        return "ok"


def _deps(backend: Any = None, **cfg: Any) -> GateDeps:
    config = SwarmQConfig(broker_url="amqp://x", backend_url="redis://x", cancel_poll_interval=0.01, **cfg)
    if backend is None:
        backend = MagicMock()
        backend.is_cancelled = AsyncMock(return_value=False)
    return GateDeps(
        broker=MagicMock(), backend=backend, config=config, signals=MagicMock(dispatch=AsyncMock()),
        registry=MagicMock(), rate_limiter=lambda: None, lock_manager=lambda: None,
        finalize_schedfire=AsyncMock(), maybe_release_unique=AsyncMock(),
        extract_valid_workflow_id=lambda _m: None,
        claim_schedfire=lambda _m, _t: None, register_lock=lambda _h, _e: None,
    )


def _ctx(task_class: type) -> GateContext:
    return GateContext(
        task_id="t1", task_name=task_class.task_info.name, queue="q", task_info=task_class.task_info,
        task_class=task_class, args=[], kwargs={},
    )


def _lifecycle(logs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [e for e in logs if e["event"] in _LIFECYCLE]


async def _execute(task_class: type, deps: GateDeps | None = None) -> None:
    await TaskExecutor(deps or _deps()).execute(_ctx(task_class), {"retry_count": 0}, MiddlewareChain([]))


async def test_successful_task_reports_started_then_succeeded_with_duration(swarmq_logs):
    await _execute(Succeeds)

    started, succeeded = _lifecycle(swarmq_logs)
    assert (started["event"], started["log_level"]) == ("task_started", "debug")
    assert (succeeded["event"], succeeded["log_level"]) == ("task_succeeded", "info")
    assert succeeded["duration_ms"] >= 0
    assert {started["logger"], succeeded["logger"]} == {"swarmq.task"}


class Fails(Task):
    task_info = TaskInfo(name="lifecycle_fails", queues=["q"])

    async def run(self) -> None:
        raise ValueError("boom")


async def test_failing_task_reports_failed_with_error_fields_and_duration(swarmq_logs):
    await _execute(Fails)

    [failed] = [e for e in swarmq_logs if e["event"] == "task_failed"]
    assert failed["log_level"] == "info"
    assert failed["logger"] == "swarmq.task"
    assert (failed["error_type"], failed["error"]) == ("ValueError", "boom")
    assert failed["duration_ms"] >= 0


class WaitsForCancel(Task):
    task_info = TaskInfo(name="lifecycle_waits_for_cancel", queues=["q"])

    async def run(self) -> None:
        await asyncio.Event().wait()  # endet nur durch den Cancel-Monitor


async def test_cancelled_task_reports_cancelled_on_info(swarmq_logs):
    backend = MagicMock()
    backend.is_cancelled = AsyncMock(return_value=True)

    await _execute(WaitsForCancel, _deps(backend))

    [cancelled] = [e for e in swarmq_logs if e["event"] == "task_cancelled"]
    assert (cancelled["log_level"], cancelled["logger"]) == ("info", "swarmq.task")
    assert cancelled["duration_ms"] >= 0


async def test_lifecycle_level_info_raises_task_started_to_info(swarmq_logs):
    await _execute(Succeeds, _deps(log_lifecycle_level="INFO"))

    [started] = [e for e in swarmq_logs if e["event"] == "task_started"]
    assert started["log_level"] == "info"


def _message(name: str = "add", *, task_id: str = "t-life", headers: dict | None = None) -> Any:
    args = [1, 2] if name == "add" else []
    msg = make_message(_default_payload(task_id=task_id, name=name, args=args, kwargs={}), headers=headers)
    msg.queue = "default"
    return msg


async def test_pipeline_reports_received_before_started_and_succeeded_with_task_context(worker, swarmq_logs):
    await worker._handle_message(_message())

    events = _lifecycle(swarmq_logs)
    assert [e["event"] for e in events] == ["task_received", "task_started", "task_succeeded"]
    received = events[0]
    assert (received["log_level"], received["logger"]) == ("debug", "swarmq.task")
    assert (received["task_id"], received["task_name"], received["queue"]) == ("t-life", "add", "default")


async def test_pipeline_lifecycle_level_info_raises_task_received_to_info(broker, backend, registry, swarmq_logs):
    registry.register(AddTask)
    config = SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x", log_lifecycle_level="INFO")
    worker = Worker(broker=broker, backend=backend, registry=registry, config=config)

    await worker._handle_message(_message())

    [received] = [e for e in swarmq_logs if e["event"] == "task_received"]
    assert received["log_level"] == "info"


async def test_unknown_task_is_received_but_never_started(worker, swarmq_logs):
    await worker._handle_message(_message("not_registered"))

    events = [e["event"] for e in swarmq_logs]
    assert "task_not_found" in events
    assert [e["event"] for e in _lifecycle(swarmq_logs)] == ["task_received"]


class FailsWithRetry(Task):
    task_info = TaskInfo(name="lifecycle_fails_with_retry", queues=["default"], max_retries=3)

    async def run(self) -> None:
        raise ValueError("boom")


async def test_failing_task_with_retry_reports_failed_then_retrying(worker, registry, swarmq_logs):
    registry.register(FailsWithRetry)

    await worker._handle_message(_message("lifecycle_fails_with_retry"))

    events = [e["event"] for e in swarmq_logs if e["event"] in ("task_failed", "task_retrying")]
    assert events == ["task_failed", "task_retrying"]


class FailsFireAndForget(Task):
    task_info = TaskInfo(name="lifecycle_faf_fails", queues=["q"], delivery=DeliveryMode.FIRE_AND_FORGET)

    async def run(self) -> None:
        raise ValueError("boom")


@pytest.fixture()
def task_logger_at_warning():
    """``--logger-level swarmq.task=WARNING`` ueber ``configure_logging``;
    das Level von ``swarmq.task`` wird danach zurueckgesetzt."""
    configure_logging(LoggingSettings(logger_levels=(("swarmq.task", "WARNING"),)))
    try:
        yield
    finally:
        logging.getLogger("swarmq.task").setLevel(logging.NOTSET)


async def test_logger_level_warning_for_swarmq_task_silences_only_the_lifecycle(
    task_logger_at_warning, swarmq_logs
):
    await _execute(FailsFireAndForget)

    assert _lifecycle(swarmq_logs) == []
    [faf] = [e for e in swarmq_logs if e["event"] == "fire_and_forget_error"]
    assert faf["logger"] == "swarmq.worker.execution"
