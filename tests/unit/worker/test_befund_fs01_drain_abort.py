"""Nachweis FS-01 (IO-Modus): Ein Task, der den Drain ueberdauert, wird am
Drain-Ende abgebrochen und als "hard timeout after None s" in den
Fehlerpfad geschickt statt neu zugestellt.

Register: docs/befunde/fehlersuche-runde-2-2026-10.md (FS-01; Quellen K-1, N-5).

``_drain_in_flight`` (``worker/shutdown.py``) bricht nach ``drain_until`` den
Handler ab. ``_await_run_result`` (``worker/execution.py``) faengt den
``CancelledError`` ohne ``cancel_reason`` und wertet ihn als Hard-Timeout:
``TASK_FAILED``, Retry-Verbrauch bzw. DLQ, Fehlerergebnis und Workflow-Fail,
obwohl die Nutzer-Doku ("wird abgebrochen und neu zugestellt") das Gegenteil
verspricht.

Offene Produktfrage 1 (Register): Die Tests pruefen die empfohlene Option –
ein Drain-Abbruch ist eine Neuzustellung (``redeliver``/nack mit Requeue oder
gar keine Quittung), KEIN Fehlerergebnis, KEIN Retry-Verbrauch, KEINE DLQ,
KEIN ``TASK_FAILED`` und kein Workflow-Fail. Die CPU-Variante (KTD2) und das
Recycling deckt ein anderer Nachweis ab.

Testliste:
# [x] max_retries=0: Drain-Abbruch -> weder DLQ noch Fehlerergebnis noch
#     TASK_FAILED, Nachricht wird neu zugestellt (nicht bestaetigt)
# [x] max_retries=3: Drain-Abbruch verbraucht keinen Retry (kein retry_later)
# [x] Workflow-Schritt: Drain-Abbruch markiert den Workflow nicht als FAILED
"""

from __future__ import annotations

import asyncio
from typing import Any, override
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.config import SwarmQConfig
from swarmq.headers import HEADER_WORKFLOW_ID, HEADER_WORKFLOW_STEP
from swarmq.registry import TaskRegistry
from swarmq.serialization import decode_result
from swarmq.signals import Signal
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker, WorkerState
from tests.support.messages import make_message
from tests.unit.worker._helpers import _default_payload

pytestmark = pytest.mark.timeout(5)

_WF_ID = "01890a5d-ac96-774b-bcce-b302099a8057"
_REASON = "FS-01: offen - Drain-Abbruch wird als Hard-Timeout-Fehler gewertet statt neu zugestellt"

# Pro Test neu angelegt (ein Event ist an die Loop des Tests gebunden).
_started: dict[str, asyncio.Event] = {}


class Fs01LongRunner(Task):
    """Laeuft laenger als jeder Drain: wartet auf ein nie gesetztes Event."""

    task_info = TaskInfo(name="fs01_long_runner", queues=["default"])

    @override
    async def run(self) -> None:
        _started["event"].set()
        await asyncio.Event().wait()


class Fs01RetryingLongRunner(Task):
    task_info = TaskInfo(name="fs01_retrying_long_runner", queues=["default"], max_retries=3)

    @override
    async def run(self) -> None:
        _started["event"].set()
        await asyncio.Event().wait()


def _drain_worker(broker: Any, backend: Any, task: type[Task]) -> tuple[Worker, list[Any]]:
    """Worker mit ``shutdown_timeout=0``: der Drain bricht sofort ab."""
    registry = TaskRegistry()
    registry.register(task)
    config = SwarmQConfig(
        broker_url="amqp://localhost",
        backend_url="valkey://localhost",
        concurrency=2,
        queues=["default"],
        shutdown_timeout=0,
    )
    worker = Worker(broker=broker, backend=backend, registry=registry, config=config)
    worker._workflow_advancer = MagicMock()
    worker._workflow_advancer.advance = AsyncMock()
    worker._workflow_advancer.fail = AsyncMock()
    failed: list[Any] = []

    async def on_failed(**kwargs: Any) -> None:
        failed.append(kwargs)

    worker._signal_dispatcher.connect(Signal.TASK_FAILED, on_failed)
    worker._state = WorkerState.RUNNING
    return worker, failed


async def _run_into_drain_abort(worker: Worker, message: Any) -> None:
    started = _started["event"] = asyncio.Event()
    await worker._on_message(message)
    await started.wait()
    worker._request_shutdown()
    await worker._execute_shutdown()


def _error_results(backend: Any) -> list[dict[str, Any]]:
    stored = [decode_result(c.args[1]) for c in backend.store_result.await_args_list]
    return [r for r in stored if r.get("is_error")]


@pytest.mark.befund("FS-01")
@pytest.mark.xfail(strict=True, reason=_REASON)
async def test_drain_abort_redelivers_without_error_result_or_dlq(broker: Any, backend: Any) -> None:
    worker, failed = _drain_worker(broker, backend, Fs01LongRunner)
    message = make_message(_default_payload(task_id="fs01-t1", name="fs01_long_runner", args=[], kwargs={}))

    await _run_into_drain_abort(worker, message)

    assert _error_results(backend) == [], "Drain-Abbruch schreibt ein Fehlerergebnis"
    assert broker.dead_letter.await_count == 0, "Drain-Abbruch schickt die Nachricht in die DLQ"
    assert failed == [], "Drain-Abbruch feuert TASK_FAILED"
    broker.ack.assert_not_awaited()
    assert broker.redeliver.await_count + broker.nack.await_count <= 1


@pytest.mark.befund("FS-01")
@pytest.mark.xfail(strict=True, reason=_REASON)
async def test_drain_abort_does_not_consume_a_retry(broker: Any, backend: Any) -> None:
    worker, failed = _drain_worker(broker, backend, Fs01RetryingLongRunner)
    message = make_message(_default_payload(task_id="fs01-t2", name="fs01_retrying_long_runner", args=[], kwargs={}))

    await _run_into_drain_abort(worker, message)

    assert broker.retry_later.await_count == 0, "Drain-Abbruch verbraucht einen Retry"
    assert broker.dead_letter.await_count == 0
    assert failed == []
    broker.ack.assert_not_awaited()


@pytest.mark.befund("FS-01")
@pytest.mark.xfail(strict=True, reason=_REASON)
async def test_drain_abort_does_not_fail_the_workflow(broker: Any, backend: Any) -> None:
    worker, _failed = _drain_worker(broker, backend, Fs01LongRunner)
    message = make_message(
        _default_payload(task_id="fs01-t3", name="fs01_long_runner", args=[], kwargs={}),
        headers={HEADER_WORKFLOW_ID: _WF_ID, HEADER_WORKFLOW_STEP: "/0"},
    )

    await _run_into_drain_abort(worker, message)

    advancer: Any = worker._workflow_advancer
    advancer.fail.assert_not_awaited()
    assert _error_results(backend) == [], "Workflow-Schritt bekommt bei Drain-Abbruch ein Fehlerergebnis"
