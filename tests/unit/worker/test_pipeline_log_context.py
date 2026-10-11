"""Task-Kontext per Scope (Plan 2026-10-06-0831, U10: R4, KTD13).

Test-Liste:
- [x] Ein Event innerhalb eines Tasks traegt task_id, task_name, queue,
      worker_id, workflow_id und attempt.
- [x] attempt zaehlt ab 1: retry_count 2 ergibt attempt 3.
- [x] Ein kaputter Retry-Header laesst attempt weg, der Task laeuft trotzdem.
- [x] Ohne Workflow-Header fehlt workflow_id im Eintrag (kein ``None``).
- [x] Mit ungueltigem Workflow-Header fehlt workflow_id ebenso.
- [x] Eine von der App gebundene Variable request_id ist nach dem Task
      wieder vorhanden und waehrend des Tasks sichtbar.
- [x] Nach dem Task ist kein Task-Kontext mehr gebunden (im request_id-Test).
- [x] Zwei parallel laufende Tasks sehen jeweils nur ihren eigenen Kontext.
- [x] Gate-Pfade (cancel, schedfire, delivery_count), die frueh enden,
      hinterlassen keinen Task-Kontext und lassen request_id stehen
      (in ``gates/test_cancel.py``, ``test_schedfire.py``,
      ``test_delivery_count.py``).
- [x] Der Executor bekommt die gueltige workflow_id fuer ``ComputeJob``.
- [x] (verschoben aus ``test_pipeline.py``) worker_id ist die Worker-Identitaet.
"""

from __future__ import annotations

import asyncio
from typing import ClassVar, override

import pytest
import structlog

from swarmq.config import SwarmQConfig
from swarmq.headers import HEADER_WORKFLOW_ID
from swarmq.logging import get_logger
from swarmq.registry import TaskRegistry
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker
from tests.support.messages import make_message
from tests.unit.worker._helpers import _default_payload

_WF_ID = "0190a3c4-5d6e-7f80-9a1b-2c3d4e5f6a7b"

_task_logger = get_logger("swarmq.tests.task_context")


class _LoggingTask(Task):
    task_info = TaskInfo(name="logs_inside", queues=["default"])

    @override
    async def run(self) -> str:
        _task_logger.info("inside_task")
        return "ok"


@pytest.fixture
def ctx_worker(worker, registry):
    registry.register(_LoggingTask)
    return worker


def _inside(entries: list[dict]) -> list[dict]:
    return [e for e in entries if e["event"] == "inside_task"]


@pytest.mark.timeout(5)
async def test_event_inside_a_task_carries_the_full_task_context(ctx_worker, swarmq_logs):
    msg = make_message(
        _default_payload(task_id="t-ctx", name="logs_inside", args=[], kwargs={}),
        headers={HEADER_WORKFLOW_ID: _WF_ID, "x-swarmq-retry-count": 0},
    )
    msg.queue = "default"

    await ctx_worker._handle_message(msg)

    [entry] = _inside(swarmq_logs)
    assert entry["task_id"] == "t-ctx"
    assert entry["task_name"] == "logs_inside"
    assert entry["queue"] == "default"
    assert entry["worker_id"] == ctx_worker._config.worker_identity
    assert entry["workflow_id"] == _WF_ID
    assert entry["attempt"] == 1


@pytest.mark.timeout(5)
async def test_log_context_carries_the_worker_identity_as_string(broker, backend, swarmq_logs):
    """U2 (R3): Der Logkontext trug ``id(self)`` — eine Speicheradresse als
    ``int``, die weder ueber Prozesse hinweg vergleichbar noch mit dem
    ``worker_id`` der Signale identisch war. (Aus ``test_pipeline.py``
    hierher verschoben, U10: Verhalten statt ``_bind_log_context``.)"""
    registry = TaskRegistry()
    registry.register(_LoggingTask)
    config = SwarmQConfig(
        broker_url="amqp://x",
        backend_url="valkey://x",
        worker_id="phx",
        worker_slot=2,
    )
    worker = Worker(broker=broker, backend=backend, registry=registry, config=config)

    await worker._handle_message(make_message(_default_payload(name="logs_inside", args=[], kwargs={})))

    [entry] = _inside(swarmq_logs)
    assert entry["worker_id"] == "phx:2"


def _message(*, task_id: str = "t-ctx", headers: dict | None = None):
    msg = make_message(_default_payload(task_id=task_id, name="logs_inside", args=[], kwargs={}), headers=headers)
    msg.queue = "default"
    return msg


@pytest.mark.timeout(5)
async def test_attempt_counts_from_one_on_top_of_the_retry_count(ctx_worker, swarmq_logs):
    await ctx_worker._handle_message(_message(headers={"x-swarmq-retry-count": 2}))

    [entry] = _inside(swarmq_logs)
    assert entry["attempt"] == 3


@pytest.mark.timeout(5)
async def test_garbage_retry_count_header_leaves_attempt_out_and_still_runs_the_task(ctx_worker, swarmq_logs):
    """Der Header kommt vom Netz: ein kaputter Wert darf den Task nicht
    verhindern -- vor U10 las nur der Fehlerpfad den Header."""
    await ctx_worker._handle_message(_message(headers={"x-swarmq-retry-count": "zwei"}))

    [entry] = _inside(swarmq_logs)
    assert "attempt" not in entry


@pytest.mark.timeout(5)
async def test_without_workflow_header_the_entry_has_no_workflow_id(ctx_worker, swarmq_logs):
    """Festgelegt (U10): fehlend statt ``None`` -- der Helfer laesst
    ``None``-Werte weg."""
    await ctx_worker._handle_message(_message())

    [entry] = _inside(swarmq_logs)
    assert "workflow_id" not in entry
    assert entry["attempt"] == 1


@pytest.mark.timeout(5)
async def test_with_invalid_workflow_header_the_entry_has_no_workflow_id(ctx_worker, swarmq_logs):
    await ctx_worker._handle_message(_message(headers={HEADER_WORKFLOW_ID: "not-a-uuid"}))

    [entry] = _inside(swarmq_logs)
    assert "workflow_id" not in entry


@pytest.mark.timeout(5)
async def test_app_bound_request_id_is_visible_in_the_task_and_survives_it(ctx_worker, swarmq_logs, app_log_context):
    """Bisher loeschte ``clear_contextvars`` die Variablen der App."""
    await ctx_worker._handle_message(_message())

    [entry] = _inside(swarmq_logs)
    assert entry["request_id"] == "req-7"
    assert structlog.contextvars.get_contextvars() == {"request_id": "req-7"}


class _InterleavedTask(Task):
    """Loggt vor und nach einem Warten, das der Test steuert."""

    task_info = TaskInfo(name="interleaved", queues=["default"])
    started: ClassVar[dict[str, asyncio.Event]] = {}
    proceed: ClassVar[dict[str, asyncio.Event]] = {}

    @override
    async def run(self, tag: str) -> str:
        _task_logger.info("before_wait", tag=tag)
        type(self).started[tag].set()
        await type(self).proceed[tag].wait()
        _task_logger.info("after_wait", tag=tag)
        return tag


@pytest.mark.timeout(5)
async def test_two_parallel_tasks_each_see_only_their_own_context(ctx_worker, registry, swarmq_logs):
    registry.register(_InterleavedTask)
    for tag in ("a", "b"):
        _InterleavedTask.started[tag] = asyncio.Event()
        _InterleavedTask.proceed[tag] = asyncio.Event()

    def _msg(tag: str):
        msg = make_message(_default_payload(task_id=f"t-{tag}", name="interleaved", args=[tag], kwargs={}))
        msg.queue = "default"
        return msg

    first = asyncio.create_task(ctx_worker._handle_message(_msg("a")))
    await _InterleavedTask.started["a"].wait()
    second = asyncio.create_task(ctx_worker._handle_message(_msg("b")))
    await _InterleavedTask.started["b"].wait()
    _InterleavedTask.proceed["a"].set()
    await first
    _InterleavedTask.proceed["b"].set()
    await second

    task_entries = [e for e in swarmq_logs if e["event"] in ("before_wait", "after_wait")]
    assert len(task_entries) == 4
    for entry in task_entries:
        assert entry["task_id"] == f"t-{entry['tag']}"


@pytest.mark.timeout(5)
async def test_executor_receives_the_valid_workflow_id_for_the_compute_child(ctx_worker):
    """Der Compute-Executor baut daraus ``ComputeJob.workflow_id``."""
    seen: list[str | None] = []
    real_execute = ctx_worker._executor.execute

    async def spy(ctx, message_dict, chain):
        seen.append(ctx.workflow_id)
        return await real_execute(ctx, message_dict, chain)

    ctx_worker._executor.execute = spy

    await ctx_worker._handle_message(_message(headers={HEADER_WORKFLOW_ID: _WF_ID}))

    assert seen == [_WF_ID]
