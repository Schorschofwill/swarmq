"""Nachweis FS-01 (CPU-Modus): Ein Task, der den Drain ueberdauert, wird am
Drain-Ende abgebrochen und als Fehler ("compute child cancelled during
worker drain") in den Fehlerpfad geschickt statt neu zugestellt.

Register: docs/befunde/fehlersuche-runde-2-2026-10.md (FS-01; Quellen K-1, MB-5).

``_drain_in_flight`` (``worker/shutdown.py``) bricht nach ``drain_until`` den
Handler ab. ``ComputeExecutor._await_envelope`` (``worker/compute/executor.py``)
faengt den ``CancelledError`` des aeusseren Abbruchs, laesst das Kind toeten und
wertet den Abbruch ohne ``cancel_reason`` als Fehler (``cancelling()`` des
Handlers bleibt dabei > 0). Folge: ``TASK_FAILED``, Retry-Verbrauch bzw. DLQ,
Fehlerergebnis und Workflow-Fail; die Nachricht laeuft danach trotzdem erneut.
KTD2 (Plan 2026-09-03-001) hat das so festgelegt, die Nutzer-Doku verspricht
"wird abgebrochen und neu zugestellt".

Offene Produktfrage 1 (Register): Die Tests pruefen die empfohlene Option –
ein Drain-Abbruch ist eine Neuzustellung (``redeliver``/nack mit Requeue oder
gar keine Quittung), KEIN Fehlerergebnis, KEIN Retry-Verbrauch, KEINE DLQ,
KEIN ``TASK_FAILED`` und kein Workflow-Fail. Den IO-Modus deckt
``tests/unit/worker/test_befund_fs01_drain_abort.py`` ab, das Recycling
``tests/unit/worker/test_befund_fs01_recycling_drain.py``.

Aufbau: echter ``Worker`` und echter ``ComputeExecutor``; nur der ``SlotPool``
ist gefaelscht (Grenze zu den Kindprozessen). Sein ``run`` rechnet "ewig" und
endet bei Abbruch so, wie ``SlotPool.run`` es nach dem Kill tut: mit
``CancelledError``.

Testliste:
# [x] max_retries=0: Drain-Abbruch -> weder DLQ noch Fehlerergebnis noch
#     TASK_FAILED, Nachricht wird neu zugestellt (nicht bestaetigt)
# [x] max_retries=3: Drain-Abbruch verbraucht keinen Retry (kein retry_later)
# [x] Workflow-Schritt: Drain-Abbruch markiert den Workflow nicht als FAILED
# [ ] MB-5 (Nachlauffrist 1 s < Kill-Dauer eines rechnenden Kindes): nicht als
#     Unit-Test nachgestellt – braucht echte Wartezeiten > 1 s oder eine
#     virtuelle Uhr; Beleg bleibt die Codelektuere in 4-mutation-B.md
"""

from __future__ import annotations

import asyncio
from typing import Any, cast, override
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.config import SwarmQConfig
from swarmq.headers import HEADER_WORKFLOW_ID, HEADER_WORKFLOW_STEP
from swarmq.registry import TaskRegistry
from swarmq.serialization import decode_result
from swarmq.signals import Signal
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker, WorkerState
from swarmq.worker.compute.envelope import ComputeJob, ResultEnvelope
from swarmq.worker.compute.executor import ComputeExecutor
from swarmq.worker.compute.pool import SlotPool
from tests.support.messages import make_message
from tests.unit.worker._helpers import _default_payload

pytestmark = pytest.mark.timeout(5)

_WF_ID = "01890a5d-ac96-774b-bcce-b302099a8057"
_REASON = "FS-01: offen - CPU-Drain-Abbruch wird als Fehler gewertet statt neu zugestellt"


class Fs01CpuLongRunner(Task):
    """Rumpf laeuft im (gefaelschten) Rechenkind; der Loop-Prozess ruft ``run`` nie."""

    task_info = TaskInfo(name="fs01_cpu_long_runner", queues=["default"])

    @override
    async def run(self) -> None:
        raise AssertionError("im CPU-Modus rechnet das Kind, nicht der Loop-Prozess")


class Fs01CpuRetryingLongRunner(Task):
    task_info = TaskInfo(name="fs01_cpu_retrying_long_runner", queues=["default"], max_retries=3)

    @override
    async def run(self) -> None:
        raise AssertionError("im CPU-Modus rechnet das Kind, nicht der Loop-Prozess")


class _Fs01EndlessPool:
    """``SlotPool``-Attrappe: das Kind rechnet, bis der Abbruch es "toetet"."""

    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.killed = 0

    async def start(self) -> None:
        return None

    async def aclose(self) -> None:
        return None

    async def run(self, job: ComputeJob, *, use_di: bool) -> ResultEnvelope:
        del job, use_di
        self.started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.killed += 1  # SlotPool.run toetet das Kind und reicht den Abbruch weiter
            raise
        raise AssertionError("unerreichbar")


def _cpu_drain_worker(broker: Any, backend: Any, task: type[Task]) -> tuple[Worker, _Fs01EndlessPool, list[Any]]:
    """Worker mit ``shutdown_timeout=0`` und ``ComputeExecutor``: der Drain bricht sofort ab."""
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
    pool = _Fs01EndlessPool()
    # Pool-Attrappe statt SlotPool (Grenze zu den Kindprozessen).
    worker._executor = ComputeExecutor(worker._gate_deps, cast("SlotPool", pool), worker_id="fs01-cpu")
    worker._workflow_advancer = MagicMock()
    worker._workflow_advancer.advance = AsyncMock()
    worker._workflow_advancer.fail = AsyncMock()
    failed: list[Any] = []

    async def on_failed(**kwargs: Any) -> None:
        failed.append(kwargs)

    worker._signal_dispatcher.connect(Signal.TASK_FAILED, on_failed)
    worker._state = WorkerState.RUNNING
    return worker, pool, failed


async def _run_into_drain_abort(worker: Worker, pool: _Fs01EndlessPool, message: Any) -> None:
    await worker._on_message(message)
    await pool.started.wait()
    worker._request_shutdown()
    await worker._execute_shutdown()
    assert pool.killed == 1, "Voraussetzung: der Drain hat das rechnende Kind abgebrochen"


def _error_results(backend: Any) -> list[dict[str, Any]]:
    stored = [decode_result(c.args[1]) for c in backend.store_result.await_args_list]
    return [r for r in stored if r.get("is_error")]


@pytest.mark.befund("FS-01")
@pytest.mark.xfail(strict=True, reason=_REASON)
async def test_cpu_drain_abort_redelivers_without_error_result_or_dlq(broker: Any, backend: Any) -> None:
    worker, pool, failed = _cpu_drain_worker(broker, backend, Fs01CpuLongRunner)
    message = make_message(_default_payload(task_id="fs01-cpu-t1", name="fs01_cpu_long_runner", args=[], kwargs={}))

    await _run_into_drain_abort(worker, pool, message)

    assert _error_results(backend) == [], "CPU-Drain-Abbruch schreibt ein Fehlerergebnis"
    assert broker.dead_letter.await_count == 0, "CPU-Drain-Abbruch schickt die Nachricht in die DLQ"
    assert failed == [], "CPU-Drain-Abbruch feuert TASK_FAILED"
    broker.ack.assert_not_awaited()
    assert broker.redeliver.await_count + broker.nack.await_count <= 1


@pytest.mark.befund("FS-01")
@pytest.mark.xfail(strict=True, reason=_REASON)
async def test_cpu_drain_abort_does_not_consume_a_retry(broker: Any, backend: Any) -> None:
    worker, pool, failed = _cpu_drain_worker(broker, backend, Fs01CpuRetryingLongRunner)
    message = make_message(
        _default_payload(task_id="fs01-cpu-t2", name="fs01_cpu_retrying_long_runner", args=[], kwargs={})
    )

    await _run_into_drain_abort(worker, pool, message)

    assert broker.retry_later.await_count == 0, "CPU-Drain-Abbruch verbraucht einen Retry"
    assert broker.dead_letter.await_count == 0
    assert failed == []
    broker.ack.assert_not_awaited()


@pytest.mark.befund("FS-01")
@pytest.mark.xfail(strict=True, reason=_REASON)
async def test_cpu_drain_abort_does_not_fail_the_workflow(broker: Any, backend: Any) -> None:
    worker, pool, _failed = _cpu_drain_worker(broker, backend, Fs01CpuLongRunner)
    message = make_message(
        _default_payload(task_id="fs01-cpu-t3", name="fs01_cpu_long_runner", args=[], kwargs={}),
        headers={HEADER_WORKFLOW_ID: _WF_ID, HEADER_WORKFLOW_STEP: "/0"},
    )

    await _run_into_drain_abort(worker, pool, message)

    advancer: Any = worker._workflow_advancer
    advancer.fail.assert_not_awaited()
    assert _error_results(backend) == [], "Workflow-Schritt bekommt bei CPU-Drain-Abbruch ein Fehlerergebnis"
