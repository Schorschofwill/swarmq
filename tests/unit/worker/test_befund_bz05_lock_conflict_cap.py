"""Nachweis BZ-05: Lock-Konflikt landet nach ~51 s still in der DLQ.

Register: docs/befunde/betrieb-ueber-zeit-2026-10.md (BZ-05).

``LOCK_CONFLICT_DLQ_CAP = 10`` (``worker/gates/lock.py``): Die Rueckstellungen
vor Konflikt 1-9 summieren sich auf 100 ms * (2^9 - 1) ~ 51 s. Der zehnte
Konflikt geht per ``dead_letter_copy`` ins ``.XQ`` -- ohne ``TASK_DLQ``, ohne
Fehlerergebnis. Der Guide (``website/guides/locking-and-dedup.md``) sagt:
"requeued ... does not fail the task", nur ``max_age`` fuehre in die DLQ.

Die Workflow-Fehlmarkierung am Deckel steht in
``test_befund_bz13_workflow_hang.py`` (BZ-13).

Testliste:
# [x] Konflikt Nr. 10 (Halter laeuft laenger als ~51 s), kein max_age:
#     weiter zurueckstellen statt DLQ (Guide-Versprechen)
# [x] Falls der Deckel doch in die DLQ fuehrt: TASK_DLQ feuert
# [x] Falls der Deckel doch in die DLQ fuehrt: Fehlerergebnis fuer get_result()
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.registry import TaskRegistry
from swarmq.signals import Signal, SignalDispatcher
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker
from swarmq.worker.gates.lock import LOCK_CONFLICT_DLQ_CAP
from tests.support.messages import make_message
from tests.unit.worker._helpers import _default_payload

pytestmark = pytest.mark.timeout(5)


class PdfRender(Task):
    """Semaphor-Task wie im Guide-Beispiel (PDF-Rendering, max_concurrency=4)."""

    task_info = TaskInfo(name="bz05_pdf", queues=["default"], lock="pdf", max_concurrency=4, hard_timeout=600)

    async def run(self) -> None:
        raise AssertionError("darf bei Lock-Konflikt nicht laufen")


def _worker_at_cap(broker: Any, backend: Any, config: Any) -> tuple[Worker, list[dict[str, Any]], MagicMock]:
    dispatcher = SignalDispatcher()
    seen: list[dict[str, Any]] = []

    @dispatcher.on_signal(Signal.TASK_DLQ)
    async def _on_dlq(**kwargs: object) -> None:
        seen.append(dict(kwargs))

    registry = TaskRegistry()
    registry.register(PdfRender)
    worker = Worker(broker=broker, backend=backend, registry=registry, config=config, signal_dispatcher=dispatcher)
    worker._lock_manager = MagicMock()
    worker._lock_manager.acquire = AsyncMock(return_value=None)  # alle Slots belegt
    worker._lock_manager.release = AsyncMock(return_value=True)
    # Neun Konflikte liegen hinter der Nachricht (~51 s Wartezeit).
    message = make_message(
        _default_payload(task_id="bz05-task", name="bz05_pdf", args=[], kwargs={}),
        headers={"x-swarmq-lock-conflict-count": LOCK_CONFLICT_DLQ_CAP - 1},
    )
    return worker, seen, message


@pytest.mark.befund("BZ-05")
@pytest.mark.xfail(
    strict=True,
    reason="BZ-05: der 10. Lock-Konflikt (~51 s) schickt den Task in die DLQ statt weiter zu warten",
)
async def test_long_lock_conflict_keeps_waiting_instead_of_dead_lettering(broker, backend, config) -> None:
    worker, _seen, message = _worker_at_cap(broker, backend, config)

    await worker._handle_message(message)

    assert broker.dead_letter.await_count == 0, "Lock-Konflikt ohne max_age ging in die DLQ"
    broker.retry_later.assert_awaited_once()


@pytest.mark.befund("BZ-05")
@pytest.mark.xfail(strict=True, reason="BZ-05: Lock-Konflikt-Deckel fuehrt in die DLQ, ohne TASK_DLQ zu feuern")
async def test_lock_conflict_dead_letter_fires_task_dlq(broker, backend, config) -> None:
    worker, seen, message = _worker_at_cap(broker, backend, config)

    await worker._handle_message(message)

    if broker.dead_letter.await_count:
        assert [s["task_id"] for s in seen] == ["bz05-task"], "Nachricht liegt im .XQ, TASK_DLQ blieb aus"


@pytest.mark.befund("BZ-05")
@pytest.mark.xfail(
    strict=True,
    reason="BZ-05: Lock-Konflikt-Deckel fuehrt in die DLQ, ohne Fehlerergebnis -- get_result() laeuft in den Timeout",
)
async def test_lock_conflict_dead_letter_stores_an_error_result(broker, backend, config) -> None:
    worker, _seen, message = _worker_at_cap(broker, backend, config)

    await worker._handle_message(message)

    if broker.dead_letter.await_count:
        stored = [c.args[0] for c in backend.store_result.await_args_list]
        assert stored == ["bz05-task"], "Nachricht liegt im .XQ, kein Fehlerergebnis gespeichert"
