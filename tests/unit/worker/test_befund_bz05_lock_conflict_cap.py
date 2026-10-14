"""Nachweis BZ-05: Lock-Konflikt landet nach ~51 s still in der DLQ.

Register: docs/befunde/betrieb-ueber-zeit-2026-10.md (BZ-05).

Bis 0.11 (``LOCK_CONFLICT_DLQ_CAP = 10``, ``worker/gates/lock.py``): Die
Rueckstellungen vor Konflikt 1-9 summierten sich auf 100 ms * (2^9 - 1)
~ 51 s. Der zehnte Konflikt ging per ``dead_letter_copy`` ins ``.XQ`` --
ohne ``TASK_DLQ``, ohne Fehlerergebnis. Der Guide
(``website/guides/locking-and-dedup.md``) sagt: "requeued ... does not fail
the task", nur ``max_age`` fuehre in die DLQ.

Seit MR 13 (Plan 0.12.0) wartet ein Konflikt zeitbasiert: bis
``SWARMQ_LOCK_WAIT_MAX_S`` ab dem ersten Konflikt oder bis derselbe Halter
laenger haelt als seine TTL + 60 s; dann DLQ mit ``LockWaitExceeded``,
``TASK_DLQ`` und Fehlerergebnis. Die Nachweise 2/3 (seit MR 3 gruen) sind
dafuer auf die Obergrenze umgeschrieben (Abschnitt 6). Der Legacy-Zaehler
``x-swarmq-lock-conflict-count`` bleibt wirkungslos.

Die Workflow-Fehlmarkierung am Ende der Wartezeit steht in
``test_befund_bz13_workflow_hang.py`` (BZ-13).

Testliste:
# [x] Konflikt Nr. 10 nach Legacy-Zaehler (Halter laeuft laenger als ~51 s),
#     kein max_age: weiter zurueckstellen statt DLQ (Guide-Versprechen)
# [x] Obergrenze der Wartezeit erreicht: TASK_DLQ feuert
# [x] Obergrenze der Wartezeit erreicht: Fehlerergebnis fuer get_result()
"""

from __future__ import annotations

from typing import Any, override
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.registry import TaskRegistry
from swarmq.signals import Signal, SignalDispatcher
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker
from tests.support.messages import make_message
from tests.unit.worker._helpers import _default_payload, lock_wait_exceeded_headers

pytestmark = pytest.mark.timeout(5)


class PdfRender(Task):
    """Semaphor-Task wie im Guide-Beispiel (PDF-Rendering, max_concurrency=4)."""

    task_info = TaskInfo(name="bz05_pdf", queues=["default"], lock="pdf", max_concurrency=4, hard_timeout=600)

    @override
    async def run(self) -> None:
        raise AssertionError("darf bei Lock-Konflikt nicht laufen")


def _worker_at_cap(
    broker: Any, backend: Any, config: Any, headers: dict[str, Any] | None = None
) -> tuple[Worker, list[dict[str, Any]], MagicMock]:
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
    # Standard: neun Konflikte nach dem Legacy-Zaehler liegen hinter der
    # Nachricht (~51 s Wartezeit) -- der zehnte war bis 0.11 der Deckel.
    message = make_message(
        _default_payload(task_id="bz05-task", name="bz05_pdf", args=[], kwargs={}),
        headers={"x-swarmq-lock-conflict-count": 9} if headers is None else headers,
    )
    return worker, seen, message


@pytest.mark.entscheidung("E-007")
@pytest.mark.befund("BZ-05")
async def test_long_lock_conflict_keeps_waiting_instead_of_dead_lettering(broker, backend, config) -> None:
    worker, _seen, message = _worker_at_cap(broker, backend, config)

    await worker._handle_message(message)

    assert broker.dead_letter.await_count == 0, "Lock-Konflikt ohne max_age ging in die DLQ"
    broker.retry_later.assert_awaited_once()


@pytest.mark.befund("BZ-05")
async def test_lock_wait_limit_dead_letter_fires_task_dlq(broker, backend, config) -> None:
    worker, seen, message = _worker_at_cap(broker, backend, config, lock_wait_exceeded_headers(config))

    await worker._handle_message(message)

    assert broker.dead_letter.await_count == 1, "Voraussetzung: die Obergrenze der Wartezeit fuehrt in die DLQ"
    assert [s["task_id"] for s in seen] == ["bz05-task"], "Nachricht liegt im .XQ, TASK_DLQ blieb aus"


@pytest.mark.befund("BZ-05")
async def test_lock_wait_limit_dead_letter_stores_an_error_result(broker, backend, config) -> None:
    worker, _seen, message = _worker_at_cap(broker, backend, config, lock_wait_exceeded_headers(config))

    await worker._handle_message(message)

    assert broker.dead_letter.await_count == 1
    stored = [c.args[0] for c in backend.store_result.await_args_list]
    assert stored == ["bz05-task"], "Nachricht liegt im .XQ, kein Fehlerergebnis gespeichert"
    assert "LockWaitExceeded" in broker.dead_letter.await_args.kwargs["headers"]["x-swarmq-traceback"]
