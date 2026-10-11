"""Nachweis BZ-13: Workflows haengen bis zur TTL, wenn ein Schritt ueber einen
Nicht-Retry-Pfad endet; ein abgelaufener Workflow-Zustand endet ohne Log.

Register: docs/befunde/betrieb-ueber-zeit-2026-10.md (BZ-13).

Nur ``RetryHandler.dead_letter`` (Retry-Erschoepfung, Delivery-Limit) und
der Ergebnis-Tail rufen ``WorkflowAdvancer.fail``. Die folgenden Pfade
beenden einen Workflow-Schritt terminal, ohne den Workflow fehlzumarkieren --
der Zustand bleibt ``RUNNING`` bis ``result_ttl`` (24 h), ``handle.get()``
liefert bis dahin ``ResultNotReadyError``, ein Chord-Body laeuft nie.

Testliste:
# [x] unbekannter Task im Workflow (route_via_dlx) -> Workflow fehlmarkiert
# [x] Lock-Render-Fehler im Workflow (dead_letter_copy) -> fehlmarkiert
# [x] max_age-Ablauf im Workflow (ExpiryGate) -> fehlmarkiert
# [x] Stillstand des Lock-Halters im Workflow (MR 13: DLQ statt Deckel) -> fehlmarkiert
# [x] WorkflowAdvancer.advance bei abgelaufenem Zustand -> WARNING mit
#     workflow_id (heute: stilles ``return``)
"""

from __future__ import annotations

import dataclasses
import time
from typing import Any, override
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.contracts.coordination import LockObservation
from swarmq.headers import (
    HEADER_ENQUEUED_AT,
    HEADER_LOCK_HOLDER,
    HEADER_LOCK_HOLDER_SINCE,
    HEADER_LOCK_HOLDER_TTL_MS,
    HEADER_WORKFLOW_ID,
    HEADER_WORKFLOW_STEP,
)
from swarmq.registry import TaskRegistry
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker
from swarmq.worker.gates.lock import holder_fingerprint
from swarmq.workflow_orchestration import WorkflowAdvancer
from tests.support.inmemory import FakeClock, InMemoryBackend
from tests.support.messages import make_message
from tests.unit._log_capture import capture_swarmq_logs
from tests.unit.worker._helpers import AddTask, _default_payload

pytestmark = pytest.mark.timeout(5)

_WF_ID = "01890a5d-ac96-774b-bcce-b302099a8057"


class RenderLocked(Task):
    task_info = TaskInfo(name="bz13_render", queues=["default"], lock="user-{user_id}", hard_timeout=60)

    @override
    async def run(self, account_id: int) -> None:  # kwarg umbenannt: Template passt nicht mehr
        raise AssertionError("darf nicht laufen")


class Expiring(Task):
    task_info = TaskInfo(name="bz13_expiring", queues=["default"], max_age=10)

    @override
    async def run(self) -> None:
        raise AssertionError("darf nicht laufen")


class Mutex(Task):
    task_info = TaskInfo(name="bz13_mutex", queues=["default"], lock="m", hard_timeout=60)

    @override
    async def run(self) -> None:
        raise AssertionError("darf nicht laufen")


def _workflow_worker(broker: Any, backend: Any, config: Any, *tasks: type[Task]) -> Worker:
    registry = TaskRegistry()
    for task in tasks:
        registry.register(task)
    worker = Worker(broker=broker, backend=backend, registry=registry, config=config)
    worker._workflow_advancer = MagicMock()
    worker._workflow_advancer.advance = AsyncMock()
    worker._workflow_advancer.fail = AsyncMock()
    worker._lock_manager = MagicMock()
    worker._lock_manager.acquire = AsyncMock(return_value=None)
    worker._lock_manager.release = AsyncMock(return_value=True)
    return worker


def _step(name: str, *, kwargs: dict[str, Any] | None = None, **headers: Any) -> MagicMock:
    message = make_message(
        _default_payload(task_id="bz13-step", name=name, args=[], kwargs=kwargs or {}),
        headers={HEADER_WORKFLOW_ID: _WF_ID, HEADER_WORKFLOW_STEP: "/1", **headers},
    )
    message.queue = "default"
    return message


def _failed_workflows(worker: Worker) -> list[str]:
    return [c.args[0] for c in worker._workflow_advancer.fail.await_args_list]  # type: ignore[union-attr]


@pytest.mark.befund("BZ-13")
async def test_unknown_task_step_marks_the_workflow_failed(broker, backend, config) -> None:
    # Gnadenfrist 0 (Plan 0.12.0, Abschnitt 6): Der unbekannte Task geht
    # sofort in die DLQ. Seit MR 11 (BZ-06) stellt der Worker ihn sonst erst
    # zurueck; die Gnadenfrist steht deshalb auf 0, statt die Voraussetzung
    # abzuschwaechen -- der Test darf nicht vakuum-gruen werden.
    worker = _workflow_worker(broker, backend, dataclasses.replace(config, unknown_task_grace_s=0), AddTask)

    await worker._handle_message(_step("renamed_away"))

    assert broker.dead_letter.await_count == 1, "Voraussetzung: unbekannter Task geht in die DLQ"
    assert _failed_workflows(worker) == [_WF_ID], "Schritt im .XQ, Workflow nicht fehlmarkiert"


@pytest.mark.befund("BZ-13")
async def test_lock_render_error_step_marks_the_workflow_failed(broker, backend, config) -> None:
    worker = _workflow_worker(broker, backend, config, RenderLocked)

    await worker._handle_message(_step("bz13_render", kwargs={"account_id": 7}))

    assert broker.dead_letter.await_count == 1, "Voraussetzung: Render-Fehler geht in die DLQ"
    assert _failed_workflows(worker) == [_WF_ID], "Schritt im .XQ, Workflow nicht fehlmarkiert"


@pytest.mark.befund("BZ-13")
async def test_expired_step_marks_the_workflow_failed(broker, backend, config) -> None:
    worker = _workflow_worker(broker, backend, config, Expiring)

    await worker._handle_message(_step("bz13_expiring", **{HEADER_ENQUEUED_AT: int(time.time()) - 100}))

    assert broker.dead_letter.await_count == 1, "Voraussetzung: abgelaufener Schritt geht in die DLQ"
    assert _failed_workflows(worker) == [_WF_ID], "Schritt abgelaufen, Workflow nicht fehlmarkiert"


@pytest.mark.befund("BZ-13")
async def test_stalled_lock_holder_step_marks_the_workflow_failed(broker, backend, config) -> None:
    """MR 13 (Abschnitt 6): Stillstand -- derselbe Halter haelt laenger als
    die beobachtete PTTL + 60 s -- statt des Deckels 10."""
    worker = _workflow_worker(broker, backend, config, Mutex)
    worker._lock_manager.observe = AsyncMock(return_value=LockObservation(frozenset({"holder"}), 90_000))
    now_ms = int(time.time() * 1000)

    await worker._handle_message(
        _step(
            "bz13_mutex",
            **{
                HEADER_LOCK_HOLDER: holder_fingerprint(frozenset({"holder"})),
                HEADER_LOCK_HOLDER_SINCE: now_ms - 90_000 - 60_000 - 1_000,
                HEADER_LOCK_HOLDER_TTL_MS: 90_000,
            },
        )
    )

    assert broker.dead_letter.await_count == 1, "Voraussetzung: Stillstand des Halters geht in die DLQ"
    assert _failed_workflows(worker) == [_WF_ID], "Schritt im .XQ, Workflow nicht fehlmarkiert"


class _NoopPublisher:
    async def pre_enqueue(self, steps: Any) -> None:
        return None

    async def publish(self, step: Any) -> Any:
        raise AssertionError("ohne Zustand darf kein Folgeschritt eingereiht werden")


@pytest.mark.befund("BZ-13")
async def test_advance_with_expired_workflow_state_logs_a_warning() -> None:
    backend = InMemoryBackend(FakeClock())
    backend._connected = True
    advancer = WorkflowAdvancer(
        backend,
        backend.chords,
        publisher=_NoopPublisher(),  # type: ignore[arg-type]
        resolve_queue=lambda _name: "default",
        make_task_id=lambda wf, path: f"{wf}:{path}",
        unpack=lambda raw: raw,
        ttl_ms=60_000,
    )

    with capture_swarmq_logs() as logs:
        await advancer.advance(_WF_ID, "/0", "step-0", 42)

    loud = [e for e in logs if e["log_level"] in ("warning", "error") and e.get("workflow_id") == _WF_ID]
    assert loud, f"Workflow-Zustand fehlt, aber kein WARNING/ERROR mit workflow_id: {logs}"
