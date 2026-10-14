"""Nachweis FS-12: Ein Workflow-Schritt, der per ``cancel(task_id)`` vor dem
Start abgebrochen wird, laesst den Workflow bis zur TTL ``running``.

Register: docs/befunde/fehlersuche-runde-2-2026-10.md (FS-12; Quelle MA-1).

Bricht ``cancel(task_id)`` einen LAUFENDEN Schritt ab, markiert
``ResultTail._advance_workflow`` den Workflow als fehlgeschlagen ("task was
cancelled"). Trifft derselbe Cancel einen noch WARTENDEN Schritt, endet er im
Cancel-Gate (``worker/gates/cancel.py``): Cancel-Ergebnis, Ack,
``Terminal.already_released()`` -- weder ``fail`` noch ``advance``. Der
Workflow bleibt ``RUNNING`` bis ``result_ttl``, ``handle.get()`` liefert bis
dahin ``ResultNotReadyError``, ein Chord-Body laeuft nie (Fehlerklasse BZ-13).

Erwartet: Der Pre-Start-Cancel eines Schritts markiert den Workflow wie der
Cancel waehrend des Laufs als fehlgeschlagen ("task was cancelled").

Testliste:
# [x] per-Task-Cancel-Flag vor dem Start, Nachricht mit Workflow-Header ->
#     Schritt terminal (Ack, Cancel-Ergebnis) UND Workflow fehlmarkiert
"""

from __future__ import annotations

from typing import Any, override
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.headers import HEADER_WORKFLOW_ID, HEADER_WORKFLOW_STEP
from swarmq.registry import TaskRegistry
from swarmq.serialization import decode_result
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker
from tests.support.messages import make_message
from tests.unit.worker._helpers import _default_payload

pytestmark = pytest.mark.timeout(5)

_WF_ID = "01890a5d-ac96-774b-bcce-b302099a8057"


class Fs12Step(Task):
    task_info = TaskInfo(name="fs12_step", queues=["default"])

    @override
    async def run(self) -> None:
        raise AssertionError("abgebrochen, darf nicht laufen")


@pytest.mark.befund("FS-12")
async def test_pre_start_task_cancel_of_a_workflow_step_fails_the_workflow(
    broker: Any, backend: Any, config: Any
) -> None:
    registry = TaskRegistry()
    registry.register(Fs12Step)
    worker = Worker(broker=broker, backend=backend, registry=registry, config=config)
    advancer = MagicMock()
    advancer.advance = AsyncMock()
    advancer.fail = AsyncMock()
    worker._workflow_advancer = advancer
    backend.is_cancelled = AsyncMock(return_value=True)  # client.cancel(step_task_id) vor dem Start
    message = make_message(
        _default_payload(task_id="fs12-step", name="fs12_step", args=[], kwargs={}),
        headers={HEADER_WORKFLOW_ID: _WF_ID, HEADER_WORKFLOW_STEP: "/0"},
    )

    await worker._handle_message(message)

    # Voraussetzung: der Schritt ist terminal als cancelled beendet.
    broker.ack.assert_awaited_once_with(message)
    assert decode_result(backend.store_result.await_args.args[1])["is_cancelled"] is True
    advancer.advance.assert_not_awaited()
    failed = [c.args[0] for c in advancer.fail.await_args_list]
    assert failed == [_WF_ID], "Workflow bleibt RUNNING bis zur TTL"
    assert "cancel" in advancer.fail.await_args.args[2]
