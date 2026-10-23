"""Drain-Ende waehrend eines Neu-Deklarierens im Retry- und DLQ-Pfad (F4-27).

Ein Task scheitert, seine Rueckstellung (Stufe ``default.DQ.4s``) bzw. seine
DLQ-Kopie (``default.XQ``) ist unroutbar, das Neu-Deklarieren haengt, und das
Herunterfahren beginnt. Ohne Nachziehen der Restfrist bricht erst der Drain
den Versuch ab; dessen ``CancelledError`` liesse den ``finally`` der Pipeline
den Schedfire-Completed-Marker auf die unbestaetigte Nachricht schreiben, und
der Fire ginge nach der Neuzustellung still verloren (wie FS-08).

Test-Liste:
- [x] Retry an eine geloeschte Stufe und DLQ an eine geloeschte ``.XQ``: kein
      Completed-Marker, kein Ack, der Handler wird nicht vom Drain abgebrochen
"""

from __future__ import annotations

import asyncio
from typing import Any, override
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.headers import HEADER_UNIQUE_HASH
from swarmq.registry import TaskRegistry
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker
from tests.support.messages import make_message
from tests.unit.broker.test_befund_f4_27_redeclare_deadline import _hanging_redeclaration
from tests.unit.worker._helpers import _default_payload

pytestmark = pytest.mark.timeout(5)

FIRE = "cron_a:1700000000000"
UNIQUE = "a" * 64


class FailingWithRetry(Task):
    task_info = TaskInfo(name="f4-27-retry", queues=["default"], max_retries=3)

    @override
    async def run(self) -> None:
        raise ValueError("boom")


class FailingToDeadLetter(Task):
    task_info = TaskInfo(name="f4-27-dead-letter", queues=["default"], max_retries=0)

    @override
    async def run(self) -> None:
        raise ValueError("boom")


@pytest.mark.befund("F4-27")
@pytest.mark.xfail(strict=True, raises=AssertionError, reason="F4-27: Neu-Deklarieren wird nicht nachgezogen")
@pytest.mark.parametrize(
    ("task_class", "routing_key"),
    [(FailingWithRetry, "default.DQ.4s"), (FailingToDeadLetter, "default.XQ")],
)
async def test_drain_end_during_a_redeclaration_writes_no_completed_marker(
    task_class: type[Task], routing_key: str, backend: Any, config: Any
) -> None:
    broker, opened, _release = await _hanging_redeclaration(routing_key)
    registry = TaskRegistry()
    registry.register(task_class)
    worker = Worker(broker=broker, backend=backend, registry=registry, config=config)
    worker._workflow_advancer = MagicMock()
    backend.release_unique = AsyncMock(return_value=True)
    msg = make_message(
        _default_payload(name=task_class.task_info.name, args=[], kwargs={}),
        headers={"x-swarmq-schedfire": FIRE, HEADER_UNIQUE_HASH: UNIQUE},
    )
    msg.queue = "default"

    handler = asyncio.create_task(worker._handle_message(msg))
    await opened.wait()
    # Wie ``_execute_shutdown``: Restfrist = Drain-Ende, bis dahin warten, dann abbrechen.
    loop = asyncio.get_running_loop()
    drain_end = loop.time() + 0.1
    broker.set_publish_deadline(drain_end)
    done, _ = await asyncio.wait({handler}, timeout=drain_end - loop.time())
    if not done:
        handler.cancel()
    await asyncio.wait({handler})

    backend.schedules.mark_fire_completed.assert_not_awaited()
    msg.ack.assert_not_awaited()
    assert not handler.cancelled()
