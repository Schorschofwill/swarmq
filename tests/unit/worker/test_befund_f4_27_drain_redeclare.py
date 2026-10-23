"""Drain-Ende waehrend eines Neu-Deklarierens im Retry- und DLQ-Pfad (F4-27).

Ein Task scheitert, seine Rueckstellung (Stufe ``default.DQ.4s``) bzw. seine
DLQ-Kopie (``default.XQ``) ist unroutbar, das Neu-Deklarieren haengt, und das
Herunterfahren beginnt. Ohne Nachziehen der Restfrist bricht erst der Drain
den Versuch ab; dessen ``CancelledError`` liesse den ``finally`` der Pipeline
den Schedfire-Completed-Marker auf die unbestaetigte Nachricht schreiben, und
der Fire ginge nach der Neuzustellung still verloren (wie FS-08).

Test-Liste:
- [x] Retry an eine geloeschte Stufe und DLQ an eine geloeschte ``.XQ``: kein
      Completed-Marker, keine Unique-Freigabe (V-R3), kein Ack, auch wenn das Schliessen des Wegwerf-Channels
      suspendiert und der Drain-Abbruch dort landet (Review P1-1)
- [x] Review P2-1: der Drain-Abbruch trifft ein anderes Warten im Fehlerpfad
      (Late-Cancel-Prüfung im Backend): ebenfalls kein Completed-Marker, keine Freigabe
- [x] Abgrenzung: ein Drain-Abbruch im Ergebnis-Tail nach einem erfolgreichen Lauf bleibt
      terminal (Marker), die Neuzustellung laeuft den Task nicht noch einmal
- [x] Review P2-1 (Nachreview): der Drain-Abbruch trifft die Unique-Freigabe nach DLQ-Kopie und
      Ack; die Freigabe laeuft trotzdem zu Ende (keine Kopie und keine Neuzustellung holt sie nach)
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
from tests.unit.broker.test_declare_chain import FakeRabbit, make_broker
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
    backend.release_unique.assert_not_awaited()
    msg.ack.assert_not_awaited()


class FailingAfterRun(Task):
    task_info = TaskInfo(name="f4-27-late-cancel", queues=["default"], max_retries=3)
    finished: asyncio.Event | None = None

    @override
    async def run(self) -> None:
        assert FailingAfterRun.finished is not None
        FailingAfterRun.finished.set()
        raise ValueError("boom")


@pytest.mark.entscheidung("E-001")
@pytest.mark.befund("F4-27")
async def test_drain_end_during_the_late_cancel_check_writes_no_completed_marker(backend: Any, config: Any) -> None:
    broker = make_broker(FakeRabbit())
    await broker.declare_queue("default")
    FailingAfterRun.finished = asyncio.Event()
    checking = asyncio.Event()

    async def is_cancelled(*_: object, **__: object) -> bool:
        assert FailingAfterRun.finished is not None
        if not FailingAfterRun.finished.is_set():
            return False
        checking.set()
        await asyncio.Event().wait()  # Backend antwortet nicht vor dem Drain-Ende
        return False

    backend.is_cancelled = AsyncMock(side_effect=is_cancelled)
    backend.release_unique = AsyncMock(return_value=True)
    registry = TaskRegistry()
    registry.register(FailingAfterRun)
    worker = Worker(broker=broker, backend=backend, registry=registry, config=config)
    worker._workflow_advancer = MagicMock()
    msg = make_message(
        _default_payload(name="f4-27-late-cancel", args=[], kwargs={}),
        headers={"x-swarmq-schedfire": FIRE, HEADER_UNIQUE_HASH: UNIQUE},
    )
    msg.queue = "default"

    handler = asyncio.create_task(worker._handle_message(msg))
    await checking.wait()
    handler.cancel()  # wie _drain_in_flight am Drain-Ende
    await asyncio.wait({handler})

    backend.schedules.mark_fire_completed.assert_not_awaited()
    backend.release_unique.assert_not_awaited()
    msg.ack.assert_not_awaited()


class Succeeding(Task):
    task_info = TaskInfo(name="f4-27-succeeding", queues=["default"])

    @override
    async def run(self) -> None:
        return None


@pytest.mark.entscheidung("E-001")
@pytest.mark.befund("F4-27")
async def test_drain_end_in_the_result_tail_after_success_stays_terminal(backend: Any, config: Any) -> None:
    broker = make_broker(FakeRabbit())
    await broker.declare_queue("default")
    storing = asyncio.Event()

    async def store_result(*_: object, **__: object) -> None:
        storing.set()
        await asyncio.Event().wait()  # Backend antwortet nicht vor dem Drain-Ende

    backend.store_result = AsyncMock(side_effect=store_result)
    registry = TaskRegistry()
    registry.register(Succeeding)
    worker = Worker(broker=broker, backend=backend, registry=registry, config=config)
    worker._workflow_advancer = MagicMock()
    msg = make_message(
        _default_payload(name="f4-27-succeeding", args=[], kwargs={}),
        headers={"x-swarmq-schedfire": FIRE},
    )
    msg.queue = "default"

    handler = asyncio.create_task(worker._handle_message(msg))
    await storing.wait()
    handler.cancel()  # wie _drain_in_flight am Drain-Ende
    await asyncio.wait({handler})

    backend.schedules.mark_fire_completed.assert_awaited_once()


@pytest.mark.befund("F4-27")
async def test_drain_end_during_the_unique_release_after_the_dead_letter_copy_finishes_the_release(
    backend: Any, config: Any
) -> None:
    broker = make_broker(FakeRabbit())
    await broker.declare_queue("default")
    releasing = asyncio.Event()
    answer = asyncio.Event()
    released: list[str] = []

    async def release_unique(unique_hash: str, *_: object, **__: object) -> bool:
        releasing.set()
        await answer.wait()  # Backend antwortet erst nach dem Drain-Abbruch
        released.append(unique_hash)
        return True

    backend.release_unique = AsyncMock(side_effect=release_unique)
    registry = TaskRegistry()
    registry.register(FailingToDeadLetter)
    worker = Worker(broker=broker, backend=backend, registry=registry, config=config)
    worker._workflow_advancer = MagicMock()
    msg = make_message(
        _default_payload(name=FailingToDeadLetter.task_info.name, args=[], kwargs={}),
        headers={HEADER_UNIQUE_HASH: UNIQUE},
    )
    msg.queue = "default"

    handler = asyncio.create_task(worker._handle_message(msg))
    await releasing.wait()
    handler.cancel()  # wie _drain_in_flight am Drain-Ende
    await asyncio.wait({handler})
    answer.set()
    await asyncio.gather(*worker._detached_releases)  # wie _execute_shutdown vor backend.close

    assert released == [UNIQUE]
