"""Nachweis BZ-14: verwaiste Queues bleiben ohne Consumer und ohne Warnung.

Register: docs/befunde/betrieb-ueber-zeit-2026-10.md (BZ-14). Wird ein Task
von Queue ``old`` nach ``new`` verschoben (oder ``--queues`` geändert), bleibt
der Rückstand in ``old`` samt den Retry-Kopien aus ``old.DQ`` liegen. Kein
Worker konsumiert ihn, keine Metrik und kein Log nennt ihn.

Spezifikationslücke: swarmq legt nicht fest, wer verwaiste Queues meldet.
Prüfbare Mindesterwartung hier: Ein Worker, der einen Task ausführt, dessen
Nachrichten noch in einer swarmq-Queue (Tripel ``old``/``old.DQ``/``old.XQ``)
ohne Consumer liegen, nennt diese Queue in einer Warnung. Wie die Erkennung
umgesetzt wird (Management-API, Queue-Register im Backend, ``swarmq check``),
ist offen; der Test prüft nur das Ergebnis.

Test-Liste:
- [x] Rückstand in nicht mehr konsumierter swarmq-Queue erzeugt eine Warnung (BZ-14)
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
import uuid

import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.headers import HEADER_ENQUEUED_AT
from swarmq.serialization import encode_json
from swarmq.task import Task, TaskInfo
from tests.support.queues import delete_queue_family
from tests.support.wait import wait_until

pytestmark = [pytest.mark.integration, pytest.mark.timeout(30)]

# Zeit nach dem Start, in der eine Warnung erscheinen darf.
_WARN_BUDGET_S = 3.0


def _body(task_id: str, task_name: str, queue: str) -> bytes:
    return encode_json({"task": {"id": task_id, "name": task_name, "queue": queue}, "args": [], "kwargs": {}})


def _mentions(record: logging.LogRecord, text: str) -> bool:
    return text in record.getMessage() or text in repr(record.__dict__)


@pytest.mark.befund("BZ-14")
@pytest.mark.xfail(
    strict=True,
    reason=(
        "BZ-14: ein Rückstand in einer umbenannten swarmq-Queue ohne Consumer "
        "bleibt still liegen; weder Log noch Metrik nennt ihn"
    ),
)
async def test_backlog_in_abandoned_queue_is_reported(
    rabbitmq_url: str, valkey_url: str, caplog: pytest.LogCaptureFixture
) -> None:
    suffix = uuid.uuid4().hex[:10]
    old_queue = f"bz14-old-{suffix}"
    new_queue = f"bz14-new-{suffix}"
    task_name = f"bz14_report_{suffix}"
    ran = asyncio.Event()

    class ReportTask(Task):
        # Nach dem Umzug: der Task lebt jetzt auf ``new``.
        task_info = TaskInfo(name=task_name, queues=[new_queue])

        async def run(self) -> str:
            ran.set()
            return "ok"

    producer = RabbitMQBroker(rabbitmq_url)
    await producer.connect()
    worker_broker = RabbitMQBroker(rabbitmq_url)
    worker_backend = ValkeyBackend(valkey_url)
    worker_app = SwarmQ(worker_broker, worker_backend, queues=[new_queue], concurrency=2)
    worker_task: asyncio.Task[None] | None = None
    caplog.set_level(logging.WARNING)
    try:
        # Stand vor dem Umzug: ein Worker hatte ``old`` deklariert, ein
        # Rückstand liegt dort.
        await producer.declare_queue(old_queue)
        headers = {HEADER_ENQUEUED_AT: int(time.time())}
        await producer.publish(old_queue, _body(f"old-{suffix}", task_name, old_queue), headers=headers)

        worker_task = asyncio.create_task(worker_app.start_worker())
        # Der Worker läuft, sobald er eine Nachricht auf ``new`` ausführt.
        await producer.declare_queue(new_queue)
        await producer.publish(new_queue, _body(f"new-{suffix}", task_name, new_queue), headers=headers)
        await asyncio.wait_for(ran.wait(), timeout=10)

        # Eine Warnung kann aus einem periodischen Check kommen; Pollen.
        warned = await wait_until(
            lambda: any(r.levelno >= logging.WARNING and _mentions(r, old_queue) for r in caplog.records),
            timeout=_WARN_BUDGET_S,
            interval=0.1,
        )

        assert await producer.queue_depth(old_queue) == 1, "precondition: backlog still in the old queue"
        assert warned, (
            f"queue {old_queue!r} holds a backlog for task {task_name!r} without any consumer, but no warning names it"
        )
    finally:
        if worker_task is not None:
            worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await worker_task
        await worker_broker.close()
        await worker_backend.close()
        await delete_queue_family(rabbitmq_url, old_queue, new_queue)
        await producer.close()
