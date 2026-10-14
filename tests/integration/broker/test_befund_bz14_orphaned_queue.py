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

Umgeschrieben vor dem Fix (Plan 0.12.0, MR 18, Abschnitt 6): Vorzustand ist
eine *gemeldete* Queue. Ein Worker hat ``old`` konsumiert und gemeldet, dann
wurde er umgestellt; erst danach landet der Rückstand in ``old``. Das Budget
deckt den Meldetakt (``scheduler_seen_interval_s``, Lebendigkeitsfenster drei
Takte) und die Übernahme der Leader-Rolle nach dem Ende des alten Workers ab.

Test-Liste:
- [x] Rückstand in nicht mehr konsumierter swarmq-Queue erzeugt eine Warnung (BZ-14)
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
import uuid
from typing import override

import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.headers import HEADER_ENQUEUED_AT
from swarmq.serialization import encode_json
from swarmq.signals import Signal
from swarmq.task import Task, TaskInfo
from tests.support.queues import delete_queue_family
from tests.support.wait import wait_until

pytestmark = [pytest.mark.integration, pytest.mark.timeout(30)]

# Zeit nach dem Start, in der eine Warnung erscheinen darf: Lock-TTL des
# abgebrochenen alten Leaders (2 s) plus Lebendigkeitsfenster (3 x 0.5 s)
# plus Reserve.
_WARN_BUDGET_S = 10.0

# Kurze Scheduler-Takte, damit Meldung, Leader-Wechsel und Prüfung in das
# Budget passen.
_SCHEDULER = {
    "scheduler_heartbeat_interval": 0.25,
    "scheduler_lock_ttl": 2.0,
    "scheduler_seen_interval_s": 0.5,
}


def _body(task_id: str, task_name: str, queue: str) -> bytes:
    return encode_json({"task": {"id": task_id, "name": task_name, "queue": queue}, "args": [], "kwargs": {}})


def _mentions(record: logging.LogRecord, text: str) -> bool:
    return text in record.getMessage() or text in repr(record.__dict__)


@pytest.mark.entscheidung("E-015")
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

        @override
        async def run(self) -> str:
            ran.set()
            return "ok"

    producer = RabbitMQBroker(rabbitmq_url)
    await producer.connect()
    old_broker = RabbitMQBroker(rabbitmq_url)
    old_backend = ValkeyBackend(valkey_url)
    old_app = SwarmQ(old_broker, old_backend, queues=[old_queue], concurrency=2, **_SCHEDULER)
    worker_broker = RabbitMQBroker(rabbitmq_url)
    worker_backend = ValkeyBackend(valkey_url)
    worker_app = SwarmQ(worker_broker, worker_backend, queues=[new_queue], concurrency=2, **_SCHEDULER)
    old_task: asyncio.Task[None] | None = None
    worker_task: asyncio.Task[None] | None = None
    old_online = asyncio.Event()

    @old_app.on_signal(Signal.WORKER_ONLINE)
    async def _old_online(**_kw: object) -> None:
        old_online.set()

    caplog.set_level(logging.WARNING)
    try:
        # Stand vor dem Umzug: ein Worker hat ``old`` konsumiert (und damit
        # gemeldet) und wurde dann umgestellt.
        old_task = asyncio.create_task(old_app.start_worker())
        await asyncio.wait_for(old_online.wait(), timeout=10)
        old_task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await old_task
        old_task = None
        # Der alte Worker ist weg: ohne Verbindung auch ohne Consumer.
        await old_broker.close()
        await old_backend.close()

        # Erst danach landet ein Rückstand in ``old``.
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
        if old_task is not None:
            old_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await old_task
        if worker_task is not None:
            worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await worker_task
        await old_broker.close()
        await old_backend.close()
        await worker_broker.close()
        await worker_backend.close()
        await delete_queue_family(rabbitmq_url, old_queue, new_queue)
        await producer.close()
