"""Nachweis-Tests (e2e, RabbitMQ + Valkey) fuer Scheduler-Befunde aus
``docs/befunde/betrieb-ueber-zeit-2026-10.md``.

Jeder Test beschreibt das korrekte Verhalten und ist mit ``xfail(strict=True)``
markiert, solange der Befund offen ist. Wird ein Befund behoben, wird der Test
XPASS und damit rot: dann den ``xfail``-Marker entfernen.

Nur ueber den Wrapper mit den Testports ausfuehren (5699/6399).

Test-Liste:
1. BZ-02: ``get_result(tid)`` liefert das Ergebnis eines ``schedule(delay=)``.
2. BZ-02: ``cancel(tid)`` vor dem Fire verhindert die Ausfuehrung.
3. BZ-02: Bei ``unique_for`` (completion) ist nach dem Lauf ein erneutes
   ``schedule(delay=)`` mit gleichen Argumenten moeglich (neue ID, neuer Lauf).
4. BZ-11: Ein Cron, dessen Task kein Worker mehr kennt, landet im ``.XQ``
   mit Ablaufzeit (``expiration`` bzw. ``x-swarmq-dead-letter-ttl-ms``).
5. BZ-23: Ein stornierter Task, der laenger als ``result_ttl`` in der Queue
   wartet, laeuft trotzdem nicht.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
import uuid
from collections.abc import AsyncIterator

import aio_pika
import msgspec
import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.headers import HEADER_DEAD_LETTER_TTL_MS
from swarmq.scheduler import ScheduleDefinition
from swarmq.signals import Signal
from swarmq.task import Task, TaskInfo
from tests.support.services import flush_valkey_db
from tests.support.wait import wait_until

_EVENT_TIMEOUT = 20.0

pytestmark = [pytest.mark.timeout(60)]


@pytest.fixture(autouse=True)
async def _flush_valkey(valkey_url):
    # Wie tests/e2e/test_scheduler.py: ein liegengebliebenes Leader-Lock eines
    # frueheren Tests verhindert sonst die Wahl.
    await flush_valkey_db(valkey_url)
    yield
    await flush_valkey_db(valkey_url)


def _queue() -> str:
    return f"e2e-befund-{uuid.uuid4().hex[:10]}"


def _name(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


@contextlib.asynccontextmanager
async def _leader_app(
    rabbitmq_url: str,
    valkey_url: str,
    queue: str,
    *,
    start_worker: bool = True,
    **config: object,
) -> AsyncIterator[SwarmQ]:
    """App mit Worker auf *queue*; kehrt zurueck, sobald sie Leader ist."""
    async with SwarmQ(
        RabbitMQBroker(rabbitmq_url),
        ValkeyBackend(valkey_url),
        queues=[queue],
        concurrency=1,
        scheduler_poll_interval=0.1,
        scheduler_heartbeat_interval=0.2,
        scheduler_lock_ttl=1.0,
        **config,
    ) as app:
        await app.broker.declare_queue(queue)
        leader = asyncio.Event()

        @app.on_signal(Signal.SCHEDULER_LEADER)
        async def _on_leader(**_kwargs: object) -> None:
            leader.set()

        if not start_worker:
            yield app
            return
        worker_task = asyncio.create_task(app.start_worker())
        try:
            await asyncio.wait_for(leader.wait(), timeout=_EVENT_TIMEOUT)
            yield app
        finally:
            worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await worker_task


async def _start_worker(app: SwarmQ) -> asyncio.Task[None]:
    return asyncio.create_task(app.start_worker())


async def _stop(task: asyncio.Task[None]) -> None:
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


# ---------------------------------------------------------------------------
# BZ-02: delay/eta liefert eine ID, gefeuert wird unter einer anderen
# ---------------------------------------------------------------------------


@pytest.mark.befund("BZ-02")
async def test_bz02_get_result_of_delayed_task(rabbitmq_url, valkey_url):
    queue, task_name = _queue(), _name("delayed")

    class DelayedTask(Task):
        task_info = TaskInfo(name=task_name, queues=[queue])

        async def run(self) -> str:
            return "done"

    async with _leader_app(rabbitmq_url, valkey_url, queue) as app:
        tid = await app.client.schedule(task_name, queue=queue, delay=0.3)

        assert await app.get_result(tid, timeout=5) == "done"


@pytest.mark.befund("BZ-02")
async def test_bz02_cancel_prevents_delayed_task(rabbitmq_url, valkey_url):
    queue = _queue()
    victim_name, marker_name = _name("victim"), _name("marker")
    victim_ran = False
    marker_done = asyncio.Event()

    class VictimTask(Task):
        task_info = TaskInfo(name=victim_name, queues=[queue])

        async def run(self) -> None:
            nonlocal victim_ran
            victim_ran = True

    class MarkerTask(Task):
        task_info = TaskInfo(name=marker_name, queues=[queue])

        async def run(self) -> None:
            marker_done.set()

    async with _leader_app(rabbitmq_url, valkey_url, queue) as app:
        tid = await app.client.schedule(victim_name, queue=queue, delay=0.5)
        await app.cancel(tid)
        # Spaeter faellig, gleiche Queue, concurrency=1: laeuft nach dem Opfer.
        await app.client.schedule(marker_name, queue=queue, delay=1.0)
        await asyncio.wait_for(marker_done.wait(), timeout=_EVENT_TIMEOUT)

    assert victim_ran is False


@pytest.mark.befund("BZ-02")
async def test_bz02_unique_completion_flag_released_after_delayed_run(rabbitmq_url, valkey_url):
    queue, task_name = _queue(), _name("uniq")
    runs = 0
    first_done = asyncio.Event()

    class UniqueTask(Task):
        task_info = TaskInfo(name=task_name, queues=[queue], unique_for=30)

        async def run(self, key: int) -> None:
            nonlocal runs
            runs += 1
            first_done.set()

    async with _leader_app(rabbitmq_url, valkey_url, queue) as app:
        first = await app.client.schedule(task_name, 1, delay=0.2)
        await asyncio.wait_for(first_done.wait(), timeout=_EVENT_TIMEOUT)
        # Die Freigabe folgt dem Lauf; bis zu 5 s Zeit geben (weit unter unique_for).
        second = first

        async def released() -> bool:
            nonlocal second
            second = await app.client.schedule(task_name, 1, delay=0.2)
            return second != first

        await wait_until(released, timeout=5.0, interval=0.1)

    assert second != first


# ---------------------------------------------------------------------------
# BZ-11: Fire eines Crons ohne Task landet ohne Ablaufzeit im .XQ
# ---------------------------------------------------------------------------


async def _get_xq_message(rabbitmq_url: str, queue: str) -> aio_pika.abc.AbstractIncomingMessage:
    connection = await aio_pika.connect(rabbitmq_url)
    try:
        channel = await connection.channel()
        xq = await channel.declare_queue(f"{queue}.XQ", passive=True)
        message: aio_pika.abc.AbstractIncomingMessage | None = None

        async def arrived() -> bool:
            nonlocal message
            message = await xq.get(no_ack=True, fail=False)
            return message is not None

        if not await wait_until(arrived, timeout=_EVENT_TIMEOUT):
            raise TimeoutError(f"keine Nachricht in {queue}.XQ")
        assert message is not None
        return message
    finally:
        await connection.close()


@pytest.mark.befund("BZ-11")
@pytest.mark.xfail(
    strict=True,
    reason="BZ-11: Fire ohne registrierten Task geht per nack/DLX ohne Ablaufzeit ins .XQ",
)
async def test_bz11_fire_of_removed_cron_task_dead_letters_with_ttl(rabbitmq_url, valkey_url):
    queue, removed = _queue(), _name("removed_cron")

    class StillThere(Task):
        task_info = TaskInfo(name=_name("still_there"), queues=[queue])

        async def run(self) -> None:
            return None

    async with _leader_app(
        rabbitmq_url,
        valkey_url,
        queue,
        dead_letter_ttl_s=3600.0,
    ) as app:
        # Der Cron eines aus dem Code entfernten Tasks bleibt bis zum Prune
        # (24 h) im Store und feuert weiter.
        due_ms = int(time.time() * 1000) - 1000
        definition = ScheduleDefinition(
            schedule_name=removed,
            task_name=removed,
            args=(),
            kwargs={},
            queue=queue,
            cron="* * * * *",
            fire_ts_ms=due_ms,
            source="static",
        )
        await app.backend.schedules.put(removed, msgspec.json.encode(definition), due_ms)

        message = await _get_xq_message(rabbitmq_url, queue)

    headers = message.headers or {}
    assert msgspec.json.decode(message.body)["task"]["name"] == removed
    assert message.expiration is not None or HEADER_DEAD_LETTER_TTL_MS in headers


# ---------------------------------------------------------------------------
# BZ-23: Cancel-Flag lebt nur result_ttl
# ---------------------------------------------------------------------------


@pytest.mark.befund("BZ-23")
async def test_bz23_cancelled_task_waiting_longer_than_result_ttl_does_not_run(
    rabbitmq_url,
    valkey_url,
):
    queue = _queue()
    victim_name, marker_name = _name("victim"), _name("marker")
    victim_ran = False
    marker_done = asyncio.Event()

    class VictimTask(Task):
        task_info = TaskInfo(name=victim_name, queues=[queue])

        async def run(self) -> None:
            nonlocal victim_ran
            victim_ran = True

    class MarkerTask(Task):
        task_info = TaskInfo(name=marker_name, queues=[queue])

        async def run(self) -> None:
            marker_done.set()

    async with _leader_app(
        rabbitmq_url,
        valkey_url,
        queue,
        start_worker=False,
        result_ttl=1,
    ) as app:
        # Kein Worker laeuft (Stillstand/Rueckstau): der Task wartet in der Queue.
        tid = await app.client.schedule(victim_name, queue=queue)
        await app.cancel(tid)
        await app.client.schedule(marker_name, queue=queue)
        # Wartezeit laenger als result_ttl (1 s, statt 24 h).
        await asyncio.sleep(1.5)
        worker = await _start_worker(app)
        try:
            await asyncio.wait_for(marker_done.wait(), timeout=_EVENT_TIMEOUT)
        finally:
            await _stop(worker)

    assert victim_ran is False
