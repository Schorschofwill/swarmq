"""Nachweis BZ-25: Task-Locks werden nicht erneuert und haben kein Fencing.

Register: docs/befunde/betrieb-ueber-zeit-2026-10.md (BZ-25).

Die Lock-TTL ist ``hard_timeout + 30 s`` (``compute_lock_ttl_ms``) und wird
waehrend des Laufs nie verlaengert (``LockStore.renew`` existiert, der Worker
ruft es fuer Task-Locks nicht auf). Laeuft der Halter laenger als die TTL --
blockierter Event-Loop im IO-Modus, Code, der ``CancelledError`` schluckt --,
verfaellt der Lock still und ein zweiter Halter bekommt ihn. Kein Log, kein
Fencing-Token.

Kleine TTL statt Warten: ``compute_lock_ttl_ms`` im Lock-Gate liefert 400 ms;
der Halter laeuft absichtlich laenger (wartet auf ein Event).

Testliste:
# [x] Halter laeuft laenger als die Lock-TTL -> ein zweiter Acquire desselben
#     Mutex scheitert, solange der Halter laeuft
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from collections.abc import AsyncIterator
from typing import Any, override

import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.task import Task, TaskInfo
from swarmq.worker.gates import lock as lock_gate

pytestmark = [pytest.mark.integration, pytest.mark.timeout(30)]

_SHORT_TTL_MS = 400


@pytest.fixture
def queue() -> str:
    return f"bz25-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def lock_name() -> str:
    return f"bz25-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def holder(queue: str, lock_name: str) -> Any:
    started = asyncio.Event()
    proceed = asyncio.Event()

    class LongHolder(Task):
        task_info = TaskInfo(name="bz25_long_holder", queues=[queue], lock=lock_name, hard_timeout=20)

        @override
        async def run(self) -> str:
            started.set()
            await proceed.wait()  # laeuft laenger als die (kleine) Lock-TTL
            return "done"

    yield started, proceed
    proceed.set()
    Task._clear_registered_tasks()


@pytest.fixture
async def second_worker_backend(valkey_url: str) -> AsyncIterator[ValkeyBackend]:
    backend = ValkeyBackend(valkey_url)
    await backend.connect()
    yield backend
    await backend.close()


@contextlib.asynccontextmanager
async def _worker(rabbitmq_url: str, valkey_url: str, queue: str) -> AsyncIterator[SwarmQ]:
    app = SwarmQ(RabbitMQBroker(rabbitmq_url), ValkeyBackend(valkey_url), queues=[queue], concurrency=2)
    async with app:
        await app.broker.declare_queue(queue)
        worker = asyncio.create_task(app.start_worker())
        try:
            yield app
        finally:
            worker.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await worker


@pytest.mark.befund("BZ-25")
async def test_mutex_stays_held_while_the_holder_runs_past_the_lock_ttl(
    rabbitmq_url,
    valkey_url,
    queue,
    lock_name,
    holder,
    second_worker_backend,
    monkeypatch,
) -> None:
    monkeypatch.setattr(lock_gate, "compute_lock_ttl_ms", lambda _hard_timeout: _SHORT_TTL_MS)
    started, proceed = holder

    async with _worker(rabbitmq_url, valkey_url, queue) as app:
        task_id = await app.schedule("bz25_long_holder")
        await asyncio.wait_for(started.wait(), timeout=10)
        locks = second_worker_backend.locks
        held_at_start = await locks.acquire(lock_name, owner="probe", ttl_ms=5_000, max_concurrency=1)
        assert held_at_start is None, "Voraussetzung: der Halter haelt den Mutex"

        # Halter laeuft weiter, die (kleine) Lock-TTL verstreicht.
        await asyncio.sleep(3 * _SHORT_TTL_MS / 1000)
        second = await locks.acquire(lock_name, owner="second-worker", ttl_ms=5_000, max_concurrency=1)
        try:
            assert second is None, "zweiter Halter bekam den Mutex, waehrend der erste noch laeuft"
        finally:
            if second is not None:
                await locks.release(second)
            proceed.set()
        assert await app.get_result(task_id, timeout=10) == "done"
