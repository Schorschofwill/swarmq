"""Ausfuehrungsidentitaet ueber die Kette Kopie -> Delay-Stufe -> Konsum
(Plan 0.12.0, MR 12, BZ-08; Spike S5).

Ein Broker-DLX baut ``x-death`` neu auf: Die Retry-Kopie einer Nachricht mit
``x-death`` fuer die Haupt-Queue (n = 1, etwa nach "Move messages" aus dem
``.XQ``) verliert beim Ablauf in der Delay-Stufe den Eintrag der Haupt-Queue.
Die Kopie traegt n deshalb als ``x-swarmq-xdeath-base`` -- der zweite Versuch
laeuft unter derselben Identitaet ``task_id#1`` wie der erste, nicht unter
``task_id``.

Gegen echtes RabbitMQ und Valkey (Testports 5699/6399 via itest.sh).

Testliste:
# [x] Retry-Kopie ueber die Stufe {q}.DQ.1s: beide Versuche unter task_id#1,
#     die Identitaet task_id (n = 0) bleibt unbenutzt
"""

from __future__ import annotations

import asyncio
import contextlib
import time
import uuid
from collections.abc import AsyncIterator
from typing import Any

import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import Retry
from swarmq.serialization import encode_json
from swarmq.task import Task, TaskInfo
from tests.support.wait import wait_until

pytestmark = [pytest.mark.integration, pytest.mark.timeout(30)]


@pytest.fixture
def queue() -> str:
    return f"idchain-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def attempts() -> list[int]:
    return []


@pytest.fixture
def _tasks(queue: str, attempts: list[int]) -> Any:
    class RetriesOnce(Task):
        task_info = TaskInfo(name="idchain_retries_once", queues=[queue], max_retries=1, retry_base_delay=1.0)

        async def run(self) -> str:
            attempts.append(len(attempts) + 1)
            if len(attempts) == 1:
                raise Retry(delay=1.0)
            return "ok"

    yield
    Task._clear_registered_tasks()


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


async def _claim_status(app: SwarmQ, identity: str) -> str:
    claim = await app.backend.execution_guard.claim(
        identity,
        owner="probe",
        retry_count=1,
        ttl_ms=1_000,
        state_ttl_ms=1_000,
        max_orphans=99,
    )
    return claim.status


async def test_retry_copy_through_a_delay_tier_keeps_the_identity(rabbitmq_url, valkey_url, queue, attempts, _tasks):
    task_id = f"idchain-{uuid.uuid4().hex[:8]}"
    task = {"id": task_id, "queue": queue, "name": "idchain_retries_once"}
    body = encode_json({"task": task, "args": [], "kwargs": {}})
    # Wie nach "Move messages" aus dem .XQ: ein Broker-Tod fuer die Haupt-Queue.
    moved = {
        "x-swarmq-enqueued-at": int(time.time()),
        "x-death": [{"queue": queue, "count": 1, "reason": "rejected", "exchange": "", "routing-keys": [queue]}],
    }
    async with _worker(rabbitmq_url, valkey_url, queue) as app:
        await app.broker.publish(queue, body, headers=moved)
        assert await app.get_result(task_id, timeout=15) == "ok"
        assert attempts == [1, 2]

        # Beide Versuche liefen unter task_id#1 (die Retry-Kopie verliert das
        # x-death der Haupt-Queue in der Stufe, traegt n aber als Base); der
        # Done-Marker folgt dem Ack nach dem Ergebnis. Die Identitaet ohne
        # Tode wurde nie beansprucht.
        async def done() -> bool:
            return await _claim_status(app, f"{task_id}#1") == "done"

        assert await wait_until(done, timeout=10)
        assert await _claim_status(app, task_id) == "claimed"
