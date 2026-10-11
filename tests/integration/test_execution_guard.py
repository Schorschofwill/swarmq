"""Ausführungsschutz gegen echtes RabbitMQ und Valkey (Plan 2026-10-08-1541,
U8, KTD5; AE5).

Ein abgestürzter Worker wird durch einen Claim ohne Freigabe mit kurzer TTL
nachgestellt -- genau das hinterlässt ein Prozess, der mitten im Task stirbt.

Test-Liste:
1. Running-Marker eines toten Workers ist abgelaufen → die erneute
   Zustellung läuft normal.
2. Drei Abstürze derselben Ausführung → die nächste Zustellung liegt als
   ``WorkerHungError`` in der DLQ, der Task lief nicht.
3. ``dlq retry`` innerhalb der Done-TTL läuft erneut; eine erneute Zustellung
   dieser Wiederholung wird übersprungen.
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from collections.abc import AsyncIterator, Callable
from typing import Any

import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import TaskError
from swarmq.serialization import encode_json
from swarmq.task import Task, TaskInfo
from tests.unit._log_capture import capture_swarmq_logs

pytestmark = [pytest.mark.integration, pytest.mark.timeout(20)]


@pytest.fixture
def queue() -> str:
    return f"guard-{uuid.uuid4().hex[:8]}"


@pytest.fixture
async def guard_backend(valkey_url: str) -> AsyncIterator[ValkeyBackend]:
    backend = ValkeyBackend(valkey_url)
    await backend.connect()
    yield backend
    await backend.close()


@pytest.fixture
def runs() -> list[str]:
    return []


@pytest.fixture
def _tasks(queue: str, runs: list[str]) -> Any:
    class Counted(Task):
        task_info = TaskInfo(name="guard_counted", queues=[queue])

        async def run(self) -> str:
            runs.append("run")
            return "ok"

    class Failing(Task):
        task_info = TaskInfo(name="guard_failing", queues=[queue], max_retries=0)

        async def run(self) -> str:
            runs.append("run")
            if len(runs) == 1:
                raise RuntimeError("first run fails")
            return "second"

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


def _body(task_id: str, queue: str, name: str = "guard_counted") -> bytes:
    return encode_json({"task": {"id": task_id, "queue": queue, "name": name}, "args": [], "kwargs": {}})


async def _crash(backend: ValkeyBackend, identity: str) -> None:
    """Claim ohne Freigabe, der Marker läuft nach 100 ms ab."""
    claim = await backend.execution_guard.claim(
        identity,
        owner=uuid.uuid4().hex,
        retry_count=0,
        ttl_ms=100,
        state_ttl_ms=600_000,
        max_orphans=99,
    )
    assert claim.status == "claimed"
    await asyncio.sleep(0.2)


async def _wait_for(predicate: Callable[[], bool], timeout: float = 10.0) -> None:
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.05)


async def test_redelivery_after_a_crashed_worker_runs_once_the_marker_expired(
    rabbitmq_url,
    valkey_url,
    queue,
    guard_backend,
    runs,
    _tasks,
) -> None:
    task_id = uuid.uuid4().hex
    await _crash(guard_backend, task_id)

    async with _worker(rabbitmq_url, valkey_url, queue) as app:
        await app.broker.publish(queue, _body(task_id, queue))
        assert await app.get_result(task_id, timeout=10) == "ok"

    assert runs == ["run"]


async def test_three_crashes_dead_letter_the_execution_as_worker_hung_error(
    rabbitmq_url,
    valkey_url,
    queue,
    guard_backend,
    runs,
    _tasks,
) -> None:
    task_id = uuid.uuid4().hex
    for _ in range(3):
        await _crash(guard_backend, task_id)

    async with _worker(rabbitmq_url, valkey_url, queue) as app:
        await app.broker.publish(queue, _body(task_id, queue))
        with pytest.raises(TaskError, match="WorkerHungError"):
            await app.get_result(task_id, timeout=10)
        # Das Fehlerergebnis wird vor dem DLQ-Publish gespeichert.
        letters: list[dict[str, Any]] = []

        async def _dead_lettered() -> bool:
            letters[:] = [e for e in (await app.broker.list_dead_letters(queue))[0] if e["task_id"] == task_id]
            return bool(letters)

        async with asyncio.timeout(10):
            while not await _dead_lettered():
                await asyncio.sleep(0.05)

    assert runs == []
    [letter] = letters
    assert "WorkerHungError" in letter["headers"]["x-swarmq-traceback"]


async def test_dlq_retry_runs_again_and_its_redelivery_is_skipped(
    rabbitmq_url,
    valkey_url,
    queue,
    runs,
    _tasks,
) -> None:
    with capture_swarmq_logs() as logs:
        async with _worker(rabbitmq_url, valkey_url, queue) as app:
            task_id = await app.schedule("guard_failing")
            with pytest.raises(TaskError):
                await app.get_result(task_id, timeout=10)
            await _wait_for(lambda: any(e["event"] == "task_sent_to_dlq" for e in logs))
        assert runs == ["run"]

        # Wiederholung aus der DLQ -- zweimal zugestellt (Original und eine
        # Neuzustellung derselben Wiederholung, gleiche Replay-ID).
        broker = RabbitMQBroker(rabbitmq_url)
        await broker.connect()
        try:
            assert (await broker.retry_dead_letter(queue, task_id))[0] is True
            channel = await broker._connection.channel()
            message = await (await channel.get_queue(queue)).get(no_ack=True)
            await channel.close()
            assert message is not None
            headers = dict(message.headers or {})
            assert headers.get("x-swarmq-replay-id")
            for _ in range(2):
                await broker.publish(queue, message.body, headers=headers)
        finally:
            await broker.close()

        async with _worker(rabbitmq_url, valkey_url, queue) as app:
            await _wait_for(lambda: any(e["event"] == "duplicate_delivery_skipped" for e in logs))
            assert await app.get_result(task_id, timeout=10) == "second"

    assert runs == ["run", "run"]
