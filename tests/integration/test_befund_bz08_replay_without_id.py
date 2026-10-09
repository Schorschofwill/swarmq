"""Nachweis BZ-08: DLQ-Replay ohne ``x-swarmq-replay-id`` wird vom
Ausfuehrungsschutz still verworfen.

Register: docs/befunde/betrieb-ueber-zeit-2026-10.md (BZ-08).

Nur ``swarmq dlq retry`` ab 0.11 setzt eine neue ``x-swarmq-replay-id``. Wer
die Nachricht anders aus dem ``.XQ`` zurueckspielt -- "Move messages" in der
RabbitMQ-UI, Shovel, swarmv (pinnt swarmq<0.8), ein CLI vor 0.11 --,
publiziert sie mit derselben Task-ID und ohne Replay-ID. ``dead_letter`` hat
den Done-Marker gesetzt (TTL 3900 s); ``guard_claim.lua`` liefert ``done``,
das Gate bestaetigt und verwirft die Nachricht mit einer INFO-Zeile
(``duplicate_delivery_skipped``). Der Task laeuft nicht.

Gegen echtes RabbitMQ und Valkey (Testports 5699/6399 via itest.sh).

Testliste:
# [x] Task scheitert -> DLQ; Nachricht unveraendert (ohne Replay-ID) aus dem
#     .XQ in die Haupt-Queue zurueckgelegt -> der Task laeuft erneut
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
from swarmq.task import Task, TaskInfo
from tests.support.wait import wait_until
from tests.unit._log_capture import capture_swarmq_logs

pytestmark = [pytest.mark.integration, pytest.mark.timeout(30)]


@pytest.fixture
def queue() -> str:
    return f"bz08-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def runs() -> list[str]:
    return []


@pytest.fixture
def _tasks(queue: str, runs: list[str]) -> Any:
    class FailsOnce(Task):
        task_info = TaskInfo(name="bz08_fails_once", queues=[queue], max_retries=0)

        async def run(self) -> str:
            runs.append("run")
            if len(runs) == 1:
                raise RuntimeError("bug, fixed before the replay")
            return "fixed"

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


async def _wait_for(predicate: Callable[[], bool], timeout: float = 10.0) -> None:
    if not await wait_until(predicate, timeout=timeout):
        raise TimeoutError(f"condition not met within {timeout} s")


async def _move_back_like_shovel(rabbitmq_url: str, queue: str) -> dict[str, Any]:
    """Nachricht aus ``{queue}.XQ`` holen und unveraendert (Body + Header,
    ohne neue Replay-ID) in die Haupt-Queue legen -- wie "Move messages"."""
    broker = RabbitMQBroker(rabbitmq_url)
    await broker.connect()
    try:
        channel = await broker._connection.channel()
        message = await (await channel.get_queue(f"{queue}.XQ")).get(no_ack=True)
        await channel.close()
        assert message is not None, "Voraussetzung: Nachricht liegt im .XQ"
        headers = dict(message.headers or {})
        assert "x-swarmq-replay-id" not in headers
        await broker.publish(queue, message.body, headers=headers)
        return headers
    finally:
        await broker.close()


@pytest.mark.befund("BZ-08")
@pytest.mark.xfail(
    strict=True,
    reason="BZ-08: Replay aus dem .XQ ohne x-swarmq-replay-id wird als Duplikat (done) still bestaetigt und verworfen",
)
async def test_replay_without_replay_id_runs_the_task_again(rabbitmq_url, valkey_url, queue, runs, _tasks) -> None:
    with capture_swarmq_logs() as logs:
        async with _worker(rabbitmq_url, valkey_url, queue) as app:
            task_id = await app.schedule("bz08_fails_once")
            with pytest.raises(TaskError):
                await app.get_result(task_id, timeout=10)
            await _wait_for(lambda: any(e["event"] == "task_sent_to_dlq" for e in logs))
        assert runs == ["run"]

        # Bug behoben, Operator spielt die DLQ mit einem Fremdwerkzeug zurueck.
        await _move_back_like_shovel(rabbitmq_url, queue)

        async with _worker(rabbitmq_url, valkey_url, queue):
            await _wait_for(
                lambda: len(runs) >= 2 or any(e["event"] == "duplicate_delivery_skipped" for e in logs),
            )

    skipped = [e for e in logs if e["event"] == "duplicate_delivery_skipped"]
    assert runs == ["run", "run"], (
        f"Replay nicht ausgefuehrt, still verworfen: {[(e['event'], e.get('reason')) for e in skipped]}"
    )
