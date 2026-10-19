"""Abbruch im Speicheralarm gibt den Unique-Claim frei (Plan 0.14, U4, E-041).

Der RabbitMQ-Broker wartet vor dem Senden, solange die Publish-Verbindung
blockiert ist (``_wait_while_blocked``, BZ-16). Bricht der Aufrufer dort ab,
ist nachweislich nichts gesendet: Der Broker meldet ``PublishCancelledUnsent``,
der Client gibt den Claim frei. Nach dem Alarm gelingt die Wiederholung, und
in der Queue liegt genau eine Nachricht.

Test-Liste:
- [x] ``schedule(unique_for=…)`` im echten Alarm, Abbruch im Warten: Claim
      frei, Wiederholung nach dem Alarm gelingt, genau eine Nachricht
"""

from __future__ import annotations

import asyncio
import uuid
from typing import override

import pytest

from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.broker.rabbitmq.blocked import unblocked_event
from swarmq.client import Client
from swarmq.exceptions import PublishCancelledUnsent
from swarmq.registry import TaskRegistry
from swarmq.task import Task, TaskInfo
from tests.integration.broker.conftest import MemoryAlarm, wait_for_alarm
from tests.support.queues import delete_queue_family
from tests.support.wait import wait_until

# exclusive: der Speicheralarm blockiert jeden Verleger des Brokers.
pytestmark = [pytest.mark.integration, pytest.mark.timeout(60), pytest.mark.exclusive]


def _registry(queue: str) -> TaskRegistry:
    class Once(Task):
        task_info = TaskInfo(name=f"{queue}.once", queues=[queue])

        @override
        async def run(self, n: int = 0) -> int:
            return n

    registry = TaskRegistry()
    registry.register(Once)
    return registry


@pytest.mark.entscheidung("E-041")
async def test_cancel_while_the_broker_blocks_releases_the_unique_claim(
    rabbitmq_url: str, valkey_url: str, memory_alarm: MemoryAlarm
) -> None:
    queue = f"u4-blocked-{uuid.uuid4().hex[:10]}"
    warm = f"{queue}-warm"
    task_name = f"{queue}.once"
    broker = RabbitMQBroker(rabbitmq_url, queue_type="classic")
    broker.publish_timeout_s = 30.0
    backend = ValkeyBackend(valkey_url)
    await broker.connect()
    await backend.connect()
    client = Client(broker, backend, registry=_registry(queue))
    warm_publish: asyncio.Task[object] | None = None
    try:
        await broker.declare_queue(queue)
        await broker.declare_queue(warm)

        memory_alarm()
        await wait_for_alarm(active=True)
        # RabbitMQ blockiert eine Verbindung (``connection.blocked``) erst,
        # wenn sie im Alarm publiziert; dieser Publish geht noch auf den Socket.
        warm_publish = asyncio.create_task(broker.publish(warm, b"{}"))

        async def blocked() -> bool:
            event = unblocked_event(broker._connection)
            return isinstance(event, asyncio.Event) and not event.is_set()

        assert await wait_until(blocked, timeout=10, interval=0.05), "precondition: connection.blocked arrived"

        schedule = asyncio.create_task(client.schedule(task_name, 1, unique_for=300))
        done, _ = await asyncio.wait({schedule}, timeout=1.0)
        assert not done, "precondition: the publish waits while the broker blocks"
        schedule.cancel()
        with pytest.raises(PublishCancelledUnsent):
            await schedule
        assert schedule.cancelled()

        await asyncio.to_thread(memory_alarm.release)
        retried = await client.schedule(task_name, 1, unique_for=300)
        duplicate = await client.schedule(task_name, 1, unique_for=300)

        assert duplicate == retried, "the retry holds the claim again"
        assert await broker.queue_depth(queue) == 1, "the task was enqueued exactly once"
    finally:
        if warm_publish is not None and not warm_publish.done():
            warm_publish.cancel()
        if warm_publish is not None:
            await asyncio.gather(warm_publish, return_exceptions=True)
        await broker.close()
        await backend.close()
        await delete_queue_family(rabbitmq_url, queue, warm)
