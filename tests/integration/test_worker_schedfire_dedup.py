"""Integration test for the consumer-side schedfire dedup (Unit 8).

Publishes two identical scheduled-fire messages carrying the same
``x-swarmq-schedfire`` header and verifies that only one execution
occurs. Uses real RabbitMQ + Valkey.
"""

from __future__ import annotations

import asyncio
import uuid

import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.serialization import encode_json
from swarmq.task import Task, TaskInfo

pytestmark = [pytest.mark.integration, pytest.mark.timeout(5)]


def _unique_queue() -> str:
    return f"schedfire-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def _cleanup_tasks():
    yield
    Task._clear_registered_tasks()


class TestSchedfireDedupIntegration:
    async def test_duplicate_schedfire_executes_only_once(
        self, rabbitmq_url, valkey_url, _cleanup_tasks,
    ):
        queue = _unique_queue()
        call_count = 0
        done = asyncio.Event()

        class PingTask(Task):
            task_info = TaskInfo(name="sf_ping", queues=[queue])

            async def run(self) -> str:
                nonlocal call_count
                call_count += 1
                done.set()
                return "ok"

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)

        async with SwarmQ(
            broker,
            backend,
            queues=[queue],
            concurrency=1,
        ) as app:
            await app.broker.declare_queue(queue)

            # Publish two messages with identical x-swarmq-schedfire header.
            schedfire_key = f"{queue}-sched:1700000000000"
            task_id = uuid.uuid4().hex
            payload = encode_json({
                "task": {"id": task_id, "queue": queue, "name": "sf_ping"},
                "args": [],
                "kwargs": {},
            })

            for _ in range(2):
                await app.broker.publish(
                    queue,
                    payload,
                    headers={"x-swarmq-schedfire": schedfire_key},
                )

            worker_task = asyncio.create_task(app.start_worker())
            try:
                # First delivery executes → done.set().
                await asyncio.wait_for(done.wait(), timeout=5)
                # Legitimate timer wait, not synchronization: we are
                # asserting the ABSENCE of a second execution. The
                # dedup path (ack + log + return True) emits no
                # observable event, so a bounded wait is the only
                # way to test "nothing happens."
                await asyncio.sleep(0.5)
            finally:
                worker_task.cancel()
                try:
                    await worker_task
                except asyncio.CancelledError:
                    pass

            assert call_count == 1, (
                f"expected exactly one execution, got {call_count}"
            )
