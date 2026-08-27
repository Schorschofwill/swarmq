"""Integration test for Worker.start()/shutdown embedding the scheduler.

Verifies that (a) Worker.start() constructs and starts a real
SchedulerLoop, (b) the worker becomes scheduler leader, and
(c) graceful shutdown emits HANDOVER + releases the leader lock.
"""

from __future__ import annotations

import asyncio
import uuid

import pytest
import valkey.asyncio as valkey_asyncio

from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.config import SwarmQConfig
from swarmq.locking import SCHEDULER_LOCK_KEY
from swarmq.registry import TaskRegistry
from swarmq.signals import Signal, SignalDispatcher
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker

pytestmark = [pytest.mark.integration, pytest.mark.timeout(5)]


def _unique_queue() -> str:
    return f"sched-int-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def _cleanup_tasks():
    yield
    Task._clear_registered_tasks()


class TestWorkerSchedulerIntegration:
    async def test_worker_start_emits_leader_signal_and_shutdown_releases(
        self, rabbitmq_url, valkey_url, _cleanup_tasks,
    ):
        queue = _unique_queue()

        class NoopTask(Task):
            task_info = TaskInfo(name=f"sched_noop_{queue}", queues=[queue])
            async def run(self):
                return "ok"

        registry = TaskRegistry()
        registry.register(NoopTask)

        config = SwarmQConfig(
            broker_url=rabbitmq_url,
            backend_url=valkey_url,
            queues=[queue],
            concurrency=1,
            scheduler_heartbeat_interval=0.2,
            scheduler_poll_interval=0.2,
            scheduler_lock_ttl=1.0,
        )
        dispatcher = SignalDispatcher()
        leader_events: list[str] = []
        handover_events: list[str] = []

        @dispatcher.on_signal(Signal.SCHEDULER_LEADER)
        async def _l(**kw):
            leader_events.append("leader")

        @dispatcher.on_signal(Signal.SCHEDULER_LEADER_HANDOVER)
        async def _ho(**kw):
            handover_events.append("handover")

        broker = RabbitMQBroker(rabbitmq_url)
        backend = ValkeyBackend(valkey_url)
        worker = Worker(
            broker=broker, backend=backend, registry=registry,
            config=config, signal_dispatcher=dispatcher,
        )

        # Event barrier for leader signal (Unit 2 / H-5).
        leader_evt = asyncio.Event()

        @dispatcher.on_signal(Signal.SCHEDULER_LEADER)
        async def _leader_barrier(**kw) -> None:
            leader_evt.set()

        worker_task = asyncio.create_task(worker.start())
        try:
            await asyncio.wait_for(leader_evt.wait(), timeout=3)
            assert leader_events, "SCHEDULER_LEADER never emitted"

            # Verify the lock exists in Valkey under the expected key.
            client = valkey_asyncio.Valkey.from_url(valkey_url)
            try:
                owner = await client.get(SCHEDULER_LOCK_KEY)
                assert owner is not None
            finally:
                await client.aclose()

            # Request graceful shutdown by firing the event. start()
            # unblocks and runs _execute_shutdown, which calls
            # scheduler.stop() (HANDOVER + release) before closing
            # the backend.
            worker._shutdown_event.set()
            await asyncio.wait_for(worker_task, timeout=10)
        finally:
            if not worker_task.done():
                worker_task.cancel()
                try:
                    await worker_task
                except (asyncio.CancelledError, Exception):
                    pass

        # Handover signal must have fired during shutdown.
        assert handover_events, "HANDOVER never emitted on shutdown"

        # Lock should be released (cleaned up).
        client = valkey_asyncio.Valkey.from_url(valkey_url)
        try:
            owner = await client.get(SCHEDULER_LOCK_KEY)
            assert owner is None
        finally:
            await client.aclose()
