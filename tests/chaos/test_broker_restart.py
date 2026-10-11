"""Chaos U2.2 — F2 Broker (RabbitMQ) restart.

The four tests assert that the RabbitMQBroker survives a full
``docker restart`` of the RabbitMQ container:

1. In-flight tasks resume + complete after recovery.
2. Durable / quorum queues + their declared DLX arguments survive.
3. The U1.1 channel-recreate fix keeps a long-lived publisher alive
   across the restart boundary (publish before + after).
4. A worker reconnects without any manual reset call.

All tests use the ``chaos_stack`` fixture from ``tests/chaos/conftest.py``
which runs a dedicated compose project with RabbitMQ + Valkey +
Toxiproxy on random ports. Direct docker access to the container uses
the compose project name: ``{project}-rabbitmq-1``.
"""

from __future__ import annotations

import asyncio
import contextlib
import subprocess
import time
import uuid

import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.task import Task, TaskInfo
from tests.chaos.conftest import ChaosStack

pytestmark = [pytest.mark.timeout(180)]


def _unique_queue() -> str:
    return f"chaos-{uuid.uuid4().hex[:10]}"


def _docker_restart(container: str) -> None:
    """Restart a docker container synchronously.

    Uses a generous timeout because RabbitMQ's shutdown handshake plus
    the healthcheck polling on the ``restart`` command can take
    ~20-30s on slower hardware.
    """
    subprocess.run(
        ["docker", "restart", container],
        check=True,
        capture_output=True,
        timeout=120,
    )


async def _wait_broker_reachable(url: str, timeout: float = 60.0) -> None:
    """Poll ``aio_pika.connect`` until it succeeds or timeout."""
    import aio_pika

    deadline = time.monotonic() + timeout
    last_exc: Exception | None = None
    while time.monotonic() < deadline:
        try:
            conn = await aio_pika.connect(url, timeout=2.0)
            await conn.close()
            return
        except Exception as exc:
            last_exc = exc
            await asyncio.sleep(0.5)
    raise RuntimeError(f"broker not reachable within {timeout}s: {last_exc!r}")


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------


class _App:
    """Client + worker bundle for a single chaos test.

    Keeps broker / backend instances addressable so tests can still
    inspect them after the restart.
    """

    def __init__(self, stack: ChaosStack, queue: str) -> None:
        self.stack = stack
        self.queue = queue
        self.client_broker = RabbitMQBroker(stack.rabbitmq_url)
        self.client_backend = ValkeyBackend(stack.valkey_url)
        self.client_app = SwarmQ(
            self.client_broker,
            self.client_backend,
            queues=[queue],
        )
        self.worker_broker = RabbitMQBroker(stack.rabbitmq_url)
        self.worker_backend = ValkeyBackend(stack.valkey_url)
        self.worker_app = SwarmQ(
            self.worker_broker,
            self.worker_backend,
            queues=[queue],
        )
        self._worker_task: asyncio.Task | None = None

    async def start(self) -> None:
        await self.client_app.__aenter__()
        await self.client_broker.declare_queue(self.queue)
        # Worker uses its own connections — it manages its own broker
        # context inside start_worker().
        self._worker_task = asyncio.create_task(self.worker_app.start_worker())
        # Allow consumer setup.
        await asyncio.sleep(0.8)

    async def stop(self) -> None:
        if self._worker_task is not None:
            self._worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._worker_task
        with contextlib.suppress(Exception):
            await self.worker_broker.close()
        with contextlib.suppress(Exception):
            await self.worker_backend.close()
        with contextlib.suppress(Exception):
            await self.client_app.__aexit__(None, None, None)


# ---------------------------------------------------------------------------
# Test class — one shared chaos_stack across the four scenarios.
# ---------------------------------------------------------------------------


class TestBrokerRestart:
    async def test_inflight_tasks_complete_after_restart(
        self,
        chaos_stack: ChaosStack,
    ) -> None:
        """10 tasks are scheduled; mid-flight RabbitMQ restart; all finish."""
        queue = _unique_queue()
        task_name = f"chaos_sleep_{uuid.uuid4().hex[:6]}"

        class SleepTask(Task):
            task_info = TaskInfo(name=task_name, queues=[queue])

            async def run(self, idx: int) -> int:
                # Short work-body — enough to span the restart window.
                await asyncio.sleep(0.2)
                return idx

        app = _App(chaos_stack, queue)
        await app.start()
        try:
            task_ids: list[str] = []
            for i in range(10):
                tid = await app.client_app.schedule(task_name, i)
                task_ids.append(tid)

            # Give the worker a moment to pick up a few, then nuke
            # the broker so we exercise the reconnect path.
            await asyncio.sleep(0.2)
            _docker_restart(f"{chaos_stack.project}-rabbitmq-1")
            await _wait_broker_reachable(chaos_stack.rabbitmq_url)

            # Worker + client must reconnect on their own. Allow a
            # generous window for message redelivery + re-consume.
            results = await app.client_app.get_results(
                task_ids,
                timeout=90,
            )
            assert sorted(results) == list(range(10)), f"not every task completed after restart: got {results!r}"
        finally:
            await app.stop()

    async def test_quorum_queue_survives_restart(
        self,
        chaos_stack: ChaosStack,
    ) -> None:
        """Quorum queue declaration + message count survive restart."""
        queue = _unique_queue()

        # Publisher-only: declare, publish 5 messages, NO consumer.
        broker = RabbitMQBroker(chaos_stack.rabbitmq_url)
        await broker.connect()
        try:
            await broker.declare_queue(queue)
            for i in range(5):
                await broker.publish(queue, f"m{i}".encode())
            # Let RabbitMQ flush to disk — quorum queues need a raft
            # commit before durability is guaranteed.
            await asyncio.sleep(1.0)
        finally:
            await broker.close()

        _docker_restart(f"{chaos_stack.project}-rabbitmq-1")
        await _wait_broker_reachable(chaos_stack.rabbitmq_url)

        # Re-connect, passive-declare (ensure=True would redefine args
        # with wrong values — we use get_queue which does a passive
        # lookup via aio_pika). Then inspect the message count.
        import aio_pika

        conn = await aio_pika.connect(chaos_stack.rabbitmq_url, timeout=5.0)
        try:
            ch = await conn.channel()
            q = await ch.get_queue(queue)
            # declaration_result.message_count is the canonical post-
            # restart counter.
            declared = await ch.declare_queue(
                queue,
                durable=True,
                arguments={
                    "x-queue-type": "quorum",
                    "x-dead-letter-exchange": "",
                    "x-dead-letter-routing-key": f"{queue}.XQ",
                },
                passive=True,
            )
            assert declared.declaration_result.message_count == 5, (
                f"quorum queue lost messages across restart: {declared.declaration_result.message_count} / 5"
            )
            # Drain so the next run leaves no cruft.
            await q.purge()
        finally:
            await conn.close()

    async def test_publisher_before_and_after_restart(
        self,
        chaos_stack: ChaosStack,
    ) -> None:
        """Long-lived publisher survives a restart (U1.1 channel-recreate)."""
        queue = _unique_queue()

        broker = RabbitMQBroker(chaos_stack.rabbitmq_url)
        await broker.connect()
        try:
            await broker.declare_queue(queue)

            # Before restart.
            await broker.publish(queue, b"pre-1")
            await broker.publish(queue, b"pre-2")

            _docker_restart(f"{chaos_stack.project}-rabbitmq-1")
            await _wait_broker_reachable(chaos_stack.rabbitmq_url)

            # After restart — the SAME broker instance must publish
            # again. Without U1.1 this raises ChannelInvalidStateError.
            # Some delay so aio_pika's robust-connection reconnects.
            await asyncio.sleep(2.0)

            await broker.publish(queue, b"post-1")
            await broker.publish(queue, b"post-2")

            # Count via a fresh channel so we don't mix state with the
            # channel that went through the reconnect.
            import aio_pika

            conn = await aio_pika.connect(
                chaos_stack.rabbitmq_url,
                timeout=5.0,
            )
            try:
                ch = await conn.channel()
                declared = await ch.declare_queue(
                    queue,
                    durable=True,
                    arguments={
                        "x-queue-type": "quorum",
                        "x-dead-letter-exchange": "",
                        "x-dead-letter-routing-key": f"{queue}.XQ",
                    },
                    passive=True,
                )
                # Pre + post messages all accounted for (durable queue,
                # no consumer).
                assert declared.declaration_result.message_count == 4, (
                    f"expected 4 messages, got {declared.declaration_result.message_count}"
                )
                q = await ch.get_queue(queue)
                await q.purge()
            finally:
                await conn.close()
        finally:
            await broker.close()

    async def test_worker_reconnect_without_manual_reset(
        self,
        chaos_stack: ChaosStack,
    ) -> None:
        """Worker picks up a task published AFTER the restart, no resets."""
        queue = _unique_queue()
        task_name = f"chaos_echo_{uuid.uuid4().hex[:6]}"

        class EchoTask(Task):
            task_info = TaskInfo(name=task_name, queues=[queue])

            async def run(self, value: str) -> str:
                return value

        app = _App(chaos_stack, queue)
        await app.start()
        try:
            # Baseline — one task pre-restart so we know the wiring
            # is healthy.
            pre_tid = await app.client_app.schedule(task_name, "pre")
            assert await app.client_app.get_result(pre_tid, timeout=20) == "pre"

            _docker_restart(f"{chaos_stack.project}-rabbitmq-1")
            await _wait_broker_reachable(chaos_stack.rabbitmq_url)
            # Let the robust connection land on both ends.
            await asyncio.sleep(3.0)

            # No manual ``connect()`` / ``start_worker()`` call — the
            # existing worker has to pick this up on its own.
            post_tid = await app.client_app.schedule(task_name, "post")
            assert await app.client_app.get_result(post_tid, timeout=60) == "post"
        finally:
            await app.stop()
