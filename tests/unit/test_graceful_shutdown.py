"""Unit tests for graceful shutdown (Unit 2.5)."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.config import SwarmQConfig
from swarmq.registry import TaskRegistry
from swarmq.serialization import encode_json
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker, WorkerState


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_message(payload: dict, headers: dict | None = None) -> MagicMock:
    msg = MagicMock()
    msg.body = encode_json(payload)
    msg.headers = headers or {}
    msg.ack = AsyncMock()
    msg.nack = AsyncMock()
    return msg


def _default_payload(
    task_id: str = "abc-123",
    queue: str = "default",
    name: str = "shutdown_add",
    args: list | None = None,
    kwargs: dict | None = None,
) -> dict:
    return {
        "task": {"id": task_id, "queue": queue, "name": name},
        "args": args if args is not None else [1, 2],
        "kwargs": kwargs if kwargs is not None else {},
    }


class ShutdownAddTask(Task):
    task_info = TaskInfo(name="shutdown_add", queues=["default"])

    async def run(self, a: int, b: int) -> int:
        return a + b


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def broker():
    b = MagicMock(spec=RabbitMQBroker)
    b.connect = AsyncMock()
    b.declare_queue = AsyncMock()
    b.consume = AsyncMock()
    b.ack = AsyncMock()
    b.nack = AsyncMock()
    b.close = AsyncMock()
    b.cancel_consume = AsyncMock()
    b.publish_to_delay_queue = AsyncMock()
    b.publish_to_dead_letter_queue = AsyncMock()
    return b


@pytest.fixture()
def backend():
    be = MagicMock(spec=ValkeyBackend)
    be.connect = AsyncMock()
    be.store_result = AsyncMock()
    be.close = AsyncMock()
    return be


@pytest.fixture()
def config():
    return SwarmQConfig(
        broker_url="amqp://localhost",
        backend_url="valkey://localhost",
        concurrency=10,
        queues=["default"],
    )


@pytest.fixture()
def registry():
    reg = TaskRegistry()
    reg.register(ShutdownAddTask)
    return reg


@pytest.fixture()
def worker(broker, backend, registry, config):
    return Worker(broker=broker, backend=backend, registry=registry, config=config)


class TestWorkerStateEnum:
    def test_worker_state_has_correct_values(self):
        assert WorkerState.STARTING.value == "STARTING"
        assert WorkerState.RUNNING.value == "RUNNING"
        assert WorkerState.DRAINING.value == "DRAINING"
        assert WorkerState.SHUTTING_DOWN.value == "SHUTTING_DOWN"
        assert WorkerState.STOPPED.value == "STOPPED"


class TestShutdownTimeoutConfig:
    def test_shutdown_timeout_default_is_30(self):
        cfg = SwarmQConfig(
            broker_url="amqp://localhost",
            backend_url="valkey://localhost",
        )
        assert cfg.shutdown_timeout == 30

    def test_shutdown_timeout_env_var_override(self, monkeypatch):
        monkeypatch.setenv("SWARMQ_SHUTDOWN_TIMEOUT", "60")
        cfg = SwarmQConfig(
            broker_url="amqp://localhost",
            backend_url="valkey://localhost",
        )
        assert cfg.shutdown_timeout == 60


class TestActiveTaskTracking:
    async def test_on_message_adds_task_and_removes_on_done(self, worker, broker, backend):
        """_on_message stores created task in _active_tasks; task removed when done."""
        msg = _make_message(_default_payload())

        await worker._on_message(msg)

        # Task should be tracked immediately after _on_message
        assert len(worker._active_tasks) == 1

        # Let the event loop run so the task completes
        await asyncio.gather(*worker._active_tasks)

        # After completion, done_callback should have removed it
        assert len(worker._active_tasks) == 0


class TestBrokerCancelConsume:
    async def test_cancel_consume_cancels_all_consumer_tags(self):
        """cancel_consume() calls queue.cancel() for each stored consumer tag."""
        broker = RabbitMQBroker("amqp://guest:guest@localhost/")
        broker._consume_channel = AsyncMock()

        # Simulate two queues being consumed
        mock_queue_1 = AsyncMock()
        mock_queue_1.consume.return_value = "tag-1"
        mock_queue_2 = AsyncMock()
        mock_queue_2.consume.return_value = "tag-2"
        broker._consume_channel.get_queue.side_effect = [mock_queue_1, mock_queue_2]

        callback = AsyncMock()
        await broker.consume("q1", callback, prefetch_count=10)
        await broker.consume("q2", callback, prefetch_count=10)

        await broker.cancel_consume()

        mock_queue_1.cancel.assert_awaited_once_with("tag-1")
        mock_queue_2.cancel.assert_awaited_once_with("tag-2")


class TestRequestShutdown:
    def test_request_shutdown_sets_draining_and_event(self, worker):
        """_request_shutdown transitions to DRAINING and sets the shutdown event."""
        worker._state = WorkerState.RUNNING

        worker._request_shutdown()

        assert worker._state is WorkerState.DRAINING
        assert worker._shutdown_event.is_set()

    def test_double_request_shutdown_is_idempotent(self, worker):
        """Second _request_shutdown call is ignored when already DRAINING."""
        worker._state = WorkerState.RUNNING

        worker._request_shutdown()
        assert worker._state is WorkerState.DRAINING

        # Second call should not change state
        worker._request_shutdown()
        assert worker._state is WorkerState.DRAINING


class TestShutdownSequence:
    async def test_shutdown_cancels_consume_waits_tasks_closes_connections(
        self, worker, broker, backend
    ):
        """Full shutdown: cancel_consume, await tasks, close broker+backend, STOPPED."""
        # Simulate the worker being in RUNNING state, then trigger shutdown
        worker._state = WorkerState.RUNNING

        # Create a fast task to be in-flight
        completed = asyncio.Event()

        async def _short_task():
            completed.set()

        task = asyncio.create_task(_short_task())
        worker._active_tasks.add(task)
        task.add_done_callback(worker._active_tasks.discard)

        # Trigger shutdown via the event (as start() would react to)
        worker._request_shutdown()
        await worker._execute_shutdown()

        # Verify sequence
        broker.cancel_consume.assert_awaited_once()
        broker.close.assert_awaited_once()
        backend.close.assert_awaited_once()
        assert worker._state is WorkerState.STOPPED

    async def test_drain_timeout_cancels_remaining_tasks(self, broker, backend, registry):
        """Tasks still running after shutdown_timeout are cancelled."""
        cfg = SwarmQConfig(
            broker_url="amqp://localhost",
            backend_url="valkey://localhost",
            concurrency=10,
            queues=["default"],
            shutdown_timeout=0,  # immediate timeout forces cancellation
        )
        worker = Worker(broker=broker, backend=backend, registry=registry, config=cfg)
        worker._state = WorkerState.RUNNING

        # Create a task that would block forever
        cancelled = asyncio.Event()

        async def _blocking_task():
            try:
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                cancelled.set()
                raise

        task = asyncio.create_task(_blocking_task())
        worker._active_tasks.add(task)
        task.add_done_callback(worker._active_tasks.discard)

        worker._request_shutdown()
        await worker._execute_shutdown()

        # Task should have been cancelled
        assert cancelled.is_set()
        assert worker._state is WorkerState.STOPPED


class TestHandleMessageDuringShutdown:
    async def test_handle_message_nacks_with_requeue_during_shutdown(
        self, worker, broker
    ):
        """After shutdown_event is set, _handle_message nacks with requeue=True."""
        # Set shutdown event before handling message
        worker._shutdown_event.set()

        msg = _make_message(_default_payload())
        await worker._handle_message(msg)

        broker.nack.assert_awaited_once_with(msg, requeue=True)
        broker.ack.assert_not_awaited()


class TestShutdownNoActiveTasks:
    async def test_no_active_tasks_goes_straight_to_stopped(
        self, worker, broker, backend
    ):
        """When no tasks are in-flight, shutdown skips wait and reaches STOPPED."""
        worker._state = WorkerState.RUNNING
        assert len(worker._active_tasks) == 0

        worker._request_shutdown()
        await worker._execute_shutdown()

        broker.cancel_consume.assert_awaited_once()
        broker.close.assert_awaited_once()
        backend.close.assert_awaited_once()
        assert worker._state is WorkerState.STOPPED
