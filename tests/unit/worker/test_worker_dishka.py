"""Worker-side Dishka REQUEST-scope integration tests (plan U3).

Exercises the real ``Worker._handle_message`` with a registered container:
per-task injection, finalizer-receives-exception, and concurrency isolation.

Intentionally no ``from __future__ import annotations`` — the DI task's
FromDishka annotation must resolve via get_type_hints at class-definition,
which needs the names in scope (module-level here, mirroring real usage).
"""

import asyncio
from collections.abc import AsyncIterable
from unittest.mock import AsyncMock, MagicMock

import pytest

pytest.importorskip("dishka")

from dishka import (
    FromDishka,
    Provider,
    Scope,
    from_context,
    make_async_container,
    provide,
)

from swarmq import dishka as di
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.config import SwarmQConfig
from swarmq.registry import TaskRegistry
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker
from tests.support.messages import make_message

# Module-level capture buffers, reset per test.
CAPTURED: list[str] = []
FINALIZED: list[str] = []


class _Svc:
    def __init__(self, ctx: di.TaskContext) -> None:
        self.tid = ctx.task_id


class _SvcProvider(Provider):
    tc = from_context(di.TaskContext, scope=Scope.REQUEST)

    @provide(scope=Scope.REQUEST)
    async def svc(self, ctx: di.TaskContext) -> AsyncIterable[_Svc]:
        s = _Svc(ctx)
        exc = yield s
        FINALIZED.append(type(exc).__name__ if exc is not None else "OK")


class DiTask(Task):
    task_info = TaskInfo(name="di_task", queues=["default"])

    async def run(self, x: int, svc: FromDishka[_Svc]) -> int:
        CAPTURED.append(svc.tid)
        return x * 10


class DiFailTask(Task):
    task_info = TaskInfo(name="di_fail", queues=["default"])

    async def run(self, svc: FromDishka[_Svc]) -> None:
        raise ValueError("boom")


def _payload(task_id: str, name: str, args: list) -> dict:
    return {
        "task": {"id": task_id, "queue": "default", "name": name},
        "args": args,
        "kwargs": {},
    }


@pytest.fixture(autouse=True)
def _clear_buffers():
    CAPTURED.clear()
    FINALIZED.clear()
    yield
    CAPTURED.clear()
    FINALIZED.clear()


@pytest.fixture
def broker():
    b = MagicMock(spec=RabbitMQBroker)
    for m in (
        "connect",
        "declare_queue",
        "consume",
        "ack",
        "nack",
        "close",
        "cancel_consume",
        "retry_later",
        "dead_letter",
    ):
        setattr(b, m, AsyncMock())
    return b


@pytest.fixture
def backend():
    be = MagicMock(spec=ValkeyBackend)
    for m in ("connect", "store_result", "notify_result", "close", "clear_cancel_flag"):
        setattr(be, m, AsyncMock())
    be.is_cancelled = AsyncMock(return_value=False)
    be.set_cancel_flag = AsyncMock(return_value=True)
    be.is_schedfire_completed = AsyncMock(return_value=False)
    be.set_schedfire_inflight = AsyncMock(return_value=True)
    be.set_schedfire_dedup = AsyncMock(return_value=True)
    be._client = MagicMock()
    be._client.register_script = MagicMock(return_value=AsyncMock())
    return be


@pytest.fixture
def config():
    return SwarmQConfig(
        broker_url="amqp://localhost",
        backend_url="valkey://localhost",
        concurrency=10,
        queues=["default"],
    )


def _worker(broker, backend, config, *task_classes):
    registry = TaskRegistry()
    for tc in task_classes:
        registry.register(tc)
    return Worker(broker=broker, backend=backend, registry=registry, config=config)


@pytest.mark.timeout(5)
async def test_injection_through_handle_message(broker, backend, config):
    """AE1/R4/R5: a registered container fills the FromDishka param; the
    request-scoped Svc is bound to this task's TaskContext."""
    container = make_async_container(_SvcProvider())
    di.setup_dishka(container)
    try:
        worker = _worker(broker, backend, config, DiTask)
        await worker._handle_message(make_message(_payload("t-1", "di_task", [7])))
    finally:
        await container.close()

    assert CAPTURED == ["t-1"]
    assert FINALIZED == ["OK"]  # success path -> finalizer sees no exception
    broker.ack.assert_awaited_once()


@pytest.mark.timeout(5)
async def test_finalizer_receives_exception(broker, backend, config):
    """AE4/R13: when run raises, post_execution is skipped but the yield
    finalizer still runs and receives the real exception (rollback signal)."""
    container = make_async_container(_SvcProvider())
    di.setup_dishka(container)
    try:
        worker = _worker(broker, backend, config, DiFailTask)
        await worker._handle_message(make_message(_payload("t-err", "di_fail", [])))
    finally:
        await container.close()

    assert FINALIZED == ["ValueError"]


@pytest.mark.timeout(5)
async def test_concurrent_tasks_get_distinct_request_scopes(broker, backend, config):
    """AE10/R12: concurrent tasks in one worker each get their own
    request-scoped Svc — no cross-task container leakage."""
    container = make_async_container(_SvcProvider())
    di.setup_dishka(container)
    try:
        worker = _worker(broker, backend, config, DiTask)
        msgs = [make_message(_payload(f"c-{i}", "di_task", [i])) for i in range(5)]
        await asyncio.gather(*(worker._handle_message(m) for m in msgs))
    finally:
        await container.close()

    # Each task captured its OWN task_id via its own request-scoped Svc.
    assert sorted(CAPTURED) == ["c-0", "c-1", "c-2", "c-3", "c-4"]
    assert FINALIZED == ["OK"] * 5


@pytest.mark.timeout(5)
async def test_no_container_registered_skips_di_path(broker, backend, config):
    """A non-DI task runs unchanged when no container is registered (the DI
    branch is never entered)."""
    di._reset_container()

    class PlainTask(Task):
        task_info = TaskInfo(name="plain_w", queues=["default"])

        async def run(self, x: int) -> int:
            return x + 1

    worker = _worker(broker, backend, config, PlainTask)
    await worker._handle_message(make_message(_payload("p-1", "plain_w", [4])))
    assert FINALIZED == []  # no scope opened
    broker.ack.assert_awaited_once()


@pytest.mark.timeout(5)
async def test_shutdown_closes_registered_container(broker, backend, config):
    """AE11/R11: shutdown closes the registered APP container after the drain,
    before broker/backend close."""
    container = MagicMock()
    container.close = AsyncMock()
    di.setup_dishka(container)

    worker = _worker(broker, backend, config, DiTask)
    await worker._execute_shutdown()

    container.close.assert_awaited_once()
    broker.close.assert_awaited_once()
    backend.close.assert_awaited_once()


@pytest.mark.timeout(5)
async def test_shutdown_without_container_does_not_error(broker, backend, config):
    """R2/R6: no registered container -> shutdown proceeds unchanged."""
    di._reset_container()
    worker = _worker(broker, backend, config, DiTask)
    await worker._execute_shutdown()
    broker.close.assert_awaited_once()
    backend.close.assert_awaited_once()


@pytest.mark.timeout(5)
async def test_container_close_failure_does_not_abort_shutdown(broker, backend, config):
    """A failing container.close() is logged, not raised — shutdown still
    closes broker and backend."""
    container = MagicMock()
    container.close = AsyncMock(side_effect=RuntimeError("close boom"))
    di.setup_dishka(container)

    worker = _worker(broker, backend, config, DiTask)
    await worker._execute_shutdown()  # must not raise

    container.close.assert_awaited_once()
    broker.close.assert_awaited_once()
    backend.close.assert_awaited_once()
