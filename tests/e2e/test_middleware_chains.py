"""E2E tests for Middleware-Ketten (U1.7).

Erweitert die bestehende Single-Middleware-Suite
(tests/e2e/test_middleware_signals.py) um Mehr-Middleware-Verhalten:

- 3-Middleware-Kette: pre_execute forward, post_execute reverse.
- Middleware in der Mitte raised: nachfolgende Middleware laufen
  TROTZDEM (Worker-Hooks swallow per Design — Plan-Annahme „skip"
  war falsch; Test pinnt das echte Verhalten).
- Middleware schreibt in message_dict → nachfolgende Middleware sehen
  die Modifikation. Task-Args bleiben unmoduliert (worker liest args
  aus payload vor pre_execute).
- Middleware + Signal-Handler-Reihenfolge: pre_execute → TASK_STARTED
  → run → post_execute → TASK_COMPLETED.
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from typing import Any

import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.middleware import MessageContext, Middleware
from swarmq.signals import Signal
from swarmq.task import Task, TaskInfo

pytestmark = [pytest.mark.timeout(20)]


def _unique_queue() -> str:
    return f"e2e-mwchain-{uuid.uuid4().hex[:10]}"


def _task_name(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def _cleanup_tasks():
    yield
    Task._clear_registered_tasks()


@contextlib.asynccontextmanager
async def _running_worker(
    rabbitmq_url: str,
    valkey_url: str,
    queue: str,
    middlewares: list[Middleware] | None = None,
):
    worker_broker = RabbitMQBroker(rabbitmq_url)
    worker_backend = ValkeyBackend(valkey_url)
    worker_app = SwarmQ(
        worker_broker,
        worker_backend,
        queues=[queue],
        concurrency=1,
    )
    for mw in middlewares or []:
        worker_app.add_middleware(mw)
    online = asyncio.Event()

    @worker_app.on_signal(Signal.WORKER_ONLINE)
    async def _on_online(**_: Any) -> None:
        online.set()

    worker_task = asyncio.create_task(worker_app.start_worker())
    try:
        await asyncio.wait_for(online.wait(), timeout=6)
        yield worker_app
    finally:
        worker_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker_task
        await worker_broker.close()
        await worker_backend.close()


class _OrderTrackingMiddleware(Middleware):
    """Records every hook invocation into a shared list. The recorder
    list is module-instance state; tests pass their own list per
    middleware instance to keep ordering observable."""

    def __init__(self, name: str, recorder: list[str]) -> None:
        self._name = name
        self._recorder = recorder

    async def pre_execute(self, message: MessageContext) -> None:
        self._recorder.append(f"pre:{self._name}")

    async def post_execute(
        self,
        message: MessageContext,
        result: Any = None,
    ) -> None:
        self._recorder.append(f"post:{self._name}")

    async def on_error(
        self,
        message: MessageContext,
        exception: BaseException,
    ) -> None:
        self._recorder.append(f"err:{self._name}")


@pytest.mark.usefixtures("_cleanup_tasks")
async def test_three_middlewares_pre_forward_post_reverse(
    rabbitmq_url: str,
    valkey_url: str,
):
    """A=B=C-Kette: pre_execute fires A→B→C; post_execute fires C→B→A."""
    queue = _unique_queue()
    task_name = _task_name("chain")
    recorder: list[str] = []

    class OkTask(Task):
        task_info = TaskInfo(name=task_name, queues=[queue], max_retries=0)

        async def run(self) -> str:
            recorder.append("task:run")
            return "ok"

    middlewares = [_OrderTrackingMiddleware(name, recorder) for name in ("A", "B", "C")]

    client_app = SwarmQ(
        RabbitMQBroker(rabbitmq_url),
        ValkeyBackend(valkey_url),
        queues=[queue],
    )
    async with client_app:
        await client_app.broker.declare_queue(queue)
        async with _running_worker(
            rabbitmq_url,
            valkey_url,
            queue,
            middlewares=middlewares,
        ):
            tid = await client_app.schedule(task_name)
            assert await client_app.get_result(tid, timeout=8) == "ok"

    assert recorder == [
        "pre:A",
        "pre:B",
        "pre:C",
        "task:run",
        "post:C",
        "post:B",
        "post:A",
    ], f"unexpected hook order: {recorder!r}"


@pytest.mark.usefixtures("_cleanup_tasks")
async def test_middleware_in_middle_raises_subsequent_still_run(
    rabbitmq_url: str,
    valkey_url: str,
):
    """B raises in pre_execute. The chain is "swallow" by design
    (middleware.py:_swallow) — A AND C still see pre_execute, the
    task runs, post_execute fires for all three in reverse order."""
    queue = _unique_queue()
    task_name = _task_name("midraise")
    recorder: list[str] = []

    class OkTask(Task):
        task_info = TaskInfo(name=task_name, queues=[queue], max_retries=0)

        async def run(self) -> str:
            recorder.append("task:run")
            return "ok"

    class RaisingMiddleware(_OrderTrackingMiddleware):
        async def pre_execute(self, message: MessageContext) -> None:
            recorder.append(f"pre:{self._name}")
            raise RuntimeError(f"boom-from-{self._name}")

    middlewares = [
        _OrderTrackingMiddleware("A", recorder),
        RaisingMiddleware("B", recorder),
        _OrderTrackingMiddleware("C", recorder),
    ]

    client_app = SwarmQ(
        RabbitMQBroker(rabbitmq_url),
        ValkeyBackend(valkey_url),
        queues=[queue],
    )
    async with client_app:
        await client_app.broker.declare_queue(queue)
        async with _running_worker(
            rabbitmq_url,
            valkey_url,
            queue,
            middlewares=middlewares,
        ):
            tid = await client_app.schedule(task_name)
            assert await client_app.get_result(tid, timeout=8) == "ok"

    # B raised, but A and C still saw pre; task ran; all three saw
    # post_execute in reverse.
    assert recorder == [
        "pre:A",
        "pre:B",
        "pre:C",
        "task:run",
        "post:C",
        "post:B",
        "post:A",
    ], f"unexpected hook order: {recorder!r}"


@pytest.mark.usefixtures("_cleanup_tasks")
async def test_middleware_chain_sees_message_dict_mutations(
    rabbitmq_url: str,
    valkey_url: str,
):
    """Middleware A writes a key into message_dict during pre_execute;
    Middleware B reads it. Cross-middleware communication via the
    shared MessageContext is the documented escape hatch when explicit
    state-passing is needed (logging correlation, request-scoped
    metrics)."""
    queue = _unique_queue()
    task_name = _task_name("mutate")
    recorder: list[str] = []

    class OkTask(Task):
        task_info = TaskInfo(name=task_name, queues=[queue], max_retries=0)

        async def run(self, x: int) -> int:
            return x

    class WriterMiddleware(Middleware):
        async def pre_execute(self, message: MessageContext) -> None:
            message["correlation_marker"] = "set-by-A"

    class ReaderMiddleware(Middleware):
        async def pre_execute(self, message: MessageContext) -> None:
            recorder.append(message.get("correlation_marker", "MISSING"))

    middlewares = [WriterMiddleware(), ReaderMiddleware()]

    client_app = SwarmQ(
        RabbitMQBroker(rabbitmq_url),
        ValkeyBackend(valkey_url),
        queues=[queue],
    )
    async with client_app:
        await client_app.broker.declare_queue(queue)
        async with _running_worker(
            rabbitmq_url,
            valkey_url,
            queue,
            middlewares=middlewares,
        ):
            tid = await client_app.schedule(task_name, 42)
            assert await client_app.get_result(tid, timeout=8) == 42

    assert recorder == ["set-by-A"], f"reader middleware did not observe writer's mutation: {recorder!r}"


@pytest.mark.usefixtures("_cleanup_tasks")
async def test_middleware_and_signal_handlers_interleave_correctly(
    rabbitmq_url: str,
    valkey_url: str,
):
    """Pin the canonical hook+signal sequence per task lifecycle:
    pre_execute → TASK_STARTED → run → post_execute → TASK_COMPLETED.

    The order is load-bearing — if a future refactor moves
    TASK_COMPLETED before post_execute, downstream observability
    pipelines that gate on TASK_COMPLETED to read post_execute side-
    effects (e.g. metrics flush) would break silently.
    """
    queue = _unique_queue()
    task_name = _task_name("seq")
    recorder: list[str] = []

    class OkTask(Task):
        task_info = TaskInfo(name=task_name, queues=[queue], max_retries=0)

        async def run(self) -> str:
            recorder.append("run")
            return "ok"

    class SeqMiddleware(Middleware):
        async def pre_execute(self, message: MessageContext) -> None:
            recorder.append("pre_execute")

        async def post_execute(
            self,
            message: MessageContext,
            result: Any = None,
        ) -> None:
            recorder.append("post_execute")

    completed_event = asyncio.Event()

    client_broker = RabbitMQBroker(rabbitmq_url)
    client_backend = ValkeyBackend(valkey_url)
    client_app = SwarmQ(client_broker, client_backend, queues=[queue])

    worker_broker = RabbitMQBroker(rabbitmq_url)
    worker_backend = ValkeyBackend(valkey_url)
    worker_app = SwarmQ(
        worker_broker,
        worker_backend,
        queues=[queue],
        concurrency=1,
    )
    worker_app.add_middleware(SeqMiddleware())

    @worker_app.on_signal(Signal.TASK_STARTED)
    async def _on_started(**_: Any) -> None:
        recorder.append("TASK_STARTED")

    @worker_app.on_signal(Signal.TASK_COMPLETED)
    async def _on_completed(**_: Any) -> None:
        recorder.append("TASK_COMPLETED")
        completed_event.set()

    online = asyncio.Event()

    @worker_app.on_signal(Signal.WORKER_ONLINE)
    async def _on_online(**_: Any) -> None:
        online.set()

    async with client_app:
        await client_app.broker.declare_queue(queue)
        worker_task = asyncio.create_task(worker_app.start_worker())
        try:
            await asyncio.wait_for(online.wait(), timeout=6)
            tid = await client_app.schedule(task_name)
            assert await client_app.get_result(tid, timeout=8) == "ok"
            await asyncio.wait_for(completed_event.wait(), timeout=4)
        finally:
            worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await worker_task
            await worker_broker.close()
            await worker_backend.close()

    assert recorder == [
        "pre_execute",
        "TASK_STARTED",
        "run",
        "post_execute",
        "TASK_COMPLETED",
    ], f"unexpected sequence: {recorder!r}"
