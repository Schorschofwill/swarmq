"""E-068: Ein Cancel während ``run()`` ruft weder ``on_error`` noch ``post_execute`` (F3-30).

Festschreibender Test zur Entscheidung E-068: Ein Cancel ist kein Fehler.
Die Middleware bekommt vorher ``pre_execute``, danach weder ``on_error``
noch ``post_execute``; beobachtbar ist der Cancel nur über das Signal
``TASK_CANCELLED``. Charakterisierung, der Code ändert sich nicht.

Test-Liste:
- [x] Cancel während run() → pre_execute ja, on_error/post_execute nein,
      TASK_CANCELLED feuert
"""

from __future__ import annotations

import asyncio
from typing import Any, override
from unittest.mock import AsyncMock

import pytest

from swarmq.config import SwarmQConfig
from swarmq.middleware import MessageContext, Middleware, MiddlewareChain
from swarmq.registry import TaskRegistry
from swarmq.signals import Signal
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker
from tests.support.messages import make_message
from tests.unit.worker._helpers import _default_payload


class _RecordingMiddleware(Middleware):
    def __init__(self) -> None:
        self.calls: list[str] = []

    @override
    async def pre_execute(self, message: MessageContext) -> None:
        self.calls.append("pre_execute")

    @override
    async def post_execute(self, message: MessageContext, result: Any = None) -> None:
        self.calls.append("post_execute")

    @override
    async def on_error(self, message: MessageContext, exception: BaseException) -> None:
        self.calls.append("on_error")


@pytest.mark.timeout(5)
@pytest.mark.entscheidung("E-068")
async def test_cancel_during_run_skips_on_error_and_post_execute(broker, backend):
    started = asyncio.Event()

    class BlockingTask(Task):
        task_info = TaskInfo(name="blocking_run", queues=["default"])

        @override
        async def run(self) -> str:
            started.set()
            await asyncio.Event().wait()  # läuft, bis der Cancel kommt
            return "unreachable"

    # Das Cancel-Flag gilt erst, wenn run() läuft; die Prüfung vor dem
    # Start sieht es noch nicht.
    async def is_cancelled(task_id: str, **_kw: Any) -> bool:
        return started.is_set()

    backend.is_cancelled = AsyncMock(side_effect=is_cancelled)
    middleware = _RecordingMiddleware()
    registry = TaskRegistry()
    registry.register(BlockingTask)
    config = SwarmQConfig(
        broker_url="amqp://localhost",
        backend_url="valkey://localhost",
        queues=["default"],
        cancel_poll_interval=0.01,
    )
    worker = Worker(
        broker=broker,
        backend=backend,
        registry=registry,
        config=config,
        middleware_chain=MiddlewareChain([middleware]),
    )
    cancelled_signals: list[dict[str, Any]] = []

    async def on_cancelled(**kwargs: Any) -> None:
        cancelled_signals.append(kwargs)

    worker._signal_dispatcher.connect(Signal.TASK_CANCELLED, on_cancelled)

    await worker._handle_message(make_message(_default_payload(name="blocking_run", args=[], kwargs={})))

    assert started.is_set()
    assert middleware.calls == ["pre_execute"]
    assert [s["task_name"] for s in cancelled_signals] == ["blocking_run"]
