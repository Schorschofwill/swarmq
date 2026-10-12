"""``error_class`` im Fehlerergebnis gegen echtes RabbitMQ und Valkey
(Plan 2026-10-12-1105, MR 3, P9).

Test-Liste:
1. Der Worker speichert ein Fehlerergebnis; ``get_result`` wirft
   ``TaskError`` mit ``error_class`` und unveraendertem Text (auch leer).
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from collections.abc import AsyncIterator, Iterator
from typing import override

import pytest

from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.exceptions import TaskError
from swarmq.task import Task, TaskInfo

pytestmark = [pytest.mark.integration, pytest.mark.timeout(20)]


class _OwnError(Exception):
    pass


@pytest.fixture
def queue() -> str:
    return f"error-class-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def _tasks(queue: str) -> Iterator[None]:
    class EmptyValueError(Task):
        task_info = TaskInfo(name="ec_empty_value_error", queues=[queue])

        @override
        async def run(self) -> None:
            raise ValueError

    class OwnError(Task):
        task_info = TaskInfo(name="ec_own_error", queues=[queue], dead_letter=False)

        @override
        async def run(self) -> None:
            raise _OwnError("boom")

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


@pytest.mark.usefixtures("_tasks")
@pytest.mark.parametrize(
    ("task_name", "message", "error_class"),
    [
        ("ec_empty_value_error", "", "builtins.ValueError"),
        ("ec_own_error", "boom", f"{__name__}._OwnError"),
    ],
    ids=["dead_letter", "result_tail"],
)
async def test_get_result_raises_task_error_with_error_class(
    rabbitmq_url, valkey_url, queue, task_name, message, error_class
):
    async with _worker(rabbitmq_url, valkey_url, queue) as app:
        task_id = await app.schedule(task_name)
        with pytest.raises(TaskError) as raised:
            await app.get_result(task_id, timeout=10)

    assert (str(raised.value), raised.value.error_class) == (message, error_class)
