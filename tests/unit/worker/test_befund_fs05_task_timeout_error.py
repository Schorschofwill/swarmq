"""Nachweis FS-05: Eine ``TimeoutError`` aus dem Task selbst wird zu
"SoftTimeoutError: soft timeout after None s", auch ohne ``soft_timeout``.

Register: docs/befunde/fehlersuche-runde-2-2026-10.md (FS-05; Quelle MA-2).

``except TimeoutError`` in ``TaskExecutor._await_run_result``
(``worker/execution.py``) soll nur den Soft-Timeout aus ``asyncio.wait_for``
fangen, faengt aber jede ``TimeoutError`` aus ``run()`` (``asyncio.timeout``,
``socket.timeout``, aiohttp-Timeouts). Folgen: falscher Fehlertext und falsche
``error_class``; ``retry_exceptions=[TimeoutError]`` greift nicht, weil
``SoftTimeoutError`` keine ``TimeoutError`` ist -> DLQ statt Retry. Der
CPU-Modus (``worker/compute/child.py``, ``_run_job``) hat dasselbe Muster;
er laesst sich ohne Fork ueber ``child.run_job`` pruefen.

Erwartet: Eine fremde ``TimeoutError`` laeuft als gewoehnlicher Task-Fehler
durch (Originalmeldung und -klasse, Retry-Filter greift). Nur ein wirklich
abgelaufener ``soft_timeout`` wird zu ``SoftTimeoutError``.

Testliste:
# [x] IO: retry_exceptions=[TimeoutError] -> Retry statt DLQ
# [x] IO: ohne soft_timeout bleibt die Originalmeldung im Fehlerergebnis
# [x] IO: mit gesetztem, NICHT abgelaufenem soft_timeout ebenso (Meldung und
#     error_class builtins.TimeoutError)
# [x] CPU (child.run_job ohne Fork): die Original-TimeoutError kommt im
#     Envelope an
"""

from __future__ import annotations

import queue as _queue
from collections.abc import Iterator
from typing import Any, override

import pytest

from swarmq.registry import TaskRegistry
from swarmq.serialization import decode_result
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker
from swarmq.worker.compute import child
from swarmq.worker.compute.envelope import ComputeJob
from tests.support.messages import make_message
from tests.unit.worker._helpers import _default_payload

pytestmark = pytest.mark.timeout(5)

_UPSTREAM = "upstream did not answer within 5s"


class Fs05RetriesOnTimeout(Task):
    task_info = TaskInfo(
        name="fs05_retries_on_timeout",
        queues=["default"],
        max_retries=3,
        retry_exceptions=[TimeoutError],
    )

    @override
    async def run(self) -> None:
        raise TimeoutError(_UPSTREAM)


class Fs05PlainTimeout(Task):
    task_info = TaskInfo(name="fs05_plain_timeout", queues=["default"])

    @override
    async def run(self) -> None:
        raise TimeoutError(_UPSTREAM)


class Fs05GenerousSoftTimeout(Task):
    task_info = TaskInfo(name="fs05_generous_soft_timeout", queues=["default"], soft_timeout=60)

    @override
    async def run(self) -> None:
        raise TimeoutError(_UPSTREAM)


def _worker(broker: Any, backend: Any, config: Any, task: type[Task]) -> Worker:
    registry = TaskRegistry()
    registry.register(task)
    return Worker(broker=broker, backend=backend, registry=registry, config=config)


async def _stored_error(broker: Any, backend: Any, config: Any, task: type[Task]) -> dict[str, Any]:
    worker = _worker(broker, backend, config, task)
    await worker._handle_message(
        make_message(_default_payload(task_id="fs05-t", name=task.task_info.name, args=[], kwargs={}))
    )
    stored: dict[str, Any] = decode_result(backend.store_result.await_args.args[1])
    assert stored["is_error"] is True  # Voraussetzung
    return stored


@pytest.mark.befund("FS-05")
async def test_task_timeout_error_is_retried_per_retry_exceptions(broker: Any, backend: Any, config: Any) -> None:
    worker = _worker(broker, backend, config, Fs05RetriesOnTimeout)

    await worker._handle_message(
        make_message(_default_payload(task_id="fs05-t1", name="fs05_retries_on_timeout", args=[], kwargs={}))
    )

    assert broker.dead_letter.await_count == 0, "TimeoutError des Tasks ging in die DLQ statt in den Retry"
    broker.retry_later.assert_awaited_once()


@pytest.mark.befund("FS-05")
async def test_task_timeout_error_keeps_its_message_without_soft_timeout(
    broker: Any, backend: Any, config: Any
) -> None:
    stored = await _stored_error(broker, backend, config, Fs05PlainTimeout)

    assert _UPSTREAM in stored["error_message"], stored["error_message"]


@pytest.mark.befund("FS-05")
async def test_task_timeout_error_is_not_a_soft_timeout_when_soft_timeout_did_not_expire(
    broker: Any, backend: Any, config: Any
) -> None:
    stored = await _stored_error(broker, backend, config, Fs05GenerousSoftTimeout)

    assert _UPSTREAM in stored["error_message"], stored["error_message"]
    assert stored["error_class"] == "builtins.TimeoutError"


@pytest.fixture
def _isolated_task_registry() -> Iterator[None]:
    """Wie in ``compute/test_child.py``: lokal definierte Tasks landen in der
    globalen Liste, nach dem Test wird sie wiederhergestellt."""
    saved = list(Task._registered_tasks)
    Task._clear_registered_tasks()
    yield
    Task._clear_registered_tasks()
    Task._registered_tasks.extend(saved)


@pytest.mark.befund("FS-05")
@pytest.mark.usefixtures("_isolated_task_registry")
async def test_cpu_child_keeps_the_task_timeout_error() -> None:
    class Fs05CpuTimeout(Task):
        task_info = TaskInfo(name="fs05_cpu_timeout", queues=["q"], soft_timeout=60)

        @override
        async def run(self) -> None:
            raise TimeoutError(_UPSTREAM)

    job = ComputeJob(task_id="fs05-cpu", task_name=Fs05CpuTimeout.task_info.name, queue="q", args=(), kwargs={})

    envelope = await child.run_job(job, _queue.Queue())

    error = envelope.error()
    assert isinstance(error, TimeoutError), f"CPU-Kind meldet {type(error).__name__}: {error}"
    assert _UPSTREAM in str(error)
