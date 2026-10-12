"""``error_class`` auf den Schreibpfaden des Workers (Plan 2026-10-12-1105, MR 3, P9).

Test-Liste (Plan-Nummern in Klammern; T12, T13, T17, E7 A):

1. (6) Task wirft ``ValueError()`` (leer), ``max_retries=0`` -> DLQ-Ergebnis
   mit ``error_class == "builtins.ValueError"``, ``error_message == ""``;
   ``x-swarmq-traceback`` der DLQ-Kopie und ``TASK_DLQ error`` bleiben ``""``.
2. (7) Wie 1 mit ``dead_letter=False`` (Ergebnis-Tail) -> dieselben Felder.
3. (8) ``raise Retry()`` mit Budget -> ``x-swarmq-traceback`` der Retry-Kopie
   bleibt ``""``.
4. (9, angepasst) FIRE_AND_FORGET mit ``result_ttl=60``: der gespeicherte
   Serialisierungs-Rueckfall traegt ``error_class``; ein Fehler aus dem Task
   speichert wie bisher kein Ergebnis (``pipeline.py`` ueberspringt den Tail).
5. (10) Nicht serialisierbarer Rueckgabewert ->
   ``swarmq.exceptions.ResultSerializationError``.
6. (11) Gate-Pfade: unbekannter Task, Lock-Wartezeit, Delivery-Limit ->
   ``swarmq.exceptions.*``; ``WorkerHungError``, Lock-Render-Fehler, Ablauf ->
   kein Schluessel.
7. (12) Worker-erzeugte Fehler -> kein Schluessel: harter Timeout (IO hier,
   CPU, Drain und Pool-Fehler in ``compute/test_executor.py``).
8. (13) CPU-Modus: siehe ``compute/test_executor.py``.
9. (17) ``dead_letter`` reicht die Klasse an die Workflow-Fehlmarkierung
   durch (``test_terminal.py``: nur mit Wert).
10. (18) Workflow-Schritt mit ``dead_letter=False`` -> ``error.class``
    (``test_retry.py``, ``test_workflow_advancer.py``).
"""

from __future__ import annotations

import dataclasses
from typing import Any, cast, override
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.exceptions import Retry
from swarmq.registry import TaskRegistry
from swarmq.serialization import decode_result
from swarmq.signals import Signal
from swarmq.task import DeliveryMode, Task, TaskInfo
from swarmq.worker import Worker
from tests.support.messages import make_message
from tests.unit.worker._helpers import AddTask, _default_payload, lock_wait_exceeded_headers

pytestmark = [pytest.mark.timeout(5)]


class _EmptyValueErrorTask(Task):
    task_info = TaskInfo(name="empty-value-error", queues=["default"], max_retries=0)

    @override
    async def run(self) -> None:
        raise ValueError


def _worker(broker: Any, backend: Any, config: Any, task: type[Task]) -> Worker:
    registry = TaskRegistry()
    registry.register(task)
    return Worker(broker=broker, backend=backend, registry=registry, config=config)


def _stored(backend: Any) -> dict[str, Any]:
    backend.store_result.assert_awaited_once()
    stored: dict[str, Any] = decode_result(backend.store_result.await_args.args[1])
    return stored


async def test_dead_letter_result_carries_class_of_empty_exception(broker, backend, config):
    worker = _worker(broker, backend, config, _EmptyValueErrorTask)
    dlq_errors: list[str] = []

    @worker._signal_dispatcher.on_signal(Signal.TASK_DLQ)
    async def _on_dlq(**kw: Any) -> None:
        dlq_errors.append(kw["error"])

    await worker._handle_message(make_message(_default_payload(name="empty-value-error", args=[])))

    stored = _stored(backend)
    assert stored["error_class"] == "builtins.ValueError"
    assert stored["error_message"] == ""
    assert broker.dead_letter.await_args.kwargs["headers"]["x-swarmq-traceback"] == ""
    assert dlq_errors == [""]


class _EmptyValueErrorNoDlqTask(Task):
    task_info = TaskInfo(name="empty-value-error-no-dlq", queues=["default"], max_retries=0, dead_letter=False)

    @override
    async def run(self) -> None:
        raise ValueError


async def test_result_tail_carries_class_of_empty_exception(broker, backend, config):
    worker = _worker(broker, backend, config, _EmptyValueErrorNoDlqTask)

    await worker._handle_message(make_message(_default_payload(name="empty-value-error-no-dlq", args=[])))

    stored = _stored(backend)
    assert stored["error_class"] == "builtins.ValueError"
    assert stored["error_message"] == ""
    broker.dead_letter.assert_not_awaited()


class _BareRetryTask(Task):
    task_info = TaskInfo(name="bare-retry", queues=["default"], max_retries=3)

    @override
    async def run(self) -> None:
        raise Retry


async def test_retry_copy_traceback_stays_empty_for_bare_retry(broker, backend, config):
    worker = _worker(broker, backend, config, _BareRetryTask)

    await worker._handle_message(make_message(_default_payload(name="bare-retry", args=[])))

    broker.retry_later.assert_awaited_once()
    assert broker.retry_later.await_args.kwargs["headers"]["x-swarmq-traceback"] == ""


class _UnserializableTask(Task):
    task_info = TaskInfo(name="unserializable", queues=["default"])

    @override
    async def run(self) -> object:
        return object()


async def test_unserializable_return_value_result_carries_serialization_error_class(broker, backend, config):
    worker = _worker(broker, backend, config, _UnserializableTask)

    await worker._handle_message(make_message(_default_payload(name="unserializable", args=[])))

    stored = _stored(backend)
    assert stored["is_error"] is True
    assert stored["error_class"] == "swarmq.exceptions.ResultSerializationError"


class _FafUnserializableTask(Task):
    task_info = TaskInfo(
        name="faf-unserializable", queues=["default"], delivery=DeliveryMode.FIRE_AND_FORGET, result_ttl=60
    )

    @override
    async def run(self) -> object:
        return object()


class _FafErrorTask(Task):
    task_info = TaskInfo(name="faf-error", queues=["default"], delivery=DeliveryMode.FIRE_AND_FORGET, result_ttl=60)

    @override
    async def run(self) -> None:
        raise KeyError("k")


async def test_fire_and_forget_stored_error_result_carries_class(broker, backend, config):
    """Plan-Test 9, angepasst: ein FAF-Fehler aus dem Task speichert auch mit
    ``result_ttl > 0`` kein Ergebnis (``pipeline.py`` ueberspringt den Tail,
    unveraendert). Der FAF-Pfad mit gespeichertem Fehlerergebnis ist der
    Serialisierungs-Rueckfall."""
    worker = _worker(broker, backend, config, _FafUnserializableTask)
    await worker._handle_message(make_message(_default_payload(name="faf-unserializable", args=[])))
    assert _stored(backend)["error_class"] == "swarmq.exceptions.ResultSerializationError"

    backend.store_result.reset_mock()
    worker = _worker(broker, backend, config, _FafErrorTask)
    await worker._handle_message(make_message(_default_payload(name="faf-error", args=[])))
    backend.store_result.assert_not_awaited()


class _LockedTask(Task):
    task_info = TaskInfo(name="locked-ec", queues=["default"], lock="resource", hard_timeout=60)

    @override
    async def run(self) -> None:
        return None


class _RenderLockTask(Task):
    task_info = TaskInfo(name="render-lock-ec", queues=["default"], lock="user-{user_id}", hard_timeout=60)

    @override
    async def run(self, user_id: int) -> None:
        return None


def _gate_scenario(name: str, broker: Any, backend: Any, config: Any) -> tuple[Worker, Any]:
    """Worker und Nachricht fuer einen Gate-Pfad, der ueber ``dead_letter`` endet."""
    if name == "unknown":
        worker = _worker(broker, backend, dataclasses.replace(config, unknown_task_grace_s=0), AddTask)
        return worker, make_message(_default_payload(name="ghost", args=[]))
    if name in {"lock_wait", "lock_render"}:
        task = _LockedTask if name == "lock_wait" else _RenderLockTask
        worker = _worker(broker, backend, config, task)
        worker._lock_manager = MagicMock()
        worker._lock_manager.acquire = AsyncMock(return_value=None)
        msg = make_message(_default_payload(name=task.task_info.name, args=[]))
        if name == "lock_wait":
            msg.headers = lock_wait_exceeded_headers(config)
        return worker, msg
    worker = _worker(broker, backend, config, AddTask)
    counter = "x-acquired-count" if name == "acquired_count" else "x-delivery-count"
    return worker, make_message(_default_payload(), headers={counter: 1_000})


@pytest.mark.parametrize(
    ("scenario", "expected"),
    [
        ("unknown", "swarmq.exceptions.TaskNotFoundError"),
        ("lock_wait", "swarmq.exceptions.LockWaitExceeded"),
        ("acquired_count", "swarmq.exceptions.RedeliveryLimitError"),
    ],
)
async def test_gate_dead_letter_result_names_the_swarmq_class(broker, backend, config, scenario, expected):
    worker, msg = _gate_scenario(scenario, broker, backend, config)

    await worker._handle_message(msg)

    broker.dead_letter.assert_awaited_once()
    assert _stored(backend)["error_class"] == expected


@pytest.mark.parametrize("scenario", ["delivery_count", "lock_render"])
async def test_gate_dead_letter_without_swarmq_class_has_no_key(broker, backend, config, scenario):
    worker, msg = _gate_scenario(scenario, broker, backend, config)

    await worker._handle_message(msg)

    broker.dead_letter.assert_awaited_once()
    assert "error_class" not in _stored(backend)


class _ExpiringTask(Task):
    task_info = TaskInfo(name="expiring-ec", queues=["default"], max_age=10)

    @override
    async def run(self) -> None:
        return None


async def test_expired_result_has_no_key(broker, backend, config):
    import time

    worker = _worker(broker, backend, config, _ExpiringTask)
    msg = make_message(_default_payload(name="expiring-ec", args=[]), enqueued_at=int(time.time()) - 60)

    await worker._handle_message(msg)

    stored = _stored(backend)
    assert stored["is_error"] is True
    assert "error_class" not in stored


class _HangingTask(Task):
    task_info = TaskInfo(name="hanging-ec", queues=["default"], hard_timeout=cast("int", 0.05))

    @override
    async def run(self) -> None:
        import asyncio

        await asyncio.Event().wait()


async def test_io_hard_timeout_result_has_no_key(broker, backend, config):
    worker = _worker(broker, backend, config, _HangingTask)

    await worker._handle_message(make_message(_default_payload(name="hanging-ec", args=[])))

    stored = _stored(backend)
    assert stored["error_message"] == "hard timeout after 0.05s"
    assert "error_class" not in stored


_WF_ID = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b"


async def test_dead_lettered_workflow_step_passes_its_class_to_the_fail_mark(broker, backend, config):
    from swarmq.headers import HEADER_WORKFLOW_ID

    worker = _worker(broker, backend, config, _EmptyValueErrorTask)
    worker._workflow_advancer = MagicMock(fail=AsyncMock())
    msg = make_message(_default_payload(name="empty-value-error", args=[]), headers={HEADER_WORKFLOW_ID: _WF_ID})

    await worker._handle_message(msg)

    worker._workflow_advancer.fail.assert_awaited_once_with(_WF_ID, "TaskError", "", error_class="builtins.ValueError")
