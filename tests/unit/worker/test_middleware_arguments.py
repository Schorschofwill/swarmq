"""Vertrag E3 A (Plan 2026-10-12-1105, MR 4, P12b): Was eine Middleware in
``pre_execute`` an den Argumenten ändert, sieht ``run()`` im IO-Modus."""

from __future__ import annotations

# Test list:
# 1. Eine Änderung am bestehenden ``message["kwargs"]``-Dict und an der
#    ``message["args"]``-Liste sieht ``run()``.
# 2. Eine Neuzuweisung (``message["kwargs"] = {...}``, ``message["args"] =
#    [...]``) hat keine Wirkung.
# (CPU-Modus: ``tests/unit/worker/compute/test_executor.py``.)
from typing import Any, override

import pytest

from swarmq.middleware import MessageContext, Middleware, MiddlewareChain
from swarmq.registry import TaskRegistry
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker
from tests.support.messages import make_message
from tests.unit.worker._helpers import _default_payload

pytestmark = [pytest.mark.timeout(5)]

_seen: list[tuple[tuple[Any, ...], dict[str, Any]]] = []


class _RecordingTask(Task):
    task_info = TaskInfo(name="mw_args_recording", queues=["default"])

    @override
    async def run(self, *args: Any, **kwargs: Any) -> None:
        _seen.append((args, kwargs))


class _Mutating(Middleware):
    @override
    async def pre_execute(self, message: MessageContext) -> None:
        message["kwargs"]["tenant"] = "t"
        message["args"].append(3)


class _Reassigning(Middleware):
    @override
    async def pre_execute(self, message: MessageContext) -> None:
        message["kwargs"] = {"tenant": "t"}
        message["args"] = [9]


async def _run_with(middleware: Middleware, broker: Any, backend: Any, config: Any) -> tuple[Any, ...]:
    _seen.clear()
    reg = TaskRegistry()
    reg.register(_RecordingTask)
    worker = Worker(
        broker=broker,
        backend=backend,
        registry=reg,
        config=config,
        middleware_chain=MiddlewareChain([middleware]),
    )
    payload = _default_payload(name="mw_args_recording", args=[1, 2], kwargs={"x": 1})
    await worker._handle_message(make_message(payload))
    assert len(_seen) == 1
    return _seen[0]


async def test_in_place_changes_reach_run(broker, backend, config):
    args, kwargs = await _run_with(_Mutating(), broker, backend, config)
    assert args == (1, 2, 3)
    assert kwargs == {"x": 1, "tenant": "t"}


async def test_reassignment_does_not_reach_run(broker, backend, config):
    args, kwargs = await _run_with(_Reassigning(), broker, backend, config)
    assert args == (1, 2)
    assert kwargs == {"x": 1}
