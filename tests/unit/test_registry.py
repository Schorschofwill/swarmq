"""Unit tests for TaskRegistry."""

from __future__ import annotations

import inspect
from typing import override

import pytest

from swarmq.exceptions import ConfigurationError, TaskNotFoundError
from swarmq.registry import TaskRegistry
from swarmq.task import Task, TaskInfo


def test_register_and_get_by_name():
    registry = TaskRegistry()

    class AddTask(Task):
        task_info = TaskInfo(name="add", queues=["default"])

        @override
        async def run(self, a: int, b: int) -> int:
            return a + b

    registry.register(AddTask)
    assert registry.get("add") is AddTask


def test_register_multiple_tasks():
    registry = TaskRegistry()

    class TaskA(Task):
        task_info = TaskInfo(name="task_a", queues=["q1"])

        @override
        async def run(self) -> str:
            return "a"

    class TaskB(Task):
        task_info = TaskInfo(name="task_b", queues=["q2"])

        @override
        async def run(self) -> str:
            return "b"

    registry.register(TaskA)
    registry.register(TaskB)
    assert registry.get("task_a") is TaskA
    assert registry.get("task_b") is TaskB


def test_get_all_returns_all_registered():
    registry = TaskRegistry()

    class TaskX(Task):
        task_info = TaskInfo(name="x", queues=["q"])

        @override
        async def run(self) -> None:
            pass

    class TaskY(Task):
        task_info = TaskInfo(name="y", queues=["q"])

        @override
        async def run(self) -> None:
            pass

    registry.register(TaskX)
    registry.register(TaskY)
    result = registry.get_all()
    assert result == {"x": TaskX, "y": TaskY}


def test_has_returns_true_and_false():
    registry = TaskRegistry()

    class PingTask(Task):
        task_info = TaskInfo(name="ping", queues=["default"])

        @override
        async def run(self) -> str:
            return "pong"

    registry.register(PingTask)
    assert registry.has("ping") is True
    assert registry.has("nonexistent") is False


def test_return_type_annotation_is_cached():
    registry = TaskRegistry()

    class MathTask(Task):
        task_info = TaskInfo(name="math", queues=["default"])

        @override
        async def run(self, x: int) -> float:
            return float(x)

    registry.register(MathTask)
    entry = registry.get_entry("math")
    assert entry.return_type is float
    assert entry.task_info is MathTask.task_info
    assert entry.task_class is MathTask


# N2 (Plan 2026-10-12-1105, MR 7): Rückgabetyp von run() tolerant lesen.
# Test-Liste:
# - kaputte String-Annotation (SyntaxError) → Registrierung gelingt, roher Text
# - ohne Return-Annotation → inspect.Parameter.empty
# - auflösbare Rückgabetypen (None, list[int]) bleiben wie bisher


def test_broken_string_return_annotation_registers_with_raw_text():
    from tests.unit._pep649_run_annotations import Pep649BrokenReturnTask

    registry = TaskRegistry()
    registry.register(Pep649BrokenReturnTask)

    assert registry.get_entry("n2_pep649_broken").return_type == "list[int"


def test_missing_return_annotation_is_stored_as_empty():
    registry = TaskRegistry()

    class NoReturnTask(Task):
        task_info = TaskInfo(name="no_return", queues=["q"])

        @override
        async def run(self, x: int):
            pass

    registry.register(NoReturnTask)

    assert registry.get_entry("no_return").return_type is inspect.Parameter.empty


def test_resolvable_return_types_are_stored_unchanged():
    registry = TaskRegistry()

    class NoneTask(Task):
        task_info = TaskInfo(name="returns_none", queues=["q"])

        @override
        async def run(self) -> None:
            pass

    class ListTask(Task):
        task_info = TaskInfo(name="returns_list", queues=["q"])

        @override
        async def run(self) -> list[int]:
            return []

    registry.register(NoneTask)
    registry.register(ListTask)

    assert registry.get_entry("returns_none").return_type is None
    assert registry.get_entry("returns_list").return_type == list[int]


def test_get_unknown_raises_task_not_found_error():
    registry = TaskRegistry()
    with pytest.raises(TaskNotFoundError):
        registry.get("unknown")


def test_duplicate_task_name_raises_configuration_error():
    registry = TaskRegistry()

    class FirstTask(Task):
        task_info = TaskInfo(name="dup", queues=["q"])

        @override
        async def run(self) -> None:
            pass

    class SecondTask(Task):
        task_info = TaskInfo(name="dup", queues=["q"])

        @override
        async def run(self) -> None:
            pass

    registry.register(FirstTask)
    with pytest.raises(ConfigurationError):
        registry.register(SecondTask)


def test_get_entry_unknown_raises_task_not_found_error():
    registry = TaskRegistry()
    with pytest.raises(TaskNotFoundError):
        registry.get_entry("unknown")


def test_registry_is_initially_empty():
    registry = TaskRegistry()
    assert registry.get_all() == {}
    assert registry.has("anything") is False
