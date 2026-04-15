"""SwarmQ TaskRegistry."""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import Any

from swarmq.exceptions import ConfigurationError, TaskNotFoundError
from swarmq.task import Task, TaskInfo


@dataclass(frozen=True)
class RegistryEntry:
    task_class: type[Task]
    task_info: TaskInfo
    return_type: Any


class TaskRegistry:
    def __init__(self) -> None:
        self._tasks: dict[str, type[Task]] = {}
        self._entries: dict[str, RegistryEntry] = {}

    def register(self, task_class: type[Task]) -> None:
        name = task_class.task_info.name
        if name in self._tasks:
            raise ConfigurationError(
                f"Task {name!r} is already registered"
            )
        return_type = inspect.get_annotations(task_class.run, eval_str=True).get(
            "return", inspect.Parameter.empty
        )
        entry = RegistryEntry(
            task_class=task_class,
            task_info=task_class.task_info,
            return_type=return_type,
        )
        self._tasks[name] = task_class
        self._entries[name] = entry

    def get(self, task_name: str) -> type[Task]:
        try:
            return self._tasks[task_name]
        except KeyError:
            raise TaskNotFoundError(task_name) from None

    def get_entry(self, task_name: str) -> RegistryEntry:
        if task_name not in self._entries:
            raise TaskNotFoundError(task_name)
        return self._entries[task_name]

    def get_all(self) -> dict[str, type[Task]]:
        return dict(self._tasks)

    def has(self, task_name: str) -> bool:
        return task_name in self._tasks
