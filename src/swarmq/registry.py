"""SwarmQ TaskRegistry."""

from __future__ import annotations

import annotationlib
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


def _read_return_type(run: Any) -> Any:
    """Liest den Rückgabetyp von ``run()`` tolerant (N2): ein unauflösbarer
    Name bleibt ein ``ForwardRef``, statt den Bau der App mit ``NameError``
    scheitern zu lassen. Auflösbare Typen kommen unverändert zurück, eine
    kaputte String-Annotation als roher Text."""
    fmt = annotationlib.Format.FORWARDREF
    return_type = annotationlib.get_annotations(run, format=fmt).get("return", inspect.Parameter.empty)
    if isinstance(return_type, str):
        try:
            return_type = annotationlib.ForwardRef(return_type, owner=run).evaluate(format=fmt)
        except SyntaxError:
            # Wie in MR 7 (T5 Regel 2): eine kaputte String-Annotation bleibt
            # als roher Text stehen.
            pass
    return return_type


class TaskRegistry:
    def __init__(self) -> None:
        self._tasks: dict[str, type[Task]] = {}
        self._entries: dict[str, RegistryEntry] = {}

    def register(self, task_class: type[Task]) -> None:
        name = task_class.task_info.name
        if name in self._tasks:
            raise ConfigurationError(f"Task {name!r} is already registered")
        return_type = _read_return_type(task_class.run)
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
