"""Unit-test conftest — fixtures that keep unit tests fast and isolated."""

from __future__ import annotations

import pytest

from swarmq.task import Task


@pytest.fixture(autouse=True)
def reset_task_registry():
    """Save and restore ``Task._registered_tasks`` between tests.

    Prevents test pollution caused by ``__init_subclass__`` registering
    task classes in a shared class-level dict.
    """
    # --- setup (save) ---
    snapshot = Task._registered_tasks.copy()
    yield
    # --- teardown (restore) ---
    Task._registered_tasks = snapshot
