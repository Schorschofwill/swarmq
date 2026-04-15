"""Root conftest — shared fixtures for all test layers."""

from __future__ import annotations

import pathlib

import pytest

from swarmq.task import Task


# ---------------------------------------------------------------------------
# Task registry isolation — prevents __init_subclass__ pollution across tests
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _reset_task_registry():
    """Save and restore Task._registered_tasks between every test."""
    snapshot = Task._registered_tasks.copy()
    yield
    Task._registered_tasks = snapshot

# ---------------------------------------------------------------------------
# Auto-marker: tag every test with unit / integration / e2e based on its path
# ---------------------------------------------------------------------------
_LAYER_MARKERS: dict[str, str] = {
    "unit": "unit",
    "integration": "integration",
    "e2e": "e2e",
}


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Automatically apply layer markers based on test file location."""
    tests_root = pathlib.Path(__file__).resolve().parent

    for item in items:
        item_path = pathlib.Path(item.fspath).resolve()
        try:
            rel = item_path.relative_to(tests_root)
        except ValueError:
            continue

        # The first directory component under tests/ determines the layer.
        parts = rel.parts
        if parts:
            layer = parts[0]
            if layer in _LAYER_MARKERS:
                item.add_marker(getattr(pytest.mark, _LAYER_MARKERS[layer]))
