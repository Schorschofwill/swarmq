"""Fuzz-test configuration (E2E-Hardening Phase 3 U3.6 / U3.7).

U3.7 (``test_external_publish_fuzz.py``) needs the ``chaos_stack`` +
``toxiproxy`` fixtures which live under ``tests/chaos/conftest.py``.
Re-export them here so tests under ``tests/fuzz/`` can request them
without forcing a nested directory layout.
"""

from __future__ import annotations

import pytest

from tests.chaos.conftest import (
    ChaosStack,  # noqa: F401
    ToxiproxyClient,  # noqa: F401
    chaos_stack,  # noqa: F401
    toxiproxy,  # noqa: F401
)


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    for item in items:
        if "tests/fuzz/" in str(item.fspath):
            item.add_marker(pytest.mark.fuzz)
