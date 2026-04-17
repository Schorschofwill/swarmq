"""Fuzz-test configuration (E2E-Hardening Phase 3 U3.6 / U3.7).

U3.7 (``test_external_publish_fuzz.py``) needs the ``chaos_stack`` +
``toxiproxy`` fixtures which live under ``tests/chaos/conftest.py``.
Re-export them here so tests under ``tests/fuzz/`` can request them
without forcing a nested directory layout.
"""

from __future__ import annotations

import pytest

from tests.chaos.conftest import (  # noqa: F401 — re-exported fixtures
    ChaosStack,
    ToxiproxyClient,
    chaos_stack,
    toxiproxy,
)


def pytest_collection_modifyitems(config, items):
    for item in items:
        if "tests/fuzz/" in str(item.fspath):
            item.add_marker(pytest.mark.fuzz)
