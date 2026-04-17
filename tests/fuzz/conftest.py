"""Fuzz-test configuration (E2E-Hardening Phase 3 U3.6 / U3.7)."""

from __future__ import annotations

import pytest


def pytest_collection_modifyitems(config, items):
    for item in items:
        if "tests/fuzz/" in str(item.fspath):
            item.add_marker(pytest.mark.fuzz)
