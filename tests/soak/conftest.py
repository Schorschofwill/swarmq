"""Soak-test fixtures (E2E-Hardening Phase 3 U3.9)."""

from __future__ import annotations

import pytest


def pytest_collection_modifyitems(config, items):
    for item in items:
        if "tests/soak/" in str(item.fspath):
            item.add_marker(pytest.mark.soak)
