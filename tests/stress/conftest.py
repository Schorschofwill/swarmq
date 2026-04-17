"""Stress-test fixtures (E2E-Hardening Phase 3 U3.8)."""

from __future__ import annotations

import pytest


def pytest_collection_modifyitems(config, items):
    for item in items:
        if "tests/stress/" in str(item.fspath):
            item.add_marker(pytest.mark.stress)
