"""Chaos-test fixtures (E2E-Hardening Phase 2).

Placeholder — the full chaos_stack + toxiproxy fixtures land in U2.0.
This file exists now so `pytest --collect-only -m chaos` is a clean
no-op until chaos tests are added.

Every test here must be marked ``@pytest.mark.chaos``.
"""

from __future__ import annotations

import pytest


def pytest_collection_modifyitems(config, items):
    """Auto-apply the chaos marker to everything under tests/chaos/.

    Forgetting the decorator on a new test would otherwise cause it
    to run in the default suite (which we explicitly exclude via
    addopts). This keeps the opt-in invariant safe against human
    error.
    """
    for item in items:
        if "tests/chaos/" in str(item.fspath):
            item.add_marker(pytest.mark.chaos)
