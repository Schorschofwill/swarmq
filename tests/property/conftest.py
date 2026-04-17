"""Property-based test configuration (E2E-Hardening Phase 3 U3.1).

Registers hypothesis profiles:
- ``quick`` (max_examples=100) for pre-merge CI.
- ``thorough`` (max_examples=10_000) for nightly CI.
Profile is selected via the ``HYPOTHESIS_PROFILE`` env var, defaulting
to ``quick``. The auto-marker ensures everything under
``tests/property/`` runs only when ``-m property`` is passed, keeping
the default dev loop fast.
"""

from __future__ import annotations

import os

import pytest
from hypothesis import HealthCheck, settings

settings.register_profile(
    "quick",
    max_examples=100,
    suppress_health_check=[HealthCheck.too_slow],
    deadline=None,
)
settings.register_profile(
    "thorough",
    max_examples=10_000,
    suppress_health_check=[HealthCheck.too_slow],
    deadline=None,
)
settings.load_profile(os.environ.get("HYPOTHESIS_PROFILE", "quick"))


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    for item in items:
        if "tests/property/" in str(item.fspath):
            item.add_marker(pytest.mark.property)
