"""Property-based test configuration (E2E-Hardening Phase 3 U3.1).

Registers hypothesis profiles:
- ``quick`` (max_examples=100) for pre-merge CI.
- ``thorough`` (max_examples=10_000) for nightly CI.
Profile is selected via the ``HYPOTHESIS_PROFILE`` env var, defaulting
to ``quick``. Full wiring (profile registration) lands in U3.1 when
the first property test is added. This stub keeps the auto-marker
applying so opt-in semantics are preserved.
"""

from __future__ import annotations

import pytest


def pytest_collection_modifyitems(config, items):
    for item in items:
        if "tests/property/" in str(item.fspath):
            item.add_marker(pytest.mark.property)
