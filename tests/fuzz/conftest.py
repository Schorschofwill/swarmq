"""Fuzz-test configuration (E2E-Hardening Phase 3 U3.6 / U3.7).

U3.7 (``test_external_publish_fuzz.py``) needs the ``chaos_stack`` +
``toxiproxy`` fixtures which live under ``tests/chaos/conftest.py``.
Re-export them here so tests under ``tests/fuzz/`` can request them
without forcing a nested directory layout.

Profil ``ci`` (Plan 2026-10-24-1118, KTD1): ``derandomize=True`` macht den
Kurzlauf in der Pipeline deterministisch (R10). Gewählt wird es über
``SWARMQ_FUZZ_PROFILE``, nicht ``HYPOTHESIS_PROFILE``: Das liest die
Property-Suite, die ``ci`` nicht kennt. Die eigenen ``@settings`` der Ziele
erben ``derandomize`` vom geladenen Profil, weil die conftest vor den
Testmodulen importiert wird. Ohne die Variable bleibt der Lauf zufällig.
"""

from __future__ import annotations

import os

import pytest
from hypothesis import settings

from tests.chaos.conftest import (
    ChaosStack,  # noqa: F401
    ToxiproxyClient,  # noqa: F401
    chaos_stack,  # noqa: F401
    toxiproxy,  # noqa: F401
)

settings.register_profile("ci", derandomize=True)
if profile := os.environ.get("SWARMQ_FUZZ_PROFILE"):
    settings.load_profile(profile)


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    for item in items:
        if "tests/fuzz/" in str(item.fspath):
            item.add_marker(pytest.mark.fuzz)
