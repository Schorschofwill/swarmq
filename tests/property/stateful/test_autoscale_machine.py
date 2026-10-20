"""Autoskalierer-Maschine (swarmq 0.14.2, Plan 2026-10-20-2100, U2; E-084).

Invarianten im Docstring von ``tests/property/stateful/autoscale_machine.py``.
Profil ``quick`` (100 Beispiele) mit 40 Schritten je Beispiel.
"""

from __future__ import annotations

import os

import pytest
from hypothesis import settings
from hypothesis.stateful import run_state_machine_as_test

from tests.property.stateful.autoscale_machine import AutoscaleMachine

_TIMEOUT_S = 60 if os.environ.get("HYPOTHESIS_PROFILE", "quick") == "quick" else 1800
pytestmark = pytest.mark.timeout(_TIMEOUT_S)


def test_autoscale_machine_holds_its_invariants() -> None:
    run_state_machine_as_test(AutoscaleMachine, settings=settings(stateful_step_count=40))
