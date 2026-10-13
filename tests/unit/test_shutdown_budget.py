"""Shutdown-Budget (BZ-18, Plan 0.12.0, MR 17): ``shutdown_timeout`` ist das Gesamtbudget ab SIGTERM.

Test-Liste:
- [x] Supervisor mit Default 30 s: SIGTERM bei 23 s, SIGKILL bei 28 s; der Worker
      ist bei 28 s fertig, sein Drain endet bei 23 s
- [x] eingebettet 30 s: fertig bei 30 s, Drain bis 25 s (wie die bisherige Publish-Endfrist)
- [x] kleine Budgets: Reserven hoechstens die Haelfte, keine Phase negativ
- [x] 0 (oder negativ) heisst sofort: alle Phasen 0
"""

from __future__ import annotations

import pytest

from swarmq.shutdown_budget import shutdown_budget


@pytest.mark.entscheidung("E-018")
def test_supervised_default_budget() -> None:
    b = shutdown_budget(30, supervised=True)
    assert (b.sigterm_at, b.sigkill_at, b.finish_by, b.drain_until) == (23.0, 28.0, 28.0, 23.0)


def test_embedded_default_budget() -> None:
    b = shutdown_budget(30, supervised=False)
    assert (b.finish_by, b.drain_until) == (30.0, 25.0)


@pytest.mark.parametrize("supervised", [True, False])
@pytest.mark.parametrize("total", [0.5, 1.0, 3.0, 5.0])
def test_small_budgets_keep_every_phase_positive_and_ordered(total: float, supervised: bool) -> None:
    b = shutdown_budget(total, supervised=supervised)
    assert 0 < b.drain_until < b.finish_by <= b.total
    assert 0 < b.sigterm_at < b.sigkill_at < b.total
    assert b.drain_until >= b.finish_by / 2


@pytest.mark.parametrize("total", [0, -1])
def test_zero_means_immediately(total: float) -> None:
    b = shutdown_budget(total, supervised=True)
    assert (b.sigterm_at, b.sigkill_at, b.finish_by, b.drain_until) == (0.0, 0.0, 0.0, 0.0)
