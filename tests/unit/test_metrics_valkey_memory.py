"""Gauge des Valkey-Speichers (Plan 0.12.0, MR 14, BZ-04).

Test-Liste:
- [x] ``SCHEDULER_VALKEY_CHECKED`` setzt ``swarmq_valkey_memory_used_ratio`` = used/maxmemory
- [x] ohne Grenze (``maxmemory`` 0) oder unbekannte Werte -> Gauge unveraendert
"""

from __future__ import annotations

import pytest

from swarmq.metrics import Metrics, install_signal_collectors
from swarmq.signals import Signal, SignalDispatcher

_SAMPLE = "swarmq_valkey_memory_used_ratio"


@pytest.fixture
def wired() -> tuple[Metrics, SignalDispatcher]:
    m = Metrics()
    if not m.enabled:
        pytest.skip("prometheus_client missing")
    disp = SignalDispatcher()
    install_signal_collectors(disp, m)
    return m, disp


async def test_ratio_follows_the_leader_check(wired: tuple[Metrics, SignalDispatcher]) -> None:
    m, disp = wired

    await disp.dispatch(
        Signal.SCHEDULER_VALKEY_CHECKED,
        worker_id="w",
        status="safe",
        policy="noeviction",
        maxmemory=1000,
        used_memory=250,
    )

    assert m.registry is not None
    assert m.registry.get_sample_value(_SAMPLE, {"worker_id": "w"}) == 0.25


@pytest.mark.parametrize(
    ("maxmemory", "used"), [(0, 250), (None, 250), (1000, None)], ids=["no-limit", "unknown-limit", "unknown-used"]
)
async def test_without_limit_or_values_the_ratio_stays(
    wired: tuple[Metrics, SignalDispatcher], maxmemory: int | None, used: int | None
) -> None:
    m, disp = wired

    await disp.dispatch(
        Signal.SCHEDULER_VALKEY_CHECKED,
        worker_id="w",
        status="safe",
        policy="noeviction",
        maxmemory=maxmemory,
        used_memory=used,
    )

    assert m.registry is not None
    assert m.registry.get_sample_value(_SAMPLE, {"worker_id": "w"}) is None
