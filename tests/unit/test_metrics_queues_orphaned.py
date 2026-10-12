"""Gauge verwaister Queues (Plan 0.12.0, MR 18, BZ-14).

Test-Liste:
- [x] ``SCHEDULER_QUEUE_ORPHANED`` setzt ``swarmq_scheduler_queues_orphaned`` = ``orphaned_count``,
      0 sobald der Worker die Fuehrung abgibt
"""

from __future__ import annotations

import pytest

from swarmq.metrics import Metrics, install_signal_collectors
from swarmq.signals import Signal, SignalDispatcher

pytestmark = pytest.mark.timeout(5)

_SAMPLE = "swarmq_scheduler_queues_orphaned"


async def test_gauge_follows_the_signal_and_drops_to_zero_on_standby() -> None:
    m = Metrics()
    if not m.enabled:
        pytest.skip("prometheus_client missing")
    disp = SignalDispatcher()
    install_signal_collectors(disp, m)

    await disp.dispatch(
        Signal.SCHEDULER_QUEUE_ORPHANED, worker_id="w", queue="old", orphaned=True, message_count=3, orphaned_count=2
    )
    assert m.registry is not None
    assert m.registry.get_sample_value(_SAMPLE, {"worker_id": "w"}) == 2.0
    await disp.dispatch(Signal.SCHEDULER_STANDBY, worker_id="w")
    assert m.registry.get_sample_value(_SAMPLE, {"worker_id": "w"}) == 0.0
