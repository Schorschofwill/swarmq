"""Nachweis F3-13: Der Supervisor toetet einen recycelnden Worker, wenn
``heartbeat_interval_s >= shutdown_timeout + 5`` gilt.

Register: docs/befunde/fehlersuche-runde-3-2026-10.md (F3-13, R3C-04).

Waehrend Drain und Recycling gilt die Lebenszeichen-Frist
``shutdown_grace_s + SHUTDOWN_TERM_S`` (``restart_budget.py``), also
``shutdown_timeout + 5``, statt ``heartbeat_timeout_s``. Liegt das
Lebenszeichen-Intervall darueber (etwa ``SWARMQ_HEARTBEAT_INTERVAL_S=15`` bei
``shutdown_timeout=10``), meldet der Supervisor ``worker_hung_during_drain``
und schickt SIGTERM, obwohl der Worker gesund drainiert. Das verletzt
FS-30/E-038 (Recycling-Drain bis ``hard_timeout``).

Festlegung (Entscheidung 10): Die Frist fuer recycelnde Worker
(``INTENT_RECYCLE``) ist mindestens ``heartbeat_timeout_s``; keine neue
Validierung, ``shutdown_timeout=0`` bleibt zulaessig. Der Scale-down-Drain
(``INTENT_DRAINING``) ist nicht Gegenstand dieses Tests.

Testliste:
# [x] Recycler (ack'd) mit letztem Lebenszeichen vor 16 s, shutdown_grace_s=10,
#     heartbeat_timeout_s=60 -> kein SIGTERM, kein worker_hung_during_drain
"""

from __future__ import annotations

import multiprocessing
import time
from typing import Any
from unittest.mock import MagicMock

import pytest

from swarmq.process_manager import ProcessManager, WorkerInfo
from swarmq.supervision import SupervisorChannel
from tests.unit.process_manager._helpers import _fake_process, _make_test_cfg, as_mock

# Lebenszeichen-Intervall 15 s (SWARMQ_HEARTBEAT_INTERVAL_S=15): ein Takt plus
# Poll-Verzug liegt zwischen zwei Beats -- deutlich unter heartbeat_timeout_s.
_STALE_S = 16.0


def _f313_entry_stub(*_args: object) -> None:
    """Wird nie gestartet (``_ctx.Process`` ist gemockt)."""


@pytest.mark.befund("F3-13")
@pytest.mark.xfail(
    strict=True,
    reason="F3-13: offen - Recycling-Frist shutdown_timeout+5 unterschreitet das Lebenszeichen-Intervall",
)
def test_recycling_worker_gets_at_least_heartbeat_timeout(swarmq_logs: list[dict[str, Any]]) -> None:
    pm = ProcessManager(
        num_workers=1,
        worker_entry=_f313_entry_stub,
        worker_args=(_make_test_cfg(),),
        shutdown_grace_s=10.0,
        heartbeat_timeout_s=60.0,
    )
    pm._ctx = MagicMock()
    as_mock(pm._ctx).Process = MagicMock(return_value=_fake_process(pid=500))
    channel = SupervisorChannel.create(multiprocessing.get_context("spawn"))
    channel.request_recycle()
    recycler = _fake_process(pid=5)
    pm.workers = {5: WorkerInfo(process=recycler, worker_id=0, channel=channel)}
    pm._check_heartbeats()  # Absicht -> Ersatz + ack
    replacement_channel = SupervisorChannel.create(multiprocessing.get_context("spawn"))
    replacement_channel.heartbeat.value = time.monotonic()
    pm.workers[500].channel = replacement_channel

    channel.heartbeat.value = time.monotonic() - _STALE_S  # gesund, nur zwischen zwei Beats
    pm._check_heartbeats()

    assert not any(e.get("event") == "worker_hung_during_drain" for e in swarmq_logs), (
        "recycelnder Worker gilt nach 16 s ohne Beat als haengend (Frist shutdown_timeout + 5 = 15 s)"
    )
    recycler.terminate.assert_not_called()
