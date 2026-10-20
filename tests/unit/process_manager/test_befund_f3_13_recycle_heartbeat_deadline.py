"""Nachweis F3-13: Der Supervisor toetet einen recycelnden Worker, wenn
``heartbeat_interval_s >= shutdown_timeout + 5`` gilt.

Register: docs/befunde/fehlersuche-runde-3-2026-10.md (F3-13, R3C-04).

Waehrend Drain und Recycling galt die Lebenszeichen-Frist
``shutdown_grace_s + SHUTDOWN_TERM_S`` (``restart_budget.py``), also
``shutdown_timeout + 5``, statt ``heartbeat_timeout_s``. Liegt das
Lebenszeichen-Intervall darueber (etwa ``SWARMQ_HEARTBEAT_INTERVAL_S=15`` bei
``shutdown_timeout=10``), meldete der Supervisor ``worker_hung_during_drain``
und schickte SIGTERM, obwohl der Worker gesund drainiert. Das verletzt
FS-30/E-038 (Recycling-Drain bis ``hard_timeout``). Behoben in 0.12.3.

Festlegung (E-018, Nachtrag 2026-10-15): Die Frist fuer recycelnde Worker
(``INTENT_RECYCLE``) ist ``max(shutdown_grace_s + SHUTDOWN_TERM_S,
heartbeat_timeout_s)``; keine neue Validierung, ``shutdown_timeout=0`` bleibt
zulaessig. Der Scale-down-Drain (``INTENT_DRAINING``) behaelt
``shutdown_grace_s + SHUTDOWN_TERM_S``.

Testliste:
# [x] Recycler (ack'd) mit letztem Lebenszeichen vor 16 s, shutdown_grace_s=10,
#     heartbeat_timeout_s=60 -> kein SIGTERM, kein worker_hung_during_drain
# [x] Scale-down (mark_draining), Gnade 10 s, Beat vor 16 s -> SIGTERM,
#     worker_hung_during_drain mit deadline_s=15
# [x] shutdown_grace_s=1 (kleines Budget; 0 nimmt ``ProcessManager`` seit F3-42 an),
#     Recycler, Beat vor 30 s -> kein SIGTERM
# [x] shutdown_grace_s=1, Recycler, Beat vor 61 s -> SIGTERM, deadline_s=60
# [x] shutdown_grace_s=100, heartbeat_timeout_s=60, Recycler, Beat vor 70 s
#     -> kein SIGTERM (Frist 105, max statt Verkuerzung)
# [x] shutdown_grace_s=100, Recycler, Beat vor 100 s -> kein SIGTERM: die Frist
#     ist Gnade *plus* SIGTERM-Frist (105), nicht 95 (T-F-04, Testlücken Runde 4)
# Eingefrorener Recycler: test_supervisor.py::
#     test_acknowledged_recycler_that_freezes_in_drain_is_killed_not_replaced
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


def _make_pm(*, shutdown_grace_s: float, heartbeat_timeout_s: float = 60.0) -> ProcessManager:
    pm = ProcessManager(
        num_workers=1,
        worker_entry=_f313_entry_stub,
        worker_args=(_make_test_cfg(),),
        shutdown_grace_s=shutdown_grace_s,
        heartbeat_timeout_s=heartbeat_timeout_s,
    )
    pm._ctx = MagicMock()
    as_mock(pm._ctx).Process = MagicMock(return_value=_fake_process(pid=500))
    return pm


def _acknowledged_recycler(pm: ProcessManager) -> tuple[MagicMock, SupervisorChannel]:
    """Recycler mit bestaetigtem Ersatz; der Ersatz schlaegt frisch."""
    channel = SupervisorChannel.create(multiprocessing.get_context("spawn"))
    channel.request_recycle()
    recycler = _fake_process(pid=5)
    pm.workers = {5: WorkerInfo(process=recycler, worker_id=0, channel=channel)}
    pm._check_heartbeats()  # Absicht -> Ersatz + ack
    replacement_channel = SupervisorChannel.create(multiprocessing.get_context("spawn"))
    replacement_channel.heartbeat.value = time.monotonic()
    pm.workers[500].channel = replacement_channel
    return recycler, channel


def _hung_events(logs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [e for e in logs if e.get("event") == "worker_hung_during_drain"]


@pytest.mark.befund("F3-13")
@pytest.mark.entscheidung("E-018")
def test_recycling_worker_gets_at_least_heartbeat_timeout(swarmq_logs: list[dict[str, Any]]) -> None:
    pm = _make_pm(shutdown_grace_s=10.0)
    recycler, channel = _acknowledged_recycler(pm)

    channel.heartbeat.value = time.monotonic() - _STALE_S  # gesund, nur zwischen zwei Beats
    pm._check_heartbeats()

    assert not _hung_events(swarmq_logs), (
        "recycelnder Worker gilt nach 16 s ohne Beat als haengend (Frist shutdown_timeout + 5 = 15 s)"
    )
    recycler.terminate.assert_not_called()


@pytest.mark.entscheidung("E-018")
def test_scale_down_drain_keeps_shutdown_grace_plus_term(swarmq_logs: list[dict[str, Any]]) -> None:
    pm = _make_pm(shutdown_grace_s=10.0)
    channel = SupervisorChannel.create(multiprocessing.get_context("spawn"))
    channel.mark_draining()
    draining = _fake_process(pid=5)
    pm.workers = {5: WorkerInfo(process=draining, worker_id=0, channel=channel, expected_shutdown=True)}

    channel.heartbeat.value = time.monotonic() - _STALE_S
    pm._check_heartbeats()

    draining.terminate.assert_called_once()
    events = _hung_events(swarmq_logs)
    assert len(events) == 1
    assert events[0]["deadline_s"] == 15.0
    assert as_mock(pm._ctx).Process.call_count == 0  # Scale-down: kein Ersatz


@pytest.mark.entscheidung("E-018")
def test_recycling_with_tiny_shutdown_timeout_is_not_killed_within_heartbeat_timeout(
    swarmq_logs: list[dict[str, Any]],
) -> None:
    pm = _make_pm(shutdown_grace_s=1.0)  # Frist ohne Fix: 6 s
    recycler, channel = _acknowledged_recycler(pm)

    channel.heartbeat.value = time.monotonic() - 30.0
    pm._check_heartbeats()

    assert not _hung_events(swarmq_logs)
    recycler.terminate.assert_not_called()


@pytest.mark.entscheidung("E-018")
def test_recycling_with_tiny_shutdown_timeout_is_killed_after_heartbeat_timeout(
    swarmq_logs: list[dict[str, Any]],
) -> None:
    pm = _make_pm(shutdown_grace_s=1.0)  # Frist ohne Fix: 6 s
    recycler, channel = _acknowledged_recycler(pm)

    channel.heartbeat.value = time.monotonic() - 61.0
    pm._check_heartbeats()

    recycler.terminate.assert_called_once()
    events = _hung_events(swarmq_logs)
    assert len(events) == 1
    assert events[0]["deadline_s"] == 60.0
    assert as_mock(pm._ctx).Process.call_count == 1  # nur der Ersatz aus dem Recycling


@pytest.mark.entscheidung("E-018")
def test_recycling_keeps_the_longer_shutdown_grace(swarmq_logs: list[dict[str, Any]]) -> None:
    pm = _make_pm(shutdown_grace_s=100.0)
    recycler, channel = _acknowledged_recycler(pm)

    channel.heartbeat.value = time.monotonic() - 70.0  # ueber heartbeat_timeout_s, unter 105
    pm._check_heartbeats()

    assert not _hung_events(swarmq_logs)
    recycler.terminate.assert_not_called()


@pytest.mark.entscheidung("E-018")
def test_recycling_deadline_adds_the_sigterm_period_to_the_grace(swarmq_logs: list[dict[str, Any]]) -> None:
    """T-F-04 (Mutant ``restart_budget.py:62`` ``self.shutdown_grace_s +
    self.SHUTDOWN_TERM_S`` → ``- self.SHUTDOWN_TERM_S``): Der Bestand prüft mit
    70 s Stille, das liegt auch unter 95 s. Mit 100 s Stille bei 100 s Gnade
    liegt der Recycler noch in seiner Frist von 105 s."""
    pm = _make_pm(shutdown_grace_s=100.0)
    recycler, channel = _acknowledged_recycler(pm)

    channel.heartbeat.value = time.monotonic() - 100.0  # über 95, unter 105
    pm._check_heartbeats()

    assert not _hung_events(swarmq_logs)
    recycler.terminate.assert_not_called()
