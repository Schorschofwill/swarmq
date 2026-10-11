"""Unit tests for R5.1 Prometheus metrics exporter."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest

from swarmq.metrics import (
    _HAVE_PROMETHEUS,
    CONTENT_TYPE,
    Metrics,
    install_signal_collectors,
    serve_metrics,
)
from swarmq.signals import Signal, SignalDispatcher

if TYPE_CHECKING:
    from prometheus_client import CollectorRegistry


def _registry(m: Metrics) -> CollectorRegistry:
    """Registry einer aktiven ``Metrics``-Instanz (ohne prometheus_client None)."""
    assert m.registry is not None
    return m.registry


def _child_value(child: object) -> float:
    """Aktueller Wert eines gelabelten Counter-/Gauge-Kindes."""
    from prometheus_client import Counter, Gauge

    assert isinstance(child, Counter | Gauge), child
    value: float = child._value.get()
    return value


# ---------------------------------------------------------------------------
# Metrics: Registry und Rendern
# ---------------------------------------------------------------------------


class TestMetrics:
    def test_enabled_matches_prometheus_presence(self):
        m = Metrics()
        assert m.enabled is _HAVE_PROMETHEUS

    def test_all_labeled_metrics_accessible(self):
        m = Metrics()
        # Each metric accepts the documented label set without blowing up.
        m.tasks_total.labels(task_name="t", queue="q", outcome="completed").inc()
        m.task_duration_seconds.labels(task_name="t", queue="q").observe(0.5)
        m.tasks_active.labels(queue="q").inc()
        m.tasks_active.labels(queue="q").dec()
        m.queue_depth.labels(queue="q", kind="main").set(42)
        m.worker_uptime_seconds.labels(worker_id="w1").set(100.0)
        m.retries_total.labels(task_name="t", queue="q").inc()
        m.dlq_messages.labels(queue="q").set(3)

    def test_render_returns_bytes(self):
        m = Metrics()
        body = m.render()
        assert isinstance(body, bytes)
        if m.enabled:
            # Real registry emits a newline-terminated text-exposition
            # frame; the label set we populated must appear.
            m.tasks_total.labels(
                task_name="t1",
                queue="q1",
                outcome="completed",
            ).inc()
            body = m.render()
            assert b"swarmq_tasks_total" in body
            assert b'task_name="t1"' in body

    @pytest.mark.skipif(
        _HAVE_PROMETHEUS,
        reason="only relevant when prometheus_client is absent",
    )
    def test_render_without_prometheus_returns_placeholder(self):
        m = Metrics()
        body = m.render()
        assert b"metrics disabled" in body


# ---------------------------------------------------------------------------
# Signal-collector wiring
# ---------------------------------------------------------------------------


class TestInstallSignalCollectors:
    async def test_task_completed_increments_counter_and_observes_duration(self):
        m = Metrics()
        disp = SignalDispatcher()
        install_signal_collectors(disp, m)

        if not m.enabled:
            pytest.skip("prometheus_client missing")

        await disp.dispatch(
            Signal.TASK_STARTED,
            queue="q1",
            task_name="t1",
            task_id="x",
        )
        await disp.dispatch(
            Signal.TASK_COMPLETED,
            queue="q1",
            task_name="t1",
            task_id="x",
            duration_ms=125.0,
        )
        body = m.render().decode()
        assert 'outcome="completed"' in body
        # Histogram observation lands in the appropriate bucket (0.25 > 0.125).
        assert "swarmq_task_duration_seconds" in body

    async def test_task_active_inc_dec_balanced(self):
        m = Metrics()
        disp = SignalDispatcher()
        install_signal_collectors(disp, m)
        if not m.enabled:
            pytest.skip("prometheus_client missing")

        await disp.dispatch(
            Signal.TASK_STARTED,
            queue="q1",
            task_name="t1",
            task_id="x",
        )
        await disp.dispatch(
            Signal.TASK_STARTED,
            queue="q1",
            task_name="t2",
            task_id="y",
        )
        # Two active.
        body = m.render().decode()
        assert 'swarmq_tasks_active{queue="q1"} 2.0' in body

        await disp.dispatch(
            Signal.TASK_FAILED,
            queue="q1",
            task_name="t2",
            task_id="y",
        )
        body = m.render().decode()
        assert 'swarmq_tasks_active{queue="q1"} 1.0' in body

    async def test_retries_and_rate_limited_increment(self):
        m = Metrics()
        disp = SignalDispatcher()
        install_signal_collectors(disp, m)
        if not m.enabled:
            pytest.skip("prometheus_client missing")

        await disp.dispatch(
            Signal.TASK_RETRYING,
            queue="q",
            task_name="t",
            task_id="x",
            retry_count=1,
            delay=0.5,
        )
        await disp.dispatch(
            Signal.TASK_RATE_LIMITED,
            queue="q",
            task_name="t",
            task_id="x",
        )
        body = m.render().decode()
        assert "swarmq_retries_total" in body
        assert 'outcome="rate_limited"' in body

    async def test_task_dlq_increments_dlq_outcome(self):
        # 2026-06-02 audit P1-4: dead-lettering must increment
        # tasks_total{outcome="dlq"}.
        m = Metrics()
        disp = SignalDispatcher()
        install_signal_collectors(disp, m)
        if not m.enabled:
            pytest.skip("prometheus_client missing")

        await disp.dispatch(
            Signal.TASK_DLQ,
            queue="q",
            task_name="t",
            task_id="x",
            error="boom",
        )
        body = m.render().decode()
        assert 'outcome="dlq"' in body

    async def test_scheduler_leader_gauge_tracks_election(self):
        # 2026-06-02 audit P1-5: SCHEDULER_* signals drive a 0/1 gauge.
        m = Metrics()
        disp = SignalDispatcher()
        install_signal_collectors(disp, m)
        if not m.enabled:
            pytest.skip("prometheus_client missing")

        await disp.dispatch(Signal.SCHEDULER_LEADER, worker_id="w1")
        assert _child_value(m.scheduler_leader.labels(worker_id="w1")) == 1

        # Graceful handover / standby / shutdown all drop it to 0.
        await disp.dispatch(Signal.SCHEDULER_LEADER_HANDOVER, worker_id="w1")
        assert _child_value(m.scheduler_leader.labels(worker_id="w1")) == 0

        # Failover re-acquire sets it back to 1.
        await disp.dispatch(Signal.SCHEDULER_LEADER_FAILOVER, worker_id="w1")
        assert _child_value(m.scheduler_leader.labels(worker_id="w1")) == 1

        await disp.dispatch(Signal.WORKER_SHUTDOWN, worker_id="w1")
        assert _child_value(m.scheduler_leader.labels(worker_id="w1")) == 0


# ---------------------------------------------------------------------------
# MetricsServer über HTTP
# ---------------------------------------------------------------------------


class TestMetricsServer:
    async def test_metrics_endpoint_returns_200(self):
        m = Metrics()
        async with serve_metrics(m, host="127.0.0.1", port=0):
            # Bind on 0 to get a free port — but start_server needs a
            # real port. Rebind with one we can read.
            pass
        # Re-run with a specific free port we own.
        import socket

        s = socket.socket()
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
        s.close()

        async with serve_metrics(m, host="127.0.0.1", port=port):
            # Small settle — server.start is awaited but OS may take a
            # beat to accept on CI.
            await asyncio.sleep(0.05)
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.write(b"GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n")
            await writer.drain()
            status_line = await reader.readline()
            assert status_line.startswith(b"HTTP/1.1 200 OK"), status_line
            # Consume headers; verify Content-Type.
            ct_seen = False
            while True:
                line = await reader.readline()
                if line in (b"\r\n", b""):
                    break
                if line.lower().startswith(b"content-type:"):
                    ct_seen = True
                    assert CONTENT_TYPE.encode() in line
            assert ct_seen
            writer.close()
            await writer.wait_closed()

    async def test_unknown_path_returns_404(self):
        import socket

        s = socket.socket()
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
        s.close()
        m = Metrics()
        async with serve_metrics(m, host="127.0.0.1", port=port):
            await asyncio.sleep(0.05)
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.write(b"GET /does-not-exist HTTP/1.1\r\nHost: x\r\n\r\n")
            await writer.drain()
            status_line = await reader.readline()
            assert status_line.startswith(b"HTTP/1.1 404"), status_line
            writer.close()
            await writer.wait_closed()

    async def test_wrong_method_returns_405(self):
        import socket

        s = socket.socket()
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
        s.close()
        m = Metrics()
        async with serve_metrics(m, host="127.0.0.1", port=port):
            await asyncio.sleep(0.05)
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.write(b"POST /metrics HTTP/1.1\r\nHost: x\r\n\r\n")
            await writer.drain()
            status_line = await reader.readline()
            assert status_line.startswith(b"HTTP/1.1 405"), status_line
            writer.close()
            await writer.wait_closed()


async def test_recycling_signal_increments_recyclings_total():
    """Plan 2026-09-03-001 (R7/R23): jedes Recycling zaehlt mit Grund und Art."""
    from swarmq.metrics import Metrics, install_signal_collectors
    from swarmq.signals import Signal, SignalDispatcher

    m = Metrics()
    disp = SignalDispatcher()
    install_signal_collectors(disp, m)
    await disp.dispatch(Signal.WORKER_RECYCLING, worker_id="w", kind="compute_child", slot=0, reason="rss", value=320.0)
    await disp.dispatch(Signal.WORKER_RECYCLING, worker_id="w", kind="worker", slot=None, reason="tasks", value=500.0)
    if m.enabled:
        body = m.render().decode()
        assert 'swarmq_recyclings_total{kind="compute_child",reason="rss"} 1.0' in body
        assert 'swarmq_recyclings_total{kind="worker",reason="tasks"} 1.0' in body


class TestRejectedCounter:
    """U6 (R13): ``swarmq_tasks_rejected_total{queue, reason}``.

    Der Reject-Weg hatte keinen Zaehler; ``swarmq_tasks_total{outcome="dlq"}``
    darf ihn auch nicht mitzaehlen -- die beiden Wege haben verschiedene
    Ursachen und verschiedene Gegenmassnahmen, und ein Alert auf "wir
    dead-lettern" soll nicht bei jeder zu grossen Nachricht anschlagen."""

    async def test_signal_increments_the_counter_by_queue_and_reason(self):
        from swarmq.metrics import Metrics, install_signal_collectors
        from swarmq.signals import Signal, SignalDispatcher

        m = Metrics()
        if not m.enabled:
            pytest.skip("prometheus_client missing")
        dispatcher = SignalDispatcher()
        install_signal_collectors(dispatcher, m)

        await dispatcher.dispatch(
            Signal.TASK_REJECTED,
            queue="tasks.high",
            reason="body_too_large",
            task_id=None,
            task_name=None,
            error="too big",
        )
        await dispatcher.dispatch(
            Signal.TASK_REJECTED,
            queue="tasks.high",
            reason="body_too_large",
            task_id=None,
            task_name=None,
            error="too big",
        )
        await dispatcher.dispatch(
            Signal.TASK_REJECTED,
            queue="tasks.high",
            reason="undecodable",
            task_id=None,
            task_name=None,
            error="bad json",
        )

        assert _child_value(m.tasks_rejected_total.labels(queue="tasks.high", reason="body_too_large")) == 2
        assert _child_value(m.tasks_rejected_total.labels(queue="tasks.high", reason="undecodable")) == 1

    async def test_dlq_outcome_stays_separate(self):
        """R13: ``TASK_DLQ`` zaehlt weiter nur sich selbst."""
        from swarmq.metrics import Metrics, install_signal_collectors
        from swarmq.signals import Signal, SignalDispatcher

        m = Metrics()
        if not m.enabled:
            pytest.skip("prometheus_client missing")
        dispatcher = SignalDispatcher()
        install_signal_collectors(dispatcher, m)

        await dispatcher.dispatch(
            Signal.TASK_REJECTED,
            queue="q",
            reason="undecodable",
            task_id=None,
            task_name=None,
            error="x",
        )
        assert _child_value(m.tasks_total.labels(task_name="", queue="q", outcome="dlq")) == 0


# ---------------------------------------------------------------------------
# Log-Drop-Zaehler (Plan 2026-10-06-0831, U3, KTD5)
# ---------------------------------------------------------------------------


class TestLogRecordsDroppedCounter:
    """Verworfene Logeintraege des Worker-Prozesses als Prometheus-Zaehler
    (R7, AE7)."""

    def test_counter_rises_with_dropped_log_records(self):
        import logging
        import queue

        from swarmq.logging_queue import _StructlogQueueHandler

        m = Metrics()
        if not m.enabled:
            return
        assert m.registry is not None
        name = "swarmq_log_records_dropped_total"
        before = m.registry.get_sample_value(name)
        assert before is not None

        handler = _StructlogQueueHandler(queue.Queue(maxsize=1))
        record = logging.LogRecord("t", logging.INFO, __file__, 1, "m", None, None)
        for _ in range(3):
            handler.enqueue(record)

        assert _registry(m).get_sample_value(name) == before + 2


async def test_cron_conflict_signal_increments_conflict_counter():
    """Plan 2026-10-08-1541 (U12, R12): ein langer Cron-Konflikt ist alertbar."""
    from swarmq.metrics import Metrics, install_signal_collectors
    from swarmq.signals import Signal, SignalDispatcher

    m = Metrics()
    if not m.enabled:
        pytest.skip("prometheus_client missing")
    disp = SignalDispatcher()
    install_signal_collectors(disp, m)
    for _ in range(2):
        await disp.dispatch(
            Signal.SCHEDULER_CRON_CONFLICT,
            worker_id="w",
            schedule_name="nightly",
            fingerprints=["a", "b"],
        )
    body = m.render().decode()
    assert 'swarmq_scheduler_cron_conflicts_total{schedule_name="nightly"} 2.0' in body


async def test_leader_outdated_signal_sets_and_clears_gauge():
    """Plan 2026-10-08-1541 (U13, KTD11): ein veralteter Leader ist alertbar,
    solange er Leader ist; ein aktueller Nachfolger setzt die Gauge zurück."""
    from swarmq.metrics import Metrics, install_signal_collectors
    from swarmq.signals import Signal, SignalDispatcher

    m = Metrics()
    if not m.enabled:
        pytest.skip("prometheus_client missing")
    disp = SignalDispatcher()
    install_signal_collectors(disp, m)
    sample = "swarmq_scheduler_leader_outdated"

    await disp.dispatch(
        Signal.SCHEDULER_LEADER_OUTDATED,
        worker_id="w",
        leader="old:1:aa",
        leader_version=None,
        outdated=True,
    )
    assert _registry(m).get_sample_value(sample, {"worker_id": "w"}) == 1.0

    await disp.dispatch(
        Signal.SCHEDULER_LEADER_OUTDATED,
        worker_id="w",
        leader="new:1:bb",
        leader_version="0.10.0",
        outdated=False,
    )
    assert _registry(m).get_sample_value(sample, {"worker_id": "w"}) == 0.0


def test_leader_outdated_help_names_the_minimum_version():
    """KTD11 (Plan 2026-10-09-2314, MR 5): die Gauge meldet einen Leader
    unter der Mindestversion 0.12, nicht mehr nur fehlende Zeitzonen."""
    m = Metrics()
    if not m.enabled:
        pytest.skip("prometheus_client missing")
    [help_line] = [
        line for line in m.render().decode().splitlines() if line.startswith("# HELP swarmq_scheduler_leader_outdated ")
    ]
    assert "before 0.12" in help_line
    assert "timezone" not in help_line


def test_consumer_reregistrations_are_counted_per_queue():
    """Plan 2026-10-08-1541 (U6, R2): jede Neuanmeldung des Consumer-Wächters."""
    m = Metrics()
    if not m.enabled:
        pytest.skip("prometheus_client missing")

    m.count_consumer_reregistration("q1", "broker_cancel")
    m.count_consumer_reregistration("q1", "channel_replaced")

    sample = "swarmq_consumer_reregistrations_total"
    assert _registry(m).get_sample_value(sample, {"queue": "q1"}) == 2.0


def test_execution_guard_events_are_counted_per_event():
    """Plan 2026-10-08-1541 (U8, KTD5): übersprungene Duplikate,
    Zurückstellungen, Waisen-Grenze und verlorene Marker."""
    m = Metrics()
    if not m.enabled:
        pytest.skip("prometheus_client missing")

    m.count_execution_guard_event("duplicate_skipped")
    m.count_execution_guard_event("duplicate_skipped")
    m.count_execution_guard_event("lease_lost")

    sample = "swarmq_execution_guard_events_total"
    assert _registry(m).get_sample_value(sample, {"event": "duplicate_skipped"}) == 2.0
    assert _registry(m).get_sample_value(sample, {"event": "lease_lost"}) == 1.0


async def test_fire_unroutable_signal_increments_unroutable_counter():
    """Plan 0.12.0 (MR 6, BZ-03): verlorene Cron-Fires und verworfene Oneshots
    sind zaehlbar."""
    m = Metrics()
    if not m.enabled:
        pytest.skip("prometheus_client missing")
    disp = SignalDispatcher()
    install_signal_collectors(disp, m)
    for kind in ("cron_lost", "cron_lost", "oneshot_dropped"):
        await disp.dispatch(
            Signal.SCHEDULER_FIRE_UNROUTABLE,
            worker_id="w",
            schedule_name="nightly",
            queue="reports",
            kind=kind,
        )
    body = m.render().decode()
    assert 'swarmq_scheduler_fires_unroutable_total{kind="cron_lost",queue="reports"} 2.0' in body
    assert 'swarmq_scheduler_fires_unroutable_total{kind="oneshot_dropped",queue="reports"} 1.0' in body


async def test_unknown_task_deferrals_are_counted_per_queue():
    """Plan 0.12.0, MR 11 (BZ-06): jede Rueckstellung eines unbekannten Tasks."""
    m = Metrics()
    if not m.enabled:
        pytest.skip("prometheus_client missing")
    disp = SignalDispatcher()
    install_signal_collectors(disp, m)

    for _ in range(2):
        await disp.dispatch(
            Signal.TASK_UNKNOWN_DEFERRED,
            task_id="t",
            task_name="new_task",
            queue="q1",
            delay=1.0,
            unknown_since=0,
        )

    assert _registry(m).get_sample_value("swarmq_tasks_unknown_deferred_total", {"queue": "q1"}) == 2.0


async def test_scheduler_crons_held_gauge_follows_the_unowned_signal():
    """Plan 0.12.0, MR 11 (BZ-11): Zahl der zurueckgehaltenen Crons des Leaders,
    0 sobald der Worker die Fuehrung abgibt."""
    m = Metrics()
    if not m.enabled:
        pytest.skip("prometheus_client missing")
    disp = SignalDispatcher()
    install_signal_collectors(disp, m)
    sample = "swarmq_scheduler_crons_held"

    await disp.dispatch(
        Signal.SCHEDULER_CRON_UNOWNED,
        worker_id="w",
        schedule_name="ghost",
        task_name="t",
        held=True,
        held_count=2,
    )
    assert _registry(m).get_sample_value(sample, {"worker_id": "w"}) == 2.0
    await disp.dispatch(Signal.SCHEDULER_STANDBY, worker_id="w")
    assert _registry(m).get_sample_value(sample, {"worker_id": "w"}) == 0.0
