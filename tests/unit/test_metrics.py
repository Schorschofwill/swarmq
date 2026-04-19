"""Unit tests for R5.1 Prometheus metrics exporter."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from swarmq.metrics import (
    CONTENT_TYPE,
    Metrics,
    MetricsServer,
    _HAVE_PROMETHEUS,
    install_signal_collectors,
    serve_metrics,
)
from swarmq.signals import Signal, SignalDispatcher


# ---------------------------------------------------------------------------
# Metrics (registry + render)
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
                task_name="t1", queue="q1", outcome="completed",
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
            Signal.TASK_STARTED, queue="q1", task_name="t1", task_id="x",
        )
        await disp.dispatch(
            Signal.TASK_COMPLETED,
            queue="q1", task_name="t1", task_id="x", duration_ms=125.0,
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
            Signal.TASK_STARTED, queue="q1", task_name="t1", task_id="x",
        )
        await disp.dispatch(
            Signal.TASK_STARTED, queue="q1", task_name="t2", task_id="y",
        )
        # Two active.
        body = m.render().decode()
        assert 'swarmq_tasks_active{queue="q1"} 2.0' in body

        await disp.dispatch(
            Signal.TASK_FAILED,
            queue="q1", task_name="t2", task_id="y",
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
            queue="q", task_name="t", task_id="x",
            retry_count=1, delay=0.5,
        )
        await disp.dispatch(
            Signal.TASK_RATE_LIMITED,
            queue="q", task_name="t", task_id="x",
        )
        body = m.render().decode()
        assert "swarmq_retries_total" in body
        assert 'outcome="rate_limited"' in body


# ---------------------------------------------------------------------------
# MetricsServer (HTTP)
# ---------------------------------------------------------------------------


class TestMetricsServer:
    async def test_metrics_endpoint_returns_200(self):
        m = Metrics()
        async with serve_metrics(m, host="127.0.0.1", port=0) as server:
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
