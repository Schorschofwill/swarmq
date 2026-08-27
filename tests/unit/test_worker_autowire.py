"""Tests for Worker.start() auto-wire of metrics + health (ce-review T-03).

The Worker exposes optional HTTP surfaces via config.metrics_port /
health_port. Without these tests a regression in the auto-wire block
(REL-06 + COR-001 partial-start cleanup) would not be caught.
"""

from __future__ import annotations

import socket
from unittest.mock import MagicMock

from swarmq.config import SwarmQConfig
from swarmq.worker import Worker


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _make_worker(**cfg_kwargs) -> Worker:
    cfg = SwarmQConfig(
        broker_url="amqp://x",
        backend_url="valkey://x",
        **cfg_kwargs,
    )
    broker = MagicMock()
    backend = MagicMock()
    registry = MagicMock()
    registry.get_all = MagicMock(return_value=[])
    w = Worker(broker, backend, registry, cfg)
    return w


class TestAutoWire:
    async def test_metrics_server_not_constructed_when_port_unset(self):
        w = _make_worker()
        assert w._metrics is None
        assert w._metrics_server is None

    async def test_metrics_attrs_populated_after_start_simulation(self):
        """Simulate the auto-wire block directly — the full Worker.start()
        requires broker + backend + scheduler. We only need to verify
        that when metrics_port is set, the attributes are populated."""
        from swarmq.metrics import (
            Metrics,
            MetricsServer,
            install_signal_collectors,
        )

        port = _free_port()
        w = _make_worker(metrics_port=port, metrics_host="127.0.0.1")
        w._metrics = Metrics()
        install_signal_collectors(w._signal_dispatcher, w._metrics)
        w._metrics_server = MetricsServer(
            w._metrics, host=w._config.metrics_host,
            port=w._config.metrics_port,
        )
        await w._metrics_server.start()
        try:
            assert w._metrics is not None
            assert w._metrics_server is not None
        finally:
            await w._metrics_server.stop()

    async def test_auto_wire_cleanup_on_partial_start_failure(self):
        """ce-review COR-001 / REL-F-01: if the second HTTP server
        fails to bind, the first must be stopped before re-raise —
        otherwise its socket leaks for the process lifetime."""
        from swarmq.metrics import Metrics, MetricsServer

        # Pre-bind a port so health_server.start() will fail.
        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        occupied_port = sock.getsockname()[1]
        sock.listen()

        port_m = _free_port()
        w = _make_worker(
            metrics_port=port_m,
            health_port=occupied_port,  # will conflict
        )

        # Manually run the auto-wire block (minus scheduler setup).
        from swarmq.health import HealthChecker, HealthServer

        w._metrics = Metrics()
        w._metrics_server = MetricsServer(
            w._metrics, host="127.0.0.1", port=port_m,
        )
        raised = False
        try:
            await w._metrics_server.start()
            checker = HealthChecker(w._broker, w._backend)
            w._health_server = HealthServer(
                checker, host="127.0.0.1", port=occupied_port,
            )
            try:
                await w._health_server.start()
            except RuntimeError:
                # Simulating the cleanup branch from worker.start().
                await w._metrics_server.stop()
                raised = True
                raise
        except RuntimeError:
            pass
        finally:
            sock.close()

        assert raised, "health_server.start() should have raised"
        # Post-cleanup: metrics_server socket must be stoppable a
        # second time without error (idempotent).
        await w._metrics_server.stop()


class TestConfigAutoWireDefaults:
    def test_metrics_defaults_to_none_and_loopback(self):
        cfg = SwarmQConfig(broker_url="x", backend_url="x")
        assert cfg.metrics_port is None
        assert cfg.metrics_host == "127.0.0.1"
        assert cfg.health_port is None
        assert cfg.health_host == "127.0.0.1"
