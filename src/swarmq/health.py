"""Health + readiness HTTP endpoints (R5.2).

Two endpoints, both stdlib-only (same asyncio HTTP server pattern as
:mod:`swarmq.metrics`):

- ``GET /health`` — liveness. Returns 200 unconditionally as long as
  the process and event loop are responsive. Used by orchestrators
  (Kubernetes, systemd) to decide whether to restart the worker.
- ``GET /ready`` — readiness. Returns 200 only when both the broker
  AND the backend are reachable (lightweight ping). Returns 503 with
  a JSON body listing which dependency is unhealthy when not. Used
  by load balancers and rolling-deploy automation to decide when a
  fresh worker is ready to accept traffic.

``swarmq worker --check`` is a separate concern — it performs
offline config validation (module importable, watchfiles available,
watch-dirs resolvable) and does NOT touch broker or backend. For a
liveness/readiness probe at runtime, call the HTTP endpoints above
directly or use ``--health-port`` to expose them.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, TypedDict

if TYPE_CHECKING:
    from swarmq.backend.valkey import ValkeyBackend
    from swarmq.broker.rabbitmq import RabbitMQBroker

logger = logging.getLogger("swarmq.health")


class _DependencyState(TypedDict):
    ok: bool
    error: str | None


class ReadinessSnapshot(TypedDict):
    """Shape of :meth:`HealthChecker.check_all` (ce-review KP-003).

    Documented for callers that compose /ready checks (Kubernetes
    probes, test fixtures, downstream health aggregators).
    """

    ready: bool
    broker: _DependencyState
    backend: _DependencyState


class HealthChecker:
    """Pings broker + backend for the readiness endpoint and CLI probe.

    Each check is bounded by a 2s asyncio.wait_for so a slow remote
    cannot wedge the readiness response. The backend uses Valkey
    ``PING``; the broker uses an aio_pika channel-status check (no
    queue declarations — purely transport-level liveness).
    """

    def __init__(
        self,
        broker: RabbitMQBroker,
        backend: ValkeyBackend,
        *,
        timeout_s: float = 2.0,
    ) -> None:
        self._broker = broker
        self._backend = backend
        self._timeout = timeout_s

    @staticmethod
    def _sanitise_error(exc: BaseException) -> str:
        """Return a minimally-informative error tag for the /ready body.

        ce-review SEC-001 / REL-07: valkey-py and aio_pika exception
        messages embed host:port (and in some code paths the full URL
        with credentials). /ready is typically behind a reverse proxy
        but may be externally reachable in some deployments, so we
        return only the exception class + a short static reason and
        log the full detail server-side.
        """
        cls = type(exc).__name__
        # Heuristic: if the exception message is short and does NOT
        # look like a URL/host:port leak, keep it; otherwise collapse
        # to the class name only.
        msg = str(exc)
        if not msg:
            return cls
        if len(msg) > 120 or "://" in msg or "@" in msg:
            return cls
        # Strip host:port fragments. Two patterns:
        #  (a) '[::1]:6379' — IPv6 bracket notation (ce-review SEC-F1)
        #  (b) 'host.example:6379' — v4 / DNS
        msg = re.sub(r"\[[0-9a-fA-F:]+\]:\d+", "<redacted>", msg)
        msg = re.sub(r"[\w.-]+:\d+", "<redacted>", msg)
        return f"{cls}: {msg}"

    async def check_backend(self) -> tuple[bool, str | None]:
        """Return (ok, error_tag_or_None)."""
        try:
            client = self._backend._client
            if client is None:
                return False, "backend not connected"
            await asyncio.wait_for(client.ping(), timeout=self._timeout)
            return True, None
        except Exception as exc:
            logger.info("check_backend_failed", exc_info=exc)
            return False, self._sanitise_error(exc)

    async def check_broker(self) -> tuple[bool, str | None]:
        try:
            conn = self._broker._connection
            if conn is None or conn.is_closed:
                return False, "broker not connected"
            # connection.ready() resolves immediately when the
            # underlying socket is open and the AMQP handshake is
            # complete.
            await asyncio.wait_for(conn.ready(), timeout=self._timeout)
            return True, None
        except Exception as exc:
            logger.info("check_broker_failed", exc_info=exc)
            return False, self._sanitise_error(exc)

    async def check_all(self) -> "ReadinessSnapshot":
        """Return a structured snapshot suitable for JSON serialisation."""
        broker_ok, broker_err = await self.check_broker()
        backend_ok, backend_err = await self.check_backend()
        return ReadinessSnapshot(
            ready=broker_ok and backend_ok,
            broker=_DependencyState(ok=broker_ok, error=broker_err),
            backend=_DependencyState(ok=backend_ok, error=backend_err),
        )


class HealthServer:
    """Minimal asyncio HTTP server exposing ``/health`` and ``/ready``.

    Same shape as :class:`swarmq.metrics.MetricsServer` — kept separate
    so an operator can run health on a different port (typical pattern:
    health on the same port as the worker proxy, metrics on the
    metrics-scraping port).

    **Bind-host default: ``127.0.0.1``** (ce-review SEC-004).
    Kubernetes liveness / readiness probes run in the same network
    namespace as the pod by default, so loopback is sufficient. Bind
    to ``0.0.0.0`` only when the probe originates outside the pod /
    container.

    **No built-in auth** (Härtung R2 P2, mirrors the /metrics note):
    ``/ready`` reveals which dependency (broker/backend) is down plus a
    sanitised error tag — an infrastructure fingerprint. When binding
    to ``0.0.0.0``, front the endpoint with a reverse proxy / firewall
    boundary.
    """

    def __init__(
        self,
        checker: HealthChecker,
        *,
        host: str = "127.0.0.1",
        port: int = 8080,
        liveness_started: float | None = None,
    ) -> None:
        from swarmq._http import AsyncHttpServer
        self._checker = checker
        self._started_at = liveness_started or time.monotonic()
        self._server = AsyncHttpServer(
            self._handle_request,
            host=host, port=port, label="health_server",
        )

    async def start(self) -> None:
        await self._server.start()

    async def stop(self) -> None:
        await self._server.stop()

    async def _handle_request(
        self,
        method: str,
        path: str,
        writer: asyncio.StreamWriter,
    ) -> None:
        from swarmq._http import write_http_response
        base_path = path.split("?")[0]
        if base_path == "/health":
            body = json.dumps({
                "status": "ok",
                "uptime_s": round(time.monotonic() - self._started_at, 3),
            }).encode()
            await write_http_response(writer, 200, body, "application/json")
        elif base_path == "/ready":
            snapshot = await self._checker.check_all()
            status = 200 if snapshot["ready"] else 503
            body = json.dumps(snapshot).encode()
            await write_http_response(writer, status, body, "application/json")
        else:
            await write_http_response(writer, 404, b"not found\n")


@asynccontextmanager
async def serve_health(
    checker: HealthChecker,
    *,
    host: str = "127.0.0.1",
    port: int = 8080,
) -> AsyncIterator[HealthServer]:
    """Async context manager: start HealthServer on enter, stop on exit."""
    server = HealthServer(checker, host=host, port=port)
    await server.start()
    try:
        yield server
    finally:
        await server.stop()
