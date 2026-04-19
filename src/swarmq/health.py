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

The ``swarmq worker --check`` CLI flag runs a one-shot probe against
both endpoints (broker + backend) and exits 0 / 1 — useful for
container HEALTHCHECK directives without exposing the HTTP server.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from swarmq.backend.valkey import ValkeyBackend
    from swarmq.broker.rabbitmq import RabbitMQBroker

logger = logging.getLogger("swarmq.health")


class HealthChecker:
    """Pings broker + backend for the readiness endpoint and CLI probe.

    Each check is bounded by a 2s asyncio.wait_for so a slow remote
    cannot wedge the readiness response. The backend uses Valkey
    ``PING``; the broker uses an aio_pika channel-status check (no
    queue declarations — purely transport-level liveness).
    """

    def __init__(
        self,
        broker: "RabbitMQBroker",
        backend: "ValkeyBackend",
        *,
        timeout_s: float = 2.0,
    ) -> None:
        self._broker = broker
        self._backend = backend
        self._timeout = timeout_s

    async def check_backend(self) -> tuple[bool, str | None]:
        """Return (ok, error_message_or_None)."""
        try:
            client = self._backend._client
            if client is None:
                return False, "backend not connected"
            await asyncio.wait_for(client.ping(), timeout=self._timeout)
            return True, None
        except Exception as exc:  # noqa: BLE001
            return False, f"{type(exc).__name__}: {exc}"

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
        except Exception as exc:  # noqa: BLE001
            return False, f"{type(exc).__name__}: {exc}"

    async def check_all(self) -> dict:
        """Return a structured snapshot suitable for JSON serialisation."""
        broker_ok, broker_err = await self.check_broker()
        backend_ok, backend_err = await self.check_backend()
        return {
            "ready": broker_ok and backend_ok,
            "broker": {
                "ok": broker_ok,
                "error": broker_err,
            },
            "backend": {
                "ok": backend_ok,
                "error": backend_err,
            },
        }


class HealthServer:
    """Minimal asyncio HTTP server exposing ``/health`` and ``/ready``.

    Same shape as :class:`swarmq.metrics.MetricsServer` — kept separate
    so an operator can run health on a different port (typical pattern:
    health on the same port as the worker proxy, metrics on the
    metrics-scraping port).
    """

    def __init__(
        self,
        checker: HealthChecker,
        *,
        host: str = "0.0.0.0",
        port: int = 8080,
        liveness_started: float | None = None,
    ) -> None:
        self._checker = checker
        self._host = host
        self._port = port
        self._started_at = liveness_started or time.monotonic()
        self._server: asyncio.base_events.Server | None = None

    async def start(self) -> None:
        self._server = await asyncio.start_server(
            self._handle_client, host=self._host, port=self._port,
        )
        logger.info(
            "health_server_started",
            extra={"host": self._host, "port": self._port},
        )

    async def stop(self) -> None:
        if self._server is None:
            return
        self._server.close()
        await self._server.wait_closed()
        self._server = None
        logger.info("health_server_stopped")

    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            request_line = await asyncio.wait_for(
                reader.readline(), timeout=5.0,
            )
            if not request_line:
                writer.close()
                return
            try:
                method, path, _ = request_line.decode("ascii").split(" ", 2)
            except ValueError:
                await self._write(writer, 400, b"bad request\n")
                return
            while True:
                line = await reader.readline()
                if line in (b"\r\n", b"\n", b""):
                    break

            if method != "GET":
                await self._write(writer, 405, b"method not allowed\n")
                return

            base_path = path.split("?")[0]
            if base_path == "/health":
                body = json.dumps({
                    "status": "ok",
                    "uptime_s": round(time.monotonic() - self._started_at, 3),
                }).encode()
                await self._write(writer, 200, body, "application/json")
            elif base_path == "/ready":
                snapshot = await self._checker.check_all()
                status = 200 if snapshot["ready"] else 503
                body = json.dumps(snapshot).encode()
                await self._write(writer, status, body, "application/json")
            else:
                await self._write(writer, 404, b"not found\n")
        except Exception:  # noqa: BLE001
            logger.exception("health_handler_failed")
        finally:
            try:
                await writer.drain()
            except Exception:  # noqa: BLE001
                pass
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:  # noqa: BLE001
                pass

    @staticmethod
    async def _write(
        writer: asyncio.StreamWriter,
        status: int,
        body: bytes,
        content_type: str = "text/plain; charset=utf-8",
    ) -> None:
        reason = {
            200: "OK", 400: "Bad Request", 404: "Not Found",
            405: "Method Not Allowed", 503: "Service Unavailable",
        }.get(status, "Unknown")
        writer.write(
            f"HTTP/1.1 {status} {reason}\r\n"
            f"Content-Type: {content_type}\r\n"
            f"Content-Length: {len(body)}\r\n"
            f"Connection: close\r\n\r\n".encode("ascii")
        )
        writer.write(body)


@asynccontextmanager
async def serve_health(
    checker: HealthChecker,
    *,
    host: str = "0.0.0.0",
    port: int = 8080,
):
    """Async context manager: start HealthServer on enter, stop on exit."""
    server = HealthServer(checker, host=host, port=port)
    await server.start()
    try:
        yield server
    finally:
        await server.stop()
