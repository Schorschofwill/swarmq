"""Unit tests for R5.2 health + readiness endpoints."""

from __future__ import annotations

import asyncio
import json
import socket

from swarmq.health import HealthChecker, serve_health
from tests.support.inmemory import FakeClock, InMemoryBackend, InMemoryBroker


class TestSanitiseError:
    """ce-review T-04 / SEC-F1 — edge cases in HealthChecker._sanitise_error."""

    def test_empty_message_returns_class_name(self):
        exc = TimeoutError()
        assert HealthChecker._sanitise_error(exc) == "TimeoutError"

    def test_url_like_message_collapses_to_class_name(self):
        exc = ConnectionError("amqp://user:pw@host/vhost")
        assert HealthChecker._sanitise_error(exc) == "ConnectionError"

    def test_at_symbol_collapses_to_class_name(self):
        # Could be embedded credentials; conservative collapse.
        exc = ConnectionError("user@host")
        assert HealthChecker._sanitise_error(exc) == "ConnectionError"

    def test_long_message_collapses_to_class_name(self):
        exc = ConnectionError("x" * 121)
        assert HealthChecker._sanitise_error(exc) == "ConnectionError"

    def test_host_port_redacted(self):
        exc = ConnectionError("connecting to localhost:6379")
        result = HealthChecker._sanitise_error(exc)
        assert "localhost:6379" not in result
        assert "<redacted>" in result

    def test_ipv6_bracket_host_port_redacted(self):
        """ce-review SEC-F1 — IPv6 bracket notation must be redacted."""
        exc = ConnectionError("Error connecting to [::1]:6379")
        result = HealthChecker._sanitise_error(exc)
        assert "[::1]:6379" not in result
        assert "<redacted>" in result

    def test_ipv6_full_address_redacted(self):
        exc = ConnectionError("tried [2001:db8::1]:8080 and failed")
        result = HealthChecker._sanitise_error(exc)
        assert "2001:db8::1" not in result

    def test_short_message_without_leak_preserved(self):
        exc = ValueError("bad config")
        assert HealthChecker._sanitise_error(exc) == "ValueError: bad config"


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


async def _http_get(host: str, port: int, path: str) -> tuple[int, dict, bytes]:
    reader, writer = await asyncio.open_connection(host, port)
    writer.write(f"GET {path} HTTP/1.1\r\nHost: x\r\n\r\n".encode())
    await writer.drain()
    status_line = await reader.readline()
    status_code = int(status_line.split()[1])
    headers: dict = {}
    while True:
        line = await reader.readline()
        if line in (b"\r\n", b""):
            break
        k, _, v = line.decode().partition(":")
        headers[k.strip().lower()] = v.strip()
    body = b""
    if "content-length" in headers:
        body = await reader.readexactly(int(headers["content-length"]))
    writer.close()
    await writer.wait_closed()
    return status_code, headers, body


# ---------------------------------------------------------------------------
# HealthChecker
# ---------------------------------------------------------------------------


class TestHealthChecker:
    """Seit Plan U9 prüft Health über die Vertragsmethode ``ping`` — gegen den
    In-Memory-Stub testbar, kein ``_client``/``_connection``-Zugriff."""

    @staticmethod
    async def _connected_stubs():
        clock = FakeClock()
        broker = InMemoryBroker(clock)
        backend = InMemoryBackend(clock)
        await broker.connect()
        await backend.connect()
        return broker, backend

    async def test_check_backend_ok_when_ping_succeeds(self):
        broker, backend = await self._connected_stubs()
        ok, err = await HealthChecker(broker, backend).check_backend()
        assert (ok, err) == (True, None)

    async def test_check_backend_fail_before_connect(self):
        backend = InMemoryBackend(FakeClock())
        broker, _ = await self._connected_stubs()
        ok, err = await HealthChecker(broker, backend).check_backend()
        assert ok is False
        assert err is not None and "not connected" in err

    async def test_check_backend_fail_on_timeout(self):
        broker, backend = await self._connected_stubs()

        async def hang():
            await asyncio.sleep(10)

        backend.ping = hang
        ok, err = await HealthChecker(broker, backend, timeout_s=0.1).check_backend()
        assert ok is False
        assert err is not None and "TimeoutError" in err

    async def test_check_broker_fail_when_closed(self):
        broker, backend = await self._connected_stubs()
        await broker.close()
        ok, err = await HealthChecker(broker, backend).check_broker()
        assert ok is False
        assert err is not None

    async def test_check_all_combines_results(self):
        broker, backend = await self._connected_stubs()
        snapshot = await HealthChecker(broker, backend).check_all()
        assert snapshot["ready"] is True
        assert snapshot["broker"]["ok"] is True
        assert snapshot["backend"]["ok"] is True

    async def test_check_all_not_ready_when_one_fails(self):
        broker, backend = await self._connected_stubs()
        await backend.close()
        snapshot = await HealthChecker(broker, backend).check_all()
        assert snapshot["ready"] is False
        assert snapshot["backend"]["ok"] is False
        assert snapshot["broker"]["ok"] is True


class TestHealthServer:
    async def _make_checker(self, *, broker_ok: bool, backend_ok: bool):
        clock = FakeClock()
        broker = InMemoryBroker(clock)
        backend = InMemoryBackend(clock)
        if broker_ok:
            await broker.connect()
        if backend_ok:
            await backend.connect()
        return HealthChecker(broker, backend)

    async def test_health_endpoint_returns_200(self):
        checker = await self._make_checker(broker_ok=True, backend_ok=True)
        port = _free_port()
        async with serve_health(checker, host="127.0.0.1", port=port):
            await asyncio.sleep(0.05)
            status, _headers, body = await _http_get("127.0.0.1", port, "/health")
            assert status == 200
            payload = json.loads(body)
            assert payload["status"] == "ok"
            assert "uptime_s" in payload

    async def test_ready_endpoint_returns_200_when_all_ok(self):
        checker = await self._make_checker(broker_ok=True, backend_ok=True)
        port = _free_port()
        async with serve_health(checker, host="127.0.0.1", port=port):
            await asyncio.sleep(0.05)
            status, _headers, body = await _http_get("127.0.0.1", port, "/ready")
            assert status == 200
            payload = json.loads(body)
            assert payload["ready"] is True

    async def test_ready_endpoint_returns_503_when_backend_down(self):
        checker = await self._make_checker(broker_ok=True, backend_ok=False)
        port = _free_port()
        async with serve_health(checker, host="127.0.0.1", port=port):
            await asyncio.sleep(0.05)
            status, _headers, body = await _http_get("127.0.0.1", port, "/ready")
            assert status == 503
            payload = json.loads(body)
            assert payload["ready"] is False
            assert payload["backend"]["ok"] is False
            assert payload["broker"]["ok"] is True

    async def test_unknown_path_returns_404(self):
        checker = await self._make_checker(broker_ok=True, backend_ok=True)
        port = _free_port()
        async with serve_health(checker, host="127.0.0.1", port=port):
            await asyncio.sleep(0.05)
            status, _, _ = await _http_get("127.0.0.1", port, "/oops")
            assert status == 404
