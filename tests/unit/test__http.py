"""Unit tests for the shared stdlib HTTP helper (ce-review PS-01 / T-01)."""

# Test list (Plan 2026-10-06-0831, U9, Logging):
# 1. Start loggt ``http_server_started`` mit ``server`` (Label), ``host``, ``port``.
# 2. Stop loggt ``http_server_stopped`` mit ``server``.
# 3. Handler-Fehler loggt ``http_server_request_failed`` auf ERROR mit
#    ``server``.

from __future__ import annotations

import asyncio
import socket
from unittest.mock import AsyncMock

import pytest

from swarmq._http import (
    AsyncHttpServer,
    BadRequest,
    RequestTimeout,
    parse_http_request,
    write_http_response,
)


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port: int = s.getsockname()[1]
    s.close()
    return port


async def _http_get(host: str, port: int, path: str) -> tuple[int, dict[str, str], bytes]:
    reader, writer = await asyncio.open_connection(host, port)
    writer.write(f"GET {path} HTTP/1.1\r\nHost: x\r\n\r\n".encode())
    await writer.drain()
    status_line = await reader.readline()
    status = int(status_line.split()[1])
    headers: dict[str, str] = {}
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
    return status, headers, body


class TestParseHttpRequest:
    async def test_valid_request_returns_method_and_path(self):
        reader = asyncio.StreamReader()
        reader.feed_data(b"GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n")
        reader.feed_eof()
        result = await parse_http_request(reader)
        assert result == ("GET", "/metrics")

    async def test_empty_read_returns_none(self):
        reader = asyncio.StreamReader()
        reader.feed_eof()
        result = await parse_http_request(reader)
        assert result is None

    async def test_malformed_request_line_raises_bad_request(self):
        reader = asyncio.StreamReader()
        reader.feed_data(b"NOTHTTP\r\n\r\n")
        reader.feed_eof()
        with pytest.raises(BadRequest, match="malformed"):
            await parse_http_request(reader)

    async def test_binary_request_line_raises_bad_request(self):
        """Binary garbage (TLS probe on a plaintext port) must surface
        as BadRequest, not UnicodeDecodeError (ce-review SEC-003)."""
        reader = asyncio.StreamReader()
        reader.feed_data(b"\xff\xfe\x00\x01 binary\r\n\r\n")
        reader.feed_eof()
        with pytest.raises(BadRequest):
            await parse_http_request(reader)

    async def test_too_many_header_lines_raises_bad_request(self):
        reader = asyncio.StreamReader()
        reader.feed_data(b"GET / HTTP/1.1\r\n")
        # 300 header lines — exceeds the 200 cap.
        for i in range(300):
            reader.feed_data(f"H{i}: v\r\n".encode())
        # No terminating blank line so the loop must hit the cap.
        reader.feed_data(b"X" * 10)
        reader.feed_eof()
        with pytest.raises(BadRequest, match="too many"):
            await parse_http_request(reader)

    async def test_overlong_request_line_raises_bad_request(self):
        """Eine Zeile über dem Puffer-Limit des Readers wird 400, kein
        ValueError aus readline (Härtung gegen übergroße Anfragen)."""
        reader = asyncio.StreamReader(limit=64)
        reader.feed_data(b"GET /" + b"a" * 200 + b" HTTP/1.1\r\n\r\n")
        reader.feed_eof()
        with pytest.raises(BadRequest, match="request line too long"):
            await parse_http_request(reader)

    async def test_overlong_header_line_raises_bad_request(self):
        reader = asyncio.StreamReader(limit=64)
        reader.feed_data(b"GET / HTTP/1.1\r\nX-Big: " + b"v" * 200 + b"\r\n\r\n")
        reader.feed_eof()
        with pytest.raises(BadRequest, match="header line too long"):
            await parse_http_request(reader)

    async def test_stalled_client_raises_request_timeout(self, monkeypatch: pytest.MonkeyPatch):
        """Ein Client, der die Kopfzeilen nie beendet, hält den Server nicht
        fest: Nach der Frist kommt RequestTimeout (Slow-Header-Schutz)."""
        monkeypatch.setattr("swarmq._http._REQUEST_TIMEOUT_S", 0.01)
        reader = asyncio.StreamReader()
        reader.feed_data(b"GET / HTTP/1.1\r\nHost: x\r\n")
        with pytest.raises(RequestTimeout, match="request deadline exceeded"):
            await parse_http_request(reader)


class TestWriteHttpResponse:
    async def test_framing_contains_status_and_content_length(self):
        writer = AsyncMock(spec=asyncio.StreamWriter)
        writer.write = lambda data: None
        written: list[bytes] = []
        writer.write = written.append
        await write_http_response(writer, 200, b"hello", "text/plain")
        full = b"".join(written)
        assert full.startswith(b"HTTP/1.1 200 OK\r\n")
        assert b"Content-Length: 5\r\n" in full
        assert b"Connection: close\r\n" in full
        assert full.endswith(b"\r\n\r\nhello")

    async def test_status_408_uses_request_timeout_reason(self):
        writer = AsyncMock(spec=asyncio.StreamWriter)
        written: list[bytes] = []
        writer.write = written.append
        await write_http_response(writer, 408, b"")
        assert b"HTTP/1.1 408 Request Timeout\r\n" in b"".join(written)

    async def test_unknown_status_falls_back(self):
        writer = AsyncMock(spec=asyncio.StreamWriter)
        written: list[bytes] = []
        writer.write = written.append
        await write_http_response(writer, 599, b"")
        assert b"HTTP/1.1 599 Unknown\r\n" in b"".join(written)


class TestAsyncHttpServer:
    async def test_bind_failure_surfaces_as_runtime_error(self):
        """Two servers on the same port — second must raise RuntimeError
        with context, not bare OSError (ce-review REL-01)."""
        port = _free_port()

        async def _noop_handler(method, path, writer):
            await write_http_response(writer, 200, b"")

        a = AsyncHttpServer(
            _noop_handler,
            host="127.0.0.1",
            port=port,
            label="test_a",
        )
        b = AsyncHttpServer(
            _noop_handler,
            host="127.0.0.1",
            port=port,
            label="test_b",
        )
        await a.start()
        try:
            with pytest.raises(RuntimeError, match="test_b bind failed"):
                await b.start()
        finally:
            await a.stop()

    async def test_malformed_request_returns_400(self):
        port = _free_port()

        async def _handler(method, path, writer):
            await write_http_response(writer, 200, b"ok")

        server = AsyncHttpServer(_handler, host="127.0.0.1", port=port)
        await server.start()
        try:
            await asyncio.sleep(0.05)
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            # Missing HTTP version.
            writer.write(b"GETBROKEN\r\n\r\n")
            await writer.drain()
            status_line = await reader.readline()
            assert status_line.startswith(b"HTTP/1.1 400 "), status_line
            writer.close()
            await writer.wait_closed()
        finally:
            await server.stop()

    async def test_stalled_request_returns_408(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setattr("swarmq._http._REQUEST_TIMEOUT_S", 0.01)
        handler = AsyncMock()
        port = _free_port()
        server = AsyncHttpServer(handler, host="127.0.0.1", port=port)
        await server.start()
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.write(b"GET / HTTP/1.1\r\n")
            await writer.drain()
            status_line = await reader.readline()
            assert status_line.startswith(b"HTTP/1.1 408 "), status_line
            handler.assert_not_awaited()
            writer.close()
            await writer.wait_closed()
        finally:
            await server.stop()

    async def test_client_closing_without_request_gets_no_response(self, swarmq_logs):
        handler = AsyncMock()
        port = _free_port()
        server = AsyncHttpServer(handler, host="127.0.0.1", port=port)
        await server.start()
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.write_eof()
            assert await reader.read() == b""
            handler.assert_not_awaited()
            # Ein stiller Verbindungsabbau ist kein Fehler (Lastbalancer-Proben).
            assert [e for e in swarmq_logs if e["log_level"] == "error"] == []
            writer.close()
            await writer.wait_closed()
        finally:
            await server.stop()

    async def test_non_get_method_returns_405(self):
        port = _free_port()

        async def _handler(method, path, writer):
            await write_http_response(writer, 200, b"ok")

        server = AsyncHttpServer(_handler, host="127.0.0.1", port=port)
        await server.start()
        try:
            await asyncio.sleep(0.05)
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.write(b"POST / HTTP/1.1\r\nHost: x\r\n\r\n")
            await writer.drain()
            status_line = await reader.readline()
            assert status_line.startswith(b"HTTP/1.1 405 "), status_line
            writer.close()
            await writer.wait_closed()
        finally:
            await server.stop()

    async def test_handler_exception_closes_without_crashing(self):
        port = _free_port()

        async def _raising_handler(method, path, writer):
            raise RuntimeError("boom")

        server = AsyncHttpServer(_raising_handler, host="127.0.0.1", port=port)
        await server.start()
        try:
            await asyncio.sleep(0.05)
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.write(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n")
            await writer.drain()
            # Connection closes without a response line.
            await reader.read(10)
            writer.close()
            await writer.wait_closed()
            # Server should still be responsive on the next request.
            reader2, writer2 = await asyncio.open_connection(
                "127.0.0.1",
                port,
            )
            writer2.write(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n")
            await writer2.drain()
            await reader2.read(10)
            writer2.close()
            await writer2.wait_closed()
        finally:
            await server.stop()


class TestAsyncHttpServerLogging:
    async def test_start_logs_server_host_and_port(self, swarmq_logs):
        port = _free_port()
        server = AsyncHttpServer(
            AsyncMock(),
            host="127.0.0.1",
            port=port,
            label="metrics_server",
        )
        await server.start()
        await server.stop()
        [entry] = [e for e in swarmq_logs if e["event"] == "http_server_started"]
        assert entry["log_level"] == "info"
        assert entry["server"] == "metrics_server"
        assert entry["host"] == "127.0.0.1"
        assert entry["port"] == port

    async def test_stop_logs_server(self, swarmq_logs):
        server = AsyncHttpServer(
            AsyncMock(),
            host="127.0.0.1",
            port=_free_port(),
            label="health_server",
        )
        await server.start()
        await server.stop()
        [entry] = [e for e in swarmq_logs if e["event"] == "http_server_stopped"]
        assert entry["log_level"] == "info"
        assert entry["server"] == "health_server"

    async def test_handler_exception_logs_request_failed_as_error(self, swarmq_logs):
        port = _free_port()
        server = AsyncHttpServer(
            AsyncMock(side_effect=RuntimeError("boom")),
            host="127.0.0.1",
            port=port,
            label="metrics_server",
        )
        await server.start()
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.write(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n")
            await writer.drain()
            await reader.read(10)
            writer.close()
            await writer.wait_closed()
        finally:
            await server.stop()
        [entry] = [e for e in swarmq_logs if e["event"] == "http_server_request_failed"]
        assert entry["log_level"] == "error"
        assert entry["server"] == "metrics_server"
