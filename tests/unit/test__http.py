"""Unit tests for the shared stdlib HTTP helper (ce-review PS-01 / T-01)."""

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
    port = s.getsockname()[1]
    s.close()
    return port


async def _http_get(host: str, port: int, path: str) -> tuple[int, dict, bytes]:
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
            _noop_handler, host="127.0.0.1", port=port, label="test_a",
        )
        b = AsyncHttpServer(
            _noop_handler, host="127.0.0.1", port=port, label="test_b",
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
            data = await reader.read(10)
            writer.close()
            await writer.wait_closed()
            # Server should still be responsive on the next request.
            reader2, writer2 = await asyncio.open_connection(
                "127.0.0.1", port,
            )
            writer2.write(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n")
            await writer2.drain()
            data = await reader2.read(10)
            writer2.close()
            await writer2.wait_closed()
        finally:
            await server.stop()
