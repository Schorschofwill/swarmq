"""Stdlib asyncio HTTP helper shared by :mod:`swarmq.metrics` and
:mod:`swarmq.health`.

Neither dependency (``/metrics``, ``/health``, ``/ready``) justifies
pulling in aiohttp or starlette — a 3-endpoint HTTP surface is one
screen of asyncio primitives. The two servers had ~90 LoC of
near-identical request parsing; this module consolidates the shared
bits while leaving per-server routing / body generation to each
caller.

Public contract:

- :class:`AsyncHttpServer` — lifecycle (start/stop) wrapping
  ``asyncio.start_server`` with typed-exception conversion on bind
  failure.
- :func:`parse_http_request` — bounded request-line + header drain
  with timeout + line-count cap + UnicodeDecodeError hardening.
  Returns (method, path) or raises :class:`BadRequest` /
  :class:`RequestTimeout`.
- :func:`write_http_response` — framing helper. Shared reason-code
  dictionary.

The server classes in metrics.py / health.py compose these primitives
with their per-endpoint routing table.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from swarmq.logging import get_logger

logger = get_logger(__name__)

# Shared reason map used by :func:`write_http_response`. Adding a new
# status code once here covers both servers.
_REASON: dict[int, str] = {
    200: "OK",
    400: "Bad Request",
    404: "Not Found",
    405: "Method Not Allowed",
    408: "Request Timeout",
    503: "Service Unavailable",
}

# Max header lines accepted in a request. Prevents slow-header DoS
# by bounding the request-processing loop.
_MAX_HEADER_LINES = 200
# Overall per-request deadline — covers request line + header drain.
_REQUEST_TIMEOUT_S = 5.0


class BadRequest(Exception):
    """Raised by :func:`parse_http_request` when the request-line is
    malformed. Handler should respond 400."""


class RequestTimeout(Exception):
    """Raised by :func:`parse_http_request` when the request deadline
    elapsed. Handler should respond 408."""


async def parse_http_request(
    reader: asyncio.StreamReader,
) -> tuple[str, str] | None:
    """Parse request line + drain headers.

    Returns (method, path) on success, ``None`` when the client closed
    without sending a request. Raises :class:`BadRequest` or
    :class:`RequestTimeout` for recoverable parse / timeout errors so
    the caller can write an appropriate response code.

    The entire operation (request line + header drain) is bounded by
    ``_REQUEST_TIMEOUT_S``. The header loop is additionally capped at
    ``_MAX_HEADER_LINES`` lines — either limit triggers
    :class:`RequestTimeout` / :class:`BadRequest` respectively.
    """
    try:
        async with asyncio.timeout(_REQUEST_TIMEOUT_S):
            # StreamReader.readline() wraps LimitOverrunError in
            # ValueError when the line exceeds the 64 KB default limit.
            # Catch at the readline boundary so the client gets 400 and
            # we log-and-move-on instead of _accept()'s broad except
            # logging a full exception trace.
            try:
                request_line = await reader.readline()
            except ValueError as exc:
                raise BadRequest("request line too long") from exc
            if not request_line:
                return None
            try:
                method, path, _ = request_line.decode("ascii").split(" ", 2)
            except (ValueError, UnicodeDecodeError) as exc:
                raise BadRequest("malformed request line") from exc
            # Drain headers (bounded by outer timeout AND line-count cap).
            for _ in range(_MAX_HEADER_LINES):
                try:
                    line = await reader.readline()
                except ValueError as exc:
                    raise BadRequest("header line too long") from exc
                if line in (b"\r\n", b"\n", b""):
                    break
            else:
                raise BadRequest("too many header lines")
    except TimeoutError as exc:
        raise RequestTimeout("request deadline exceeded") from exc
    return method, path


async def write_http_response(
    writer: asyncio.StreamWriter,
    status: int,
    body: bytes,
    content_type: str = "text/plain; charset=utf-8",
) -> None:
    """Frame and write an HTTP/1.1 response. Always closes the
    connection after (``Connection: close``)."""
    reason = _REASON.get(status, "Unknown")
    writer.write(
        f"HTTP/1.1 {status} {reason}\r\n"
        f"Content-Type: {content_type}\r\n"
        f"Content-Length: {len(body)}\r\n"
        f"Connection: close\r\n\r\n".encode("ascii")
    )
    writer.write(body)


class AsyncHttpServer:
    """Shared start/stop lifecycle for stdlib asyncio HTTP servers.

    Caller supplies the request handler (a coroutine that takes
    ``(method, path, writer)`` and writes the response). This class
    handles the accept loop, request parsing, timeout / bad-request
    responses, and socket cleanup.
    """

    def __init__(
        self,
        handler: Callable[
            [str, str, asyncio.StreamWriter], Awaitable[None],
        ],
        *,
        host: str = "127.0.0.1",
        port: int = 0,
        label: str = "http_server",
    ) -> None:
        self._handler = handler
        self._host = host
        self._port = port
        self._label = label
        self._server: asyncio.base_events.Server | None = None

    async def start(self) -> None:
        try:
            self._server = await asyncio.start_server(
                self._accept, host=self._host, port=self._port,
            )
        except OSError as exc:
            # Port conflict / bind failure surfaces
            # as typed RuntimeError with operational context instead
            # of raw OSError.
            raise RuntimeError(
                f"{self._label} bind failed on {self._host}:{self._port}: "
                f"{exc.strerror or exc}"
            ) from exc
        logger.info(
            "http_server_started",
            server=self._label, host=self._host, port=self._port,
        )

    async def stop(self) -> None:
        if self._server is None:
            return
        self._server.close()
        await self._server.wait_closed()
        self._server = None
        logger.info("http_server_stopped", server=self._label)

    async def _accept(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            try:
                parsed = await parse_http_request(reader)
            except BadRequest:
                await write_http_response(writer, 400, b"bad request\n")
                return
            except RequestTimeout:
                await write_http_response(writer, 408, b"request timeout\n")
                return
            if parsed is None:
                writer.close()
                return
            method, path = parsed
            if method != "GET":
                await write_http_response(
                    writer, 405, b"method not allowed\n",
                )
                return
            await self._handler(method, path, writer)
        except Exception:
            logger.exception("http_server_request_failed", server=self._label)
        finally:
            try:
                await writer.drain()
            except Exception:
                pass
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
