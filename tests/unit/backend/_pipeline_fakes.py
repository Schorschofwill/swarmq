"""Gefälschte valkey-py-Verbindung für ``ScriptPipeline``-Tests: zerlegt die
gepackten RESP-Kommandos und antwortet je Kommando über eine Funktion."""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Callable
from typing import Any
from unittest.mock import MagicMock

from valkey.asyncio.retry import Retry
from valkey.backoff import NoBackoff


def _parse(data: bytes) -> list[list[bytes]]:
    """RESP-Arrays aus Bulk-Strings -> Liste der Kommandos."""
    commands: list[list[bytes]] = []
    pos = 0
    while pos < len(data):
        end = data.index(b"\r\n", pos)
        assert data[pos : pos + 1] == b"*"
        count = int(data[pos + 1 : end])
        pos = end + 2
        argv: list[bytes] = []
        for _ in range(count):
            end = data.index(b"\r\n", pos)
            assert data[pos : pos + 1] == b"$"
            size = int(data[pos + 1 : end])
            pos = end + 2
            argv.append(data[pos : pos + size])
            pos += size + 2
        commands.append(argv)
    return commands


Reply = Callable[[list[bytes]], Any]


class FakeConnection:
    def __init__(self, reply: Reply) -> None:
        self.reply = reply
        self.sent: list[list[list[bytes]]] = []
        self.replies: deque[Any] = deque()
        self.disconnects = 0
        self.retry = Retry(NoBackoff(), 0)
        self.read_gate: asyncio.Event | None = None
        self.sent_event = asyncio.Event()

    async def send_packed_command(self, data: bytes, check_health: bool = True) -> None:
        commands = _parse(data)
        self.sent.append(commands)
        self.sent_event.set()
        self.replies.extend(self.reply(argv) for argv in commands)

    async def read_response(self) -> Any:
        if self.read_gate is not None:
            await self.read_gate.wait()
        reply = self.replies.popleft()
        if isinstance(reply, BaseException):
            raise reply
        return reply

    async def disconnect(self, nowait: bool = False) -> None:
        self.disconnects += 1
        self.replies.clear()


class FakePool:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection
        self.in_use = 0
        self.connect_error: BaseException | None = None

    async def get_connection(self, command_name: str, *keys: Any, **options: Any) -> FakeConnection:
        if self.connect_error is not None:
            raise self.connect_error
        self.in_use += 1
        return self.connection

    async def release(self, connection: FakeConnection) -> None:
        self.in_use -= 1


class FakeScript:
    def __init__(self, sha: str = "a" * 40, direct_error: BaseException | None = None) -> None:
        self.sha = sha
        self.direct_calls: list[tuple[list[str], list[Any]]] = []
        self.direct_error = direct_error

    async def __call__(self, keys: Any = None, args: Any = None, client: Any = None) -> Any:
        self.direct_calls.append((list(keys), list(args)))
        if self.direct_error is not None:
            raise self.direct_error
        return b"direct"


def echo(argv: list[bytes]) -> Any:
    """Antwort = erstes Argument nach den Keys."""
    nkeys = int(argv[2])
    return argv[3 + nkeys]


def fake_client(reply: Reply = echo) -> tuple[MagicMock, FakeConnection, FakePool]:
    connection = FakeConnection(reply)
    pool = FakePool(connection)
    client = MagicMock()
    client.connection_pool = pool
    return client, connection, pool
