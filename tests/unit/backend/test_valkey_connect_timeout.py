"""E-075: ``ValkeyBackend.connect()`` führt den ``PING`` beim Verbinden unter
einer Gesamtfrist aus und endet nach ihr mit ``BackendConnectionError``.

Register: ``compound-packs/swarmq-entscheidungen/e075-backend-verbindungsaufbau-gesamtfrist.md``.

Testliste:
# [x] PING hängt: nach ``connect_timeout_s`` ``BackendConnectionError`` mit eigenem Text, kein Client
# [x] echter Socket: Server nimmt an und antwortet nie -> Abbruch nach der Frist, nicht nach ``socket_timeout``
# [x] Vorgabe 30 s ohne Parameter und ohne Umgebung
# [x] ``SWARMQ_BACKEND_CONNECT_TIMEOUT_S`` gilt für ein Backend ohne Parameter (CLI, Producer)
# [x] ungültige Umgebung (``inf``, ``0``, ``abc``, über ``TIMEOUT_MAX``) -> ``ConfigurationError``
# [x] ungültiger Parameter -> ``ConfigurationError``
"""

from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import AsyncIterator
from unittest.mock import AsyncMock, Mock, patch

import pytest

from swarmq.backend.valkey import ValkeyBackend
from swarmq.exceptions import BackendConnectionError, ConfigurationError

pytestmark = [pytest.mark.timeout(5), pytest.mark.entscheidung("E-075")]

_FROM_URL = "swarmq.backend.valkey.connection.valkey.asyncio.Valkey.from_url"


def _hanging_client() -> AsyncMock:
    """Client, dessen ``PING`` nie antwortet (Host verwirft Pakete)."""
    client = AsyncMock()
    client.register_script = Mock()

    async def never_answers() -> bool:
        await asyncio.Event().wait()
        return True

    client.ping.side_effect = never_answers
    return client


async def test_hanging_ping_ends_after_the_connect_deadline() -> None:
    backend = ValkeyBackend("valkey://valkey.invalid:6379/0", eviction_check="off", connect_timeout_s=0.01)
    client = _hanging_client()

    with (
        patch(_FROM_URL, return_value=client),
        pytest.raises(BackendConnectionError, match=r"Valkey connect timed out after 0\.01s"),
    ):
        await backend.connect()

    assert backend._client is None
    client.aclose.assert_awaited_once()


def test_default_connect_deadline_is_thirty_seconds(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SWARMQ_BACKEND_CONNECT_TIMEOUT_S", raising=False)

    assert ValkeyBackend("valkey://valkey.invalid:6379/0").connect_timeout_s == 30.0


def test_environment_sets_the_deadline_of_a_backend_without_parameter(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SWARMQ_BACKEND_CONNECT_TIMEOUT_S", "7.5")

    assert ValkeyBackend("valkey://valkey.invalid:6379/0").connect_timeout_s == 7.5


@pytest.mark.parametrize("raw", ["inf", "nan", "0", "-1", "abc", str(threading.TIMEOUT_MAX * 2)])
def test_invalid_environment_deadline_is_a_configuration_error(monkeypatch: pytest.MonkeyPatch, raw: str) -> None:
    monkeypatch.setenv("SWARMQ_BACKEND_CONNECT_TIMEOUT_S", raw)

    with pytest.raises(ConfigurationError, match="SWARMQ_BACKEND_CONNECT_TIMEOUT_S"):
        ValkeyBackend("valkey://valkey.invalid:6379/0")


@pytest.mark.parametrize("seconds", [float("inf"), float("nan"), 0.0, -1.0, threading.TIMEOUT_MAX * 2])
def test_invalid_deadline_parameter_is_a_configuration_error(seconds: float) -> None:
    with pytest.raises(ConfigurationError, match="connect_timeout_s"):
        ValkeyBackend("valkey://valkey.invalid:6379/0", connect_timeout_s=seconds)


@pytest.fixture
async def silent_server() -> AsyncIterator[int]:
    """Nimmt Verbindungen an, liest mit und antwortet nie (angehaltener
    Server, Host hinter einer Firewall, die nur Antworten verwirft)."""
    writers: list[asyncio.StreamWriter] = []

    async def swallow(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        writers.append(writer)
        while await reader.read(1024):
            pass

    server = await asyncio.start_server(swallow, "127.0.0.1", 0)
    try:
        yield server.sockets[0].getsockname()[1]
    finally:
        server.close()
        for writer in writers:
            writer.close()
        await server.wait_closed()


# Über 100 ms: die Frist läuft gegen einen echten Socket ab (0,2 s); kürzer
# ginge nur auf Kosten der Unterscheidung vom Planungsrauschen.
@pytest.mark.timing
async def test_silent_server_ends_connect_after_the_deadline(silent_server: int) -> None:
    backend = ValkeyBackend(f"valkey://127.0.0.1:{silent_server}/0", eviction_check="off", connect_timeout_s=0.2)

    started = time.monotonic()
    with pytest.raises(BackendConnectionError, match=r"timed out after 0\.2s"):
        await backend.connect()
    elapsed = time.monotonic() - started

    assert 0.2 <= elapsed < 2.0, f"connect() endete nach {elapsed:.3f} s, erwartet nach der Frist von 0,2 s"
    assert backend._client is None
