"""Chaos: Valkey verstummt hinter dem Proxy (PF-16 c, E-075, Plan 0.14 U3).

Der Fall „stumm“: Toxiproxy nimmt TCP selbst an, lässt aber keine Daten mehr
durch (``timeout``-Toxic mit ``timeout: 0`` auf ``vkey``, upstream und
downstream). Jeder Befehl und jeder Neuaufbau läuft damit in
``socket_timeout``, nicht in ``socket_connect_timeout``. Einen SYN-Verwurf,
den PF-16 (c) eigentlich betrifft, kann Toxiproxy nicht erzeugen; den
schreiben die ``timing``-Tests mit echtem Blackhole in
``tests/unit/backend/test_valkey_handshake_retry.py`` fest.

Regressionswächter, nicht festschreibend für Variante (c): Die Gegenprobe
(``_mark_tcp_connect_timeouts`` aus ``make_marked_connection`` genommen) war
am 2026-10-19 ebenfalls grün (2,72 s, 5 TCP-Versuche, wie mit Wrapper), denn
im Fall „stumm“ gelingt jeder TCP-Aufbau, der Wrapper greift nie. Der Test
bewacht, dass die Grenze bleibt, wie sie vor 0.14 war: höchstens sechs
Befehlsversuche zu je ``socket_timeout`` plus Backoff, und keine
Vervielfachung der TCP-Versuche. Dass er anschlagen kann, zeigte eine
zweite Gegenprobe mit ``_RETRY_ATTEMPTS = 10``: rot mit 10 TCP-Versuchen
und 5,73 s.

Testliste:
# [x] stummes Valkey: get_result endet in <= 6 x socket_timeout + Backoff mit
#     BackendConnectionError, höchstens 6 TCP-Versuche
"""

from __future__ import annotations

import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass

import httpx
import pytest
import valkey.asyncio.connection

from swarmq.backend.valkey import ValkeyBackend
from swarmq.backend.valkey import connection as connection_module
from swarmq.exceptions import BackendConnectionError
from tests.chaos.conftest import ChaosStack

# Start des Chaos-Stacks (rund 10 s) gehört zur Testfrist.
pytestmark = [pytest.mark.timeout(120)]

# Skaliert wie die ``timing``-Tests mit Blackhole: Connect-Timeout 0,2 s,
# socket_timeout 0,4 s, Backoff ein Zehntel (0,01..0,16 s, Summe 0,31 s).
_SCALED_CONNECT_TIMEOUT_S = 0.2
_SCALED_SOCKET_TIMEOUT_S = 0.4
_SCALED_BACKOFF_BASE_S = 0.005
_SCALED_BACKOFF_CAP_S = 0.2
_SCALED_COMMAND_WINDOW_S = sum(min(_SCALED_BACKOFF_CAP_S, _SCALED_BACKOFF_BASE_S * 2**n) for n in range(1, 6))
# Ein Befehl und seine fünf Wiederholungen (``retries=5``).
_COMMAND_ATTEMPTS = 6
# Planungsrauschen und Proxy-Umweg gegen die echte Uhr.
_SCHEDULING_SLACK_S = 1.0


@dataclass
class _TcpAttempts:
    count: int = 0


@pytest.fixture
def tcp_attempts(monkeypatch: pytest.MonkeyPatch) -> _TcpAttempts:
    """Spion an ``Connection._connect``: zählt TCP-Versuche, ändert nichts."""
    attempts = _TcpAttempts()
    original = valkey.asyncio.connection.Connection._connect

    async def counting(self: valkey.asyncio.connection.Connection) -> None:
        attempts.count += 1
        await original(self)

    monkeypatch.setattr(valkey.asyncio.connection.Connection, "_connect", counting)
    return attempts


@pytest.fixture
def scaled_timeouts(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(connection_module, "_SOCKET_CONNECT_TIMEOUT_S", _SCALED_CONNECT_TIMEOUT_S)
    monkeypatch.setattr(connection_module, "_SOCKET_TIMEOUT_S", _SCALED_SOCKET_TIMEOUT_S)
    monkeypatch.setattr(connection_module, "_RETRY_BACKOFF_BASE_S", _SCALED_BACKOFF_BASE_S)
    monkeypatch.setattr(connection_module, "_RETRY_BACKOFF_CAP_S", _SCALED_BACKOFF_CAP_S)


@asynccontextmanager
async def _valkey_goes_silent(stack: ChaosStack) -> AsyncIterator[None]:
    """``timeout``-Toxic mit ``timeout: 0`` in beide Richtungen auf ``vkey``.

    Bestehende und neue Verbindungen bleiben offen, es fließen aber keine
    Daten mehr: der Proxy nimmt TCP an und antwortet nie."""
    names = ("silent-up", "silent-down")
    async with httpx.AsyncClient(base_url=stack.toxi_admin_url, timeout=5.0) as admin:
        try:
            for name, stream in zip(names, ("upstream", "downstream"), strict=True):
                response = await admin.post(
                    "/proxies/vkey/toxics",
                    json={"name": name, "type": "timeout", "stream": stream, "attributes": {"timeout": 0}},
                )
                response.raise_for_status()
            yield
        finally:
            for name in names:
                await admin.delete(f"/proxies/vkey/toxics/{name}")


@pytest.mark.entscheidung("E-075")
@pytest.mark.usefixtures("scaled_timeouts")
async def test_silent_backend_bounds_a_command_without_multiplying_tcp_attempts(
    chaos_stack: ChaosStack, tcp_attempts: _TcpAttempts
) -> None:
    backend = ValkeyBackend(chaos_stack.valkey_url, eviction_check="off")
    try:
        await backend.connect()
        await backend.store_result("silent", b"v", 60_000)
        assert await backend.get_result("silent") == b"v"

        async with _valkey_goes_silent(chaos_stack):
            tcp_attempts.count = 0
            started = time.monotonic()
            with pytest.raises(BackendConnectionError):
                await backend.get_result("silent")
            elapsed = time.monotonic() - started
    finally:
        await backend.close()

    print(f"stummes Valkey: get_result nach {elapsed:.3f} s, {tcp_attempts.count} TCP-Versuche")
    assert tcp_attempts.count <= _COMMAND_ATTEMPTS, (
        f"{tcp_attempts.count} TCP-Versuche, erwartet höchstens einer je Befehlsversuch ({_COMMAND_ATTEMPTS})"
    )
    bound = _COMMAND_ATTEMPTS * _SCALED_SOCKET_TIMEOUT_S + _SCALED_COMMAND_WINDOW_S
    assert elapsed <= bound + _SCHEDULING_SLACK_S, (
        f"get_result endete nach {elapsed:.3f} s, erwartet höchstens 6 x socket_timeout + Backoff = {bound:.2f} s"
    )
