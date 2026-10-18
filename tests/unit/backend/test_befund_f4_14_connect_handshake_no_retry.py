"""Nachweis F4-14: Der Valkey-Client wiederholt einen Befehl nicht, wenn die
Pool-Verbindung tot ist und der sofortige Neuaufbau im Handshake getrennt
wird.

Register: docs/befunde/fehlersuche-runde-4-2026-10.md (F4-14; Verdacht
R4I-01, Teil a; Gegenprüfung ``6-gegenpruefung-I.md``). Beobachtet in den
Loadsuite-Läufen ``stoerung-smoke-20261017T132823`` und
``stoerung-1h-20261017T135831`` (Fenster ``valkey_restart`` mit Persistenz,
Backlog B26): Valkey war nach 0,8 bzw. 1,2 s zurück, das Speichern scheiterte
nach 1–3 ms, sechs erfolgreiche Tasks liefen komplett erneut.

valkey-py 6.1.1 holt die Verbindung in ``execute_command`` mit
``pool.get_connection`` **vor** ``conn.retry.call_with_retry``.
``ensure_connection`` erkennt die tote Pool-Verbindung, baut neu auf, und
``connect()`` wiederholt nur den TCP-Aufbau; der Handshake (``on_connect``)
liegt außerhalb jeder Wiederholung. Nimmt ein Zwischenglied (Toxiproxy,
Sidecar, HAProxy) die Verbindung an und trennt sie, weil Valkey gerade neu
startet, kommt ``BackendConnectionError`` sofort, das Wiederholfenster des
Clients (``Retry(ExponentialBackoff(cap=2, base=0.05), retries=5)``,
rund 3,1 s) beginnt nie.

Erwartet: ``website/reference/errors.md:198-201`` („Runtime connection losses
are recovered automatically by the valkey-py reconnect loop — this exception
fires only when the retry window is exhausted.“); Kommentar
``backend/valkey/connection.py:241-244`` (Befehls-Wiederholung bei
vorübergehenden Transportfehlern); Spike
``docs/solutions/spikes/2026-04-17-toxiproxy-reconnect-spike.md:44-50``.

Grenze: ein Fake-RESP-Server auf ``127.0.0.1`` (freier Port, keine
Testdienste). Die erste Verbindung bedient er normal; der Test lässt ihn sie
per FIN schließen (wie Valkey beim Shutdown); ein PING über eine zweite
Verbindung stellt sicher, dass der Client das FIN gesehen hat. Die nächsten zwei Verbindungen
nimmt er an und setzt sie nach dem ersten Handshake-Byte zurück (RST, wie
Toxiproxy ohne Upstream), danach bedient er wieder normal. Der Ablauf hängt an
der Zahl der Verbindungen, nicht an der Uhr. Der FIN-Fall scheitert auch mit
dem Python-Parser (libvalkey ist im Projekt nicht installiert; mit libvalkey
scheitert zusätzlich der RST-Fall, siehe Gegenprüfung).

Laufzeit: heute rund 2 ms (der Fehler kommt sofort). Nach dem Fix wartet der
Client den Backoff zweier Wiederholungen ab (0,1 + 0,2 s), der Test liegt dann
über 100 ms; er läuft deshalb im seriellen ``timing``-Durchgang. Ein kürzerer
Ablauf ginge nur mit gekürztem Backoff, also an der Zusage vorbei.

Testliste:
# [x] Pool-Verbindung per FIN getrennt, zwei Neuaufbauten im Handshake zurückgesetzt, dann Valkey zurück
#     -> store_result gelingt innerhalb des Wiederholfensters
"""

from __future__ import annotations

import asyncio
import socket
import struct
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass, field

import pytest

from swarmq.backend.valkey import ValkeyBackend
from swarmq.exceptions import BackendConnectionError

pytestmark = [pytest.mark.timeout(10), pytest.mark.timing]

_RESETS_BEFORE_RECOVERY = 2


@dataclass
class _FakeValkey:
    """RESP-Server mit drei Phasen: normal, Neuaufbau zurücksetzen, normal."""

    resets_left: int = 0
    connections: int = 0
    resets: int = 0
    _served: list[asyncio.StreamWriter] = field(default_factory=list)

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.connections += 1
        if self.resets_left > 0:
            self.resets_left -= 1
            self.resets += 1
            await reader.read(1)
            sock = writer.get_extra_info("socket")
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            writer.transport.abort()
            return
        self._served.append(writer)
        try:
            await self._serve(reader, writer)
        except ConnectionError:
            pass
        finally:
            writer.close()

    @staticmethod
    async def _serve(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        while line := await reader.readline():
            if not line.startswith(b"*"):
                continue
            parts = []
            for _ in range(int(line[1:])):
                await reader.readline()
                parts.append((await reader.readline()).strip())
            writer.write(b"+PONG\r\n" if parts[0].upper() == b"PING" else b"+OK\r\n")
            await writer.drain()

    async def close_served_connections(self) -> None:
        """Schließt die bedienten Verbindungen per FIN (Valkey-Shutdown)."""
        for writer in self._served:
            writer.close()
            await writer.wait_closed()
        self._served.clear()


async def _round_trip(port: int) -> None:
    """Barriere: ein PING über eine eigene Verbindung. Bis seine Antwort da ist,
    hat die Ereignisschleife das FIN der geschlossenen Pool-Verbindung
    verarbeitet (der Kernel hat es vor dem PING zugestellt); erst dann sieht
    der Client sie wie im Betrieb als tot (``StreamReader.at_eof``)."""
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    try:
        writer.write(b"*1\r\n$4\r\nPING\r\n")
        await writer.drain()
        assert await reader.readline() == b"+PONG\r\n"
    finally:
        writer.close()
        await writer.wait_closed()


@pytest.fixture
async def fake_valkey() -> AsyncIterator[tuple[_FakeValkey, int]]:
    fake = _FakeValkey()
    server = await asyncio.start_server(fake.handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        yield fake, port
    finally:
        server.close()
        await fake.close_served_connections()
        await server.wait_closed()


@pytest.mark.befund("F4-14")
@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="F4-14: offen - Neuaufbau im Handshake liegt außerhalb der Befehls-Wiederholung",
)
async def test_store_result_retries_when_the_reconnect_is_reset_in_the_handshake(
    fake_valkey: tuple[_FakeValkey, int],
) -> None:
    fake, port = fake_valkey
    backend = ValkeyBackend(f"valkey://127.0.0.1:{port}/0", eviction_check="off")
    await backend.connect()
    try:
        await backend.store_result("before", b"x", 60_000, op_timeout=5.0)
        await fake.close_served_connections()
        await _round_trip(port)
        fake.resets_left = _RESETS_BEFORE_RECOVERY

        started = time.monotonic()
        try:
            await backend.store_result("after", b"x", 60_000, op_timeout=5.0)
        except BackendConnectionError as exc:
            elapsed = time.monotonic() - started
            raise AssertionError(
                f"store_result scheitert nach {elapsed:.3f} s mit {exc!s} nach {fake.resets} zurückgesetzten "
                f"Neuaufbau(ten); erwartet: Wiederholung im Fenster von rund 3,1 s, Valkey ist nach "
                f"{_RESETS_BEFORE_RECOVERY} Neuaufbauten zurück (errors.md:198-201: 'fires only when the "
                "retry window is exhausted')"
            ) from exc
        assert fake.resets == _RESETS_BEFORE_RECOVERY
    finally:
        await backend.close()
