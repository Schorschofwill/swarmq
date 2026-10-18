"""Wiederholpolitik des eigenen Valkey-Clients (F4-14, Testlücke Nr. 4 der
Sichtung D, ``docs/befunde/fehlersuche-runde-4-recherche/3c-sichtung-D.md``).

Der Client wiederholt einen Befehl, dessen Verbindung mitten im Befehl
abreißt, mit ``Retry(ExponentialBackoff(cap=2, base=0.05), retries=5)``. Den
Neuaufbau einer Pool-Verbindung, deren Handshake getrennt wird, wiederholt
swarmq selbst (valkey-py tut es nicht, F4-14), begrenzt durch einen
Zeitdeckel: Wiederholung nur, solange seit dem ersten Fehlschlag weniger als
die nominelle Summe des Befehls-Retry (3,1 s) vergangen ist. Innere
TCP-Wiederholungen und äußere Handshake-Wiederholungen vervielfachen sich
nicht.

Grenzen: ein Fake-RESP-Server auf ``127.0.0.1`` (freier Port, keine
Testdienste); Uhr und Warten der Wiederholungen (``connection._monotonic``,
``connection._sleep``, ``valkey.asyncio.retry.sleep``) durch eine Fake-Uhr
ersetzt, die nicht wirklich wartet.

Testliste:
# [x] Verbindung reißt mitten im Befehl ab -> Befehl wird nach 0,1 s Backoff wiederholt
# [x] Valkey weist TCP ab -> fünf Wiederholungen mit 0,1..1,6 s, keine äußere Wiederholung
# [x] Handshake wird immer getrennt -> Wiederholung bis zum Fenster von 3,1 s, dann BackendConnectionError
# [x] Handshake-Trennungen, dann TCP-Abweisung -> Gesamtdauer unter 2 x Befehlsfenster
# [x] Anmeldefehler im Handshake -> keine Wiederholung
# [x] Handshake hängt -> Abbruch nach ``socket_timeout``, eine einzige Verbindung
# [x] ``close()`` während des Backoffs -> kein weiterer Verbindungsaufbau, Fehler des Siegels
# [x] Anmelde- und Autorisierungsfehler (Fake-Pool) -> keine Wiederholung
# [x] Handshake verschiebt den nächsten Health-Check um ``health_check_interval``
#     (``health_check_interval == 0`` ist über ``_build_client`` nicht erreichbar: fest 30)
"""

from __future__ import annotations

import asyncio
import socket
import struct
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from unittest.mock import patch

import pytest
import valkey.asyncio.retry
from valkey.exceptions import AuthenticationError, AuthorizationError

from swarmq.backend.valkey import ValkeyBackend
from swarmq.backend.valkey import connection as connection_module
from swarmq.exceptions import BackendConnectionError

pytestmark = pytest.mark.timeout(5)

_FROM_URL = "swarmq.backend.valkey.connection.valkey.asyncio.Valkey.from_url"

# Nominelle Wartezeiten des Befehls-Retry: min(2, 0,05 * 2**n) für n = 1..5.
_COMMAND_BACKOFFS = [0.1, 0.2, 0.4, 0.8, 1.6]
_COMMAND_WINDOW_S = sum(_COMMAND_BACKOFFS)


@dataclass
class _FakeClock:
    """Uhr der Wiederholungen: ``sleep`` rückt die Zeit vor, ohne zu warten."""

    now: float = 0.0
    sleeps: list[float] = field(default_factory=list)
    on_sleep: Callable[[], Awaitable[None]] | None = None

    def monotonic(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds
        if self.on_sleep is not None:
            await self.on_sleep()


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> _FakeClock:
    fake = _FakeClock()
    monkeypatch.setattr(connection_module, "_monotonic", fake.monotonic)
    monkeypatch.setattr(connection_module, "_sleep", fake.sleep)
    monkeypatch.setattr(valkey.asyncio.retry, "sleep", fake.sleep)
    return fake


@dataclass
class _FakeValkey:
    """RESP-Server, der jede Verbindung nach dem nächsten Plan-Eintrag
    behandelt: ``serve`` (normal), ``reset`` (RST nach dem ersten Byte),
    ``drop_on_ping`` (Handshake normal, beim PING getrennt), ``silent``
    (nimmt an, antwortet nie), ``noauth`` (Fehlerantwort NOAUTH),
    ``reset_then_refuse`` (RST und danach keine Annahme mehr). Ist der Plan
    leer, bedient er normal."""

    plan: list[str] = field(default_factory=list)
    connections: int = 0
    server: asyncio.Server | None = None
    _writers: list[asyncio.StreamWriter] = field(default_factory=list)

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.connections += 1
        self._writers.append(writer)
        mode = self.plan.pop(0) if self.plan else "serve"
        try:
            if mode in {"reset", "reset_then_refuse"}:
                if mode == "reset_then_refuse" and self.server is not None:
                    self.server.close()
                await reader.read(1)
                self._reset(writer)
                return
            if mode == "silent":
                while await reader.read(1024):
                    pass
                return
            await self._serve(reader, writer, mode)
        except ConnectionError:
            pass
        finally:
            writer.close()

    @staticmethod
    def _reset(writer: asyncio.StreamWriter) -> None:
        sock = writer.get_extra_info("socket")
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        writer.transport.abort()

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, mode: str) -> None:
        while line := await reader.readline():
            if not line.startswith(b"*"):
                continue
            parts = []
            for _ in range(int(line[1:])):
                await reader.readline()
                parts.append((await reader.readline()).strip())
            command = parts[0].upper()
            if mode == "noauth":
                writer.write(b"-NOAUTH Authentication required.\r\n")
            elif command == b"PING" and mode == "drop_on_ping":
                self._reset(writer)
                return
            else:
                writer.write(b"+PONG\r\n" if command == b"PING" else b"+OK\r\n")
            await writer.drain()

    async def close(self) -> None:
        for writer in self._writers:
            writer.close()
        self._writers.clear()


@pytest.fixture
async def fake_valkey() -> AsyncIterator[tuple[_FakeValkey, int]]:
    fake = _FakeValkey()
    server = await asyncio.start_server(fake.handle, "127.0.0.1", 0)
    fake.server = server
    port = server.sockets[0].getsockname()[1]
    try:
        yield fake, port
    finally:
        server.close()
        await fake.close()
        await server.wait_closed()


def _backend(port: int, *, connect_timeout_s: float = 5.0) -> ValkeyBackend:
    return ValkeyBackend(f"valkey://127.0.0.1:{port}/0", eviction_check="off", connect_timeout_s=connect_timeout_s)


async def test_a_command_whose_connection_drops_is_retried_after_backoff(
    fake_valkey: tuple[_FakeValkey, int], clock: _FakeClock
) -> None:
    fake, port = fake_valkey
    fake.plan = ["drop_on_ping"]
    backend = _backend(port)

    try:
        await backend.connect()
    finally:
        await backend.close()

    assert clock.sleeps == [0.1]
    assert fake.connections == 2


@pytest.mark.entscheidung("E-075")
async def test_refused_tcp_is_retried_five_times_without_outer_retry(
    fake_valkey: tuple[_FakeValkey, int], clock: _FakeClock
) -> None:
    fake, port = fake_valkey
    fake.plan = ["reset_then_refuse"]
    backend = _backend(port)

    with pytest.raises(BackendConnectionError, match="Valkey connect failed"):
        await backend.connect()

    # Eine Handshake-Trennung (äußere Wiederholung nach 0,1 s), dann nur die
    # fünf inneren TCP-Wiederholungen des Verbindungsaufbaus.
    assert clock.sleeps == [0.1, *_COMMAND_BACKOFFS]
    assert fake.connections == 1


@pytest.mark.entscheidung("E-075")
async def test_handshake_resets_are_retried_until_the_window_is_spent(
    fake_valkey: tuple[_FakeValkey, int], clock: _FakeClock
) -> None:
    fake, port = fake_valkey
    fake.plan = ["reset"] * 20
    backend = _backend(port)

    with pytest.raises(BackendConnectionError, match="Valkey connect failed"):
        await backend.connect()

    assert clock.sleeps == _COMMAND_BACKOFFS
    assert clock.now == pytest.approx(_COMMAND_WINDOW_S)
    assert fake.connections == len(_COMMAND_BACKOFFS) + 1
    assert backend._client is None


@pytest.mark.entscheidung("E-075")
async def test_handshake_resets_then_refused_tcp_stay_below_twice_the_window(
    fake_valkey: tuple[_FakeValkey, int], clock: _FakeClock
) -> None:
    fake, port = fake_valkey
    fake.plan = ["reset", "reset", "reset", "reset_then_refuse"]
    backend = _backend(port)

    with pytest.raises(BackendConnectionError):
        await backend.connect()

    assert clock.now < 2 * _COMMAND_WINDOW_S
    assert clock.sleeps == [0.1, 0.2, 0.4, 0.8, *_COMMAND_BACKOFFS]


async def test_authentication_error_in_the_handshake_is_not_retried(
    fake_valkey: tuple[_FakeValkey, int], clock: _FakeClock
) -> None:
    fake, port = fake_valkey
    fake.plan = ["noauth"] * 3
    backend = ValkeyBackend(f"valkey://:wrong@127.0.0.1:{port}/0", eviction_check="off", connect_timeout_s=5.0)

    with pytest.raises(BackendConnectionError):
        await backend.connect()

    assert clock.sleeps == []
    assert fake.connections == 1


# Gegen die echte Uhr (``socket_timeout`` 0,05 s ist eine Socket-Frist des
# Treibers, die die Fake-Uhr nicht sieht): im seriellen ``timing``-Durchgang.
@pytest.mark.timing
async def test_hanging_handshake_ends_after_the_socket_timeout_with_a_single_connection(
    fake_valkey: tuple[_FakeValkey, int], monkeypatch: pytest.MonkeyPatch
) -> None:
    fake, port = fake_valkey
    fake.plan = ["silent"] * 20
    monkeypatch.setattr(connection_module, "_SOCKET_TIMEOUT_S", 0.05)
    backend = _backend(port)

    started = time.monotonic()
    with pytest.raises(BackendConnectionError, match="Timeout reading from"):
        await backend.connect()
    elapsed = time.monotonic() - started

    assert fake.connections == 1
    assert elapsed < 1.0, f"connect() endete nach {elapsed:.3f} s, erwartet nach socket_timeout (0,05 s)"


async def test_close_during_the_handshake_backoff_opens_no_further_connection(
    fake_valkey: tuple[_FakeValkey, int], clock: _FakeClock
) -> None:
    fake, port = fake_valkey
    backend = _backend(port)
    await backend.connect()
    # Die Pool-Verbindung trennen; der nächste Befehl baut neu auf, und der
    # Handshake wird getrennt. Im Backoff schließt die Anwendung das Backend.
    await backend.client.connection_pool.disconnect()
    fake.plan = ["reset"] * 20
    connections_before = fake.connections
    clock.on_sleep = backend.close

    with pytest.raises(BackendConnectionError, match="closed"):
        await backend.store_result("after-close", b"x", 60_000, op_timeout=5.0)

    assert fake.connections == connections_before + 1, "nach close() wurde weiter neu verbunden (Siegel umgangen)"


@dataclass
class _HandshakeConnection:
    """Pool-Verbindung an der Grenze ``on_connect``: der Handshake wirft
    *error* oder gelingt."""

    health_check_interval: float
    next_health_check: float = -1.0
    error: Exception | None = None

    async def on_connect(self) -> None:
        if self.error is not None:
            raise self.error

    async def connect(self) -> None:
        await self.on_connect()


class _HandshakePool:
    """Pool-Ersatz: ``get_connection`` baut über ``make_connection`` neu auf
    und führt den Handshake aus (wie valkey-py beim Neuaufbau)."""

    def __init__(self, *, health_check_interval: float, error: Exception | None) -> None:
        self._health_check_interval = health_check_interval
        self._error = error
        self.connections: list[_HandshakeConnection] = []

    def make_connection(self) -> _HandshakeConnection:
        connection = _HandshakeConnection(self._health_check_interval, error=self._error)
        self.connections.append(connection)
        return connection

    async def get_connection(self, *_args: object, **_kwargs: object) -> _HandshakeConnection:
        connection = self.make_connection()
        await connection.connect()
        return connection

    async def release(self, connection: _HandshakeConnection) -> None:
        return None


class _HandshakeClient:
    """Client aus ``Valkey.from_url``: ``ping`` holt eine Pool-Verbindung."""

    def __init__(self, pool: _HandshakePool) -> None:
        self.connection_pool = pool

    async def ping(self) -> bool:
        await self.connection_pool.get_connection("PING")
        return True

    async def aclose(self) -> None:
        return None


# Gleiche Einstellung wie ``_build_client`` (``health_check_interval=30``).
_HEALTH_CHECK_INTERVAL_S = 30.0


# AuthorizationError erzeugt valkey-py nur in der OCSP-Prüfung, nicht über
# eine Fehlerantwort; der Fake-Server kann sie nicht auslösen, die
# Fake-Verbindung schon.
@pytest.mark.parametrize("error_type", [AuthenticationError, AuthorizationError])
async def test_credential_errors_in_the_handshake_are_not_retried(
    error_type: type[Exception], clock: _FakeClock
) -> None:
    pool = _HandshakePool(health_check_interval=_HEALTH_CHECK_INTERVAL_S, error=error_type("not allowed"))
    backend = ValkeyBackend("valkey://valkey.invalid:6379/0", eviction_check="off", connect_timeout_s=5.0)

    with (
        patch(_FROM_URL, return_value=_HandshakeClient(pool)),
        pytest.raises(BackendConnectionError, match="not allowed"),
    ):
        await backend.connect()

    assert clock.sleeps == [], "Anmeldefehler im Handshake wurde wiederholt"
    assert len(pool.connections) == 1


async def test_handshake_postpones_the_next_health_check_by_the_interval() -> None:
    pool = _HandshakePool(health_check_interval=_HEALTH_CHECK_INTERVAL_S, error=None)
    backend = ValkeyBackend("valkey://valkey.invalid:6379/0", eviction_check="off", connect_timeout_s=5.0)
    before = asyncio.get_running_loop().time()

    with patch(_FROM_URL, return_value=_HandshakeClient(pool)):
        await backend.connect()
    try:
        (connection,) = pool.connections
        assert connection.next_health_check >= before + _HEALTH_CHECK_INTERVAL_S
    finally:
        await backend.close()
