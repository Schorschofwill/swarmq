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

PF-16 (b) Variante (c), Nachtrag 0.14 zu E-075: ein Zeitablauf beim TCP-Aufbau
einer Pool-Verbindung wird innerhalb des Verbindungsaufbaus nicht wiederholt
(die Befehls-Retry bleibt); ``connect()`` wiederholt den PING danach bis zur
Gesamtfrist.

# [x] Laufzeit: Verbindung stirbt mitten im GET, TCP läuft in den Zeitablauf -> höchstens 6 TCP-Versuche, nicht 30
# [x] connect() gegen TCP-Zeitablauf: PING-Versuche bis zur Frist (30 s und 600 s), dann "timed out after", ein aclose
# [x] connect(): TCP-Zeitablauf, danach Antwort -> verbunden
# [x] connect(): TCP-Zeitablauf, danach abweisender Port -> keine weitere Schleife, "connect failed"
# [x] Abbruch während der Schleife schließt den eigenen Client (F4-13)
# [x] timing, echtes Blackhole: connect() wartet bis zur Frist
# [x] timing, echtes Blackhole: Laufzeit-GET endet in <= socket_timeout + 5 x connect_timeout + Backoff
# [x] timing, echtes Blackhole: Befehl mit neuer Pool-Verbindung endet nach einem TCP-Versuch
"""

from __future__ import annotations

import asyncio
import contextlib
import errno
import socket
import struct
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any
from unittest.mock import patch

import pytest
import valkey.asyncio.connection
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
# Ein Befehl und seine fünf Wiederholungen.
_COMMAND_ATTEMPTS = len(_COMMAND_BACKOFFS) + 1
_RETRY_ATTEMPTS = len(_COMMAND_BACKOFFS)


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
    ``drop_on_ping`` (Handshake normal, beim PING getrennt), ``drop_on_get`` (beim GET getrennt), ``silent_on_get``
    (verstummt beim GET), ``silent``
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
            elif (command, mode) in {(b"PING", "drop_on_ping"), (b"GET", "drop_on_get")}:
                self._reset(writer)
                return
            elif command == b"GET" and mode == "silent_on_get":
                while await reader.read(1024):
                    pass
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


@dataclass
class _Tcp:
    """TCP-Aufbau an der Grenze ``Connection._connect``: zählt die Versuche;
    mit ``blackhole`` läuft jeder Versuch in den Zeitablauf
    (``socket_connect_timeout``, Host verwirft Pakete) und rückt die
    Fake-Uhr um 5 s vor. ``plan`` legt die ersten Versuche fest:
    ``timeout``, ``refuse`` (Port weist ab) oder ``real``."""

    clock: _FakeClock
    blackhole: bool = False
    attempts: int = 0
    plan: list[str] = field(default_factory=list)


_CONNECT_TIMEOUT_S = 5.0


@pytest.fixture
def tcp(clock: _FakeClock, monkeypatch: pytest.MonkeyPatch) -> _Tcp:
    state = _Tcp(clock)
    original = valkey.asyncio.connection.Connection._connect

    async def connect(self: valkey.asyncio.connection.Connection) -> None:
        state.attempts += 1
        step = state.plan.pop(0) if state.plan else ("timeout" if state.blackhole else "real")
        if step == "timeout":
            state.clock.now += _CONNECT_TIMEOUT_S
            raise TimeoutError
        if step == "refuse":
            raise ConnectionRefusedError(errno.ECONNREFUSED, "Connection refused")
        await original(self)

    monkeypatch.setattr(valkey.asyncio.connection.Connection, "_connect", connect)
    return state


@pytest.mark.entscheidung("E-075")
async def test_a_tcp_timeout_after_a_dropped_command_is_not_retried_inside_the_command_retry(
    fake_valkey: tuple[_FakeValkey, int], tcp: _Tcp
) -> None:
    fake, port = fake_valkey
    fake.plan = ["drop_on_get"]
    backend = _backend(port)
    await backend.connect()
    tcp.attempts = 0
    tcp.blackhole = True

    try:
        with pytest.raises(BackendConnectionError):
            await backend.get_result("partitioned")
    finally:
        await backend.close()

    # Die Befehls-Retry baut fünfmal neu auf, jeder Aufbau ein einziger
    # TCP-Versuch (vorher 5 x 6 = 30).
    assert tcp.attempts <= _COMMAND_ATTEMPTS
    assert tcp.clock.now <= 5 * _CONNECT_TIMEOUT_S + _COMMAND_WINDOW_S


@dataclass
class _BuiltClients:
    """Spion an der Grenze ``Valkey.from_url``: die gebauten Clients und ihre
    ``aclose``-Aufrufe."""

    clients: list[valkey.asyncio.Valkey] = field(default_factory=list)
    closed: list[valkey.asyncio.Valkey] = field(default_factory=list)


@pytest.fixture
def built_clients(monkeypatch: pytest.MonkeyPatch) -> _BuiltClients:
    spy = _BuiltClients()
    from_url = valkey.asyncio.Valkey.from_url

    def build(*args: Any, **kwargs: Any) -> valkey.asyncio.Valkey:
        client: valkey.asyncio.Valkey = from_url(*args, **kwargs)
        aclose = client.aclose

        async def aclose_and_record(*a: Any, **kw: Any) -> None:
            spy.closed.append(client)
            await aclose(*a, **kw)

        client.aclose = aclose_and_record  # type: ignore[method-assign]  # Spion am gebauten Client
        spy.clients.append(client)
        return client

    monkeypatch.setattr(valkey.asyncio.Valkey, "from_url", build)
    return spy


@pytest.mark.entscheidung("E-075")
@pytest.mark.parametrize("deadline_s", [30.0, 600.0])
async def test_connect_retries_the_ping_after_tcp_timeouts_until_the_deadline(
    tcp: _Tcp, built_clients: _BuiltClients, deadline_s: float
) -> None:
    tcp.blackhole = True
    backend = ValkeyBackend("valkey://valkey.invalid:6379/0", eviction_check="off", connect_timeout_s=deadline_s)

    with pytest.raises(BackendConnectionError, match=rf"Valkey connect timed out after {deadline_s}s"):
        await backend.connect()

    # Bis zur Frist, nicht nach 6 Versuchen (rund 33 s); der letzte Versuch
    # beginnt vor der Frist.
    assert deadline_s <= tcp.clock.now < deadline_s + _CONNECT_TIMEOUT_S + 2.0
    assert backend._client is None
    assert built_clients.closed == built_clients.clients
    assert len(built_clients.clients) == 1


@pytest.mark.entscheidung("E-075")
async def test_connect_succeeds_once_tcp_answers_after_timeouts(
    fake_valkey: tuple[_FakeValkey, int], tcp: _Tcp
) -> None:
    _, port = fake_valkey
    tcp.plan = ["timeout"] * 7
    backend = _backend(port, connect_timeout_s=60.0)

    try:
        await backend.connect()
        assert backend._client is not None
    finally:
        await backend.close()

    assert tcp.attempts == 8
    assert tcp.clock.sleeps == [0.1, 0.2, 0.4, 0.8, 1.6, 2.0, 2.0]


@pytest.mark.entscheidung("E-075")
async def test_connect_does_not_loop_when_a_refused_port_follows_a_tcp_timeout(tcp: _Tcp) -> None:
    tcp.plan = ["timeout", "refuse", "refuse", "refuse", "refuse", "refuse", "refuse"]
    backend = ValkeyBackend("valkey://valkey.invalid:6379/0", eviction_check="off", connect_timeout_s=600.0)

    with pytest.raises(BackendConnectionError, match="Valkey connect failed"):
        await backend.connect()

    # Nach dem Zeitablauf ein zweiter PING; dessen abgewiesener Port wird wie
    # bisher innen fünfmal wiederholt, dann endet connect() ohne Schleife.
    assert tcp.attempts == 1 + _COMMAND_ATTEMPTS
    assert tcp.clock.sleeps == [0.1, *_COMMAND_BACKOFFS]


@pytest.mark.entscheidung("E-075")
async def test_cancel_during_the_tcp_timeout_loop_closes_the_own_client(
    tcp: _Tcp, built_clients: _BuiltClients
) -> None:
    tcp.blackhole = True
    backend = ValkeyBackend("valkey://valkey.invalid:6379/0", eviction_check="off", connect_timeout_s=600.0)
    task = asyncio.current_task()
    assert task is not None

    async def cancel_on_third_backoff() -> None:
        if len(tcp.clock.sleeps) == 3:
            task.cancel()
            await asyncio.sleep(0)

    tcp.clock.on_sleep = cancel_on_third_backoff

    with pytest.raises(asyncio.CancelledError):
        await backend.connect()
    task.uncancel()

    assert backend._client is None
    assert len(built_clients.clients) == 1
    assert built_clients.closed == built_clients.clients


@dataclass
class _Blackhole:
    """Echtes SYN-Blackhole auf ``127.0.0.1`` (Spike PF-16 b): ein
    Listen-Socket mit ``listen(0)``; sobald die Füllverbindung in der
    Annahme-Warteschlange liegt, verwirft der Kernel jedes weitere SYN, und
    ``connect`` läuft in ``socket_connect_timeout`` (wie Firewall-``DROP``)."""

    listener: socket.socket
    port: int
    sockets: list[socket.socket] = field(default_factory=list)

    async def accept_one(self, fake: _FakeValkey) -> None:
        """Nimmt genau eine Verbindung an und bedient sie mit *fake*."""
        loop = asyncio.get_running_loop()
        accepted, _ = await loop.sock_accept(self.listener)
        self.sockets.append(accepted)
        reader, writer = await asyncio.open_connection(sock=accepted)
        await fake.handle(reader, writer)

    async def fill(self) -> None:
        """Belegt die Annahme-Warteschlange; danach verwirft der Kernel SYNs."""
        filler = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        filler.setblocking(False)
        self.sockets.append(filler)
        await asyncio.get_running_loop().sock_connect(filler, ("127.0.0.1", self.port))


@pytest.fixture
async def blackhole() -> AsyncIterator[_Blackhole]:
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(0)
    listener.setblocking(False)
    hole = _Blackhole(listener, listener.getsockname()[1])
    try:
        yield hole
    finally:
        for sock in hole.sockets:
            sock.close()
        listener.close()


# Skaliert wie im Spike: Connect-Timeout 0,2 s, socket_timeout 0,4 s, Backoff
# ein Zehntel (0,01..0,16 s, Summe 0,31 s). Vorher endete connect() nach
# 6 x 0,2 + 0,31 = 1,51 s, unabhängig von der Frist.
_SCALED_CONNECT_TIMEOUT_S = 0.2
_SCALED_SOCKET_TIMEOUT_S = 0.4
_SCALED_BACKOFF_BASE_S = 0.005
_SCALED_BACKOFF_CAP_S = 0.2
_SCALED_COMMAND_WINDOW_S = sum(min(_SCALED_BACKOFF_CAP_S, _SCALED_BACKOFF_BASE_S * 2**n) for n in range(1, 6))
# Planungsrauschen gegen die echte Uhr im seriellen ``timing``-Durchgang.
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


# Gegen die echte Uhr und einen echten Kernel-Verwurf: im seriellen
# ``timing``-Durchgang, Laufzeit rund die Frist (2,5 s).
@pytest.mark.timing
@pytest.mark.entscheidung("E-075")
@pytest.mark.usefixtures("scaled_timeouts")
async def test_connect_against_a_blackhole_waits_until_the_deadline(blackhole: _Blackhole) -> None:
    await blackhole.fill()
    deadline_s = 2.5
    backend = _backend(blackhole.port, connect_timeout_s=deadline_s)

    started = time.monotonic()
    with pytest.raises(BackendConnectionError, match=r"Valkey connect timed out after 2\.5s"):
        await backend.connect()
    elapsed = time.monotonic() - started

    assert deadline_s <= elapsed < deadline_s + _SCHEDULING_SLACK_S, (
        f"connect() endete nach {elapsed:.3f} s, erwartet nach der Frist von {deadline_s} s"
    )
    assert backend._client is None


# Gegen die echte Uhr und einen echten Kernel-Verwurf, Laufzeit rund 1,7 s;
# vorher rund 8,3 s (deshalb die längere Testfrist: rot an der Assertion).
@pytest.mark.timing
@pytest.mark.timeout(15)
@pytest.mark.entscheidung("E-075")
@pytest.mark.usefixtures("scaled_timeouts")
async def test_a_command_after_a_partition_ends_within_one_tcp_attempt_per_command_retry(
    blackhole: _Blackhole, tcp_attempts: _TcpAttempts
) -> None:
    fake = _FakeValkey(plan=["silent_on_get"])
    server = asyncio.create_task(blackhole.accept_one(fake))
    backend = _backend(blackhole.port)
    try:
        await backend.connect()
        # Partition: die bestehende Verbindung verstummt beim GET, neue SYNs
        # verwirft der Kernel.
        await blackhole.fill()
        tcp_attempts.count = 0

        started = time.monotonic()
        with pytest.raises(BackendConnectionError):
            await backend.get_result("partitioned")
        elapsed = time.monotonic() - started
    finally:
        await backend.close()
        await fake.close()
        server.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await server

    assert tcp_attempts.count == _RETRY_ATTEMPTS, "TCP-Versuche je Befehl (vorher 5 x 6 = 30)"
    bound = _SCALED_SOCKET_TIMEOUT_S + _RETRY_ATTEMPTS * _SCALED_CONNECT_TIMEOUT_S + _SCALED_COMMAND_WINDOW_S
    assert elapsed <= bound + _SCHEDULING_SLACK_S, (
        f"GET endete nach {elapsed:.3f} s, erwartet höchstens socket_timeout + 5 x Connect-Timeout + Backoff "
        f"= {bound:.2f} s"
    )


# Gegen die echte Uhr und einen echten Kernel-Verwurf, Laufzeit rund 0,2 s
# (vorher 6 x 0,2 + 0,31 = 1,51 s): Der Neuaufbau in ``get_connection`` liegt
# ausserhalb der Befehls-Retry, ein Zeitablauf endet nach einem Versuch.
@pytest.mark.timing
@pytest.mark.entscheidung("E-075")
@pytest.mark.usefixtures("scaled_timeouts")
async def test_a_command_needing_a_new_pool_connection_ends_after_one_tcp_attempt(
    blackhole: _Blackhole, tcp_attempts: _TcpAttempts
) -> None:
    fake = _FakeValkey()
    server = asyncio.create_task(blackhole.accept_one(fake))
    backend = _backend(blackhole.port)
    try:
        await backend.connect()
        await backend.client.connection_pool.disconnect()
        await blackhole.fill()
        tcp_attempts.count = 0

        started = time.monotonic()
        with pytest.raises(BackendConnectionError, match="Timeout connecting to server"):
            await backend.get_result("needs-a-new-connection")
        elapsed = time.monotonic() - started
    finally:
        await backend.close()
        await fake.close()
        server.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await server

    assert tcp_attempts.count == 1
    assert elapsed < _SCALED_CONNECT_TIMEOUT_S + _SCHEDULING_SLACK_S, f"GET endete nach {elapsed:.3f} s"
