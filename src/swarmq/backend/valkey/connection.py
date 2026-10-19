"""Valkey-Verbindung: Client-Aufbau, Retry, Socket-Timeouts, ``ping`` und der
gebündelte ``op_timeout``-Wrapper (Plan U17; Entwurf §5).

``ValkeyConnection`` hält den Client; ``results.py`` und (ab U18) die
Koordinations-Stores bauen darauf auf.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from typing import Any

import valkey.asyncio
from valkey.asyncio.retry import Retry
from valkey.backoff import ExponentialBackoff
from valkey.exceptions import (
    AuthenticationError,
    AuthorizationError,
    ClusterDownError,
    OutOfMemoryError,
    ReadOnlyError,
    ResponseError,
    TryAgainError,
    ValkeyError,
)
from valkey.exceptions import ConnectionError as ValkeyConnectionError
from valkey.exceptions import TimeoutError as ValkeyTimeoutError

from swarmq.backend.valkey.environment import log_eviction_check, probe_eviction
from swarmq.config import INJECTED_URL, env_backend_connect_timeout_s, env_namespace, env_valkey_eviction_check
from swarmq.config_validation import valid_backend_connect_timeout
from swarmq.exceptions import BackendConnectionError, BackendResponseError, ConfigurationError
from swarmq.keyspace import KeySpace
from swarmq.valkey_policy import EvictionCheck

# Transient transport errors that should be retried transparently by the
# valkey-py client. Application-level errors (ResponseError, DataError,
# etc.) are NOT retried — they're programmer errors, not connectivity.
_TRANSIENT_ERRORS: list[type[Exception]] = [
    ValkeyConnectionError,
    ValkeyTimeoutError,
    OSError,
]

# Fehlerantworten, die ein voruebergehender Serverzustand sind und durch
# Warten heilen (maxmemory, Failover, Cluster-Umbau): wie ein Transportfehler
# ein Ausfall. ``ClusterDownError`` deckt ``MasterDownError`` ab;
# ``BusyLoadingError`` (LOADING) ist in valkey-py bereits ein
# ``ConnectionError``.
_OUTAGE_REPLIES: tuple[type[Exception], ...] = (OutOfMemoryError, ReadOnlyError, TryAgainError, ClusterDownError)

# Dieselben Zustaende als Fehlerpraefix: der Standalone-Parser von valkey-py
# kennt MASTERDOWN, MISCONF (Persistenz scheitert, z. B. Platte voll),
# CLUSTERDOWN und TRYAGAIN nicht und liefert einen einfachen
# ``ResponseError`` mit dem Praefix im Text. Ebenso BUSY (ein langes Skript
# blockiert den Server) und NOREPLICAS (``min-replicas-to-write`` nicht
# erfuellt); beide heilen durch Warten (Review 0.13.1, Nachtrag E-079).
_OUTAGE_REPLY_PREFIXES = frozenset(
    {"OOM", "READONLY", "LOADING", "TRYAGAIN", "CLUSTERDOWN", "MASTERDOWN", "MISCONF", "BUSY", "NOREPLICAS"}
)


def _is_outage_reply(exc: BaseException) -> bool:
    if isinstance(exc, _OUTAGE_REPLIES):
        return True
    if not isinstance(exc, ResponseError):
        return False
    words = str(exc).split(maxsplit=1)
    return bool(words) and words[0] in _OUTAGE_REPLY_PREFIXES


def as_backend_error(message: str, exc: BaseException) -> BackendConnectionError:
    """Abbildung eines Treiberfehlers an der Backend-Grenze: ein
    Transportfehler (Verbindung, Timeout, Socket) und eine Fehlerantwort, die
    durch Warten heilt (OOM, READONLY, LOADING, TRYAGAIN, CLUSTERDOWN,
    MASTERDOWN, MISCONF, BUSY, NOREPLICAS), sind ein Ausfall (``BackendConnectionError``); jede
    andere Ursache -- WRONGTYPE, Skriptfehler -- ein ``BackendResponseError``."""
    if isinstance(exc, tuple(_TRANSIENT_ERRORS)) or _is_outage_reply(exc):
        return BackendConnectionError(message)
    return BackendResponseError(message)


# Socket-level bounds for the owned client. Without
# socket_timeout, an in-flight recv on a half-open connection (peer
# silently gone, no RST) blocks forever — health_check_interval only
# PINGs before a command after idle; it cannot interrupt a running
# read. The socket timeout bounds EVERY operation (results, scheduler
# reads, Lua) without per-call op_timeout plumbing; a timed-out op
# surfaces as ValkeyTimeoutError → retried (transient) → wrapped to
# BackendConnectionError. Safe for the Pub/Sub paths: all get_message
# polls use 0.05s slices, far below this bound. Injected clients keep
# the caller's own socket configuration (documented caveat).
_SOCKET_TIMEOUT_S = 10.0
_SOCKET_CONNECT_TIMEOUT_S = 5.0

# Befehls-Retry des eigenen Clients (2026-04-17 toxiproxy spike): Backoff
# ``min(cap, base * 2**n)`` fuer n = 1..5, also 0,1 + 0,2 + 0,4 + 0,8 + 1,6 s.
_RETRY_BACKOFF_CAP_S = 2.0
_RETRY_BACKOFF_BASE_S = 0.05
_RETRY_ATTEMPTS = 5

# F4-14: Fenster der Handshake-Wiederholung, die nominelle Summe des
# Befehls-Retry (3,1 s), gemessen ab dem ersten Fehlschlag. Ein Zeitdeckel
# statt einer Zahl von Versuchen, damit sich die inneren TCP-Wiederholungen
# eines Versuchs und die aeusseren nicht vervielfachen.
_HANDSHAKE_RETRY_WINDOW_S = sum(
    min(_RETRY_BACKOFF_CAP_S, _RETRY_BACKOFF_BASE_S * 2**failures) for failures in range(1, _RETRY_ATTEMPTS + 1)
)

# Uhr und Warten der Handshake-Wiederholung (Systemgrenze, in Tests ersetzbar).
_monotonic = time.monotonic
_sleep = asyncio.sleep

# ``ping`` prüft mit derselben Schranke wie der bisherige HealthChecker.
_PING_TIMEOUT_S = 2.0


def _keyspace(namespace: str | None) -> KeySpace:
    """KeySpace aus dem Parameter, sonst aus ``SWARMQ_NAMESPACE``; ein
    ungueltiger Wert ist ein ``ConfigurationError`` (Exit 78)."""
    if namespace is None:
        return KeySpace(env_namespace())
    try:
        return KeySpace(namespace)
    except ValueError as exc:
        raise ConfigurationError(f"namespace: {exc}") from exc


async def _refuse_connection(*_args: Any, **_kwargs: Any) -> Any:
    """Ersatz fuer ``ConnectionPool.get_connection`` nach ``close()``."""
    msg = "ValkeyBackend is closed; no new connections"
    raise ValkeyConnectionError(msg)


class _HandshakeReset(ValkeyConnectionError):
    """Die Verbindung wurde im Handshake getrennt, nach erfolgreichem
    TCP-Aufbau (Proxy oder Sidecar vor einem neu startenden Valkey; F4-14)."""


def _mark_handshake_resets(connection: Any) -> Any:
    """Markiert einen Transportfehler im Handshake von *connection* als
    ``_HandshakeReset``, damit ``_retry_handshake`` ihn wiederholt.

    Nicht markiert werden Anmeldefehler (Warten heilt sie nicht) und ein
    Zeitablauf (``socket_timeout``; E-075: die Laufzeit bleibt). Ersetzt
    ``on_connect`` der Instanz statt ``valkey_connect_func`` zu setzen: dafuer
    ruft valkey-py 6.1.1 ``asyncio.iscoroutinefunction`` auf, das unter
    Python 3.14 eine ``DeprecationWarning`` ausloest."""
    on_connect = connection.on_connect

    async def handshake() -> None:
        # Kein Health-Check-PING im Handshake einer gerade aufgebauten
        # Verbindung: valkey-py schickt ihn vor dem ersten Befehl, wenn die
        # letzte Antwort ``health_check_interval`` zurueckliegt (bei einer
        # neuen Verbindung immer), und wiederholt ihn mit der Retry-Politik
        # samt erneutem TCP-Aufbau und Handshake. Innere und aeussere
        # Wiederholungen vervielfachten sich sonst. Jede Antwort setzt die
        # Frist danach wie gewohnt neu.
        if connection.health_check_interval:
            connection.next_health_check = asyncio.get_running_loop().time() + connection.health_check_interval
        try:
            await on_connect()
        except AuthenticationError, AuthorizationError:
            raise
        except ValkeyConnectionError as exc:
            raise _HandshakeReset(str(exc)) from exc

    connection.on_connect = handshake
    return connection


def _retry_handshake(pool: Any) -> None:
    """Wiederholt den Neuaufbau einer Pool-Verbindung, wenn der Handshake
    getrennt wird (F4-14).

    valkey-py holt die Verbindung in ``execute_command`` mit
    ``pool.get_connection`` vor ``conn.retry.call_with_retry``; ein Neuaufbau
    dort liegt ausserhalb der Befehls-Wiederholung. Wiederholt wird nur
    ``_HandshakeReset``, mit dem Backoff des Befehls-Retry, solange seit dem
    ersten Fehlschlag weniger als ``_HANDSHAKE_RETRY_WINDOW_S`` vergangen ist.
    Die Wiederholung greift vor dem Senden des Befehls; ein nicht
    idempotentes Skript laeuft dadurch nicht doppelt (F3-33 unveraendert)."""
    get_connection = pool.get_connection
    backoff = ExponentialBackoff(cap=_RETRY_BACKOFF_CAP_S, base=_RETRY_BACKOFF_BASE_S)

    def make_marked_connection() -> Any:
        # Ueber die Klasse erst beim Aufruf: ein Pool-Ersatz ohne
        # ``make_connection`` laesst sich weiter bauen.
        return _mark_handshake_resets(type(pool).make_connection(pool))

    async def get_connection_with_retry(*args: Any, **kwargs: Any) -> Any:
        first_failure: float | None = None
        failures = 0
        while True:
            try:
                return await get_connection(*args, **kwargs)
            except _HandshakeReset:
                now = _monotonic()
                if first_failure is None:
                    first_failure = now
                if now - first_failure >= _HANDSHAKE_RETRY_WINDOW_S:
                    raise
                failures += 1
                await _sleep(backoff.compute(failures))
                if pool.get_connection is not get_connection_with_retry:
                    # ``close()`` hat den Pool im Backoff versiegelt
                    # (``_seal_pool``): mit dessen Fehler enden, statt die
                    # Sperre mit der originalen ``get_connection`` zu umgehen.
                    return await pool.get_connection(*args, **kwargs)

    pool.make_connection = make_marked_connection
    pool.get_connection = get_connection_with_retry


def _seal_pool(pool: Any) -> None:
    """Sperrt *pool* fuer die Zeit nach ``close()``.

    Stores (Locks, Ausfuehrungsschutz, ...) halten den Client ueber
    ``close()`` hinaus, etwa ein Worker, dessen Handler erst nach dem Abbruch
    abschliessen. Neue Verbindungen verweigert der Pool. Ein Befehl, der beim
    ``close()`` schon eine Verbindung hielt, verbindet sie ueber die
    Retry-Politik neu; bei der Rueckgabe wird sie deshalb getrennt, sonst
    bliebe sie offen, ohne dass sie noch jemand schliesst.
    """
    release = pool.release

    async def release_and_disconnect(connection: Any) -> None:
        await release(connection)
        await connection.disconnect()

    pool.get_connection = _refuse_connection
    pool.release = release_and_disconnect


async def _close_quietly(client: valkey.asyncio.Valkey) -> None:
    """Schliesst einen Client, dessen ``connect()`` gescheitert ist (F4-13).
    Ein Fehler beim Schliessen verdeckt nicht die eigentliche Ursache."""
    with contextlib.suppress(Exception):
        await client.aclose()


class ValkeyConnection:
    def __init__(
        self,
        url: str | None = None,
        *,
        client: valkey.asyncio.Valkey | None = None,
        pool: valkey.asyncio.ConnectionPool | None = None,
        keys: KeySpace | None = None,
        eviction_check: str | None = None,
        namespace: str | None = None,
        connect_timeout_s: float | None = None,
    ) -> None:
        """Construct a backend from a URL, an existing client, or a pool.

        Exactly one source must be given:

        * ``url`` — swarmq builds and owns the client (default path).
        * ``client`` — an already-constructed ``valkey.asyncio.Valkey``;
          swarmq borrows it and never closes it (see :meth:`close`).
        * ``pool`` — a ``valkey.asyncio.ConnectionPool``; swarmq wraps a
          client around it but leaves the pool's lifecycle to the caller.

        Passing more than one (e.g. ``url`` *and* ``client``) is a
        :class:`ConfigurationError` rather than a silent precedence rule.

        Injected client/pool objects are intended for the producer
        (task-sending) path only — see ``SwarmQ.start_worker``.

        Caveats for the injected path: the borrowed client/pool keeps the
        caller's own retry / health-check configuration (it does NOT
        inherit swarmq's), and it must target the same Valkey database the
        rest of the swarmq config expects — swarmq does not validate this,
        and a mismatch silently reads/writes results in the wrong namespace.
        """
        sources = [
            ("url", url is not None),
            ("client", client is not None),
            ("pool", pool is not None),
        ]
        given = [name for name, present in sources if present]
        # Alle Keys und Kanaele dieses Backends (Plan 0.12.0, Q4).
        if keys is not None and namespace is not None:
            raise ConfigurationError("ValkeyBackend accepts keys or namespace, not both")
        # Namensraum (BZ-10): ohne Angabe aus der Umgebung (Q11, ueber ``config.py``).
        self._keys = keys if keys is not None else _keyspace(namespace)
        # Ausdruecklich (``namespace=`` oder ``keys=``) geht vor der Konfiguration
        # (Review MR 15, E1/P1-1, ``reconcile_namespace``).
        self._namespace_explicit = keys is not None or namespace is not None
        # Ergebnis der Eviction-Pruefung beim Verbinden (BZ-04); ``None``, solange
        # nicht geprueft wurde.
        self.eviction_check: EvictionCheck | None = None
        # Schalter der Pruefung; ohne Angabe aus der Umgebung (Q11, ueber ``config.py``).
        self._eviction_mode = eviction_check if eviction_check is not None else env_valkey_eviction_check()
        # E-075: Gesamtfrist des PING beim Verbinden; ohne Angabe aus der
        # Umgebung (Backends ohne ``SwarmQConfig``: CLI, Producer).
        # Ausdruecklich uebergeben gewinnt vor der Konfiguration
        # (``reconcile_connect_timeout``, Nachtrag E-075).
        self._connect_timeout_explicit = connect_timeout_s is not None
        if connect_timeout_s is None:
            connect_timeout_s = env_backend_connect_timeout_s()
        elif not valid_backend_connect_timeout(connect_timeout_s):
            raise ConfigurationError(
                f"connect_timeout_s must be > 0 and at most threading.TIMEOUT_MAX seconds, got {connect_timeout_s}"
            )
        self._connect_timeout_s = connect_timeout_s
        if not given:
            raise ConfigurationError("ValkeyBackend requires exactly one of: url, client, or pool")
        if len(given) > 1:
            raise ConfigurationError(
                f"ValkeyBackend accepts exactly one of url, client, or pool — got {', '.join(given)}"
            )

        # _owns_client is the single source of truth for whether close()
        # may tear the client down. A borrowed client/pool belongs to the
        # caller (see docs/solutions worker-cancel-safe-teardown): closing
        # it would pull the connection out from under the caller's app.
        self._client: valkey.asyncio.Valkey | None
        if client is not None:
            self._url: str = INJECTED_URL
            self._client = client
            self._owns_client = False
        elif pool is not None:
            self._url = INJECTED_URL
            # valkey-py sets auto_close_connection_pool=False automatically
            # when a pool is passed in, so the borrowed pool survives even
            # an accidental aclose() — _owns_client=False is the contract.
            self._client = valkey.asyncio.Valkey(connection_pool=pool)
            self._owns_client = False
        else:
            assert url is not None
            self._url = url
            self._client = None
            self._owns_client = True

    @property
    def connect_timeout_s(self) -> float:
        """Gesamtfrist des PING beim Verbinden in Sekunden (E-075)."""
        return self._connect_timeout_s

    def reconcile_connect_timeout(self, configured: float) -> None:
        """Die Gesamtfrist mit der Konfiguration abgleichen (Nachtrag E-075):
        ohne ausdruecklichen Parameter gilt *configured* (``SwarmQConfig``-Feld
        bzw. ``SWARMQ_BACKEND_CONNECT_TIMEOUT_S``), sonst der Parameter."""
        if not self._connect_timeout_explicit:
            self._connect_timeout_s = configured

    @property
    def namespace(self) -> str | None:
        """Der Namensraum aller Keys und Kanaele dieses Backends (BZ-10)."""
        return self._keys.namespace

    def reconcile_namespace(self, configured: str | None) -> str | None:
        """Den Namensraum mit der Konfiguration abgleichen (Review MR 15, E1/P1-1).

        Ohne ausdruecklichen Parameter uebernimmt das Backend *configured*.
        Mit ausdruecklichem Parameter gilt er; ist *configured* gesetzt und
        verschieden, ist das ein ``ConfigurationError`` (Exit 78). Liefert den
        wirksamen Namensraum, mit dem der Aufrufer die Konfiguration fuellt."""
        if not self._namespace_explicit:
            if configured != self._keys.namespace:
                self._keys = _keyspace(configured or "")
            return self._keys.namespace
        if configured is not None and configured != self._keys.namespace:
            raise ConfigurationError(
                f"namespace mismatch: ValkeyBackend(namespace={self._keys.namespace or ''!r}) and "
                f"SWARMQ_NAMESPACE/SwarmQConfig.namespace={configured!r} differ; set the namespace in one "
                "place or to the same value (the backend parameter wins only when the configuration is unset)"
            )
        return self._keys.namespace

    async def connect(self) -> None:
        # Idempotent, and a no-op when a client was injected: the borrowed
        # client/pool already exists, so swarmq builds nothing here.
        if self._client is not None:
            return
        # F4-13: Der Client gilt erst nach PING und Eviction-Pruefung als
        # verbunden; scheitert ein Schritt (auch durch Abbruch), wird der
        # eigene Client geschlossen und ``connect()`` bleibt wiederholbar.
        client = self._build_client()
        connected = False
        try:
            # E-075: Gesamtfrist nur um den PING; die Wiederholpolitik zur
            # Laufzeit bleibt.
            deadline = asyncio.timeout(self._connect_timeout_s)
            try:
                async with deadline:
                    await client.ping()
            except (ValkeyError, OSError) as exc:
                if deadline.expired():
                    msg = f"Valkey connect timed out after {self._connect_timeout_s}s"
                    raise BackendConnectionError(msg) from exc
                # F4-12: jeder Treiberfehler typisiert, auch Zeitablauf und
                # Fehlerantwort auf PING (Vertrag ``ResultBackend.connect``).
                raise as_backend_error(f"Valkey connect failed: {exc}", exc) from exc
            # BZ-04 (Plan 0.12.0, MR 14): verdraengende Policy laut melden.
            if self._eviction_mode != "off":
                self.eviction_check = await probe_eviction(client)
                log_eviction_check(self.eviction_check)
            connected = True
        finally:
            if not connected:
                await _close_quietly(client)
        self._client = client

    def _build_client(self) -> valkey.asyncio.Valkey:
        # Reliability hardening (2026-04-17 toxiproxy spike): command-
        # level retry on transient transport errors with exponential
        # backoff capped at 2s. health_check_interval=30 detects
        # half-open connections before they surface to callers.
        client: valkey.asyncio.Valkey = valkey.asyncio.Valkey.from_url(
            self._url,
            retry=Retry(
                ExponentialBackoff(cap=_RETRY_BACKOFF_CAP_S, base=_RETRY_BACKOFF_BASE_S), retries=_RETRY_ATTEMPTS
            ),
            retry_on_error=_TRANSIENT_ERRORS,
            health_check_interval=30,
            # Bound every op at the socket so a
            # half-open connection cannot wedge a read forever.
            socket_timeout=_SOCKET_TIMEOUT_S,
            socket_connect_timeout=_SOCKET_CONNECT_TIMEOUT_S,
        )
        # F4-14: Neuaufbau mit getrenntem Handshake wiederholen. Ein Client
        # ohne Pool (Ersatz an der Grenze ``from_url``) bleibt, wie er ist.
        pool = getattr(client, "connection_pool", None)
        if pool is not None:
            _retry_handshake(pool)
        return client

    async def recheck_eviction(self) -> EvictionCheck | None:
        """Erneute Pruefung (Leader, stuendlich; BZ-04) samt ``used_memory``;
        warnt bei Befund wie ``connect``. ``None`` mit ``off`` oder ohne
        Verbindung. Wirft nie."""
        if self._eviction_mode == "off" or self._client is None:
            return None
        self.eviction_check = await probe_eviction(self._client, memory=True)
        log_eviction_check(self.eviction_check)
        return self.eviction_check

    async def close(self) -> None:
        if self._client is not None:
            # Only tear down a client swarmq built itself. A borrowed
            # client/pool (injected at construction) is left open for the
            # caller — closing it here would break the caller's own app.
            if self._owns_client:
                # Vor dem Trennen: Ein Befehl, der waehrend aclose() erst
                # startet, bekaeme sonst die gerade getrennte Verbindung.
                _seal_pool(self._client.connection_pool)
                await self._client.aclose()
            self._client = None

    async def ping(self) -> None:
        """Wirft ``BackendConnectionError`` vor ``connect()`` oder wenn der
        Server nicht innerhalb von ``_PING_TIMEOUT_S`` antwortet."""
        await self._run("ping", self._require_client().ping(), _PING_TIMEOUT_S)

    def _require_client(self) -> valkey.asyncio.Valkey:
        if self._client is None:
            raise BackendConnectionError("ValkeyBackend not connected; call connect() first")
        return self._client

    @property
    def client(self) -> valkey.asyncio.Valkey:
        """Verbundener Client (für die Koordinations-Stores ab U18)."""
        return self._require_client()

    async def _run(self, what: str, coro: Any, op_timeout: float | None = None) -> Any:
        """Führt eine Valkey-Operation aus: optional mit ``op_timeout``
        (Sekunden) begrenzt, Treiber- und Timeout-Fehler als
        ``BackendConnectionError``. Ersetzt die zwölf gleichartigen Blöcke des
        früheren ``valkey.py`` (ein langsames Backend darf den
        Worker-``finally``-Block nicht blockieren). ``coro`` ist ``Any``, weil
        valkey-py seine Rückgaben als ``Awaitable[T] | T`` tippt."""
        try:
            if op_timeout is not None:
                return await asyncio.wait_for(coro, timeout=op_timeout)
            return await coro
        except TimeoutError as exc:
            raise BackendConnectionError(f"{what} timed out after {op_timeout}s") from exc
        except (ValkeyError, OSError) as exc:
            raise as_backend_error(str(exc), exc) from exc

    def _pubsub(self) -> Any:
        return self._require_client().pubsub()
