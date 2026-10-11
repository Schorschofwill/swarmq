"""Valkey-Verbindung: Client-Aufbau, Retry, Socket-Timeouts, ``ping`` und der
gebündelte ``op_timeout``-Wrapper (Plan U17; Entwurf §5).

``ValkeyConnection`` hält den Client; ``results.py`` und (ab U18) die
Koordinations-Stores bauen darauf auf.
"""

from __future__ import annotations

import asyncio
from typing import Any

import valkey.asyncio
from valkey.asyncio.retry import Retry
from valkey.backoff import ExponentialBackoff
from valkey.exceptions import (
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
from swarmq.config import INJECTED_URL, env_namespace, env_valkey_eviction_check
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
# ``ResponseError`` mit dem Praefix im Text.
_OUTAGE_REPLY_PREFIXES = frozenset({"OOM", "READONLY", "LOADING", "TRYAGAIN", "CLUSTERDOWN", "MASTERDOWN", "MISCONF"})


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
    MASTERDOWN, MISCONF), sind ein Ausfall (``BackendConnectionError``); jede
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
        try:
            # Reliability hardening (2026-04-17 toxiproxy spike): command-
            # level retry on transient transport errors with exponential
            # backoff capped at 2s. health_check_interval=30 detects
            # half-open connections before they surface to callers.
            self._client = valkey.asyncio.Valkey.from_url(
                self._url,
                retry=Retry(ExponentialBackoff(cap=2.0, base=0.05), retries=5),
                retry_on_error=_TRANSIENT_ERRORS,
                health_check_interval=30,
                # Bound every op at the socket so a
                # half-open connection cannot wedge a read forever.
                socket_timeout=_SOCKET_TIMEOUT_S,
                socket_connect_timeout=_SOCKET_CONNECT_TIMEOUT_S,
            )
            await self._client.ping()
        except (ValkeyConnectionError, OSError) as exc:
            raise BackendConnectionError(str(exc)) from exc
        # BZ-04 (Plan 0.12.0, MR 14): verdraengende Policy laut melden.
        if self._eviction_mode != "off":
            self.eviction_check = await probe_eviction(self._client)
            log_eviction_check(self.eviction_check)

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
