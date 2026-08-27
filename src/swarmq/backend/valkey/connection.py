"""Valkey-Verbindung: Client-Aufbau, Retry, Socket-Timeouts, ``ping`` und der
gebündelte ``op_timeout``-Wrapper (Plan U17; Entwurf §5).

``ValkeyConnection`` hält den Client; ``results.py`` und (ab U18) die
Koordinations-Stores bauen darauf auf.
"""

from __future__ import annotations

import asyncio
from typing import Any

import structlog
import valkey.asyncio
from valkey.asyncio.retry import Retry
from valkey.backoff import ExponentialBackoff
from valkey.exceptions import ConnectionError as ValkeyConnectionError
from valkey.exceptions import TimeoutError as ValkeyTimeoutError
from valkey.exceptions import ValkeyError

from swarmq.config import INJECTED_URL
from swarmq.exceptions import BackendConnectionError, ConfigurationError

logger = structlog.get_logger()

# Transient transport errors that should be retried transparently by the
# valkey-py client. Application-level errors (ResponseError, DataError,
# etc.) are NOT retried — they're programmer errors, not connectivity.
_TRANSIENT_ERRORS: list[type[Exception]] = [
    ValkeyConnectionError,
    ValkeyTimeoutError,
    OSError,
]

# Härtung R2 P1-2: socket-level bounds for the owned client. Without
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


class ValkeyConnection:
    def __init__(
        self,
        url: str | None = None,
        *,
        client: valkey.asyncio.Valkey | None = None,
        pool: valkey.asyncio.ConnectionPool | None = None,
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
                # Härtung R2 P1-2: bound every op at the socket so a
                # half-open connection cannot wedge a read forever.
                socket_timeout=_SOCKET_TIMEOUT_S,
                socket_connect_timeout=_SOCKET_CONNECT_TIMEOUT_S,
            )
            await self._client.ping()
        except (ValkeyConnectionError, OSError) as exc:
            raise BackendConnectionError(str(exc)) from exc

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
        früheren ``valkey.py`` (REV-P1-3: ein langsames Backend darf den
        Worker-``finally``-Block nicht blockieren). ``coro`` ist ``Any``, weil
        valkey-py seine Rückgaben als ``Awaitable[T] | T`` tippt."""
        try:
            if op_timeout is not None:
                return await asyncio.wait_for(coro, timeout=op_timeout)
            return await coro
        except TimeoutError as exc:
            raise BackendConnectionError(f"{what} timed out after {op_timeout}s") from exc
        except (ValkeyError, OSError) as exc:
            raise BackendConnectionError(str(exc)) from exc

    def create_pubsub(self) -> valkey.asyncio.client.PubSub:
        """Alias (KTD10, entfällt in U15): rohes Pub/Sub-Objekt. Neue
        Aufrufer nutzen ``wait_for_result``/``watch_progress``."""
        return self._require_client().pubsub()

    def _pubsub(self) -> Any:
        return self._require_client().pubsub()
