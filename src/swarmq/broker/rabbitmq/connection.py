"""RabbitMQ-Verbindung: connect/close/ping, Channel-Recreate, injizierte Pools
(Plan U5; Entwurf §5).

``RabbitMQConnection`` hält den gesamten Verbindungszustand; die Mixins in
``delivery.py`` und ``admin.py`` bauen darauf auf. Die Fassade
``RabbitMQBroker`` in ``__init__`` vereint alles.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable
from typing import Any

import aio_pika
from aio_pika.exceptions import (
    AMQPChannelError,
    ChannelClosed,
    ChannelInvalidStateError,
    ConnectionClosed,
)

from swarmq.broker.rabbitmq.arg_conflict import AdoptionLog
from swarmq.broker.rabbitmq.consumer_watch import ConsumerWatch
from swarmq.config import INJECTED_URL
from swarmq.exceptions import BrokerConnectionError, ConfigurationError
from swarmq.logging import _error_fields, get_logger, sanitize_url

logger = get_logger(__name__)

# How long to wait for the connection/channel to become ready again
# during a reconnect before surfacing BrokerConnectionError. U1.1
# (2026-04-17): the toxiproxy spike showed publish-during-reconnect
# fails with CancelledError; the broker now waits instead.
_CHANNEL_READY_TIMEOUT_S = 10.0

# ``ping`` prüft ``connection.ready()`` mit derselben Schranke wie der
# bisherige HealthChecker (health.py, 2.0 s).
_PING_TIMEOUT_S = 2.0

# Transient failure modes that indicate the channel/connection is in a
# reconnect-transition state. Safe to retry after a short backoff.
# CancelledError is included because aio_pika's internal drain_future
# surfaces as CancelledError when the transport is closed mid-publish
# (2026-04-17 spike evidence). We guard against swallowing task-level
# cancellation via asyncio.current_task().cancelling() below.
_CHANNEL_TRANSIENT_ERRORS: tuple[type[BaseException], ...] = (
    ChannelInvalidStateError,
    ChannelClosed,
    AMQPChannelError,
    ConnectionClosed,
    asyncio.CancelledError,
)
_PUBLISH_MAX_ATTEMPTS = 5
# Fallback fuer einen ohne App konstruierten Broker; die kanonische Quelle
# ist ``SwarmQConfig.publish_timeout_s`` (Plan 2026-08-28-001, R3).
_DEFAULT_PUBLISH_TIMEOUT_S = 120.0
_DEFAULT_DELIVERY_LIMIT = 20
# Fallback fuer einen ohne App konstruierten Broker; kanonisch ist
# ``SwarmQConfig.dead_letter_ttl_s`` (Plan 2026-10-08-1541, KTD6).
_DEFAULT_DEAD_LETTER_TTL_S = 604800.0
# Frist fuer das nebenlaeufige Schliessen eines verworfenen Kanals. Kurz:
# gelingt es nicht, ist der Transport ohnehin tot und der Kanal verwaist.
_CHANNEL_CLOSE_TIMEOUT_S = 5.0

# Bound for acquiring a connection from an injected
# pool. aio_pika's Pool.acquire() blocks indefinitely on Queue.get()
# when the pool is exhausted (all items checked out, no overflow) —
# without this bound, `async with SwarmQ(...)` hung silently.
_POOL_ACQUIRE_TIMEOUT_S = 10.0

ChannelFactory = Callable[[], Awaitable[aio_pika.abc.AbstractChannel]]


async def close_channel_quietly(channel: aio_pika.abc.AbstractChannel | None) -> None:
    """``close()`` ohne ``is_closed``-Pruefung und ohne Ausnahme: nach einem
    vom Broker geschlossenen Channel (NOT_FOUND, 406) ist ``is_closed`` schon
    True; ohne eigenes ``close`` oeffnete ein RobustChannel sich wieder und
    bliebe offen, bis die Verbindung ``channel_max`` erreicht (Review MR 10,
    P0-1; wie ``arg_conflict``)."""
    if channel is None:
        return
    try:
        await channel.close()
    except Exception:  # noqa: BLE001, S110 - Schliessen best effort
        pass


async def _connect_robust_closing_on_cancel(url: str) -> aio_pika.abc.AbstractRobustConnection:
    """``aio_pika.connect_robust``, abbruchsicher: Wird der Aufbau abgebrochen
    (etwa ein abgebrochener Worker-Start), baut aio-pika die angelegte
    Verbindung sonst im Hintergrund weiter auf und verbindet sich alle 5 s neu.
    Sie wird darum geschlossen (Plan 0.12.0, MR 18; Publish-Verbindung:
    Nachtrag, von Georg am 2026-10-11 entschieden)."""
    created: list[aio_pika.abc.AbstractRobustConnection] = []

    class _Tracked(aio_pika.RobustConnection):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            created.append(self)

    try:
        return await aio_pika.connect_robust(url, connection_class=_Tracked)
    except asyncio.CancelledError:
        for connection in created:
            with contextlib.suppress(Exception):
                await connection.close()
        raise


class RabbitMQConnection:
    def __init__(
        self,
        url: str | None = None,
        queue_type: str = "quorum",
        *,
        connection: aio_pika.abc.AbstractConnection | None = None,
        connection_pool: aio_pika.pool.Pool[Any] | None = None,
    ) -> None:
        """Construct a broker from a URL, an existing connection, or a pool.

        Exactly one source must be given:

        * ``url`` — swarmq dials and owns the connection (default path).
        * ``connection`` — an existing aio-pika ``(Robust)Connection``;
          swarmq borrows it, opens its *own* publish channel on it, and at
          teardown closes that channel but never the borrowed connection.
        * ``connection_pool`` — an ``aio_pika.pool.Pool``; swarmq acquires
          from it and leaves the pool's lifecycle to the caller.

        Passing more than one source is a :class:`ConfigurationError`
        rather than a silent precedence rule.

        Injected objects are for the producer (task-sending) path only —
        ``SwarmQ.start_worker`` rejects an injected broker.

        Caveats for the injected path: prefer a robust connection
        (``aio_pika.connect_robust``) so swarmq's channel-reconnect
        handling behaves as designed, and the connection must target the
        same vhost the rest of the swarmq config expects — swarmq does not
        validate this, and a mismatch silently publishes to the wrong vhost.
        """
        sources = [
            ("url", url is not None),
            ("connection", connection is not None),
            ("connection_pool", connection_pool is not None),
        ]
        given = [name for name, present in sources if present]
        if not given:
            raise ConfigurationError("RabbitMQBroker requires exactly one of: url, connection, or connection_pool")
        if len(given) > 1:
            raise ConfigurationError(
                f"RabbitMQBroker accepts exactly one of url, connection, or connection_pool — got {', '.join(given)}"
            )

        self._queue_type = queue_type
        # Obergrenze fuer einen kompletten Publish-Vorgang (Plan
        # 2026-08-28-001, R1/R3). ``SwarmQ.__init__`` ueberschreibt sie aus
        # der Konfiguration; ein direkt konstruierter Broker behaelt den
        # Default, damit der Producer-Pfad ohne App weiter funktioniert.
        self.publish_timeout_s: float = _DEFAULT_PUBLISH_TIMEOUT_S
        # Zustellbudget der Quorum-Queues (B5). Wie oben: ``SwarmQ.__init__``
        # ueberschreibt aus der Konfiguration, ein direkt konstruierter
        # Broker behaelt den Default.
        self.delivery_limit: int = _DEFAULT_DELIVERY_LIMIT
        # Verfallsfrist der von swarmq in die DLQ gelegten Nachrichten (KTD6);
        # None heisst unbegrenzt. Wie oben aus der Konfiguration ueberschrieben.
        self.dead_letter_ttl_s: float | None = _DEFAULT_DEAD_LETTER_TTL_S
        # Plan 0.12.0, MR 8: die zuletzt erfolgreich deklarierten Argumente je
        # Queue (das Neu-Deklarieren aus MR 6 legt eine geloeschte Queue so
        # wieder an, wie sie war) und die gedrosselten Uebernahme-Logs.
        self._declared_arguments: dict[str, dict[str, Any]] = {}
        # Cache: Queue-Familien, die dieser Prozess schon deklariert hat.
        self._declaration_cache: set[str] = set()
        # Delay-Stufen (BZ-01, MR 10), die dieser Prozess deklariert hat; nur
        # an sie geht eine Rueckstellung, sonst an ``{queue}.DQ``.
        self._ready_delay_tiers: set[str] = set()
        # ... und die, deren Deklaration der Broker abgelehnt hat (nicht fatal).
        self._unavailable_delay_tiers: set[str] = set()
        # Bestandsqueues ohne at-least-once (BZ-19) fuer die eine Warnung.
        self._at_most_once_queues: set[str] = set()
        self._at_most_once_reported = False
        # ``SWARMQ_QUEUE_DEAD_LETTER_STRATEGY`` (BZ-19): ``auto``, ``at-least-once``
        # oder ``legacy``; ``SwarmQ.__init__`` ueberschreibt aus der Konfiguration.
        self.dead_letter_strategy: str = "auto"
        self._adoption_log = AdoptionLog()
        self._publish_channel: aio_pika.abc.AbstractChannel | None = None
        self._consume_channel: aio_pika.abc.AbstractChannel | None = None
        # BZ-16 (Plan 0.12.0, MR 16): ``separate`` konsumiert ueber eine
        # zweite Verbindung, die erst der erste ``consume()`` oeffnet. Bei
        # einem Broker-Alarm blockiert RabbitMQ nur publizierende
        # Verbindungen; Acks laufen dann weiter. ``shared`` = wie vor 0.12.
        # ``SwarmQ.__init__`` ueberschreibt aus der Konfiguration.
        self.publish_connection: str = "separate"
        self._consume_connection: aio_pika.abc.AbstractConnection | None = None
        # Consumer-Wächter (Plan 2026-10-08-1541, U6, KTD3): kennt jeden
        # angemeldeten Consumer und meldet verlorene selbst neu an.
        self._consumer_watch = ConsumerWatch()
        # U1.1: QoS state re-applied after channel recreate. Without
        # this, a reconnected consumer silently loses its prefetch.
        self._consume_prefetch: int | None = None
        # Guard against two concurrent channel-reopen operations.
        self._channel_lock = asyncio.Lock()
        # Starke Referenzen auf die nebenlaeufigen close()-Tasks aus
        # _discard_publish_channel; ohne sie kann der Event-Loop einen
        # noch laufenden Task einsammeln (asyncio.create_task haelt nur
        # eine schwache Referenz).
        self._pending_channel_closes: set[asyncio.Task[None]] = set()
        # Absoluter Zeitpunkt (loop.time-Skala), ab dem kein Publish mehr
        # laufen darf — vom Worker beim Herunterfahren gesetzt (R12).
        # ``None`` = nur die konfigurierte Deadline gilt.
        self._publish_deadline_at: float | None = None
        # Aktive Publish-Zeitdeckel. Ein einmal gesetzter asyncio-Timeout
        # verkuerzt sich nicht von selbst; ohne Nachziehen liefe ein vor
        # dem Shutdown gestarteter Publish weiter gegen die volle Frist.
        self._active_publish_timeouts: set[asyncio.Timeout] = set()
        # Test-Naht (U5, KTD9 „dead_letter/retry_later werfen bei
        # Publish-Fehler"): eine injizierte Factory ersetzt den Publish-Kanal.
        # ``None`` = ``self._connection.channel()``.
        self._publish_channel_factory: ChannelFactory | None = None

        # _owns_connection is the single source of truth for whether close()
        # may tear the connection down (mirrors ValkeyBackend._owns_client
        # and the worker-cancel-safe-teardown convention). _connection_pool
        # is set only on the pool path so connect() can acquire from it;
        # _pool_ctx holds the live acquire() context so close() can release
        # the borrowed connection back to the pool instead of closing it.
        self._connection_pool = connection_pool
        self._pool_ctx: Any = None
        self._connection: aio_pika.abc.AbstractConnection | None
        if connection is not None:
            # Credential-free placeholder so every _url reader (config,
            # sanitize_url log sites) stays safe without a None-guard.
            self._url: str = INJECTED_URL
            self._connection = connection
            self._owns_connection = False
        elif connection_pool is not None:
            self._url = INJECTED_URL
            self._connection = None
            self._owns_connection = False
        else:
            assert url is not None
            self._url = url
            self._connection = None
            self._owns_connection = True

    async def connect(self) -> None:
        # Owned (URL) path: dial once, open both channels. Idempotent via
        # the connection check below.
        if self._owns_connection:
            await self._connect_owned()
            return
        await self._connect_injected()

    async def _connect_owned(self) -> None:
        if self._connection is not None:
            return
        # Frischer Verbindungsaufbau: eine Endfrist aus einem
        # abgeschlossenen Shutdown darf nicht weiterwirken (R12).
        self._publish_deadline_at = None
        try:
            self._connection = await _connect_robust_closing_on_cancel(self._url)
            # Channel creation can fail too (vhost misconfigured, per-
            # connection channel cap, broker restart between
            # connect_robust success and channel() call). These two
            # lines were once outside the BrokerConnectionError
            # wrapper, letting raw aio_pika exceptions escape to the CLI
            # as unformatted tracebacks; keep them inside.
            # on_return_raises: ein basic.return (keine Queue) scheitert als
            # PublishError statt still bestaetigt zu werden (BZ-03).
            self._publish_channel = await self._connection.channel(on_return_raises=True)
            if not self._separate_consume_connection():
                self._consume_channel = await self._connection.channel()
        except Exception as exc:
            raise BrokerConnectionError(str(exc)) from exc

    def _separate_consume_connection(self) -> bool:
        """Eigene Consume-Verbindung (BZ-16): nur mit eigener URL. Bei
        injizierter Verbindung oder Pool gibt es keine URL, dann ``shared``."""
        return self._owns_connection and self.publish_connection == "separate"

    async def _connection_for_consume(self) -> aio_pika.abc.AbstractConnection | None:
        """Die Verbindung, auf der der Consume-Kanal liegt; oeffnet die
        zweite Verbindung beim ersten Bedarf (BZ-16)."""
        if not self._separate_consume_connection() or self._connection is None:
            return self._connection
        if self._consume_connection is None:
            try:
                self._consume_connection = await _connect_robust_closing_on_cancel(self._url)
            except Exception as exc:
                raise BrokerConnectionError(f"consume connection to {sanitize_url(self._url)!r} failed: {exc}") from exc
        return self._consume_connection

    async def _connect_injected(self) -> None:
        # Injected path (producer-only). The borrowed connection (or a
        # connection drawn from the borrowed pool) already exists; swarmq
        # only needs its OWN publish channel on it. No consume channel —
        # consuming is the worker's job, and start_worker() rejects an
        # injected broker (U4).
        if self._publish_channel is not None:
            return
        try:
            if self._connection is None and self._connection_pool is not None:
                await self._acquire_from_pool()
            # A plain (non-robust) injected connection has
            # no reconnect self-healing — after a transport drop every
            # publish runs into the full ready()-timeout wall and fails.
            # The constructor docstring caveat alone is too quiet.
            if not isinstance(self._connection, aio_pika.abc.AbstractRobustConnection):
                logger.warning(
                    "injected_connection_not_robust",
                    hint=(
                        "the injected connection has no reconnect "
                        "self-healing; after a drop every publish waits "
                        f"{_CHANNEL_READY_TIMEOUT_S}s and fails — prefer "
                        "aio_pika.connect_robust()"
                    ),
                )
            assert self._connection is not None
            self._publish_channel = await self._connection.channel(on_return_raises=True)
        except BrokerConnectionError:
            raise
        except Exception as exc:
            # If we acquired a connection from the pool but failed before a
            # usable publish channel, release the checkout — SwarmQ.__aenter__
            # calls connect() outside its try/except, so a failed connect()
            # is never followed by close(), and the borrowed pool connection
            # would otherwise leak. A directly-injected (bare) connection is
            # left untouched: it is the caller's and is not ours to release.
            if self._pool_ctx is not None:
                try:
                    await self._pool_ctx.__aexit__(type(exc), exc, exc.__traceback__)
                except Exception:  # noqa: BLE001, S110 - Pool-Rueckgabe best effort, Originalfehler folgt
                    pass
                self._pool_ctx = None
                self._connection = None
            raise BrokerConnectionError(str(exc)) from exc

    async def _acquire_from_pool(self) -> None:
        # Acquire a connection and HOLD it for the publish channel's
        # lifetime. acquire() is a context manager that releases on
        # exit, so we enter it manually here and release it in
        # close() — releasing returns the connection to the pool
        # rather than closing it (the pool stays the caller's).
        # The acquire is bounded — an exhausted
        # pool otherwise blocks forever on Queue.get().
        assert self._connection_pool is not None
        self._pool_ctx = self._connection_pool.acquire()
        try:
            self._connection = await asyncio.wait_for(
                self._pool_ctx.__aenter__(),
                timeout=_POOL_ACQUIRE_TIMEOUT_S,
            )
        except TimeoutError as exc:
            # Best-effort release: wait_for cancelled
            # __aenter__ mid-Queue.get(), so normally nothing was
            # checked out — but if cancellation landed in the
            # narrow window after get() returned, __aexit__
            # returns that item to the pool instead of leaking it
            # (aio-pika's PoolItemContextManager.__aexit__ is a
            # no-op when no item was assigned).
            try:
                await self._pool_ctx.__aexit__(None, None, None)
            except Exception:  # noqa: BLE001, S110 - Best-effort release
                pass
            self._pool_ctx = None
            self._connection = None
            raise BrokerConnectionError(
                f"injected pool acquire timed out after {_POOL_ACQUIRE_TIMEOUT_S}s — pool exhausted or closed?"
            ) from exc

    async def ping(self) -> None:
        """Wirft ``BrokerConnectionError``, wenn nicht verbunden oder die
        Verbindung nicht innerhalb von ``_PING_TIMEOUT_S`` bereit ist."""
        if self._connection is None or self._connection.is_closed:
            raise BrokerConnectionError("broker not connected")
        try:
            await asyncio.wait_for(self._connection.ready(), timeout=_PING_TIMEOUT_S)
        except TimeoutError as exc:
            raise BrokerConnectionError(f"broker connection not ready within {_PING_TIMEOUT_S}s") from exc
        except Exception as exc:
            raise BrokerConnectionError(str(exc)) from exc

    async def _drain_pending_channel_closes(self) -> None:
        """Sammelt die nebenlaeufigen close()-Tasks ein, bevor die Verbindung faellt.

        Ohne das laeuft ein noch offener Schliess-Task in eine abgebaute
        Verbindung und der Event-Loop meldet beim Teardown "Task was destroyed
        but it is pending" — Rauschen genau im Diagnosefenster, das dieser
        Plan verbessern soll. Gebunden, damit ein toter Transport ``close()``
        nicht erneut blockiert; was dann noch laeuft, wird abgebrochen.
        """
        pending = tuple(self._pending_channel_closes)
        if not pending:
            return
        await asyncio.wait(pending, timeout=_CHANNEL_CLOSE_TIMEOUT_S)
        for task in pending:
            if not task.done():
                task.cancel()

    async def close(self) -> None:
        # Der Wächter darf nicht gegen eine abgebaute Verbindung weitertakten.
        await self._consumer_watch.stop()
        await self._drain_pending_channel_closes()
        if self._owns_connection:
            # Owned (URL) path unchanged: closing the connection closes its
            # channels too. Die Consume-Verbindung (BZ-16) zuerst.
            if self._consume_connection is not None:
                consume_connection, self._consume_connection = self._consume_connection, None
                self._consume_channel = None
                await consume_connection.close()
            if self._connection is not None:
                await self._connection.close()
                self._connection = None
            return

        # Injected path: close ONLY the channels swarmq opened itself —
        # they belong to swarmq regardless of who owns the connection. Do
        # NOT swallow channel.close() in a bare except: a swallowed close
        # corrupts later channel() calls (aiormq-reader-leak learning).
        # The channel-close loop runs in try/finally so a raised
        # channel.close() still propagates but never skips the pool release.
        try:
            for attr in ("_publish_channel", "_consume_channel"):
                chan: aio_pika.abc.AbstractChannel | None = getattr(self, attr)
                if chan is not None:
                    if not chan.is_closed:
                        await chan.close()
                    setattr(self, attr, None)
        finally:
            if self._pool_ctx is not None:
                # Borrowed from a pool: release the connection back to the
                # pool (do not close it — the pool is the caller's) and drop
                # the reference so the next connect() re-acquires a fresh one.
                await self._pool_ctx.__aexit__(None, None, None)
                self._pool_ctx = None
                self._connection = None
            # A directly-injected (bare) connection is intentionally KEPT on
            # self._connection: it stays the caller's open connection, and
            # retaining it lets a subsequent connect() reopen a publish
            # channel on it — mirroring the owned path's connect/close reuse.

    # ------------------------------------------------------------------
    # Channel-Recreate
    # ------------------------------------------------------------------

    def _effective_publish_timeout(self) -> float:
        """Frist fuer einen kompletten Publish-Vorgang (R1, R12).

        Ohne gesetzte Shutdown-Endfrist gilt die Konfiguration. Mit gilt
        die kuerzere von beiden; eine bereits verstrichene Endfrist ergibt
        0.0 und laesst den Publish sofort scheitern, statt in einen
        negativen Deckel zu laufen.
        """
        if self._publish_deadline_at is None:
            return self.publish_timeout_s
        remaining = self._publish_deadline_at - asyncio.get_running_loop().time()
        return max(0.0, min(self.publish_timeout_s, remaining))

    def set_publish_deadline(self, at: float | None) -> None:
        """Setzt oder loescht die Shutdown-Endfrist (R12, KTD9).

        Laufende Publishes werden nachgezogen — nur nach vorne: Das
        Herunterfahren darf eine Frist verkuerzen, nie verlaengern.
        """
        self._publish_deadline_at = at
        if at is None:
            return
        for timeout_cm in tuple(self._active_publish_timeouts):
            current = timeout_cm.when()
            if current is None or current > at:
                try:
                    timeout_cm.reschedule(at)
                except RuntimeError:
                    # Der Deckel ist bereits abgelaufen oder verlassen —
                    # dann braucht er kein Nachziehen mehr.
                    continue

    async def _discard_publish_channel(self, expected: aio_pika.abc.AbstractChannel | None) -> None:
        """Verwirft den Publish-Kanal nach einem Deadline-Bruch (R10, KTD3).

        Zwei Feinheiten, ohne die der Verwurf mehr schadet als nuetzt:

        * **Identitaetspruefung unter der Sperre.** Eine parallele
          Ausfuehrung kann den Kanal waehrenddessen neu geoeffnet haben;
          den wegzuwerfen liesse ein offenes, referenzloses Kanalobjekt
          zurueck — die Leckklasse aus
          ``docs/solutions/logic-errors/aiormq-reader-leak-and-stale-connection-recovery-2026-04-16.md``.
        * **Nebenlaeufiges Schliessen.** ``close()`` wartet auf ein
          Channel.Close-Ok, das ein still toter Transport nie liefert —
          abgewartet ueberschritte es genau die Frist, die wir gerade
          durchgesetzt haben. Das Schliessen ist trotzdem noetig: aiormq
          gibt die offene Confirmation-Future erst dabei frei.
        """
        try:
            await asyncio.wait_for(self._channel_lock.acquire(), timeout=_CHANNEL_CLOSE_TIMEOUT_S)
        except TimeoutError:
            # Die Sperre ist belegt. Der Aufrufer steht hier bereits AUSSERHALB
            # seiner Frist — ihn weiter warten zu lassen, waere genau der
            # Stillstand, den der Deckel beenden soll. Der stehengelassene
            # Kanal kostet den naechsten Publish hoechstens einen Fehlversuch.
            logger.warning("publish_channel_discard_skipped", reason="lock_busy")
            return
        try:
            current = self._publish_channel
            if current is not expected:
                # Nicht der Kanal dieses Vorgangs — entweder hat eine parallele
                # Ausfuehrung neu geoeffnet, oder wir kamen nie zu einem
                # eigenen (``expected is None``). In beiden Faellen gibt es
                # hier nichts zu verwerfen.
                return
            self._publish_channel = None
        finally:
            self._channel_lock.release()

        if current is None:
            return
        task = asyncio.create_task(self._close_channel_detached(current))
        self._pending_channel_closes.add(task)
        task.add_done_callback(self._pending_channel_closes.discard)

    async def _close_channel_detached(self, channel: aio_pika.abc.AbstractChannel) -> None:
        """Best-effort-Schliessen mit eigener Frist, entkoppelt vom Aufrufer."""
        try:
            async with asyncio.timeout(_CHANNEL_CLOSE_TIMEOUT_S):
                if not channel.is_closed:
                    await channel.close()
        except Exception as exc:
            # Best effort — nie eskalieren, aber auch nicht verschweigen: Ein
            # systematischer Fehlschlag bedeutet, dass aiormq die offene
            # Confirmation-Future nie freigibt, und das waere auf DEBUG genau
            # im Diagnosefall unsichtbar.
            logger.warning("publish_channel_close_incomplete", **_error_fields(exc), exc_info=True)

    async def _ensure_publish_channel(self) -> aio_pika.abc.AbstractChannel:
        """Return a ready publish channel; recreate if dropped.

        aio_pika's RobustChannel auto-reopens after a transport blip,
        but operations initiated during the `is_closed=True` window
        fail (CancelledError / ChannelInvalidStateError). Spike
        evidence: docs/solutions/spikes/2026-04-17-toxiproxy-
        reconnect-spike.md. This guard waits for the connection to
        be ready, then reopens the channel if still closed, before
        returning it to the caller.
        """
        chan, _ = await self._ensure_channel_tracked("_publish_channel")
        return chan

    async def _ensure_consume_channel(self) -> aio_pika.abc.AbstractChannel:
        """Return a ready consume channel; recreate if dropped.

        Same contract as :meth:`_ensure_publish_channel`, but also
        re-applies the previous QoS prefetch when the channel was
        freshly recreated — otherwise the reconnected consumer
        silently loses its rate-limiting setting. On the fast path
        (healthy channel), set_qos is NOT called — an AMQP Basic.Qos
        RPC per consume access is wasteful and its failure was
        previously unguarded.
        """
        chan, was_recreated = await self._ensure_channel_tracked("_consume_channel")
        if was_recreated and self._consume_prefetch is not None:
            await chan.set_qos(prefetch_count=self._consume_prefetch)
        return chan

    async def _ensure_channel_tracked(self, attr: str) -> tuple[aio_pika.abc.AbstractChannel, bool]:
        """Return (channel, was_recreated). Lets callers apply per-
        recreate state (QoS prefetch, consumer re-registration)
        exactly when a new underlying channel was opened."""
        channel: aio_pika.abc.AbstractChannel | None = getattr(self, attr)
        if channel is not None and not channel.is_closed:
            return channel, False
        async with self._channel_lock:
            # Re-check under the lock — another coroutine may have
            # reopened while we were waiting.
            channel = getattr(self, attr)
            if channel is not None and not channel.is_closed:
                return channel, False
            if self._connection is None:
                raise BrokerConnectionError("broker not connected")
            connection = self._connection
            if attr == "_consume_channel":
                connection = await self._connection_for_consume() or connection
            try:
                await asyncio.wait_for(connection.ready(), timeout=_CHANNEL_READY_TIMEOUT_S)
            except TimeoutError as exc:
                raise BrokerConnectionError(
                    f"connection not ready after {_CHANNEL_READY_TIMEOUT_S}s "
                    f"(reopening {attr!r} on {sanitize_url(self._url)!r})"
                ) from exc
            try:
                if attr == "_publish_channel" and self._publish_channel_factory is not None:
                    channel = await self._publish_channel_factory()
                else:
                    # Gebunden, weil dieser Aufruf die Kanalsperre haelt:
                    # Channel.Open ist ein RPC ohne eigene Schranke und kehrt
                    # auf einem still toten Transport nie zurueck. Ungebunden
                    # wartet jeder andere Sperr-Interessent mit — auch der
                    # Verwurf nach einem Deadline-Bruch, der dadurch seine
                    # eigene Frist ueberschreiten wuerde.
                    # Nur der Publish-Kanal meldet basic.return (BZ-03).
                    opened = (
                        connection.channel(on_return_raises=True)
                        if attr == "_publish_channel"
                        else connection.channel()
                    )
                    channel = await asyncio.wait_for(opened, timeout=_CHANNEL_READY_TIMEOUT_S)
            except Exception as exc:
                raise BrokerConnectionError(
                    f"channel reopen failed for {attr!r} on {sanitize_url(self._url)!r}: {exc}"
                ) from exc
            setattr(self, attr, channel)
            return channel, True

    def _admin_connection(self) -> aio_pika.abc.AbstractConnection | None:
        """Verbindung fuer Admin-Lesen: die Consume-Verbindung, falls offen
        (BZ-16) -- im Broker-Alarm haengt auf der publizierenden Verbindung
        jeder Frame, auch ein passives Declare --, sonst wie bisher."""
        consume = self._consume_connection
        if consume is not None and not consume.is_closed:
            return consume
        return self._connection

    async def _throwaway_channel_message_count(self, name: str) -> int:
        """Message count of one queue via passive declare on a throwaway
        channel (a failed passive-declare closes the channel). A missing
        queue counts as 0. Shared by the admin depth queries."""
        connection = self._admin_connection()
        if connection is None or connection.is_closed:
            raise BrokerConnectionError("broker is not connected")
        channel: aio_pika.abc.AbstractChannel | None = None
        try:
            channel = await connection.channel()
            q = await channel.declare_queue(name, passive=True)
            result = getattr(q, "declaration_result", None)
            if result is not None:
                return int(getattr(result, "message_count", 0))
        except aio_pika.exceptions.ChannelNotFoundEntity:
            # Der Broker hat den Channel geschlossen (``is_closed`` ist schon
            # True): trotzdem ``close()``, sonst oeffnet ein RobustChannel
            # sich wieder und bleibt offen (Review MR 10, P0-1).
            await close_channel_quietly(channel)
            channel = None
            return 0
        except Exception as exc:
            raise BrokerConnectionError(str(exc)) from exc
        else:
            return 0
        finally:
            if channel is not None and not channel.is_closed:
                await close_channel_quietly(channel)
