"""RabbitMQ: Veröffentlichen, Zustellen und die vier Verben (Plan U5, KTD2).

Abbildung auf AMQP: ``redeliver`` → ``nack(requeue=True)``; ``retry_later`` →
Publish an ``{queue}.DQ`` mit Per-Message-``expiration`` + ``ack``;
``dead_letter`` → Publish an ``{queue}.XQ`` + ``ack``. Beide werfen
``BrokerConnectionError``, wenn der Publish scheitert, und lassen das Original
unbestätigt. Ab RabbitMQ 4.3 hält ``_may_settle`` ein Settle auf einen vom
Broker beendeten Consumer zurück, bis der Wächter ihn selbst abgemeldet hat
(Plan 2026-10-08-1541, U7, KTD4). Die Legacy-Namen (``nack``,
``publish_to_*``) sind seit U15 entfernt; die Delay-/DLQ-Publishes sind
private Helfer der Verben.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Mapping
from typing import Any, cast

import aio_pika
from aio_pika.exceptions import ChannelInvalidStateError, ChannelNotFoundEntity, DeliveryError, PublishError

from swarmq.broker.rabbitmq.blocked import UNREADABLE, blocked_waiters, wait_until_unblocked
from swarmq.broker.rabbitmq.connection import (
    _CHANNEL_TRANSIENT_ERRORS,
    _PUBLISH_MAX_ATTEMPTS,
)
from swarmq.broker.rabbitmq.consumer_watch import ConsumerWatch, ReregisteredHook
from swarmq.broker.rabbitmq.declarations import RabbitMQDeclarations
from swarmq.broker.rabbitmq.header_guard import guard_headers, negotiated_frame_max
from swarmq.broker.rabbitmq.publish_phase import enqueued_before_failure, sent_before_failure
from swarmq.broker.rabbitmq.redeclare import publish_redeclaring_once
from swarmq.contracts import Delivery, DeliveryCallback, PublishReceipt
from swarmq.exceptions import (
    BrokerConnectionError,
    BrokerPublishBlocked,
    PublishCancelledUnsent,
    PublishIndeterminate,
    PublishRejected,
    SettleFailedAfterPublish,
    UnroutableMessage,
)
from swarmq.headers import HEADER_DEAD_LETTER_TTL_MS, HEADER_DEAD_LETTERED_AT, LOOP_STATE_HEADERS
from swarmq.identity import dead_letter_generation
from swarmq.logging import _error_fields, get_logger

logger = get_logger(__name__)


def message_of(delivery: object) -> aio_pika.abc.AbstractIncomingMessage:
    """Das aio_pika-Handle einer Zustellung. Akzeptiert bis U7/U8 auch das
    rohe aio_pika-Objekt (Worker-Pfade, die noch nicht auf ``Delivery``
    umgestellt sind)."""
    handle = delivery.handle if isinstance(delivery, Delivery) else delivery
    return cast("aio_pika.abc.AbstractIncomingMessage", handle)


def dead_letter_ttl_header(ttl_s: float | None) -> int | str:
    """Wert von ``x-swarmq-dead-letter-ttl-ms``: Millisekunden oder
    ``"none"`` (unbegrenzt)."""
    return "none" if ttl_s is None else int(ttl_s * 1000)


def _cancelling_now() -> int:
    """``cancelling()`` der laufenden Task (0 ausserhalb einer Task)."""
    current = asyncio.current_task()
    return current.cancelling() if current is not None else 0


def _is_own_cancel(exc: BaseException, cancelling_at_entry: int) -> bool:
    """Ist *exc* der Abbruch der eigenen Task (KTD9, wie ``asyncio.Timeout``)?

    Nur ein Anstieg von ``cancelling()`` gegenueber *cancelling_at_entry*
    zaehlt: Ein vom Drain abgebrochener Handler laeuft nach E-001 mit
    ``cancelling() > 0`` weiter. Jede andere ``CancelledError`` stammt aus
    aio-pika (Kanal- oder Verbindungsverlust) und heisst "nicht erledigt"."""
    return isinstance(exc, asyncio.CancelledError) and _cancelling_now() > cancelling_at_entry


async def settle(action: Awaitable[None], verb: str) -> None:
    """Fuehrt ein Abschluss-Verb (ack/nack) aus und uebersetzt einen
    Kanal- oder Verbindungsverlust in ``BrokerConnectionError`` (B22).

    Der Ausgang ist dabei klar: RabbitMQ stellt unbestaetigte Nachrichten
    nach dem Kanalverlust neu zu. Der Abbruch der eigenen Task geht wie im
    Publish-Pfad unveraendert durch; nur aio_pikas interne
    ``CancelledError`` zaehlt als Kanalverlust.

    "Eigener Abbruch" heisst ein Anstieg von ``cancelling()`` gegenueber dem
    Eintritt (KTD9, wie ``asyncio.Timeout``): Ein vom Drain abgebrochener
    Handler laeuft nach E-001 mit ``cancelling() > 0`` weiter; ein
    Kanalverlust beim ``nack`` darf dort nicht als Abbruch durchgehen, sonst
    schriebe die Pipeline den Completed-Marker auf eine unbestaetigte
    Nachricht (fix-r-flow.md 2.2)."""
    cancelling_at_entry = _cancelling_now()
    try:
        await action
    except _CHANNEL_TRANSIENT_ERRORS as exc:
        if _is_own_cancel(exc, cancelling_at_entry):
            raise
        raise BrokerConnectionError(f"{verb} failed, channel or connection lost: {exc}") from exc


async def _backoff_unless_last(attempt: int) -> None:
    """Backoff zwischen Publish-Versuchen. Nach dem letzten nicht: Es folgt
    ohnehin die Ausnahme, gespart sind bis zu 800 ms."""
    if attempt < _PUBLISH_MAX_ATTEMPTS - 1:
        await asyncio.sleep(0.05 * (2**attempt))


def _channel_open_failed(exc: BrokerConnectionError) -> bool:
    """Ob ``_ensure_publish_channel`` am Oeffnen selbst scheiterte (F4-31).

    Nur dann lohnt ein weiterer Versuch. Ohne Ursache ist der Broker
    geschlossen; ein ``TimeoutError`` hat schon die volle Bereitschafts- oder
    Oeffnungsfrist gewartet, eine Wiederholung vervielfachte sie."""
    cause = exc.__cause__
    return cause is not None and not isinstance(cause, TimeoutError)


def _raise_if_sent(routing_key: str, exc: BaseException) -> None:
    """B24: Hatte aiormq die Nachricht schon gesendet und fehlt nur das
    Confirm, ist der Ausgang unbestimmt -- gleich, mit welcher Ausnahme die
    offenen Futures scheiterten. Kein erneutes Senden, sonst liefe der Task
    womoeglich doppelt; der Aufrufer bekommt ``PublishIndeterminate``."""
    if sent_before_failure(exc):
        raise PublishIndeterminate(
            f"publish to {routing_key!r} lost the connection while waiting for the broker's "
            f"confirm; delivery is indeterminate: {exc}"
        ) from exc


async def _publish_once(
    pub: aio_pika.abc.AbstractChannel,
    msg: aio_pika.Message,
    routing_key: str,
    maybe_sent: list[bool],
) -> None:
    """Ein Publish-Versuch (KTD9, a3).

    ``maybe_sent`` bekommt unmittelbar vor dem Senden einen Eintrag;
    ``False`` setzt ihn nur ein Abbruch vor dem Einreihen der Frames (E-042).
    Wer beim Schliessen des Kanals in der Warteschlange der Kanal-Sperre steht,
    endet seit aiormq 7.2 selbst mit ``ChannelInvalidStateError`` (#240, FS-08);
    ein eigener Waechter gegen ``closing`` ist nicht mehr noetig."""
    attempt = len(maybe_sent)
    maybe_sent.append(True)
    try:
        await pub.default_exchange.publish(msg, routing_key=routing_key)
    except BaseException as exc:
        if not enqueued_before_failure(exc):
            maybe_sent[attempt] = False
        raise


class RabbitMQDelivery(RabbitMQDeclarations):
    #: Höchstens so lange wartet ein Settle auf einen vom Broker beendeten
    #: Consumer auf den eigenen Cancel des Wächters (U7, KTD4). Deckt einen
    #: Takt mit Channel-, Cancel- und Consume-RPC (je 2 s) mit Luft ab.
    settle_cancel_wait_s: float = 10.0
    #: Wanduhr fuer ``x-swarmq-dead-lettered-at`` und die Rest-TTL (KTD6).
    clock: Callable[[], float] = staticmethod(time.time)
    #: Fail-open beim unlesbaren Blocked-Zustand meldet je Broker einmal (BZ-16).
    _blocked_state_unreadable_reported: bool = False

    # ------------------------------------------------------------------
    # Publish
    # ------------------------------------------------------------------

    async def publish(self, queue: str, body: bytes, headers: Mapping[str, Any] | None = None) -> PublishReceipt:
        msg = aio_pika.Message(
            body=body,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            headers=dict(headers) if headers else None,
        )
        await self._publish_with_retry(queue, msg)
        return PublishReceipt(queue=queue, message_id=msg.message_id)

    async def _publish_to_delay_queue(
        self,
        queue_name: str,
        message_body: bytes,
        delay_ms: int,
        headers: Mapping[str, Any] | None = None,
        target: str | None = None,
    ) -> None:
        msg = aio_pika.Message(
            body=message_body,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            expiration=delay_ms / 1000,
            headers=dict(headers or {}),
        )
        await self._publish_with_retry(target or f"{queue_name}.DQ", msg)

    async def _publish_to_dead_letter_queue(
        self,
        queue_name: str,
        message_body: bytes,
        headers: Mapping[str, Any] | None = None,
        *,
        at_ms: int | None = None,
    ) -> None:
        # Q6 (Plan 0.12.0, MR 11, MR 13): Zustands-Header der Rueckstellschleifen
        # enden am DLQ-Stempel; ein ``dlq retry`` beginnt mit frischer Frist.
        stamped = {k: v for k, v in (headers or {}).items() if k not in LOOP_STATE_HEADERS}
        msg = aio_pika.Message(
            body=message_body,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            expiration=self.dead_letter_ttl_s,
            headers={
                **stamped,
                HEADER_DEAD_LETTERED_AT: int(self.clock() * 1000) if at_ms is None else at_ms,
                HEADER_DEAD_LETTER_TTL_MS: dead_letter_ttl_header(self.dead_letter_ttl_s),
            },
        )
        await self._publish_with_retry(f"{queue_name}.XQ", msg)

    async def _publish_with_retry(self, routing_key: str, msg: aio_pika.Message) -> None:
        """Publish with bounded retry across reconnect transitions.

        aio_pika's RobustChannel recovers automatically, but operations
        initiated during the transition window fail with
        ChannelInvalidStateError or CancelledError from the internal
        drain_future. We retry with exponential backoff, recreating
        the channel between attempts. Task-level cancellation (caller
        cancelled the publish) is preserved by re-raising when
        current_task().cancelling() rose above its value at entry (KTD9:
        a drain-cancelled handler keeps running with cancelling() > 0,
        E-001; its internal aio-pika cancel is a lost channel, not its own).
        Kam der eigene Abbruch, bevor ein Versuch gesendet haben kann
        (``maybe_sent``, E-042), geht er als ``PublishCancelledUnsent`` weiter
        (Plan 0.14, U4).
        """
        cancelling_at_entry = _cancelling_now()
        # Vor jedem Versuch und ohne Wiederholung: zu grosse Header schloessen
        # die ganze Verbindung (BZ-17).
        msg.headers = guard_headers(msg.headers, negotiated_frame_max(self._connection)) or {}
        self._raise_if_publish_deadline_passed(routing_key)
        timeout_s = self._effective_publish_timeout()
        # Pro-Aufruf-Container statt Instanzzustand: bei nebenlaeufigen
        # Publishes darf der Verwurf nur den Kanal treffen, auf dem GENAU
        # dieser Vorgang haengen blieb.
        in_flight: list[aio_pika.abc.AbstractChannel] = []
        blocked: list[bool] = []
        # a3 (KTD9): ein Eintrag je Versuch, gesetzt unmittelbar vor dem
        # Senden; ein als "nicht gesendet" erkannter Versuch nimmt nur seinen
        # eigenen zurueck, ein negatives Confirm bleibt stehen (E-041).
        maybe_sent: list[bool] = []
        try:
            # Ein beginnendes Herunterfahren zieht diesen Deckel nach (R12, KTD9).
            async with self._bounded_until_shutdown(timeout_s):
                await self._wait_while_blocked(routing_key, blocked)
                return await self._publish_attempts(routing_key, msg, in_flight, maybe_sent, cancelling_at_entry)
        except asyncio.CancelledError as exc:
            # U4 (Plan 0.14, E-041, E-042): Der eigene Abbruch, bevor ein
            # Versuch gesendet haben kann (Blocked-Warten, Kanalaufbau,
            # Sperren-Warteschlange, Backoff nach nicht gesendeten Versuchen),
            # bleibt ein Abbruch, traegt aber "nichts gesendet" zum Client.
            if _is_own_cancel(exc, cancelling_at_entry) and not any(maybe_sent):
                raise PublishCancelledUnsent(*exc.args) from exc
            raise
        except TimeoutError as exc:
            if blocked:
                # Nichts gesendet, der Kanal ist gesund: kein Verwurf, und
                # anders als ``PublishIndeterminate`` sicher wiederholbar.
                raise BrokerPublishBlocked(
                    f"publish to {routing_key!r} not sent: the broker blocked the publish "
                    f"connection (resource alarm) for the whole publish deadline of {timeout_s}s"
                ) from exc
            # Der Deckel ist gerissen. Ohne Verwurf bliebe die tote
            # Verbindung stehen und jeder Folge-Publish verbrennte erneut
            # die volle Frist (R10).
            await self._discard_publish_channel(in_flight[-1] if in_flight else None)
            if not any(maybe_sent):
                # a3 (KTD9, E-042): Die Frist riss vor dem Senden, etwa beim
                # Warten auf die Verbindung oder im Backoff nach einem nicht
                # gesendeten Versuch. Sicher wiederholbar, nicht unbestimmt.
                logger.warning(
                    "publish_deadline_exceeded",
                    routing_key=routing_key,
                    timeout_s=timeout_s,
                    sent=False,
                    hint="publish was not sent within the configured budget; it is safe to retry",
                )
                raise BrokerConnectionError(
                    f"publish to {routing_key!r} not sent within the publish deadline of {timeout_s}s"
                ) from exc
            logger.warning(
                "publish_deadline_exceeded",
                routing_key=routing_key,
                timeout_s=timeout_s,
                hint=(
                    "publish did not complete within the configured budget; "
                    "delivery is indeterminate — the broker may already hold "
                    "the message"
                ),
            )
            raise PublishIndeterminate(
                f"publish to {routing_key!r} exceeded the publish deadline of {timeout_s}s; delivery is indeterminate"
            ) from exc

    def _raise_if_publish_deadline_passed(self, routing_key: str) -> None:
        """FS-02 Teil a (KTD3, E-042): Ist die Publish-Endfrist schon
        verstrichen, geht nichts auf die Leitung.

        ``asyncio.timeout(0)`` bricht erst beim ersten Suspendieren ab; auf
        aiormqs Fast-Path liegt der Frame da schon in der ``write_queue`` und
        wird geschrieben. Deshalb vor dem Zeitdeckel pruefen, ohne Kanal und
        ohne Verwurf: Der Aufrufer bekommt ein sicher wiederholbares "nicht
        gesendet" statt ``PublishIndeterminate``."""
        deadline = self._publish_deadline_at
        if deadline is not None and asyncio.get_running_loop().time() >= deadline:
            raise BrokerConnectionError(
                f"publish to {routing_key!r} not sent: the shutdown publish deadline has passed"
            )

    async def _wait_while_blocked(self, routing_key: str, blocked: list[bool]) -> None:
        """Wartet, solange der Broker die Publish-Verbindung blockiert (BZ-16).

        Unblockiert kostet das nur ein synchrones Lesen -- kein Roundtrip,
        kein ``wait_for``. Blockiert, wuerde ein Publish auf dem Socket
        haengen und als ``PublishIndeterminate`` enden; das Warten innerhalb
        der Publish-Frist sendet nichts, bis der Alarm vorbei ist.

        Gewartet wird auf ``unblocked`` oder ``closing`` derselben
        aiormq-Verbindung (F4-21): aiormq setzt das Event beim Schliessen
        nicht. Endet die Verbindung, wird der Zustand neu gelesen: Hat aio-pika
        schon neu verbunden und ist die neue Verbindung blockiert, wird auf
        sie weiter gewartet. Ohne Verbindung oder noch auf der toten endet das
        Warten, und ``_publish_attempts`` wartet auf den Reconnect; den
        Blocked-Zustand der neuen Verbindung prueft es nicht erneut (E-025,
        Grenze D-1), ein Publish auf ihr haengt dann bis zur Frist. Die Frist
        bleibt die Obergrenze."""
        while True:
            waiters = blocked_waiters(self._connection)
            if not isinstance(waiters, tuple):
                break
            if not blocked:
                logger.warning(
                    "publish_connection_blocked",
                    routing_key=routing_key,
                    hint=(
                        "the broker blocked the publish connection (resource alarm: memory or disk); "
                        "publishes wait until it is unblocked or the publish deadline expires"
                    ),
                )
                blocked.append(True)
            await wait_until_unblocked(waiters, timeout=None)
        blocked.clear()
        if waiters is UNREADABLE and not self._blocked_state_unreadable_reported:
            self._blocked_state_unreadable_reported = True
            logger.warning(
                "publish_blocked_state_unreadable",
                hint=(
                    "cannot read the connection.blocked state from aiormq; publishes run "
                    "unchecked (fail-open) and hang on the socket during a broker alarm"
                ),
            )

    async def _publish_attempts(
        self,
        routing_key: str,
        msg: aio_pika.Message,
        in_flight: list[aio_pika.abc.AbstractChannel],
        maybe_sent: list[bool],
        cancelling_at_entry: int,
    ) -> None:
        """Die bounded-retry-Schleife. Der Zeitdeckel liegt beim Aufrufer,
        damit er den gesamten Vorgang umspannt und nicht den Einzelversuch
        (KTD1). ``in_flight`` traegt den zuletzt benutzten Kanal nach oben,
        damit der Verwurf genau ihn trifft, ``maybe_sent``, ob ein Versuch
        womoeglich gesendet hat (a3). ``cancelling_at_entry`` ist der Stand
        beim Eintritt in ``_publish_with_retry`` (KTD9)."""
        last_exc: BaseException | None = None
        for attempt in range(_PUBLISH_MAX_ATTEMPTS):
            try:
                pub = await self._ensure_publish_channel()
            except BrokerConnectionError as exc:
                # F4-31 (E-042): Scheiterte das Oeffnen an der sterbenden
                # Verbindung, ist nichts gesendet; aio-pika verbindet neu.
                if not _channel_open_failed(exc):
                    raise
                last_exc = exc
                await _backoff_unless_last(attempt)
                continue
            in_flight.append(pub)
            sent_before_attempt = len(maybe_sent)
            try:
                await _publish_once(pub, msg, routing_key, maybe_sent)
            except PublishError as exc:
                # basic.return: keine Queue mit diesem Namen. Eine Wiederholung
                # aendert daran nichts; vor DeliveryError fangen (Unterklasse).
                self.invalidate_declaration(routing_key)
                raise UnroutableMessage(
                    f"publish to {routing_key!r} was returned unroutable: no queue with this name (BZ-03)"
                ) from exc
            except DeliveryError as exc:
                # A publisher-confirm Nack/Reject (e.g. a quorum queue briefly
                # without disk quorum) is a NEGATIVE confirm, not a channel
                # fault — the channel stays healthy, so retry WITHOUT tearing
                # it down. Persisting past the retry budget surfaces as a
                # neutral BrokerConnectionError (below), which callers — incl.
                # the workflow advance seam — treat as transient, so a
                # redelivery retries the idempotent publish rather than the
                # whole unit being marked permanently failed.
                last_exc = exc
                await _backoff_unless_last(attempt)
            except _CHANNEL_TRANSIENT_ERRORS as exc:
                # Distinguish task-level cancellation from aio_pika's
                # internal drain cancellation. If our own task is being
                # cancelled (relative to entry, KTD9), propagate.
                if _is_own_cancel(exc, cancelling_at_entry):
                    raise
                _raise_if_sent(routing_key, exc)
                # Nur der eigene Eintrag: ein frueherer Versuch mit negativem
                # Confirm hat womoeglich gesendet (E-041).
                del maybe_sent[sent_before_attempt:]
                last_exc = exc
                # Den womoeglich kaputten Kanal verwerfen, damit der
                # naechste _ensure_publish_channel neu oeffnet. Derselbe
                # Weg wie beim Deadline-Bruch: Identitaetspruefung unter
                # der Sperre, Schliessen nebenlaeufig.
                await self._discard_publish_channel(pub)
                await _backoff_unless_last(attempt)
            except Exception as exc:
                # Kein bekannter Kanalfehler (etwa ein pamqp-Framefehler, an
                # dem aiormqs Reader endete): nach dem Senden ebenso unbestimmt.
                _raise_if_sent(routing_key, exc)
                raise
            else:
                return
        # Review MR 15, P2-2: ein negatives Confirm haengt an der Queue, nicht
        # an der Verbindung (der Scheduler zaehlt es nicht fuer den Ruecktritt).
        error = PublishRejected if isinstance(last_exc, DeliveryError) else BrokerConnectionError
        message = f"publish to {routing_key!r} failed after {_PUBLISH_MAX_ATTEMPTS} attempts: {last_exc}"
        raise error(message) from last_exc

    # ------------------------------------------------------------------
    # Consume
    # ------------------------------------------------------------------

    async def consume(self, queue: str, on_delivery: DeliveryCallback, *, concurrency: int) -> None:
        """Jede Nachricht kommt als ``Delivery`` mit ``queue`` = Zustellungs-
        Queue und ``handle`` = aio_pika-Nachricht; ``concurrency`` ist der
        AMQP-Prefetch (höchstens so viele unbestätigte Zustellungen)."""
        # Remember the prefetch so _ensure_consume_channel can re-apply
        # it after a channel recreate.
        self._consume_prefetch = concurrency
        chan = await self._ensure_consume_channel()

        async def _on_raw(message: aio_pika.abc.AbstractIncomingMessage) -> None:
            delivery = Delivery(body=message.body, headers=dict(message.headers or {}), queue=queue, handle=message)
            await on_delivery(delivery)

        try:
            # Apply QoS on the initial consume() — _ensure_consume_channel
            # only re-applies after a recreate (avoids an AMQP RPC per
            # consume-channel access).
            await chan.set_qos(prefetch_count=concurrency)
            amqp_queue = await chan.get_queue(queue)
            consumer_tag = await amqp_queue.consume(_on_raw)
        except ChannelNotFoundEntity:
            # Die Queue ist weg: die naechste Deklaration legt sie neu an (MR 8).
            # Bleibt roh, darauf baut PF-20 / E-083 auf.
            self.invalidate_declaration(queue)
            raise
        except (aio_pika.exceptions.AMQPConnectionError, aio_pika.exceptions.InvalidFrameError, RuntimeError) as exc:
            # Verbindungsabriss beim Anmelden (aiormq ``ConnectionClosed``,
            # ``ChannelInvalidStateError``, ``InvalidFrameError`` (KTD4),
            # ``RuntimeError`` aus aio_pika): wie
            # in ``declare_queue`` ein ``BrokerConnectionError`` (F4-23, Review MR 8, H5a).
            raise BrokerConnectionError(
                f"consume {queue!r}: the connection was lost while subscribing: {exc!r}"
            ) from exc
        self._consumer_watch.add(queue, amqp_queue, _on_raw, consumer_tag)

    @property
    def consumer_watch(self) -> ConsumerWatch:
        """Der Consumer-Wächter (U6); Settle-Regeln (U7) stoßen ihn an und
        fragen die selbst abgemeldeten Tags ab."""
        return self._consumer_watch

    def start_consumer_watch(self, interval_s: float, on_reregistered: ReregisteredHook | None = None) -> None:
        """Startet den Consumer-Wächter (U6, KTD3) nach den ``consume``-Aufrufen.

        ``on_reregistered(queue, reason)`` zählt jede Neuanmeldung für die
        Metrik. ``cancel_consume`` und ``close`` stoppen den Wächter."""
        self._consumer_watch.on_reregistered = on_reregistered
        self._consumer_watch.start(interval_s)

    def consumer_gaps(self) -> dict[str, float]:
        """Je konsumierter Queue: Sekunden ohne aktiven Consumer (0.0 = aktiv).

        Leer vor dem ersten ``consume`` und nach ``cancel_consume``; Grundlage
        des Consumer-Checks von ``/ready`` (U6, R2)."""
        return self._consumer_watch.gaps()

    def consumer_missing_queues(self) -> list[str]:
        """Konsumierte Queues, die beim Broker fehlen (PF-20, E-083); ``/ready``
        nennt sie, bis es sie wieder gibt."""
        return self._consumer_watch.missing_queues()

    async def cancel_consume(self) -> None:
        """Cancel all active consumers to stop receiving new messages.

        Stoppt zuerst den Consumer-Wächter, damit er nichts mehr neu anmeldet,
        und meldet dann die aktuellen Tags ab — nach einer Neuanmeldung (U6)
        sind das nicht mehr die aus ``consume``.

        Per-consumer error isolation: with multiple
        queues/priority-bands a single ``queue.cancel`` that raises (e.g.
        channel transition, AMQP error) must not abort cancellation of the
        remaining consumers, nor leave the registry populated. A leak
        here would let the worker keep pulling messages during the DRAINING
        window and could abort the graceful-shutdown sequence
        (in-flight drain, lock release, scheduler stop) in _execute_shutdown.
        """
        errors = await self._consumer_watch.cancel_all()
        for consumer_tag, exc in errors:
            logger.warning("consumer_cancel_failed", consumer_tag=consumer_tag, **_error_fields(exc))
        if errors:
            # Surface that cancellation was imperfect, but only after every
            # consumer has been attempted and the registry cleared.
            first = errors[0][1]
            raise BrokerConnectionError(
                f"cancel_consume: {len(errors)} consumer(s) failed to cancel; first error: {first}"
            ) from first

    async def _may_settle(self, message: aio_pika.abc.AbstractIncomingMessage, verb: str) -> bool:
        """Settle-Regel ab RabbitMQ 4.3 (U7, KTD4): ``False`` heißt, das Verb
        wird nicht gesendet.

        Fehlt der Tag eines vom Wächter geführten Consumers auf dem noch
        offenen Channel, hat der Broker ihn per ``basic.cancel`` beendet. Ein
        Settle darauf weckte den Zombie-Consumer; es geht erst raus, nachdem
        der Wächter den Tag selbst abgemeldet hat (danach unschädlich, auf
        4.3.6 belegt). Kommt dieser Cancel nicht binnen
        ``settle_cancel_wait_s``, unterbleibt das Settle: Die Nachricht hat
        der Broker mit dem Cancel ohnehin zurückgenommen.

        Ohne Wartezeit durch: gültige Tags, von swarmq selbst abgemeldete
        (``cancel_consume``, Wächter — sie gelten nicht mehr als ``is_watched``)
        und geschlossene Channels (bis 4.2; ``settle`` übersetzt den Fehler
        in ``BrokerConnectionError``, B22).
        """
        tag = message.consumer_tag
        if tag is None or not self._consumer_watch.is_watched(tag):
            return True
        try:
            # ``consumers`` steht nur am konkreten aiormq-Channel, nicht am ABC.
            channel: Any = message.channel
        except ChannelInvalidStateError:
            return True
        if tag in channel.consumers:
            return True
        self._consumer_watch.request_check()
        if await self._consumer_watch.wait_cancelled(tag, self.settle_cancel_wait_s):
            logger.info("delivery_settle_deferred", verb=verb, consumer_tag=tag)
            return True
        logger.warning(
            "delivery_settle_withheld",
            verb=verb,
            consumer_tag=tag,
            wait_s=self.settle_cancel_wait_s,
            hint=(
                "consumer was cancelled by the broker and the own cancel did not "
                "complete in time; settle not sent to avoid reviving it — the broker "
                "redelivers the message"
            ),
        )
        return False

    # ------------------------------------------------------------------
    # Die vier Verben (KTD2)
    # ------------------------------------------------------------------

    async def _ack_after_publish(self, delivery: Delivery) -> None:
        """Ack des Originals nach dem Publish seiner Kopie: ein Kanalverlust
        wird ``SettleFailedAfterPublish`` -- die Kopie ist unterwegs (KTD5)."""
        try:
            await self.ack(delivery)
        except BrokerConnectionError as exc:
            raise SettleFailedAfterPublish(str(exc)) from exc

    async def ack(self, delivery: Delivery) -> None:
        message = message_of(delivery)
        if await self._may_settle(message, "ack"):
            await settle(message.ack(), "ack")

    async def redeliver(self, delivery: Delivery) -> None:
        message = message_of(delivery)
        if await self._may_settle(message, "redeliver"):
            await settle(message.nack(requeue=True), "redeliver")

    async def retry_later(self, delivery: Delivery, *, delay_ms: int, headers: Mapping[str, Any]) -> None:
        target = self._delay_target(delivery.queue, delay_ms)
        due = self.due_delay_tier(delivery.queue, delay_ms)
        if due is not None and await self.restore_delay_tier(due):
            # Nach der Pause (PF-21): erst Neu-Deklarieren, dann Publish.
            target = due

        async def publish() -> None:
            nonlocal target
            if target not in self._ready_delay_tiers and target != f"{delivery.queue}.DQ":
                # Die Delay-Stufe (BZ-01, MR 10) war unroutbar und ist verworfen:
                # nach dem Neu-Deklarieren wieder die Stufe, sonst ``{queue}.DQ``.
                target = self._delay_target(delivery.queue, delay_ms)
            await self._publish_to_delay_queue(delivery.queue, delivery.body, delay_ms, headers, target)

        # Unroutbar: eine Stufe wird allein neu deklariert, ``{queue}.DQ`` mit
        # der Familie (Review MR 10, P2-3).
        await publish_redeclaring_once(self, target, publish)
        await self._ack_after_publish(delivery)

    async def dead_letter(self, delivery: Delivery, *, headers: Mapping[str, Any]) -> None:
        # Neue Ausfuehrungsgeneration (Plan 0.12.0, MR 12, BZ-08): Generations-
        # ID und ``x-swarmq-dlq-gen`` entstehen mit ``x-swarmq-dead-lettered-at``
        # im selben Stempel; alle Dead-Letter-Pfade des Workers laufen hier durch.
        at_ms = int(self.clock() * 1000)
        stamped = dead_letter_generation(headers, delivery.body, at_ms)
        await publish_redeclaring_once(
            self,
            delivery.queue,
            lambda: self._publish_to_dead_letter_queue(delivery.queue, delivery.body, stamped, at_ms=at_ms),
        )
        await self._ack_after_publish(delivery)
