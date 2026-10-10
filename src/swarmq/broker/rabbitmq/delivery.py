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
from aio_pika.exceptions import ChannelInvalidStateError, DeliveryError

from swarmq.broker.rabbitmq.connection import (
    _CHANNEL_TRANSIENT_ERRORS,
    _PUBLISH_MAX_ATTEMPTS,
    RabbitMQConnection,
)
from swarmq.broker.rabbitmq.consumer_watch import ConsumerWatch, ReregisteredHook
from swarmq.broker.rabbitmq.header_guard import guard_headers, negotiated_frame_max
from swarmq.broker.rabbitmq.topology import (
    dead_letter_queue_arguments,
    delay_queue_arguments,
    main_queue_arguments,
    queue_family,
)
from swarmq.contracts import Delivery, DeliveryCallback, PublishReceipt
from swarmq.exceptions import BrokerConnectionError, PublishIndeterminate, SettleFailedAfterPublish
from swarmq.headers import HEADER_DEAD_LETTER_TTL_MS, HEADER_DEAD_LETTERED_AT
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


async def settle(action: Awaitable[None], verb: str) -> None:
    """Fuehrt ein Abschluss-Verb (ack/nack) aus und uebersetzt einen
    Kanal- oder Verbindungsverlust in ``BrokerConnectionError`` (B22).

    Der Ausgang ist dabei klar: RabbitMQ stellt unbestaetigte Nachrichten
    nach dem Kanalverlust neu zu. Der Abbruch der eigenen Task geht wie im
    Publish-Pfad unveraendert durch; nur aio_pikas interne
    ``CancelledError`` zaehlt als Kanalverlust."""
    try:
        await action
    except _CHANNEL_TRANSIENT_ERRORS as exc:
        if isinstance(exc, asyncio.CancelledError):
            current = asyncio.current_task()
            if current is not None and current.cancelling() > 0:
                raise
        raise BrokerConnectionError(f"{verb} failed, channel or connection lost: {exc}") from exc


class RabbitMQDelivery(RabbitMQConnection):
    #: Höchstens so lange wartet ein Settle auf einen vom Broker beendeten
    #: Consumer auf den eigenen Cancel des Wächters (U7, KTD4). Deckt einen
    #: Takt mit Channel-, Cancel- und Consume-RPC (je 2 s) mit Luft ab.
    settle_cancel_wait_s: float = 10.0
    #: Wanduhr fuer ``x-swarmq-dead-lettered-at`` und die Rest-TTL (KTD6).
    clock: Callable[[], float] = staticmethod(time.time)

    # ------------------------------------------------------------------
    # Deklaration
    # ------------------------------------------------------------------

    async def declare_queue(self, queue_name: str) -> None:
        pub = await self._ensure_publish_channel()
        # Main queue — DLX points to dead-letter queue
        main_args = main_queue_arguments(queue_name, self._queue_type, self.delivery_limit)
        try:
            await self._declare_queue_triplet(pub, queue_name, main_args)
        except aio_pika.exceptions.AMQPChannelError as exc:
            # A pre-existing queue with mismatched
            # arguments raises aio_pika ChannelClosed (PRECONDITION_FAILED).
            # Without this wrap, the CLI surfaces a raw aio_pika traceback
            # instead of a clean 'queue arg conflict' message.
            raise BrokerConnectionError(
                f"declare_queue {queue_name!r} failed: {exc}. "
                f"Existing queue likely has different x-queue-type or "
                f"x-dead-letter-* args. Delete the queue via the RabbitMQ "
                f"management UI / CLI and retry, or match the existing args."
            ) from exc

    async def _declare_queue_triplet(
        self,
        pub: aio_pika.abc.AbstractChannel,
        queue_name: str,
        main_args: dict[str, Any],
    ) -> None:
        main, delay, dead_letter = queue_family(queue_name)
        await pub.declare_queue(main, durable=True, arguments=main_args)
        # Delay queue — DLX points back to main queue
        await pub.declare_queue(delay, durable=True, arguments=delay_queue_arguments(queue_name, self._queue_type))
        # Dead-letter queue
        await pub.declare_queue(dead_letter, durable=True, arguments=dead_letter_queue_arguments(self._queue_type))

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
    ) -> None:
        msg = aio_pika.Message(
            body=message_body,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            expiration=delay_ms / 1000,
            headers=dict(headers or {}),
        )
        await self._publish_with_retry(f"{queue_name}.DQ", msg)

    async def _publish_to_dead_letter_queue(
        self,
        queue_name: str,
        message_body: bytes,
        headers: Mapping[str, Any] | None = None,
    ) -> None:
        msg = aio_pika.Message(
            body=message_body,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            expiration=self.dead_letter_ttl_s,
            headers={
                **(headers or {}),
                HEADER_DEAD_LETTERED_AT: int(self.clock() * 1000),
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
        current_task().cancelling() indicates our task is being
        cancelled.
        """
        # Vor jedem Versuch und ohne Wiederholung: zu grosse Header schloessen
        # die ganze Verbindung (BZ-17).
        msg.headers = guard_headers(msg.headers, negotiated_frame_max(self._connection)) or {}
        timeout_s = self._effective_publish_timeout()
        # Pro-Aufruf-Container statt Instanzzustand: bei nebenlaeufigen
        # Publishes darf der Verwurf nur den Kanal treffen, auf dem GENAU
        # dieser Vorgang haengen blieb.
        in_flight: list[aio_pika.abc.AbstractChannel] = []
        try:
            async with asyncio.timeout(timeout_s) as timeout_cm:
                # Registrieren, damit ein beginnendes Herunterfahren diesen
                # bereits laufenden Deckel nachziehen kann (R12, KTD9).
                self._active_publish_timeouts.add(timeout_cm)
                try:
                    return await self._publish_attempts(routing_key, msg, in_flight)
                finally:
                    self._active_publish_timeouts.discard(timeout_cm)
        except TimeoutError as exc:
            # Der Deckel ist gerissen. Ohne Verwurf bliebe die tote
            # Verbindung stehen und jeder Folge-Publish verbrennte erneut
            # die volle Frist (R10).
            await self._discard_publish_channel(in_flight[-1] if in_flight else None)
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
                f"publish to {routing_key!r} exceeded the publish deadline "
                f"of {timeout_s}s; delivery is indeterminate"
            ) from exc

    async def _publish_attempts(
        self,
        routing_key: str,
        msg: aio_pika.Message,
        in_flight: list[aio_pika.abc.AbstractChannel],
    ) -> None:
        """Die bounded-retry-Schleife. Der Zeitdeckel liegt beim Aufrufer,
        damit er den gesamten Vorgang umspannt und nicht den Einzelversuch
        (KTD1). ``in_flight`` traegt den zuletzt benutzten Kanal nach oben,
        damit der Verwurf genau ihn trifft."""
        last_exc: BaseException | None = None
        for attempt in range(_PUBLISH_MAX_ATTEMPTS):
            pub = await self._ensure_publish_channel()
            in_flight.append(pub)
            try:
                await pub.default_exchange.publish(msg, routing_key=routing_key)
                return
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
                if attempt < _PUBLISH_MAX_ATTEMPTS - 1:
                    await asyncio.sleep(0.05 * (2**attempt))
            except _CHANNEL_TRANSIENT_ERRORS as exc:
                # Distinguish task-level cancellation from aio_pika's
                # internal drain cancellation. If our own task is being
                # cancelled, propagate.
                if isinstance(exc, asyncio.CancelledError):
                    current = asyncio.current_task()
                    if current is not None and current.cancelling() > 0:
                        raise
                last_exc = exc
                # Den womoeglich kaputten Kanal verwerfen, damit der
                # naechste _ensure_publish_channel neu oeffnet. Derselbe
                # Weg wie beim Deadline-Bruch: Identitaetspruefung unter
                # der Sperre, Schliessen nebenlaeufig.
                await self._discard_publish_channel(pub)
                # Skip the backoff sleep on the final attempt — we are
                # about to raise anyway. Saves up to 800ms latency at
                # exhaustion.
                if attempt < _PUBLISH_MAX_ATTEMPTS - 1:
                    await asyncio.sleep(0.05 * (2**attempt))
        raise BrokerConnectionError(
            f"publish to {routing_key!r} failed after {_PUBLISH_MAX_ATTEMPTS} attempts: {last_exc}"
        ) from last_exc

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
        # Apply QoS on the initial consume() — _ensure_consume_channel
        # only re-applies after a recreate (avoids an AMQP RPC per
        # consume-channel access).
        await chan.set_qos(prefetch_count=concurrency)
        amqp_queue = await chan.get_queue(queue)

        async def _on_raw(message: aio_pika.abc.AbstractIncomingMessage) -> None:
            delivery = Delivery(body=message.body, headers=dict(message.headers or {}), queue=queue, handle=message)
            await on_delivery(delivery)

        consumer_tag = await amqp_queue.consume(_on_raw)
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
        await self._publish_to_delay_queue(delivery.queue, delivery.body, delay_ms, headers)
        await self._ack_after_publish(delivery)

    async def dead_letter(self, delivery: Delivery, *, headers: Mapping[str, Any]) -> None:
        await self._publish_to_dead_letter_queue(delivery.queue, delivery.body, headers)
        await self._ack_after_publish(delivery)
