"""RabbitMQ: Veröffentlichen, Zustellen und die vier Verben (Plan U5, KTD2).

Abbildung auf AMQP: ``redeliver`` → ``nack(requeue=True)``; ``retry_later`` →
Publish an ``{queue}.DQ`` mit Per-Message-``expiration`` + ``ack``;
``dead_letter`` → Publish an ``{queue}.XQ`` + ``ack``. Beide werfen
``BrokerConnectionError``, wenn der Publish scheitert, und lassen das Original
unbestätigt. Die alten Methoden ``nack``, ``publish_to_delay_queue`` und
``publish_to_dead_letter_queue`` bleiben, bis der Worker in U7/U8 auf die
Verben wechselt.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from typing import Any, cast

import aio_pika
import structlog
from aio_pika.exceptions import DeliveryError

from swarmq.broker.rabbitmq.connection import (
    _CHANNEL_TRANSIENT_ERRORS,
    _PUBLISH_MAX_ATTEMPTS,
    RabbitMQConnection,
)
from swarmq.contracts import Delivery, DeliveryCallback, PublishReceipt
from swarmq.exceptions import BrokerConnectionError

logger = structlog.get_logger()


def message_of(delivery: object) -> aio_pika.abc.AbstractIncomingMessage:
    """Das aio_pika-Handle einer Zustellung. Akzeptiert bis U7/U8 auch das
    rohe aio_pika-Objekt (Worker-Pfade, die noch nicht auf ``Delivery``
    umgestellt sind)."""
    handle = delivery.handle if isinstance(delivery, Delivery) else delivery
    return cast("aio_pika.abc.AbstractIncomingMessage", handle)


class RabbitMQDelivery(RabbitMQConnection):
    # ------------------------------------------------------------------
    # Deklaration
    # ------------------------------------------------------------------

    async def declare_queue(self, queue_name: str) -> None:
        pub = await self._ensure_publish_channel()
        # Main queue — DLX points to dead-letter queue
        main_args: dict[str, Any] = {
            "x-queue-type": self._queue_type,
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": f"{queue_name}.XQ",
        }
        try:
            await self._declare_queue_triplet(pub, queue_name, main_args)
        except aio_pika.exceptions.AMQPChannelError as exc:
            # ce-review REL-05: a pre-existing queue with mismatched
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
        # Quorum-queue safety net (ce-review / U2.1 skip): without
        # x-delivery-limit a deterministically-crashing task
        # (SIGSEGV/OOM) is redelivered forever by the broker, never
        # reaching max_retries. 20 covers ~3-4 task-level retries per
        # delivery plus generous broker-hiccup headroom while still
        # providing a terminal DLQ path.
        if self._queue_type == "quorum":
            main_args["x-delivery-limit"] = 20
        await pub.declare_queue(queue_name, durable=True, arguments=main_args)
        # Delay queue — DLX points back to main queue
        await pub.declare_queue(
            f"{queue_name}.DQ",
            durable=True,
            arguments={
                "x-queue-type": self._queue_type,
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": queue_name,
            },
        )
        # Dead-letter queue
        await pub.declare_queue(
            f"{queue_name}.XQ",
            durable=True,
            arguments={"x-queue-type": self._queue_type},
        )

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

    async def publish_to_delay_queue(
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

    async def publish_to_dead_letter_queue(
        self,
        queue_name: str,
        message_body: bytes,
        headers: Mapping[str, Any] | None = None,
    ) -> None:
        msg = aio_pika.Message(
            body=message_body,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            headers=dict(headers or {}),
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
        last_exc: BaseException | None = None
        for attempt in range(_PUBLISH_MAX_ATTEMPTS):
            pub = await self._ensure_publish_channel()
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
                # Drop the potentially broken channel so the next
                # _ensure_publish_channel recreates it.
                if self._publish_channel is not None:
                    try:
                        if not self._publish_channel.is_closed:
                            await self._publish_channel.close()
                    except Exception:
                        pass
                    self._publish_channel = None
                # Skip the backoff sleep on the final attempt — we are
                # about to raise anyway. Saves up to 800ms latency at
                # exhaustion (ce-review R-4).
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
        # only re-applies after a recreate (ce-review R-2/C-02: avoid
        # an AMQP RPC per every consume-channel access).
        await chan.set_qos(prefetch_count=concurrency)
        amqp_queue = await chan.get_queue(queue)

        async def _on_raw(message: aio_pika.abc.AbstractIncomingMessage) -> None:
            delivery = Delivery(body=message.body, headers=dict(message.headers or {}), queue=queue, handle=message)
            await on_delivery(delivery)

        consumer_tag = await amqp_queue.consume(_on_raw)
        self._consumer_tags.append((amqp_queue, consumer_tag))

    async def cancel_consume(self) -> None:
        """Cancel all active consumers to stop receiving new messages.

        Per-consumer error isolation (2026-06-02 audit): with multiple
        queues/priority-bands a single ``queue.cancel`` that raises (e.g.
        channel transition, AMQP error) must not abort cancellation of the
        remaining consumers, nor leave ``_consumer_tags`` populated. A leak
        here would let the worker keep pulling messages during the DRAINING
        window and could abort the graceful-shutdown sequence
        (in-flight drain, lock release, scheduler stop) in _execute_shutdown.
        """
        errors: list[Exception] = []
        for amqp_queue, consumer_tag in self._consumer_tags:
            try:
                await amqp_queue.cancel(consumer_tag)
            except Exception as exc:
                errors.append(exc)
                logger.warning("consumer_cancel_failed", consumer_tag=consumer_tag, error=str(exc))
        self._consumer_tags.clear()
        if errors:
            # Surface that cancellation was imperfect, but only after every
            # consumer has been attempted and the registry cleared.
            raise BrokerConnectionError(
                f"cancel_consume: {len(errors)} consumer(s) failed to cancel; first error: {errors[0]}"
            ) from errors[0]

    # ------------------------------------------------------------------
    # Die vier Verben (KTD2) + Legacy ``nack`` bis U7/U8
    # ------------------------------------------------------------------

    async def ack(self, delivery: Delivery) -> None:
        await message_of(delivery).ack()

    async def nack(self, delivery: Delivery, requeue: bool = False) -> None:
        """Legacy (bis U7/U8): ``requeue=True`` entspricht ``redeliver``,
        ``requeue=False`` entspricht ``BrokerAdmin.route_via_dlx``."""
        await message_of(delivery).nack(requeue=requeue)

    async def redeliver(self, delivery: Delivery) -> None:
        await message_of(delivery).nack(requeue=True)

    async def retry_later(self, delivery: Delivery, *, delay_ms: int, headers: Mapping[str, Any]) -> None:
        await self.publish_to_delay_queue(delivery.queue, delivery.body, delay_ms, headers)
        await self.ack(delivery)

    async def dead_letter(self, delivery: Delivery, *, headers: Mapping[str, Any]) -> None:
        await self.publish_to_dead_letter_queue(delivery.queue, delivery.body, headers)
        await self.ack(delivery)
