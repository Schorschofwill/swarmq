"""RabbitMQ broker implementation."""

import aio_pika

from swarmq.exceptions import BrokerConnectionError


class RabbitMQBroker:
    def __init__(self, url: str) -> None:
        self._url = url
        self._connection = None
        self._publish_channel = None
        self._consume_channel = None
        self._consumer_tags: list[tuple] = []  # list of (queue, consumer_tag)

    async def connect(self) -> None:
        if self._connection is not None:
            return
        try:
            self._connection = await aio_pika.connect_robust(self._url)
        except Exception as exc:
            raise BrokerConnectionError(str(exc)) from exc
        self._publish_channel = await self._connection.channel()
        self._consume_channel = await self._connection.channel()

    async def declare_queue(self, queue_name: str) -> None:
        # Main queue — DLX points to dead-letter queue
        await self._publish_channel.declare_queue(
            queue_name,
            durable=True,
            arguments={
                "x-queue-type": "quorum",
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": f"{queue_name}.XQ",
            },
        )
        # Delay queue — DLX points back to main queue
        await self._publish_channel.declare_queue(
            f"{queue_name}.DQ",
            durable=True,
            arguments={
                "x-queue-type": "quorum",
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": queue_name,
            },
        )
        # Dead-letter queue
        await self._publish_channel.declare_queue(
            f"{queue_name}.XQ",
            durable=True,
            arguments={
                "x-queue-type": "quorum",
            },
        )

    async def publish(self, queue_name: str, message: bytes, **properties) -> None:
        msg = aio_pika.Message(
            body=message,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            **properties,
        )
        await self._publish_channel.default_exchange.publish(
            msg,
            routing_key=queue_name,
        )

    async def publish_to_delay_queue(
        self,
        queue_name: str,
        message_body: bytes,
        delay_ms: int,
        headers: dict | None = None,
    ) -> None:
        msg = aio_pika.Message(
            body=message_body,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            expiration=delay_ms / 1000,
            headers=headers or {},
        )
        await self._publish_channel.default_exchange.publish(
            msg,
            routing_key=f"{queue_name}.DQ",
        )

    async def publish_to_dead_letter_queue(
        self,
        queue_name: str,
        message_body: bytes,
        headers: dict | None = None,
    ) -> None:
        msg = aio_pika.Message(
            body=message_body,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            headers=headers or {},
        )
        await self._publish_channel.default_exchange.publish(
            msg,
            routing_key=f"{queue_name}.XQ",
        )

    async def consume(self, queue_name: str, callback, prefetch_count: int) -> None:
        await self._consume_channel.set_qos(prefetch_count=prefetch_count)
        queue = await self._consume_channel.get_queue(queue_name)
        consumer_tag = await queue.consume(callback)
        self._consumer_tags.append((queue, consumer_tag))

    async def cancel_consume(self) -> None:
        """Cancel all active consumers to stop receiving new messages."""
        for queue, consumer_tag in self._consumer_tags:
            await queue.cancel(consumer_tag)
        self._consumer_tags.clear()

    async def ack(self, message) -> None:
        await message.ack()

    async def nack(self, message, requeue: bool = False) -> None:
        await message.nack(requeue=requeue)

    async def close(self) -> None:
        if self._connection is not None:
            await self._connection.close()
            self._connection = None
