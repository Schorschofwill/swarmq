"""RabbitMQ broker implementation."""

import json

import aio_pika

from swarmq.exceptions import BrokerConnectionError


class RabbitMQBroker:
    def __init__(self, url: str, queue_type: str = "quorum") -> None:
        self._url = url
        self._queue_type = queue_type
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
                "x-queue-type": self._queue_type,
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": f"{queue_name}.XQ",
            },
        )
        # Delay queue — DLX points back to main queue
        await self._publish_channel.declare_queue(
            f"{queue_name}.DQ",
            durable=True,
            arguments={
                "x-queue-type": self._queue_type,
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": queue_name,
            },
        )
        # Dead-letter queue
        await self._publish_channel.declare_queue(
            f"{queue_name}.XQ",
            durable=True,
            arguments={
                "x-queue-type": self._queue_type,
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

    # ------------------------------------------------------------------
    # R3.10 DLQ inspection / recovery primitives
    # ------------------------------------------------------------------

    async def _dlq_queue(self, queue_name: str):
        """Get a handle on ``{queue_name}.XQ``."""
        return await self._publish_channel.get_queue(f"{queue_name}.XQ")

    @staticmethod
    def _extract_task_id(body: bytes) -> str | None:
        """Pull ``task.id`` from a SwarmQ message body. None on malformed JSON."""
        try:
            payload = json.loads(body)
        except (ValueError, TypeError):
            return None
        if not isinstance(payload, dict):
            return None
        task = payload.get("task")
        if not isinstance(task, dict):
            return None
        task_id = task.get("id")
        return task_id if isinstance(task_id, str) else None

    async def _drain_dlq_with_filter(
        self,
        queue_name: str,
        limit: int,
        match_task_id: str | None = None,
        move_to_main: bool = False,
    ) -> tuple[list[dict], list[dict]]:
        """Drain up to *limit* messages from the DLQ destructively, then
        republish each one to either the DLQ (default) or the main queue
        (when *move_to_main* is set and either no filter is in effect or
        the task_id matches).

        Why drain+republish and not ``nack(requeue=True)``:
        ``basic.nack(requeue=True)`` re-inserts messages at the HEAD of
        the queue, so a plain get+nack loop iterates forever on the same
        message. Destructive get + in-finally republish is the only
        portable-RabbitMQ pattern that traverses the queue and preserves
        content.

        Returns ``(all_entries, matched_entries)`` — each entry has
        ``task_id``, ``task_name``, ``queue``, ``headers``, ``body``
        (decoded dict or None), ``body_raw`` (utf-8-decoded string), and
        ``body_size``.

        Crash safety: if the process dies mid-drain, any messages that
        have been read but not yet republished are lost. Operator-
        inspection callers accept that risk; production recovery should
        use ``retry_all`` (single transfer into main) or ``purge``.
        """
        queue = await self._dlq_queue(queue_name)
        drained: list[tuple[bytes, dict, bool]] = []
        entries: list[dict] = []
        matched: list[dict] = []
        try:
            for _ in range(limit):
                msg = await queue.get(no_ack=True, fail=False)
                if msg is None:
                    break
                body = msg.body
                headers = dict(msg.headers or {})
                task_id = self._extract_task_id(body)
                try:
                    payload = json.loads(body)
                    if not isinstance(payload, dict):
                        payload = None
                except ValueError:
                    payload = None

                task_name = None
                if payload is not None:
                    task = payload.get("task")
                    if isinstance(task, dict):
                        task_name_raw = task.get("name")
                        if isinstance(task_name_raw, str):
                            task_name = task_name_raw

                entry = {
                    "task_id": task_id,
                    "task_name": task_name,
                    "queue": queue_name,
                    "headers": headers,
                    "body": payload,
                    "body_raw": body.decode("utf-8", errors="replace"),
                    "body_size": len(body),
                }
                is_match = (
                    match_task_id is not None and task_id == match_task_id
                )
                entries.append(entry)
                if is_match:
                    matched.append(entry)
                drained.append((body, headers, is_match))
        finally:
            for body, headers, is_match in drained:
                # Route to main when:
                #  * move_to_main is set AND no filter (retry --all), OR
                #  * move_to_main is set AND this entry matched the filter.
                to_main = move_to_main and (match_task_id is None or is_match)
                if to_main:
                    forward_headers = dict(headers)
                    # Reset retry-count so the operator's "try this
                    # again" intent isn't blocked by a stale counter.
                    forward_headers.pop("x-swarmq-retry-count", None)
                    msg_out = aio_pika.Message(
                        body=body,
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                        headers=forward_headers,
                    )
                    await self._publish_channel.default_exchange.publish(
                        msg_out, routing_key=queue_name,
                    )
                else:
                    # Back into the DLQ. Headers preserved verbatim.
                    msg_out = aio_pika.Message(
                        body=body,
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                        headers=dict(headers),
                    )
                    await self._publish_channel.default_exchange.publish(
                        msg_out, routing_key=f"{queue_name}.XQ",
                    )
        return entries, matched

    async def list_dead_letter_queue(
        self, queue_name: str, limit: int = 100,
    ) -> list[dict]:
        """Return up to *limit* messages from ``{queue_name}.XQ`` without
        draining the queue.

        Every entry has ``task_id`` (or None for malformed payloads),
        ``task_name``, ``queue``, ``headers``, and ``body_size``. The
        heavy ``body``/``body_raw`` fields are intentionally stripped
        to keep list output compact — use ``inspect`` for the full
        payload.
        """
        entries, _matched = await self._drain_dlq_with_filter(
            queue_name, limit=limit,
        )
        return [
            {
                "task_id": e["task_id"],
                "task_name": e["task_name"],
                "queue": e["queue"],
                "headers": e["headers"],
                "body_size": e["body_size"],
            }
            for e in entries
        ]

    async def inspect_dead_letter_message(
        self, queue_name: str, task_id: str, scan_limit: int = 1000,
    ) -> dict | None:
        """Find the first DLQ message whose payload's ``task.id`` equals
        *task_id* and return its full metadata (headers + decoded body
        + raw body + size). Non-destructive: scanned messages are
        republished to the DLQ. Returns None when no match is found
        within *scan_limit* messages.
        """
        _entries, matched = await self._drain_dlq_with_filter(
            queue_name, limit=scan_limit, match_task_id=task_id,
        )
        return matched[0] if matched else None

    async def retry_dead_letter_message(
        self, queue_name: str, task_id: str, scan_limit: int = 1000,
    ) -> bool:
        """Republish the first matching DLQ message into *queue_name*
        and drop the DLQ copy. Returns True on success, False when no
        match is found within *scan_limit*.

        The ``x-swarmq-retry-count`` header is stripped on forward so
        the message gets the full retry budget again (operator intent:
        "try this again"). All other headers — including the original
        traceback and any R3.5 dedup headers — pass through unchanged.

        Messages scanned but not matched are republished back to the
        DLQ via ``_drain_dlq_with_filter``.
        """
        _entries, matched = await self._drain_dlq_with_filter(
            queue_name, limit=scan_limit,
            match_task_id=task_id, move_to_main=True,
        )
        return len(matched) > 0

    async def retry_all_dead_letter_messages(
        self, queue_name: str, limit: int = 10_000,
    ) -> int:
        """Republish every DLQ message into *queue_name*. DLQ ends empty
        when *limit* was not reached. Returns the number moved.

        Unlike ``retry_dead_letter_message``, this path transfers every
        scanned message to the main queue in a single pass — no message
        goes back to the DLQ, so a crash mid-drain loses only messages
        that were already pulled off the wire but not yet re-published
        to main (same risk window as any operator-triggered drain).
        """
        _entries, matched = await self._drain_dlq_with_filter(
            queue_name, limit=limit, move_to_main=True,
        )
        return len(_entries)

    async def purge_dead_letter_queue(self, queue_name: str) -> int:
        """Drop every message in ``{queue_name}.XQ`` via the native
        AMQP purge. Returns the message count as reported by RabbitMQ.
        """
        queue = await self._dlq_queue(queue_name)
        result = await queue.purge()
        return int(getattr(result, "message_count", 0))
