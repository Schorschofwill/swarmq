"""RabbitMQ broker implementation."""

import asyncio
import json
from typing import Any

import aio_pika
from aio_pika.exceptions import (
    AMQPChannelError,
    ChannelClosed,
    ChannelInvalidStateError,
    ChannelNotFoundEntity,
    ConnectionClosed,
    DeliveryError,
)

from swarmq.exceptions import BrokerConnectionError

# How long to wait for the connection/channel to become ready again
# during a reconnect before surfacing BrokerConnectionError. U1.1
# (2026-04-17): the toxiproxy spike showed publish-during-reconnect
# fails with CancelledError; the broker now waits instead.
_CHANNEL_READY_TIMEOUT_S = 10.0

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


class RabbitMQBroker:
    def __init__(self, url: str, queue_type: str = "quorum") -> None:
        self._url = url
        self._queue_type = queue_type
        self._connection = None
        self._publish_channel = None
        self._consume_channel = None
        self._consumer_tags: list[tuple] = []  # list of (queue, consumer_tag)
        # U1.1: QoS state re-applied after channel recreate. Without
        # this, a reconnected consumer silently loses its prefetch.
        self._consume_prefetch: int | None = None
        # Guard against two concurrent channel-reopen operations.
        self._channel_lock = asyncio.Lock()

    async def connect(self) -> None:
        if self._connection is not None:
            return
        try:
            self._connection = await aio_pika.connect_robust(self._url)
            # Channel creation can fail too (vhost misconfigured, per-
            # connection channel cap, broker restart between
            # connect_robust success and channel() call). R3.10 review
            # R-5: these two lines were outside the BrokerConnectionError
            # wrapper, letting raw aio_pika exceptions escape to the CLI
            # as unformatted tracebacks. Pulled inside.
            self._publish_channel = await self._connection.channel()
            self._consume_channel = await self._connection.channel()
        except Exception as exc:
            raise BrokerConnectionError(str(exc)) from exc

    async def _ensure_publish_channel(self) -> "aio_pika.abc.AbstractChannel":
        """Return a ready publish channel; recreate if dropped.

        aio_pika's RobustChannel auto-reopens after a transport blip,
        but operations initiated during the `is_closed=True` window
        fail (CancelledError / ChannelInvalidStateError). Spike
        evidence: docs/solutions/spikes/2026-04-17-toxiproxy-
        reconnect-spike.md. This guard waits for the connection to
        be ready, then reopens the channel if still closed, before
        returning it to the caller.
        """
        return await self._ensure_channel("_publish_channel")

    async def _ensure_consume_channel(self) -> "aio_pika.abc.AbstractChannel":
        """Return a ready consume channel; recreate if dropped.

        Same contract as :meth:`_ensure_publish_channel`, but also
        re-applies the previous QoS prefetch when the channel was
        freshly recreated — otherwise the reconnected consumer
        silently loses its rate-limiting setting. On the fast path
        (healthy channel), set_qos is NOT called (ce-review R-2/C-02:
        an AMQP Basic.Qos RPC per consume access is wasteful and its
        failure was previously unguarded).
        """
        chan, was_recreated = await self._ensure_channel_tracked(
            "_consume_channel",
        )
        if was_recreated and self._consume_prefetch is not None:
            await chan.set_qos(prefetch_count=self._consume_prefetch)
        return chan

    async def _publish_with_retry(
        self, routing_key: str, msg: "aio_pika.Message",
    ) -> None:
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
                    except Exception:  # noqa: BLE001
                        pass
                    self._publish_channel = None
                # Skip the backoff sleep on the final attempt — we are
                # about to raise anyway. Saves up to 800ms latency at
                # exhaustion (ce-review R-4).
                if attempt < _PUBLISH_MAX_ATTEMPTS - 1:
                    await asyncio.sleep(0.05 * (2 ** attempt))
        raise BrokerConnectionError(
            f"publish to {routing_key!r} failed after "
            f"{_PUBLISH_MAX_ATTEMPTS} attempts: {last_exc}"
        ) from last_exc

    async def _ensure_channel(self, attr: str) -> "aio_pika.abc.AbstractChannel":
        """Thin wrapper around :meth:`_ensure_channel_tracked` for
        callers that don't need the was-recreated signal."""
        chan, _ = await self._ensure_channel_tracked(attr)
        return chan

    async def _ensure_channel_tracked(
        self, attr: str,
    ) -> tuple["aio_pika.abc.AbstractChannel", bool]:
        """Return (channel, was_recreated). Lets callers apply per-
        recreate state (QoS prefetch, consumer re-registration)
        exactly when a new underlying channel was opened."""
        channel = getattr(self, attr)
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
            try:
                await asyncio.wait_for(
                    self._connection.ready(),
                    timeout=_CHANNEL_READY_TIMEOUT_S,
                )
            except TimeoutError as exc:
                raise BrokerConnectionError(
                    f"connection not ready after {_CHANNEL_READY_TIMEOUT_S}s "
                    f"(reopening {attr!r} on {self._url!r})"
                ) from exc
            try:
                channel = await self._connection.channel()
            except Exception as exc:  # noqa: BLE001
                raise BrokerConnectionError(
                    f"channel reopen failed for {attr!r} on "
                    f"{self._url!r}: {exc}"
                ) from exc
            setattr(self, attr, channel)
            return channel, True

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
        pub: "aio_pika.abc.AbstractChannel",
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
        await pub.declare_queue(
            queue_name,
            durable=True,
            arguments=main_args,
        )
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
        await self._publish_with_retry(queue_name, msg)

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
        await self._publish_with_retry(f"{queue_name}.DQ", msg)

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
        await self._publish_with_retry(f"{queue_name}.XQ", msg)

    async def consume(self, queue_name: str, callback, prefetch_count: int) -> None:
        # Remember the prefetch so _ensure_consume_channel can re-apply
        # it after a channel recreate.
        self._consume_prefetch = prefetch_count
        chan = await self._ensure_consume_channel()
        # Apply QoS on the initial consume() — _ensure_consume_channel
        # only re-applies after a recreate (ce-review R-2/C-02: avoid
        # an AMQP RPC per every consume-channel access).
        await chan.set_qos(prefetch_count=prefetch_count)
        queue = await chan.get_queue(queue_name)
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
        pub = await self._ensure_publish_channel()
        return await pub.get_queue(f"{queue_name}.XQ")

    @staticmethod
    def _parse_task_fields(body: bytes) -> tuple[str | None, str | None, dict | None]:
        """Extract (task_id, task_name, payload_dict) from a SwarmQ message body.

        Single-pass replacement for the earlier double-parse (plan M-5 /
        KP-3). All three values are None when the body isn't valid
        SwarmQ-shaped JSON. Catches both ``ValueError`` (bad JSON) and
        ``TypeError`` (body isn't bytes/str) so malformed inputs never
        crash the drain loop.
        """
        try:
            payload = json.loads(body)
        except (ValueError, TypeError):
            return None, None, None
        if not isinstance(payload, dict):
            return None, None, None
        task = payload.get("task")
        if not isinstance(task, dict):
            return None, None, payload
        task_id = task.get("id")
        task_name = task.get("name")
        return (
            task_id if isinstance(task_id, str) else None,
            task_name if isinstance(task_name, str) else None,
            payload,
        )

    @staticmethod
    def _normalise_header_keys(headers: dict) -> dict:
        """Decode bytes-valued header keys to str.

        aio_pika occasionally surfaces AMQP header keys as ``bytes``
        instead of ``str`` (depends on wire encoding path). Callers that
        do ``headers.pop("x-swarmq-retry-count")`` miss a bytes-keyed
        entry silently (R-3). Normalise at the broker boundary so
        downstream code can use plain string keys.
        """
        out = {}
        for k, v in (headers or {}).items():
            if isinstance(k, bytes):
                k = k.decode("utf-8", errors="replace")
            out[k] = v
        return out

    # Back-compat alias for any in-module caller still using the old name.
    @staticmethod
    def _extract_task_id(body: bytes) -> str | None:
        task_id, _name, _payload = RabbitMQBroker._parse_task_fields(body)
        return task_id

    async def _drain_dlq_with_filter(
        self,
        queue_name: str,
        limit: int,
        match_task_id: str | None = None,
        move_to_main: bool = False,
        break_after_first_match: bool = False,
        skip_malformed_on_move: bool = False,
    ) -> tuple[list[dict], list[dict]]:
        """Drain up to *limit* messages from the DLQ destructively, then
        republish each one to either the DLQ (default) or the main queue
        (when *move_to_main* is set and either no filter is in effect or
        the task_id matches).

        Four call-site modes encoded by the flag combinations:
          * SCAN-ONLY (match=None, move=False): list_dead_letter_queue —
            drain + republish every scanned message back to DLQ.
          * INSPECT (match=task_id, move=False): inspect_dead_letter_message —
            drain + republish every scanned message back to DLQ, with
            break_after_first_match to minimise the crash-window for a
            targeted lookup.
          * RETRY-ONE (match=task_id, move=True, break=True): republishes
            ONE match to main, puts the rest back. docstring-accurate.
          * RETRY-ALL (match=None, move=True, skip_malformed=True):
            drain everything, forward valid messages to main, but route
            unparseable bodies back to the DLQ instead of re-publishing
            garbage into main that the worker would just dead-letter
            again (plan P2 #6 / ADV-003).

        Why drain+republish and not ``nack(requeue=True)``:
        ``basic.nack(requeue=True)`` re-inserts messages at the HEAD of
        the queue, so a plain get+nack loop iterates forever on the same
        message. Destructive get + in-finally republish is the only
        portable-RabbitMQ pattern that traverses the queue and preserves
        content.

        Returns ``(all_entries, matched_entries)`` — each entry has
        ``task_id``, ``task_name``, ``queue``, ``headers``, ``body``
        (decoded dict or None), ``body_raw`` (utf-8-decoded string), and
        ``body_size``. Header keys normalised to ``str`` at the
        boundary so downstream code can use string keys safely (plan
        R-3). ``len(all_entries)`` is the number of messages actually
        scanned (not the *limit* cap) — callers use it for
        operator-facing count reporting.

        Crash safety: if the process dies mid-drain, any messages that
        have been read but not yet republished are lost. Per-message
        republish-error handling: an AMQP error on one republish does
        NOT abort the rest (plan P1 #1 / R-1) — the loop catches each
        publish individually, logs, and continues. After the loop runs
        to completion, the first collected error is re-raised so the
        caller observes the failure.
        """
        queue = await self._dlq_queue(queue_name)
        drained: list[tuple[bytes, dict, bool, bool]] = []  # (body, headers, is_match, is_malformed)
        entries: list[dict] = []
        matched: list[dict] = []
        try:
            for _ in range(limit):
                msg = await queue.get(no_ack=True, fail=False)
                if msg is None:
                    break
                body = msg.body
                headers = self._normalise_header_keys(dict(msg.headers or {}))
                task_id, task_name, payload = self._parse_task_fields(body)
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
                # Malformed = we could not extract a task_id; such
                # messages cannot be tracked for dedup or retried
                # meaningfully. retry_all skips them to avoid an
                # infinite cycle (plan ADV-003).
                is_malformed = task_id is None
                entries.append(entry)
                if is_match:
                    matched.append(entry)
                drained.append((body, headers, is_match, is_malformed))
                if break_after_first_match and is_match:
                    break
        finally:
            # Per-item try/except so one publish failure doesn't truncate
            # the rest of the drain (plan R-1). Failures are collected
            # and logged; the first is re-raised after the loop.
            errors: list[BaseException] = []
            for body, headers, is_match, is_malformed in drained:
                try:
                    # Route to main when:
                    #  * move_to_main is set, AND
                    #  * (no filter OR this entry matched the filter), AND
                    #  * NOT (skip_malformed_on_move set AND this is malformed)
                    to_main = move_to_main and (
                        match_task_id is None or is_match
                    )
                    if to_main and skip_malformed_on_move and is_malformed:
                        to_main = False

                    # Route through _publish_with_retry (ce-review R-1 /
                    # C-01 / KP-002): transient channel errors during
                    # reconnect windows must not silently drop drained
                    # messages. Same contract as every other publish
                    # site.
                    if to_main:
                        forward_headers = dict(headers)
                        # Reset retry-count so the operator's "try this
                        # again" intent isn't blocked by a stale counter.
                        forward_headers.pop(
                            "x-swarmq-retry-count", None,
                        )
                        msg_out = aio_pika.Message(
                            body=body,
                            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                            headers=forward_headers,
                        )
                        await self._publish_with_retry(queue_name, msg_out)
                    else:
                        # Back into the DLQ. Headers preserved verbatim.
                        msg_out = aio_pika.Message(
                            body=body,
                            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                            headers=dict(headers),
                        )
                        await self._publish_with_retry(
                            f"{queue_name}.XQ", msg_out,
                        )
                except asyncio.CancelledError:
                    # Honour task-level cancellation — do not collect
                    # and swallow. _publish_with_retry already handles
                    # the aio_pika internal drain_future CancelledError
                    # via current_task().cancelling() guard.
                    raise
                except Exception as exc:  # noqa: BLE001
                    errors.append(exc)
            if errors:
                # Wrap the first collected failure in a
                # BrokerConnectionError so the CLI surfaces a clean
                # error line rather than a raw aio_pika traceback. The
                # rest are lost-to-log; a single transient broker blip
                # usually affects every subsequent publish identically.
                raise BrokerConnectionError(
                    f"dlq republish failed on {len(errors)} of "
                    f"{len(drained)} message(s); first error: {errors[0]}"
                ) from errors[0]
        return entries, matched

    async def list_dead_letter_queue(
        self, queue_name: str, limit: int = 100,
    ) -> tuple[list[dict], int]:
        """Return up to *limit* messages from ``{queue_name}.XQ``.

        Returns ``(entries, scanned_count)`` where:
          * ``entries`` — list of summaries (task_id / task_name / queue
            / headers / body_size). Heavy ``body`` / ``body_raw`` fields
            are stripped for compact list output; use ``inspect`` for
            the full payload.
          * ``scanned_count`` — the number of messages actually read
            (may be ``len(entries)``, but exposed explicitly so callers
            can tell "DLQ had fewer than limit" from "stopped at limit").

        Non-destructive: every scanned message is republished to the DLQ
        in the drain's finally-block.
        """
        entries, _matched = await self._drain_dlq_with_filter(
            queue_name, limit=limit,
        )
        summaries = [
            {
                "task_id": e["task_id"],
                "task_name": e["task_name"],
                "queue": e["queue"],
                "headers": e["headers"],
                "body_size": e["body_size"],
            }
            for e in entries
        ]
        return summaries, len(entries)

    async def inspect_dead_letter_message(
        self, queue_name: str, task_id: str, scan_limit: int = 1000,
    ) -> tuple[dict | None, int]:
        """Find the first DLQ message whose payload's ``task.id`` equals
        *task_id* and return its full metadata.

        Returns ``(match_or_None, scanned_count)``. ``scanned_count``
        lets the caller distinguish "genuinely not in the DLQ" from
        "scan stopped at limit before the match" for operator-facing
        output. Non-destructive: scanned messages are republished.

        Uses ``break_after_first_match`` to minimise the number of
        messages pulled off the wire once the target is found — this
        shortens the crash-exposure window for targeted lookups.
        """
        entries, matched = await self._drain_dlq_with_filter(
            queue_name, limit=scan_limit, match_task_id=task_id,
            break_after_first_match=True,
        )
        return (matched[0] if matched else None), len(entries)

    async def retry_dead_letter_message(
        self, queue_name: str, task_id: str, scan_limit: int = 1000,
    ) -> tuple[bool, int]:
        """Republish the first matching DLQ message into *queue_name*.

        Returns ``(found, scanned_count)``. Uses
        ``break_after_first_match`` so duplicate task_ids in the DLQ
        are NOT all forwarded by a single call (plan C-2 / ADV-002).
        The docstring-promised "first match only" semantics are now
        enforced.

        The ``x-swarmq-retry-count`` header is stripped on forward so
        the message gets the full retry budget again (operator intent:
        "try this again"). All other headers — including the original
        traceback and any R3.5 dedup headers — pass through unchanged.

        Messages scanned but not matched are republished back to the
        DLQ via ``_drain_dlq_with_filter``.
        """
        entries, matched = await self._drain_dlq_with_filter(
            queue_name, limit=scan_limit,
            match_task_id=task_id, move_to_main=True,
            break_after_first_match=True,
        )
        return len(matched) > 0, len(entries)

    async def retry_all_dead_letter_messages(
        self, queue_name: str, limit: int = 10_000,
    ) -> tuple[int, int]:
        """Republish every valid DLQ message into *queue_name*.

        Returns ``(moved_count, scanned_count)``. Malformed-body
        messages (invalid JSON, missing task.id) are routed back to the
        DLQ instead of forward to main — the worker would just fail to
        deserialize them and dead-letter them again, causing an
        operator-invisible infinite cycle (plan ADV-003). Operator
        clears these via ``purge``.

        ``scanned_count`` equals the total number of messages drained
        (including malformed). ``moved_count`` is strictly less-or-
        equal to ``scanned_count``; the gap is the malformed set.
        """
        entries, _matched = await self._drain_dlq_with_filter(
            queue_name, limit=limit,
            move_to_main=True, skip_malformed_on_move=True,
        )
        moved = sum(1 for e in entries if e["task_id"] is not None)
        return moved, len(entries)

    async def purge_dead_letter_queue(self, queue_name: str) -> int:
        """Drop every message in ``{queue_name}.XQ`` via the native
        AMQP purge. Returns the message count as reported by RabbitMQ.
        """
        try:
            queue = await self._dlq_queue(queue_name)
            result = await queue.purge()
            return int(getattr(result, "message_count", 0))
        except BrokerConnectionError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise BrokerConnectionError(str(exc)) from exc

    async def get_queue_depth(self, queue_name: str) -> int:
        """Return the total pending-message count for ``queue_name``
        and its delay companion ``{queue_name}.DQ`` (R4.7 Autoscaling).

        Uses AMQP ``declare(passive=True)`` which returns the current
        ``message_count`` without modifying the queue — safe to poll
        at a 10s cadence. If a queue does not exist yet (worker not
        started, fresh deployment) the broker raises
        :class:`ChannelNotFoundEntity` **and closes the channel**
        (AMQP semantics); we treat that as 0 depth and use a
        short-lived dedicated channel per call so a missing queue
        does not poison the publish channel.

        DLQ depth is deliberately excluded: XQ backlog is not
        work-to-do, it's work-that-failed and should not drive
        scale-up.
        """
        if self._connection is None or self._connection.is_closed:
            raise BrokerConnectionError("broker is not connected")
        total = 0
        for name in (queue_name, f"{queue_name}.DQ"):
            # Throwaway channel per queue — a failed passive-declare
            # closes the channel, so we cannot share one across both
            # lookups without re-opening in between.
            channel = None
            try:
                channel = await self._connection.channel()
                q = await channel.declare_queue(name, passive=True)
                result = getattr(q, "declaration_result", None)
                if result is not None:
                    total += int(getattr(result, "message_count", 0))
            except ChannelNotFoundEntity:
                # Queue does not exist yet — counts as 0 depth.
                continue
            except Exception as exc:  # noqa: BLE001
                raise BrokerConnectionError(str(exc)) from exc
            finally:
                if channel is not None and not channel.is_closed:
                    try:
                        await channel.close()
                    except Exception:  # noqa: BLE001
                        pass
        return total
