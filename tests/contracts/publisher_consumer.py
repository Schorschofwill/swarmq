"""Contract-Klasse für ``Broker`` (= ``Publisher`` + ``Consumer``), Entwurf §2.2.

Fixtures, die ein Binding liefert:

- ``impl``: verbundener ``Broker``.
- ``fresh_impl``: derselbe Typ, noch nicht verbunden.
- ``unreachable_impl``: gegen einen nicht erreichbaren Endpunkt konfiguriert.
- ``queue``: eindeutiger, bereits deklarierter Queue-Name (mit Prioritäts-
  Suffix, z. B. ``ctr-<uuid>.high``), damit der Band-Erhalt geprüft wird.
- ``dead_letters``: ``async (queue) -> list[tuple[bytes, dict]]`` — Inhalt
  des Dead-Letter-Ziels (Body, Header), ohne die Nachrichten zu verbrauchen.
- ``publish_failure``: Context-Manager, unter dem jede Veröffentlichung
  des ``impl`` fehlschlägt (injizierter Kanalfehler beim RabbitMQ-Binding).
- ``clock``: siehe ``tests/contracts/__init__``.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import AbstractContextManager
from typing import Any

import pytest

from swarmq.contracts import Broker, Delivery, PublishReceipt
from swarmq.exceptions import BrokerConnectionError
from tests.contracts._support import Clock, consume_into

DeadLetters = Callable[[str], Awaitable[list[tuple[bytes, dict[str, Any]]]]]
PublishFailure = Callable[[], AbstractContextManager[None]]


class BrokerContract:
    # --- Lebenszyklus ------------------------------------------------------

    async def test_connect_is_idempotent(self, fresh_impl: Broker, queue: str) -> None:
        await fresh_impl.connect()
        await fresh_impl.connect()
        await fresh_impl.declare_queue(queue)
        receipt = await fresh_impl.publish(queue, b"x")
        assert isinstance(receipt, PublishReceipt)
        await fresh_impl.close()

    async def test_ping_before_connect_raises(self, fresh_impl: Broker) -> None:
        with pytest.raises(BrokerConnectionError):
            await fresh_impl.ping()

    async def test_ping_after_connect_succeeds(self, impl: Broker) -> None:
        await impl.ping()

    async def test_driver_errors_are_wrapped_on_connect(self, unreachable_impl: Broker) -> None:
        with pytest.raises(BrokerConnectionError):
            await unreachable_impl.connect()

    async def test_declare_queue_is_idempotent(self, impl: Broker, queue: str) -> None:
        await impl.declare_queue(queue)
        await impl.declare_queue(queue)

    # --- publish / consume ------------------------------------------------

    async def test_publish_returns_receipt_for_the_queue(self, impl: Broker, queue: str) -> None:
        receipt = await impl.publish(queue, b"body", headers={"x-swarmq-enqueued-at": 1})
        assert receipt.queue == queue

    async def test_delivery_carries_body_headers_and_delivery_queue(self, impl: Broker, queue: str) -> None:
        await impl.publish(queue, b'{"task": {"queue": "elsewhere"}}', headers={"x-swarmq-retry-count": 2})
        inbox = await consume_into(impl, queue)
        delivery = await inbox.next()
        assert delivery.body == b'{"task": {"queue": "elsewhere"}}'
        assert delivery.headers["x-swarmq-retry-count"] == 2
        assert delivery.queue == queue  # nicht die Body-Queue
        await impl.ack(delivery)

    async def test_consume_respects_concurrency_window(self, impl: Broker, queue: str) -> None:
        for i in range(3):
            await impl.publish(queue, f"m{i}".encode())
        inbox = await consume_into(impl, queue, concurrency=1)
        first = await inbox.next()
        await inbox.expect_nothing()
        await impl.ack(first)
        second = await inbox.next()
        await impl.ack(second)
        third = await inbox.next()
        await impl.ack(third)

    async def test_cancel_consume_stops_deliveries_but_keeps_acks_possible(self, impl: Broker, queue: str) -> None:
        await impl.publish(queue, b"a")
        await impl.publish(queue, b"b")
        inbox = await consume_into(impl, queue, concurrency=1)
        first = await inbox.next()
        await impl.cancel_consume()
        await impl.ack(first)
        await inbox.expect_nothing()
        inbox2 = await consume_into(impl, queue)
        second = await inbox2.next()
        assert second.body == b"b"
        await impl.ack(second)

    # --- die vier Verben ---------------------------------------------------

    async def test_ack_removes_delivery(self, impl: Broker, queue: str) -> None:
        await impl.publish(queue, b"once")
        inbox = await consume_into(impl, queue)
        delivery = await inbox.next()
        await impl.ack(delivery)
        await impl.cancel_consume()
        inbox2 = await consume_into(impl, queue)
        await inbox2.expect_nothing()

    async def test_redeliver_delivers_same_delivery_again(self, impl: Broker, queue: str) -> None:
        await impl.publish(queue, b"again", headers={"x-swarmq-unique-hash": "h"})
        inbox = await consume_into(impl, queue)
        first = await inbox.next()
        await impl.redeliver(first)
        second = await inbox.next()
        # Body und Queue gleich, Original-Header erhalten; der Transport darf
        # eigene Header ergänzen (RabbitMQ-Quorum-Queues: ``x-delivery-count``).
        assert (second.body, second.queue) == (first.body, first.queue)
        assert {k: v for k, v in second.headers.items() if k.startswith("x-swarmq-")} == dict(first.headers)
        await impl.ack(second)

    async def test_retry_later_holds_delay_and_returns_to_delivery_queue(
        self, impl: Broker, queue: str, clock: Clock
    ) -> None:
        await impl.publish(queue, b"later", headers={"x-swarmq-retry-count": 0})
        inbox = await consume_into(impl, queue)
        first = await inbox.next()
        await impl.retry_later(first, delay_ms=1500, headers={**first.headers, "x-swarmq-retry-count": 1})
        await clock.advance(0.5)
        await inbox.expect_nothing(within=0.2)
        await clock.advance(1.5)
        second = await inbox.next(timeout=5.0)
        assert second.body == b"later"
        assert second.queue == queue  # Band bleibt erhalten
        assert second.headers["x-swarmq-retry-count"] == 1
        await impl.ack(second)

    async def test_retry_later_acks_the_original(self, impl: Broker, queue: str, clock: Clock) -> None:
        await impl.publish(queue, b"orig")
        inbox = await consume_into(impl, queue)
        first = await inbox.next()
        await impl.retry_later(first, delay_ms=60_000, headers=dict(first.headers))
        await impl.cancel_consume()
        inbox2 = await consume_into(impl, queue)
        await inbox2.expect_nothing()  # Original ist bestätigt, Kopie liegt im Verzögerungsziel

    async def test_dead_letter_is_inspectable_with_all_original_headers(
        self, impl: Broker, queue: str, dead_letters: DeadLetters
    ) -> None:
        original = {"x-swarmq-unique-hash": "abc", "x-swarmq-enqueued-at": 7}
        await impl.publish(queue, b"dead", headers=original)
        inbox = await consume_into(impl, queue)
        delivery = await inbox.next()
        await impl.dead_letter(delivery, headers={**delivery.headers, "x-swarmq-traceback": "boom"})
        letters = await dead_letters(queue)
        assert [
            (b, h["x-swarmq-unique-hash"], h["x-swarmq-enqueued-at"], h["x-swarmq-traceback"]) for b, h in letters
        ] == [(b"dead", "abc", 7, "boom")]
        await impl.cancel_consume()
        inbox2 = await consume_into(impl, queue)
        await inbox2.expect_nothing()  # Original bestätigt

    async def test_replay_of_a_dead_letter_with_a_matching_generation_keeps_its_replay_id(
        self, impl: Any, queue: str, dead_letters: DeadLetters
    ) -> None:
        """MR 12 (BZ-08): traegt die DLQ-Kopie ``x-swarmq-dlq-gen`` gleich
        ``x-swarmq-dead-lettered-at`` (Stempel eines 0.12-Workers), behaelt
        ``retry_dead_letter`` ihre ``x-swarmq-replay-id`` -- ein wiederholter
        Retry derselben Nachricht ergibt dieselbe Identitaet; der Stempel und
        das alte Alter (A6) reisen nicht mit."""
        body = b'{"task": {"id": "ctr-gen", "name": "t", "queue": "q"}, "args": [], "kwargs": {}}'
        await impl.publish(queue, body, headers={"x-swarmq-enqueued-at": 7})
        inbox = await consume_into(impl, queue)
        delivery = await inbox.next()
        fresh = {**delivery.headers, "x-swarmq-replay-id": "r1", "x-swarmq-dlq-gen": 5, "x-swarmq-dead-lettered-at": 5}
        await impl.dead_letter(delivery, headers=fresh)
        [(_, letter)] = await dead_letters(queue)
        assert letter["x-swarmq-dlq-gen"] == letter["x-swarmq-dead-lettered-at"]
        assert (await impl.retry_dead_letter(queue, "ctr-gen"))[0] is True
        replay = await inbox.next()
        assert replay.headers["x-swarmq-replay-id"] == letter["x-swarmq-replay-id"]
        assert "x-swarmq-dlq-gen" not in replay.headers
        assert "x-swarmq-dead-lettered-at" not in replay.headers
        assert replay.headers["x-swarmq-enqueued-at"] != 7
        await impl.ack(replay)

    async def test_retry_later_raises_on_publish_failure_and_leaves_original_unacked(
        self, impl: Broker, queue: str, publish_failure: PublishFailure
    ) -> None:
        await impl.publish(queue, b"keep")
        inbox = await consume_into(impl, queue)
        first = await inbox.next()
        with publish_failure(), pytest.raises(BrokerConnectionError):
            await impl.retry_later(first, delay_ms=10, headers=dict(first.headers))
        # Original ist noch in flight: ein Verb darauf ist weiterhin zulässig
        # (nach erfolgreichem retry_later wäre es bereits bestätigt).
        await impl.redeliver(first)
        again = await inbox.next()
        assert again.body == b"keep"
        await impl.ack(again)

    async def test_dead_letter_raises_on_publish_failure_and_leaves_original_unacked(
        self, impl: Broker, queue: str, publish_failure: PublishFailure, dead_letters: DeadLetters
    ) -> None:
        await impl.publish(queue, b"keep2")
        inbox = await consume_into(impl, queue)
        first = await inbox.next()
        with publish_failure(), pytest.raises(BrokerConnectionError):
            await impl.dead_letter(first, headers=dict(first.headers))
        assert await dead_letters(queue) == []
        await impl.redeliver(first)
        again = await inbox.next()
        assert again.body == b"keep2"
        await impl.ack(again)

    async def test_callback_exceptions_do_not_stop_the_consumer(self, impl: Broker, queue: str) -> None:
        seen: list[Delivery] = []
        done = asyncio.Event()

        async def flaky(delivery: Delivery) -> None:
            seen.append(delivery)
            await impl.ack(delivery)
            if len(seen) == 2:
                done.set()
            if len(seen) == 1:
                raise RuntimeError("handler failure must not kill consume")

        await impl.publish(queue, b"1")
        await impl.publish(queue, b"2")
        await impl.consume(queue, flaky, concurrency=1)
        await asyncio.wait_for(done.wait(), timeout=5.0)
        assert [d.body for d in seen] == [b"1", b"2"]
