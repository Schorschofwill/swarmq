"""In-Memory-``Broker`` (+ ``BrokerAdmin``) — der Litmus-Test der Abstraktion (R2).

Queues sind Listen, Zustellungen haben einen Zustand (in flight / settled),
Verzögerungen liegen in einer fälligkeitssortierten Liste, die die Fake-Clock
ausleert. Der Stub ist absichtlich streng: ein zweites Verb auf einer
bereits erledigten Zustellung ist ein ``RuntimeError``.
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import json
from collections import deque
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from swarmq.contracts import Broker, BrokerAdmin, Delivery, DeliveryCallback, PublishReceipt
from swarmq.exceptions import BrokerConnectionError
from tests.support.inmemory.clock import FakeClock

_ids = itertools.count(1)


@dataclass
class _Message:
    body: bytes
    headers: dict[str, Any]
    message_id: str = field(default_factory=lambda: f"im-{next(_ids)}")


@dataclass
class _Consumer:
    callback: DeliveryCallback
    concurrency: int
    inflight: int = 0


@dataclass(frozen=True)
class DeadLetter:
    body: bytes
    headers: dict[str, Any]
    reason: str  # "published" (dead_letter) | "routed" (route_via_dlx)


class InMemoryBroker:
    def __init__(self, clock: FakeClock, *, unreachable: bool = False) -> None:
        self._clock = clock
        self._unreachable = unreachable
        self._connected = False
        self._queues: dict[str, deque[_Message]] = {}
        self._delayed: list[tuple[int, str, _Message]] = []
        self._dead: dict[str, list[DeadLetter]] = {}
        self._consumers: dict[str, _Consumer] = {}
        self._inflight: dict[int, tuple[str, _Message]] = {}
        self._fail_publish = False
        self._tasks: set[asyncio.Task[None]] = set()
        clock.subscribe(self._on_tick)

    # --- Teststeuerung ------------------------------------------------------

    @contextlib.contextmanager
    def publish_failure(self) -> Iterator[None]:
        self._fail_publish = True
        try:
            yield
        finally:
            self._fail_publish = False

    def dead_letters(self, queue: str) -> list[DeadLetter]:
        return list(self._dead.get(queue, []))

    # --- Publisher ------------------------------------------------------------

    async def connect(self) -> None:
        if self._unreachable:
            raise BrokerConnectionError("in-memory broker configured unreachable")
        self._connected = True

    async def close(self) -> None:
        self._connected = False
        for task in list(self._tasks):
            task.cancel()

    async def ping(self) -> None:
        if not self._connected:
            raise BrokerConnectionError("not connected")

    async def declare_queue(self, queue: str) -> None:
        self._require_connected()
        self._queues.setdefault(queue, deque())
        self._dead.setdefault(queue, [])

    async def publish(self, queue: str, body: bytes, headers: Mapping[str, Any] | None = None) -> PublishReceipt:
        self._require_connected()
        if self._fail_publish:
            raise BrokerConnectionError("publish failed (injected)")
        message = _Message(body=body, headers=dict(headers or {}))
        self._queues.setdefault(queue, deque()).append(message)
        self._dispatch(queue)
        return PublishReceipt(queue=queue, message_id=message.message_id)

    # --- Consumer -------------------------------------------------------------

    async def consume(self, queue: str, on_delivery: DeliveryCallback, *, concurrency: int) -> None:
        self._require_connected()
        self._queues.setdefault(queue, deque())
        self._consumers[queue] = _Consumer(on_delivery, concurrency)
        self._dispatch(queue)

    async def cancel_consume(self) -> None:
        self._consumers.clear()

    async def ack(self, delivery: Delivery) -> None:
        self._settle(delivery)

    async def redeliver(self, delivery: Delivery) -> None:
        queue, message = self._settle(delivery)
        self._queues[queue].appendleft(message)
        self._dispatch(queue)

    async def retry_later(self, delivery: Delivery, *, delay_ms: int, headers: Mapping[str, Any]) -> None:
        queue, message = self._peek(delivery)
        if self._fail_publish:
            raise BrokerConnectionError("delay publish failed (injected)")
        due = self._clock.now_ms() + delay_ms
        self._delayed.append((due, queue, _Message(body=message.body, headers=dict(headers))))
        self._delayed.sort(key=lambda item: item[0])
        self._settle(delivery)

    async def dead_letter(self, delivery: Delivery, *, headers: Mapping[str, Any]) -> None:
        queue, message = self._peek(delivery)
        if self._fail_publish:
            raise BrokerConnectionError("dead-letter publish failed (injected)")
        self._dead.setdefault(queue, []).append(DeadLetter(message.body, dict(headers), "published"))
        self._settle(delivery)

    # --- BrokerAdmin ------------------------------------------------------------

    async def route_via_dlx(self, delivery: Delivery) -> None:
        queue, message = self._settle(delivery)
        self._dead.setdefault(queue, []).append(DeadLetter(message.body, dict(message.headers), "routed"))

    async def list_dead_letters(self, queue: str, limit: int = 100) -> tuple[list[dict[str, Any]], int]:
        letters = self._dead.get(queue, [])
        summaries = [
            {"task_id": _task_id(d.body), "headers": dict(d.headers), "reason": d.reason} for d in letters[:limit]
        ]
        return summaries, len(letters)

    async def inspect_dead_letter(
        self, queue: str, task_id: str, scan_limit: int = 1000
    ) -> tuple[dict[str, Any] | None, int]:
        letters = self._dead.get(queue, [])[:scan_limit]
        for d in letters:
            if _task_id(d.body) == task_id:
                found = {"task_id": task_id, "body": d.body, "headers": dict(d.headers), "reason": d.reason}
                return found, len(letters)
        return None, len(letters)

    async def retry_dead_letter(self, queue: str, task_id: str, scan_limit: int = 1000) -> tuple[bool, int]:
        letters = self._dead.get(queue, [])
        for index, d in enumerate(letters[:scan_limit]):
            if _task_id(d.body) == task_id:
                letters.pop(index)
                headers = {k: v for k, v in d.headers.items() if k != "x-swarmq-retry-count"}
                await self.publish(queue, d.body, headers)
                return True, len(letters) + 1
        return False, len(letters)

    async def retry_all_dead_letters(self, queue: str, limit: int = 10_000) -> tuple[int, int]:
        letters = self._dead.get(queue, [])
        movable = [d for d in letters[:limit] if _task_id(d.body) is not None]
        for d in movable:
            letters.remove(d)
            await self.publish(queue, d.body, {k: v for k, v in d.headers.items() if k != "x-swarmq-retry-count"})
        return len(movable), len(letters) + len(movable)

    async def purge_dead_letters(self, queue: str) -> int:
        count = len(self._dead.get(queue, []))
        self._dead[queue] = []
        return count

    async def queue_depth(self, queue: str) -> int:
        delayed = sum(1 for _, q, _m in self._delayed if q == queue)
        return len(self._queues.get(queue, ())) + delayed

    async def dead_letter_depth(self, queue: str) -> int:
        return len(self._dead.get(queue, []))

    # --- intern ---------------------------------------------------------------

    def _require_connected(self) -> None:
        if not self._connected:
            raise BrokerConnectionError("not connected")

    def _peek(self, delivery: Delivery) -> tuple[str, _Message]:
        key = id(delivery.handle)
        if key not in self._inflight:
            raise RuntimeError("delivery already settled or unknown")
        return self._inflight[key]

    def _settle(self, delivery: Delivery) -> tuple[str, _Message]:
        queue, message = self._peek(delivery)
        del self._inflight[id(delivery.handle)]
        consumer = self._consumers.get(queue)
        if consumer is not None:
            consumer.inflight = max(0, consumer.inflight - 1)
        self._dispatch(queue)
        return queue, message

    def _dispatch(self, queue: str) -> None:
        consumer = self._consumers.get(queue)
        pending = self._queues.get(queue)
        if consumer is None or not pending:
            return
        while pending and consumer.inflight < consumer.concurrency:
            message = pending.popleft()
            consumer.inflight += 1
            handle = object()
            self._inflight[id(handle)] = (queue, message)
            delivery = Delivery(body=message.body, headers=dict(message.headers), queue=queue, handle=handle)
            task = asyncio.get_running_loop().create_task(self._run_callback(consumer.callback, delivery))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

    @staticmethod
    async def _run_callback(callback: DeliveryCallback, delivery: Delivery) -> None:
        try:
            await callback(delivery)
        except Exception:  # Callback-Fehler töten den Consumer nicht (wie aio_pika)
            return

    async def _on_tick(self, now_ms: int) -> None:
        due = [item for item in self._delayed if item[0] <= now_ms]
        self._delayed = [item for item in self._delayed if item[0] > now_ms]
        for _, queue, message in due:
            self._queues.setdefault(queue, deque()).append(message)
            self._dispatch(queue)


def _task_id(body: bytes) -> str | None:
    try:
        payload = json.loads(body)
        task_id = payload["task"]["id"]
    except (ValueError, KeyError, TypeError):
        return None
    return str(task_id)


if TYPE_CHECKING:  # Konformitäts-Anker (KTD1)
    _conforms_broker: type[Broker] = InMemoryBroker
    _conforms_admin: type[BrokerAdmin] = InMemoryBroker
