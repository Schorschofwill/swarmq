"""Kleine Helfer für die Contract-Klassen (kein Test-Modul)."""

from __future__ import annotations

import asyncio
from typing import Protocol

from swarmq.contracts import Consumer, Delivery


class Clock(Protocol):
    def now_ms(self) -> int: ...

    async def advance(self, seconds: float) -> None: ...


class Inbox:
    """Sammelt Zustellungen eines ``consume``-Callbacks."""

    def __init__(self) -> None:
        self._queue: asyncio.Queue[Delivery] = asyncio.Queue()

    async def on_delivery(self, delivery: Delivery) -> None:
        self._queue.put_nowait(delivery)

    async def next(self, *, timeout: float = 5.0) -> Delivery:
        return await asyncio.wait_for(self._queue.get(), timeout=timeout)

    async def expect_nothing(self, *, within: float = 0.3) -> None:
        try:
            got = await asyncio.wait_for(self._queue.get(), timeout=within)
        except TimeoutError:
            return
        raise AssertionError(f"unerwartete Zustellung: {got!r}")


async def consume_into(impl: Consumer, queue: str, *, concurrency: int = 1) -> Inbox:
    inbox = Inbox()
    await impl.consume(queue, inbox.on_delivery, concurrency=concurrency)
    return inbox
