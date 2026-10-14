"""Helfer der Spikes S1–S9: Wegwerf-Channels, Policies über die Management-API, Zustellzähler."""

from __future__ import annotations

import re
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any

import aio_pika
import httpx
from aio_pika.abc import AbstractConnection, AbstractIncomingMessage

from tests.support.services import RabbitMQNode
from tests.support.wait import wait_until

AUTH = ("guest", "guest")
QUORUM: dict[str, Any] = {"x-queue-type": "quorum"}


async def delete_queues(url: str, *names: str) -> None:
    """Löscht beliebige Queues (keine Familien) je auf eigenem Channel; best effort."""
    connection = await aio_pika.connect(url)
    try:
        for name in names:
            channel = await connection.channel()
            try:
                await channel.queue_delete(name)
            except Exception:  # Aufräumen ist best effort
                pass
            finally:
                if not channel.is_closed:
                    await channel.close()
    finally:
        await connection.close()


async def declare(connection: AbstractConnection, name: str, arguments: dict[str, Any]) -> str | None:
    """Deklariert auf einem Wegwerf-Channel; ``None`` bei Erfolg, sonst ``"<Klasse>: <Broker-Text>"``."""
    channel = await connection.channel()
    try:
        await channel.declare_queue(name, durable=True, arguments=arguments)
        return None
    except Exception as exc:
        return f"{type(exc).__name__}: {exc.args[0] if exc.args else exc}"
    finally:
        if not channel.is_closed:
            await channel.close()


async def publish(connection: AbstractConnection, queue: str, message: aio_pika.Message) -> None:
    channel = await connection.channel(publisher_confirms=True)
    try:
        await channel.default_exchange.publish(message, routing_key=queue)
    finally:
        await channel.close()


async def depth(connection: AbstractConnection, queue: str) -> int:
    channel = await connection.channel()
    try:
        declared = await channel.declare_queue(queue, passive=True)
        count = declared.declaration_result.message_count
        assert count is not None
        return int(count)
    finally:
        await channel.close()


async def get_message(connection: AbstractConnection, queue: str, timeout: float = 10.0) -> AbstractIncomingMessage:
    """Holt (``no_ack``) die nächste Nachricht aus ``queue``; pollt bis ``timeout``."""
    channel = await connection.channel()
    try:
        target = await channel.get_queue(queue, ensure=False)
        got: list[AbstractIncomingMessage] = []

        async def arrived() -> bool:
            message = await target.get(no_ack=True, fail=False)
            if message is not None:
                got.append(message)
            return bool(got)

        assert await wait_until(arrived, timeout=timeout, interval=0.02), f"no message in {queue}"
        return got[0]
    finally:
        await channel.close()


@asynccontextmanager
async def queue_policy(node: RabbitMQNode, queue: str, definition: dict[str, Any]) -> AsyncIterator[None]:
    """Policy nur für ``queue``. Vor dem Declare setzen: dann gilt sie ab der Anlage.
    Wird im ``finally`` entfernt."""
    url = f"{node.management_url}/policies/{node.vhost_api}/spike-{queue}"
    body = {"pattern": f"^{re.escape(queue)}$", "definition": definition, "apply-to": "queues", "priority": 1000}
    async with httpx.AsyncClient(auth=AUTH, timeout=5.0) as client:
        (await client.put(url, json=body)).raise_for_status()
        try:
            yield
        finally:
            await client.delete(url)


async def delivery_counts(
    connection: AbstractConnection,
    queue: str,
    give_back: Callable[[AbstractIncomingMessage], Awaitable[None]],
    *,
    cap: int = 30,
) -> list[Any]:
    """``basic.get`` ohne Ack und mit ``give_back`` zurückgeben, bis die Nachricht weg
    ist oder ``cap`` Zustellungen erreicht sind. Liefert ``x-delivery-count`` je Zustellung."""
    channel = await connection.channel()
    try:
        target = await channel.get_queue(queue, ensure=False)
        counts: list[Any] = []
        while len(counts) < cap:
            message = await target.get(no_ack=False, fail=False)
            if message is None:
                break
            counts.append((message.headers or {}).get("x-delivery-count"))
            await give_back(message)
        return counts
    finally:
        if not channel.is_closed:
            await channel.close()


async def nack_requeue(message: AbstractIncomingMessage) -> None:
    await message.nack(requeue=True)


async def reject_requeue(message: AbstractIncomingMessage) -> None:
    await message.reject(requeue=True)
