"""Eigene Queue nach einem unroutbaren Publish neu deklarieren (Plan 0.12.0,
MR 6, BZ-03).

Der Worker veroeffentlicht Retry- und DLQ-Kopien an ``{queue}.DQ`` bzw.
``{queue}.XQ`` seiner eigenen Zustellungs-Queue. Fehlt eine davon (geloescht,
waehrend der Worker lief), meldet der Broker ``basic.return``. Dann wird die
Queue-Familie **einmal** auf einem eigenen Wegwerf-Channel neu deklariert --
nie auf dem Publish-Channel, ein 406 risse dort parallele Publishes mit
offenen Confirms ab (MR 8) -- und der Publish ein zweites Mal versucht. Im
Hot-Path wird jede Ausnahme des Neu-Deklarierens zu ``BrokerConnectionError``;
das Original bleibt unbestaetigt und kommt per Redelivery wieder.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

from swarmq.broker.rabbitmq.connection import _CHANNEL_CLOSE_TIMEOUT_S
from swarmq.broker.rabbitmq.topology import main_queue_arguments
from swarmq.exceptions import BrokerConnectionError, UnroutableMessage
from swarmq.logging import _error_fields, get_logger

logger = get_logger(__name__)


async def publish_redeclaring_once(broker: Any, queue: str, publish: Callable[[], Awaitable[None]]) -> None:
    """``publish()``; ist es unroutbar, *queue* neu deklarieren und einmal
    wiederholen. Ein zweites ``UnroutableMessage`` geht unveraendert durch."""
    try:
        await publish()
        return
    except UnroutableMessage as exc:
        logger.warning(
            "own_queue_unroutable_redeclaring",
            queue=queue,
            **_error_fields(exc),
            hint="a queue of this worker was deleted while it ran; declaring it again",
        )
    try:
        await redeclare_on_throwaway_channel(broker, queue)
    except Exception as exc:
        raise BrokerConnectionError(f"redeclaring queue {queue!r} after an unroutable publish failed: {exc}") from exc
    await publish()


async def redeclare_on_throwaway_channel(broker: Any, queue: str) -> None:
    """Deklariert ``queue`` mit ``.DQ``/``.XQ`` auf einem eigenen Channel,
    der danach geschlossen wird; gedeckelt durch die Publish-Frist (Q6)."""
    connection = broker._connection
    if connection is None or connection.is_closed:
        raise BrokerConnectionError("broker is not connected")
    channel: Any = None
    try:
        async with asyncio.timeout(broker._effective_publish_timeout()):
            channel = await connection.channel()
            main_args = main_queue_arguments(queue, broker._queue_type, broker.delivery_limit)
            await broker._declare_queue_triplet(channel, queue, main_args, adopt=False)
    finally:
        if channel is not None and not channel.is_closed:
            try:
                await asyncio.wait_for(channel.close(), timeout=_CHANNEL_CLOSE_TIMEOUT_S)
            except Exception:
                pass
