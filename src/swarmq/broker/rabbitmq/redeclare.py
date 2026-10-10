"""Eigene Queue nach einem unroutbaren Publish neu deklarieren (Plan 0.12.0,
MR 6, BZ-03).

Der Worker veroeffentlicht Retry- und DLQ-Kopien an ``{queue}.DQ`` bzw.
``{queue}.XQ`` seiner eigenen Zustellungs-Queue. Fehlt eine davon (geloescht,
waehrend der Worker lief), meldet der Broker ``basic.return``. Dann wird die
Queue-Familie **einmal** ueber die Kette aus MR 8 neu deklariert (eigene
Wegwerf-Channels, nie der Publish-Channel: ein 406 risse dort parallele
Publishes mit offenen Confirms ab; Uebernahme wie beim Start, Review MR 8,
H1) und der Publish ein zweites Mal versucht. Im
Hot-Path wird jede Ausnahme des Neu-Deklarierens zu ``BrokerConnectionError``;
das Original bleibt unbestaetigt und kommt per Redelivery wieder.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

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
    """Deklariert ``queue`` mit ``.DQ``/``.XQ`` erneut ueber die Kette mit
    Uebernahme wie beim Start (Review MR 8, H1): Wegwerf-Channels, passives
    Declare, Uebernahme; danach sind Cache und Argumente wieder gesetzt.
    Gedeckelt durch die Publish-Frist (Q6)."""
    connection = broker._connection
    if connection is None or connection.is_closed:
        raise BrokerConnectionError("broker is not connected")
    async with asyncio.timeout(broker._effective_publish_timeout()):
        broker.invalidate_declaration(queue)
        await broker.declare_queue(queue)
