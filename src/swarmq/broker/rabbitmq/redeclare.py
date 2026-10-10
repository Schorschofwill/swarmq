"""Eigene Queue nach einem unroutbaren Publish neu deklarieren (Plan 0.12.0,
MR 6, BZ-03).

Der Worker veroeffentlicht Retry- und DLQ-Kopien an ``{queue}.DQ`` (oder eine
Delay-Stufe ``{queue}.DQ.<stufe>``, BZ-01) bzw. ``{queue}.XQ`` seiner eigenen
Zustellungs-Queue. Fehlt eine davon (geloescht, waehrend der Worker lief),
meldet der Broker ``basic.return``. Dann wird die Queue-Familie -- bei einer
Stufe nur die Stufe (Review MR 10, P2-3) -- **einmal** ueber die Kette aus MR
8 neu deklariert (eigene
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
from swarmq.queue_names import is_delay_tier, main_queue_of

logger = get_logger(__name__)


async def publish_redeclaring_once(broker: Any, queue: str, publish: Callable[[], Awaitable[None]]) -> None:
    """``publish()``; ist es unroutbar, *queue* (eine Queue der Familie oder
    eine Delay-Stufe) neu deklarieren und einmal wiederholen. Ein zweites
    ``UnroutableMessage`` geht unveraendert durch."""
    try:
        await publish()
        return
    except UnroutableMessage as exc:
        logger.warning(
            "own_queue_unroutable_redeclaring",
            queue=queue,
            **_error_fields(exc),
            hint="a queue of this worker was deleted while it ran; declaring it again (a delay tier alone)",
        )
    try:
        await redeclare_on_throwaway_channel(broker, queue)
    except Exception as exc:
        raise BrokerConnectionError(f"redeclaring queue {queue!r} after an unroutable publish failed: {exc}") from exc
    await publish()


async def redeclare_on_throwaway_channel(broker: Any, queue: str) -> None:
    """Deklariert die Familie von ``queue`` (Haupt-Queue, ``.DQ``, ``.XQ``
    und Stufen) erneut ueber die Kette mit Uebernahme wie beim Start (Review
    MR 8, H1): Wegwerf-Channels, passives Declare, Uebernahme; danach sind
    Cache und Argumente wieder gesetzt. Ist ``queue`` eine Delay-Stufe, nur
    diese (Review MR 10, P2-3). Gedeckelt durch die Publish-Frist (Q6)."""
    connection = broker._connection
    if connection is None or connection.is_closed:
        raise BrokerConnectionError("broker is not connected")
    async with asyncio.timeout(broker._effective_publish_timeout()):
        if is_delay_tier(queue):
            # Nur die Stufe, nicht die Familie (Review MR 10, P2-3).
            await broker.redeclare_delay_tier(queue)
            return
        broker.invalidate_declaration(queue)
        await broker.declare_queue(main_queue_of(queue))
