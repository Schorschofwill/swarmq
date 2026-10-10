"""Vorhandene Queue-Argumente nach einem 406 übernehmen (Patch 0.11.1).

Ab 0.12 legt swarmq Queues mit ``x-dead-letter-strategy`` und ``x-overflow``
an, unter Umständen auch mit einem anderen ``x-delivery-limit``. Damit ein
Rollback auf 0.11.1 diese Queues weiter deklarieren kann, übernimmt der
Broker-Adapter für genau diese Argumente den vorhandenen Wert aus dem
406-Text. Andere Konflikte bleiben ein Fehler wie in 0.11.0.

Die Texte stammen aus Spike S1 (RabbitMQ 4.2.9 und 4.3.6)::

    PRECONDITION_FAILED - inequivalent arg '<arg>' for queue '<q>' in vhost '<v>': <rest>
"""

from __future__ import annotations

import asyncio
import re
from collections.abc import Awaitable, Callable, Mapping
from contextlib import suppress
from typing import Any

import aio_pika
from aio_pika.exceptions import ChannelPreconditionFailed

from swarmq.exceptions import BrokerConnectionError
from swarmq.logging import get_logger

logger = get_logger(__name__)

_INEQUIVALENT = re.compile(r"inequivalent arg '(?P<arg>[^']+)' for queue '.*' in vhost '.*': (?P<rest>.*)$")
_CURRENT_VALUE = re.compile(r"received none but current is the value '(?P<cur>.*)' of type '(?P<type>[^']+)'$")
_CURRENT_NONE = re.compile(r"received the value '.*' of type '[^']+' but current is none$")
_BOTH_PRESENT = re.compile(r"received '.*' but current is '(?P<cur>.*)'$")
_INTEGER_ARGUMENTS = frozenset({"x-delivery-limit"})
#: Nur diese Argumente übernimmt 0.11.1 von einer vorhandenen Queue.
ADOPTABLE_ARGUMENTS = frozenset({"x-dead-letter-strategy", "x-overflow", "x-delivery-limit"})


def adopt_existing_argument(text: str, arguments: Mapping[str, Any]) -> tuple[str, dict[str, Any]] | None:
    """``(argument, neue Argumente)`` mit dem vorhandenen Wert aus dem
    406-Text ``text``; ``None``, wenn der Konflikt nicht übernehmbar ist."""
    match = _INEQUIVALENT.search(text)
    if match is None:
        return None
    arg = match["arg"]
    if arg not in ADOPTABLE_ARGUMENTS:
        return None
    rest = match["rest"]
    if _CURRENT_NONE.match(rest):
        return arg, {key: value for key, value in arguments.items() if key != arg}
    current = _CURRENT_VALUE.match(rest) or _BOTH_PRESENT.match(rest)
    if current is None:
        return None
    value: Any = current["cur"]
    if arg in _INTEGER_ARGUMENTS:
        try:
            value = int(value)
        except ValueError:
            return None
    return arg, {**arguments, arg: value}


async def declare_adopting_existing(
    open_channel: Callable[[], Awaitable[aio_pika.abc.AbstractChannel]],
    queue: str,
    arguments: Mapping[str, Any],
    *,
    timeout_s: float,
) -> dict[str, Any]:
    """Deklariert ``queue`` auf Wegwerf-Channels und übernimmt bei einem 406
    den vorhandenen Wert eines übernehmbaren Arguments. Liefert die Argumente,
    mit denen die Deklaration gelang. Jedes Argument wird höchstens einmal
    übernommen; damit endet die Schleife nach höchstens
    ``1 + len(ADOPTABLE_ARGUMENTS)`` Versuchen. Ein nicht übernehmbarer 406
    geht unverändert an den Aufrufer; ein Versuch über ``timeout_s`` wird
    ``BrokerConnectionError``."""
    current = dict(arguments)
    adopted: set[str] = set()
    while True:
        channel: aio_pika.abc.AbstractChannel | None = None
        try:
            async with asyncio.timeout(timeout_s):
                channel = await open_channel()
                await channel.declare_queue(queue, durable=True, arguments=current)
            return current
        except TimeoutError as exc:
            raise BrokerConnectionError(
                f"declare_queue {queue!r} timed out after {timeout_s}s on a probe channel"
            ) from exc
        except ChannelPreconditionFailed as exc:
            result = adopt_existing_argument(str(exc.args[0] if exc.args else exc), current)
            if result is None or result[0] in adopted:
                raise
            arg, adopted_arguments = result
            adopted.add(arg)
            _log_adoption(queue, arg, current.get(arg), adopted_arguments.get(arg))
            current = adopted_arguments
        finally:
            # Auch nach einem 406: ein RobustChannel öffnet sich sonst selbst wieder.
            if channel is not None:
                with suppress(Exception):
                    await asyncio.wait_for(channel.close(), timeout=timeout_s)


def _log_adoption(queue: str, argument: str, declared: object, existing: object) -> None:
    if argument == "x-delivery-limit":
        # Der Broker dead-lettert dann nach dem vorhandenen Limit, nicht nach
        # der Konfiguration: das soll im Betrieb auffallen.
        logger.warning(
            "queue_argument_adopted",
            queue=queue,
            argument=argument,
            declared=declared,
            existing=existing,
            hint=(
                "the queue keeps its existing x-delivery-limit; the broker dead-letters "
                "after that limit, not after SWARMQ_DELIVERY_LIMIT"
            ),
        )
        return
    logger.info("queue_argument_adopted", queue=queue, argument=argument, declared=declared, existing=existing)
