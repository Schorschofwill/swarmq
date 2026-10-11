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
from collections.abc import Mapping
from contextlib import suppress
from typing import Any

import aio_pika
from aio_pika.exceptions import ChannelNotFoundEntity, ChannelPreconditionFailed

from swarmq.exceptions import BrokerConnectionError, QueueArgumentsMismatch
from swarmq.logging import get_logger

logger = get_logger(__name__)

_INEQUIVALENT = re.compile(r"inequivalent arg '(?P<arg>[^']+)' for queue '.*' in vhost '.*': (?P<rest>.*)$")
_CURRENT_VALUE = re.compile(r"received none but current is the value '(?P<cur>.*)' of type '(?P<type>[^']+)'$")
_BOTH_PRESENT = re.compile(r"received '.*' but current is '(?P<cur>.*)'$")
_CURRENT_NONE = re.compile(r"received the value '.*' of type '[^']+' but current is none$")
_RECEIVED = re.compile(
    r"^received (?:none|the value '(?P<value>.*?)' of type '[^']+'|'(?P<plain>.*?)') but current is "
)
_CURRENT = re.compile(r"but current is (?:none|the value '(?P<value>.*)' of type '[^']+'|'(?P<plain>.*)')$")
_INTEGER_ARGUMENTS = frozenset({"x-delivery-limit"})
#: Nur diese Argumente werden von einer vorhandenen Queue übernommen (0.11.1, 0.12).
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
        # Fehlt das Argument auf der Queue, wird es weggelassen (0.12, MR 8):
        # eine Queue, die ein 0.11-Pod nach dem passiven Declare angelegt hat,
        # oder eine Queue ohne Delivery-Limit (dann gilt der Broker-Default 20).
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


def recognized_mismatch(queue: str, text: str) -> QueueArgumentsMismatch | None:
    """``QueueArgumentsMismatch`` für einen erkannten "inequivalent arg"-Text
    (Spike S1); ``None`` für jeden anderen 406 (S8: Queue-Limit u. a.)."""
    match = _INEQUIVALENT.search(text)
    if match is None:
        return None
    rest = match["rest"]
    received, current = _RECEIVED.match(rest), _CURRENT.search(rest)
    declared = None if received is None else (received["value"] or received["plain"])
    existing = None if current is None else (current["value"] or current["plain"])
    return QueueArgumentsMismatch(queue, match["arg"], declared, existing)


async def declare_adopting_existing(
    connection: aio_pika.abc.AbstractConnection,
    queue: str,
    arguments: Mapping[str, Any],
    *,
    timeout_s: float,
    channel: aio_pika.abc.AbstractChannel | None = None,
    log: AdoptionLog | None = None,
) -> dict[str, Any]:
    """Deklariert ``queue`` auf Wegwerf-Channels und übernimmt bei einem 406
    den vorhandenen Wert eines übernehmbaren Arguments. Liefert die Argumente,
    mit denen die Deklaration gelang. Jedes Argument wird höchstens einmal
    übernommen; damit endet die Schleife nach höchstens
    ``1 + len(ADOPTABLE_ARGUMENTS)`` Versuchen. Ein nicht übernehmbarer 406
    geht unverändert an den Aufrufer. ``timeout_s`` begrenzt nur das Öffnen
    des Wegwerf-Channels; darüber folgt ``BrokerConnectionError``. Ein schon
    offener ``channel`` dient dem ersten Versuch und wird danach geschlossen."""
    log = log or AdoptionLog()
    current = dict(arguments)
    adopted: set[str] = set()
    while True:
        try:
            if channel is None:
                channel = await _open_probe_channel(connection, queue, timeout_s)
            # Queue.Declare ohne Client-Frist wie in 0.11.0: das Anlegen einer
            # Quorum-Queue (Raft) kann unter Last lange dauern.
            await channel.declare_queue(queue, durable=True, arguments=current)
            return current
        except ChannelPreconditionFailed as exc:
            text = str(exc.args[0] if exc.args else exc)
            result = adopt_existing_argument(text, current)
            if result is None:
                # Andere erkannte Konflikte enden mit Exit 78 (BZ-15, MR 8);
                # ein unerkannter 406 geht unverändert an den Aufrufer.
                mismatch = recognized_mismatch(queue, text)
                if mismatch is not None:
                    raise mismatch from exc
                raise
            if result[0] in adopted:
                raise
            arg, adopted_arguments = result
            adopted.add(arg)
            log.adopted(queue, arg, current.get(arg), adopted_arguments.get(arg))
            current = _with_full_set(arg, adopted_arguments, adopted)
        finally:
            # Auch nach einem 406: ein RobustChannel öffnet sich sonst selbst wieder.
            if channel is not None:
                with suppress(Exception):
                    await asyncio.wait_for(channel.close(), timeout=timeout_s)
            channel = None


def _with_full_set(argument: str, arguments: dict[str, Any], adopted: set[str]) -> dict[str, Any]:
    """Hat die Queue ``at-least-once``, gehört ``reject-publish`` dazu (0.12-Satz):
    gleich mitschicken statt einen zweiten 406 abzuwarten (Plan 0.12.0, MR 8).
    Ein schon übernommenes ``x-overflow`` bleibt (Review MR 8, P2-2)."""
    if (
        argument == "x-dead-letter-strategy"
        and arguments.get(argument) == "at-least-once"
        and "x-overflow" not in adopted
    ):
        return {**arguments, "x-overflow": "reject-publish"}
    return arguments


async def declare_chain(
    connection: aio_pika.abc.AbstractConnection,
    queue: str,
    legacy: Mapping[str, Any],
    target: Mapping[str, Any],
    *,
    timeout_s: float,
    log: AdoptionLog | None = None,
) -> dict[str, Any]:
    """Kette je Queue (Plan 0.12.0, MR 8): passives Declare auf einem
    Wegwerf-Channel. Existiert die Queue, folgt auf demselben Channel die
    Deklaration mit ``legacy`` (Bestandsqueues bekommen so keinen 406), sonst
    wird sie mit ``target`` angelegt. Danach wie ``declare_adopting_existing``."""
    channel = await _open_probe_channel(connection, queue, timeout_s)
    try:
        await channel.declare_queue(queue, passive=True)
    except ChannelNotFoundEntity:
        # Der Broker hat den Channel geschlossen; schliessen, damit ein
        # RobustChannel sich nicht selbst wieder oeffnet.
        with suppress(Exception):
            await asyncio.wait_for(channel.close(), timeout=timeout_s)
        return await declare_adopting_existing(connection, queue, target, timeout_s=timeout_s, log=log)
    except BaseException:
        with suppress(Exception):
            await asyncio.wait_for(channel.close(), timeout=timeout_s)
        raise
    return await declare_adopting_existing(connection, queue, legacy, timeout_s=timeout_s, channel=channel, log=log)


async def _open_probe_channel(
    connection: aio_pika.abc.AbstractConnection, queue: str, timeout_s: float
) -> aio_pika.abc.AbstractChannel:
    """Wegwerf-Channel öffnen, binnen ``timeout_s`` (inklusive Warten auf die
    Verbindung). Eine abgerissene Verbindung (aio_pika wirft dann synchron
    ``RuntimeError``) und das Zeitlimit werden ``BrokerConnectionError``."""
    pending: Any = None
    try:
        async with asyncio.timeout(timeout_s):
            await connection.ready()
            # aio_pika liefert das Channel-Objekt sofort, erst ``await`` öffnet
            # es. So bleibt es greifbar, falls das Öffnen ins Zeitlimit läuft.
            pending = connection.channel()
            channel: aio_pika.abc.AbstractChannel = await pending
            return channel
    except TimeoutError as exc:
        if pending is not None and not asyncio.iscoroutine(pending):
            with suppress(Exception):
                await asyncio.wait_for(pending.close(), timeout=timeout_s)
        raise BrokerConnectionError(
            f"declare_queue {queue!r}: opening a probe channel timed out after {timeout_s}s"
        ) from exc
    except RuntimeError as exc:  # inklusive ChannelInvalidStateError
        raise BrokerConnectionError(f"declare_queue {queue!r}: opening a probe channel failed: {exc}") from exc


class AdoptionLog:
    """Logzeilen der Übernahme für einen Broker (Plan 0.12.0, MR 8).

    Ab 0.12 übernimmt jeder Start die Strategy der von 0.12 angelegten Queues;
    das ist Normalbetrieb. Nur die erste solche Übernahme kommt als INFO, jede
    weitere als DEBUG. Ein abweichendes ``x-delivery-limit`` bleibt je Queue
    eine WARNING: es wirkt anders als konfiguriert."""

    def __init__(self) -> None:
        self._info_logged = False

    def adopted(self, queue: str, argument: str, declared: object, existing: object) -> None:
        fields: dict[str, Any] = {"queue": queue, "argument": argument, "declared": declared, "existing": existing}
        if argument == "x-delivery-limit":
            logger.warning("queue_argument_adopted", **fields, hint=_delivery_limit_hint(declared, existing))
        elif self._info_logged:
            logger.debug("queue_argument_adopted", **fields)
        else:
            self._info_logged = True
            logger.info("queue_argument_adopted", **fields)


def _delivery_limit_hint(declared: object, existing: object) -> str:
    """``declared`` ist ``SWARMQ_DELIVERY_LIMIT``. Das Gate des Workers nutzt
    ab 0.12 ``min(SWARMQ_DELIVERY_LIMIT, vorhandenes Limit)`` (Plan MR 8)."""
    if existing is None:
        return (
            "the queue has no x-delivery-limit, so RabbitMQ applies its default of 20: "
            "the worker dead-letters at min(SWARMQ_DELIVERY_LIMIT, 20) - 1 deliveries"
        )
    if isinstance(declared, int) and isinstance(existing, int) and existing < declared:
        return (
            "the queue keeps its existing x-delivery-limit, which is below SWARMQ_DELIVERY_LIMIT: "
            "the worker dead-letters at the existing limit - 1 deliveries"
        )
    return (
        "the queue keeps its existing x-delivery-limit, which is above SWARMQ_DELIVERY_LIMIT: "
        "the worker still dead-letters at SWARMQ_DELIVERY_LIMIT - 1 deliveries"
    )
