"""RabbitMQ: Deklaration der Queue-Familie (Plan 0.12.0, MR 8, BZ-15/BZ-19).

Deklarationen laufen auf eigenen Wegwerf-Channels, nie auf dem
Publish-Channel: ein 406 risse dort parallele Publishes mit offenen Confirms
ab. Je Queue gilt die Kette aus ``arg_conflict``: passives Declare,
Bestandsqueue mit den Argumenten aus 0.11, neue Queue mit den Zielargumenten
(at-least-once im Modus ``auto``), Übernahme von ``x-dead-letter-strategy``,
``x-overflow`` und ``x-delivery-limit``, andere erkannte Konflikte als
``QueueArgumentsMismatch`` (Exit 78). Das Ergebnis wird je Familie gecacht;
``UnroutableMessage`` und NOT_FOUND verwerfen den Eintrag.
"""

from __future__ import annotations

from typing import Any

import aio_pika

from swarmq.broker.rabbitmq.arg_conflict import declare_adopting_existing, declare_chain
from swarmq.broker.rabbitmq.connection import _CHANNEL_READY_TIMEOUT_S, RabbitMQConnection
from swarmq.broker.rabbitmq.topology import (
    AT_LEAST_ONCE_ARGUMENTS,
    at_least_once,
    dead_letter_queue_arguments,
    delay_queue_arguments,
    main_queue_arguments,
    queue_family,
)
from swarmq.exceptions import MIGRATION_HINT, BrokerConnectionError, QueueArgumentsMismatch
from swarmq.logging import get_logger

logger = get_logger(__name__)

#: Wirksames Delivery-Limit einer Quorum-Queue ohne ``x-delivery-limit``
#: ab RabbitMQ 4 (Spike S1, belegt auf 4.2.9 und 4.3.6).
_BROKER_DEFAULT_DELIVERY_LIMIT = 20


def _family_of(name: str) -> str:
    """Name der Haupt-Queue zur Haupt-, Delay- oder Dead-Letter-Queue ``name``."""
    for suffix in (".DQ", ".XQ"):
        if name.endswith(suffix):
            return name.removesuffix(suffix)
    return name


def _dead_letters_at_least_once(arguments: dict[str, Any]) -> bool:
    """At-least-once-Dead-Lettering wirkt nur zusammen mit
    ``x-overflow=reject-publish``; sonst faellt RabbitMQ auf at-most-once
    zurueck (Review MR 8, P2-2)."""
    return all(arguments.get(key) == value for key, value in AT_LEAST_ONCE_ARGUMENTS.items())


class RabbitMQDeclarations(RabbitMQConnection):
    #: Obergrenze je Deklarationsversuch auf dem Wegwerf-Channel (Patch 0.11.1).
    declare_probe_timeout_s: float = _CHANNEL_READY_TIMEOUT_S

    # ------------------------------------------------------------------
    # Deklaration
    # ------------------------------------------------------------------

    async def declare_queue(self, queue_name: str) -> None:
        """Deklariert die Queue-Familie einmal je Prozess (Cache, Plan 0.12.0,
        MR 8); ``invalidate_declaration`` verwirft den Eintrag."""
        if queue_name in self._declaration_cache:
            return
        pub = await self._ensure_publish_channel()
        # Main queue — DLX points to dead-letter queue
        main_args = main_queue_arguments(queue_name, self._queue_type, self.delivery_limit)
        try:
            await self._declare_queue_triplet(pub, queue_name, main_args)
            if queue_name in self._declared_arguments:
                self._declaration_cache.add(queue_name)
        except aio_pika.exceptions.AMQPChannelError as exc:
            # Ein vom Broker abgelehntes Declare, das die Kette nicht erkennt
            # (S8: z. B. Queue-Limit, Rechte). Ohne diese Huelle zeigte die CLI
            # einen rohen aio_pika-Traceback. Kein Rat zum Loeschen: die Queue
            # kann Retries und Termine enthalten (Review MR 8, P2-5).
            raise BrokerConnectionError(
                f"declare_queue {queue_name!r} was refused by the broker: {exc}. "
                f"Existing queue arguments, policies or vhost limits do not accept this declaration; "
                f"see the broker message."
            ) from exc
        except (aio_pika.exceptions.AMQPConnectionError, RuntimeError) as exc:
            # Verbindungsabriss mitten in der Kette (aiormq ``ConnectionClosed``,
            # ``ChannelInvalidStateError``, ``RuntimeError`` aus aio_pika): wie
            # jeder Verbindungsverlust ein ``BrokerConnectionError`` (Review MR 8, H5a).
            raise BrokerConnectionError(
                f"declare_queue {queue_name!r}: the connection was lost while declaring: {exc!r}"
            ) from exc

    async def _declare_queue_triplet(
        self,
        pub: aio_pika.abc.AbstractChannel,
        queue_name: str,
        main_args: dict[str, Any],
    ) -> None:
        """Haupt-, Delay- und Dead-Letter-Queue ueber die Kette; auch das
        Neu-Deklarieren aus MR 6 nimmt diesen Weg (Review MR 8, H1)."""
        main, delay, dead_letter = queue_family(queue_name)
        delay_args = delay_queue_arguments(queue_name, self._queue_type)
        # Ziel at-least-once nur fuer Quorum-Queues mit Broker-DLX und nicht im
        # Modus ``legacy`` (BZ-19).
        wants_at_least_once = self._queue_type == "quorum" and self.dead_letter_strategy != "legacy"

        def target(arguments: dict[str, Any]) -> dict[str, Any] | None:
            return at_least_once(arguments) if wants_at_least_once else None

        await self._declare_adopting(pub, main, main_args, target(main_args))
        # Delay queue — DLX points back to main queue
        await self._declare_adopting(pub, delay, delay_args, target(delay_args))
        # Dead-letter queue
        await self._declare_adopting(pub, dead_letter, dead_letter_queue_arguments(self._queue_type))

    async def _declare_adopting(
        self,
        pub: aio_pika.abc.AbstractChannel,
        name: str,
        arguments: dict[str, Any],
        target: dict[str, Any] | None = None,
    ) -> None:
        """Deklariert ``name`` auf eigenen Wegwerf-Channels (Kette aus
        ``arg_conflict``), nie auf dem Publish-Channel: ein 406 risse dort
        parallele Publishes mit offenen Confirms ab (Plan 0.12.0, MR 8).

        Ohne offene eigene Verbindung gibt es keinen Wegwerf-Channel; dann
        gilt das Verhalten aus 0.11.0 (direkt auf dem Publish-Channel). Dieser
        Rueckfall existiert nur fuer Unit-Tests und den Golden-Generator
        (``_publish_channel`` ohne ``connect``); ein verbundener Broker hat
        immer eine Verbindung und erreicht ihn im Betrieb nicht (Review MR 8,
        H2: bewusst beibehalten)."""
        connection = self._connection
        if connection is None or connection.is_closed:
            await pub.declare_queue(name, durable=True, arguments=arguments)
            return
        timeout_s = self.declare_probe_timeout_s
        log = self._adoption_log
        if target is None:
            arguments = await declare_adopting_existing(connection, name, arguments, timeout_s=timeout_s, log=log)
        else:
            arguments = await declare_chain(connection, name, arguments, target, timeout_s=timeout_s, log=log)
            self._require_at_least_once(name, arguments)
            if not _dead_letters_at_least_once(arguments):
                self._at_most_once_queues.add(name)
        self._declared_arguments[name] = arguments

    async def report_queue_declarations(self) -> None:
        """Höchstens eine WARNING ``queue_dead_letter_at_most_once`` je Prozess
        (Broker) mit allen Bestandsqueues ohne at-least-once-Dead-Lettering
        (BZ-19). Der Worker ruft es nach dem Deklarieren seiner Queues."""
        if self._at_most_once_reported or not self._at_most_once_queues:
            return
        self._at_most_once_reported = True
        logger.warning(
            "queue_dead_letter_at_most_once",
            queues=sorted(self._at_most_once_queues),
            hint=(
                "these existing quorum queues dead-letter at-most-once (RabbitMQ default): retries "
                "and DLQ transfers can be lost on a leader change. Set a policy with "
                "dead-letter-strategy=at-least-once and overflow=reject-publish, or set "
                "SWARMQ_QUEUE_DEAD_LETTER_STRATEGY=legacy to silence this warning"
            ),
        )

    def effective_delivery_limit(self, queue: str) -> int | None:
        """Das wirksame ``x-delivery-limit`` einer von diesem Prozess deklarierten
        Haupt-Queue (BZ-15, Plan 0.12.0, MR 8); ``None`` = unbekannt oder
        unbegrenzt, dann gilt die Konfiguration.

        Fehlt das Argument, gilt ab RabbitMQ 4 der Broker-Default 20; ``-1``
        heisst unbegrenzt, ``0`` "keine Wiederzustellung" (Spike S1). Eine
        ``delivery-limit``-Policy ist ohne Management-API nicht sichtbar."""
        # Aus den zuletzt erfolgreich deklarierten Argumenten, nicht aus dem
        # Cache: dessen Verwerfen (F5) aendert das Limit der Queue nicht
        # (Review MR 8, P2-1).
        arguments = self._declared_arguments.get(queue)
        if self._queue_type != "quorum" or arguments is None or _family_of(queue) != queue:
            return None
        limit = arguments.get("x-delivery-limit", _BROKER_DEFAULT_DELIVERY_LIMIT)
        if not isinstance(limit, int) or limit < 0:
            return None
        return limit

    def invalidate_declaration(self, name: str) -> None:
        """Verwirft den Cache-Eintrag der Familie von ``name`` (Haupt-, Delay-
        oder Dead-Letter-Queue): bei ``UnroutableMessage`` und NOT_FOUND ist
        die Queue weg, die naechste Deklaration legt sie neu an (Rev. 3 F5)."""
        self._declaration_cache.discard(_family_of(name))

    def _require_at_least_once(self, name: str, arguments: dict[str, Any]) -> None:
        """Strenger Modus ``at-least-once`` (BZ-19): eine Bestandsqueue ohne
        at-least-once-Dead-Lettering ist ein Konfigurationsfehler (Exit 78)."""
        if self.dead_letter_strategy != "at-least-once" or _dead_letters_at_least_once(arguments):
            return
        # Das erste abweichende Argument des 0.12-Satzes (Review MR 8, P2-2).
        argument, expected = next(
            (key, value) for key, value in AT_LEAST_ONCE_ARGUMENTS.items() if arguments.get(key) != value
        )
        raise QueueArgumentsMismatch(
            name,
            argument,
            expected,
            arguments.get(argument),
            hint=(
                "SWARMQ_QUEUE_DEAD_LETTER_STRATEGY=at-least-once requires at-least-once dead-lettering "
                "(x-dead-letter-strategy=at-least-once and x-overflow=reject-publish). Use "
                f"SWARMQ_QUEUE_DEAD_LETTER_STRATEGY=auto, set a policy, or {MIGRATION_HINT}"
            ),
        )
