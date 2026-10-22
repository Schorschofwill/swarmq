"""RabbitMQ: Deklaration der Queue-Familie (Plan 0.12.0, MR 8, BZ-15/BZ-19).

Deklarationen laufen auf eigenen Wegwerf-Channels, nie auf dem
Publish-Channel: ein 406 risse dort parallele Publishes mit offenen Confirms
ab. Je Queue gilt die Kette aus ``arg_conflict``: passives Declare,
Bestandsqueue mit den Argumenten aus 0.11, neue Queue mit den Zielargumenten
(at-least-once im Modus ``auto``), Übernahme von ``x-dead-letter-strategy``,
``x-overflow`` und ``x-delivery-limit``, andere erkannte Konflikte als
``QueueArgumentsMismatch`` (Exit 78). Das Ergebnis wird je Familie gecacht;
``UnroutableMessage`` und NOT_FOUND verwerfen den Eintrag. Die Delay-Stufen
(BZ-01, MR 10) folgen beim Start auf die Familie, ein Fehler je Stufe ist
nicht fatal (Review MR 10, P1-2).
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from functools import cached_property
from typing import Any

import aio_pika

from swarmq.broker.rabbitmq.arg_conflict import declare_adopting_existing, declare_chain
from swarmq.broker.rabbitmq.connection import _CHANNEL_READY_TIMEOUT_S, RabbitMQConnection
from swarmq.broker.rabbitmq.topology import (
    AT_LEAST_ONCE_ARGUMENTS,
    at_least_once,
    dead_letter_queue_arguments,
    delay_queue_arguments,
    delay_queue_for,
    delay_tier_for,
    delay_tiers,
    main_queue_arguments,
    queue_family,
)
from swarmq.exceptions import MIGRATION_HINT, BrokerConnectionError, QueueArgumentsMismatch
from swarmq.logging import _error_fields, get_logger
from swarmq.queue_names import is_delay_tier, main_queue_of

logger = get_logger(__name__)

#: Wirksames Delivery-Limit einer Quorum-Queue ohne ``x-delivery-limit``
#: ab RabbitMQ 4 (Spike S1, belegt auf 4.2.9 und 4.3.6).
_BROKER_DEFAULT_DELIVERY_LIMIT = 20


def _dead_letters_at_least_once(arguments: dict[str, Any]) -> bool:
    """At-least-once-Dead-Lettering wirkt nur zusammen mit
    ``x-overflow=reject-publish``; sonst faellt RabbitMQ auf at-most-once
    zurueck (Review MR 8, P2-2)."""
    return all(arguments.get(key) == value for key, value in AT_LEAST_ONCE_ARGUMENTS.items())


#: Pause nach einem voruebergehend gescheiterten Neu-Deklarieren einer
#: Delay-Stufe im Betrieb (PF-21, E-026): so lange gehen Rueckstellungen an
#: die Stufe ohne Versuch an ``{queue}.DQ``.
DELAY_TIER_RETRY_PAUSE_S = 30.0


@dataclass
class _DelayTierRetries:
    """Zustand je Prozess fuer Delay-Stufen, deren Neu-Deklarieren im Betrieb
    voruebergehend gescheitert ist (PF-21)."""

    #: Zeitpunkt (``monotonic``) des naechsten Versuchs je Stufe.
    pending: dict[str, float] = field(default_factory=dict)
    #: Stufen, deren Neu-Deklarieren gerade laeuft (hoechstens eins je Stufe).
    running: set[str] = field(default_factory=set)


class RabbitMQDeclarations(RabbitMQConnection):
    #: Obergrenze je Deklarationsversuch auf dem Wegwerf-Channel (Patch 0.11.1).
    declare_probe_timeout_s: float = _CHANNEL_READY_TIMEOUT_S
    #: Uhr der Pause nach einem gescheiterten Neu-Deklarieren (PF-21).
    monotonic: Callable[[], float] = staticmethod(time.monotonic)

    @cached_property
    def _tier_retries(self) -> _DelayTierRetries:
        return _DelayTierRetries()

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
        # Delay-Stufen (BZ-01, MR 10) beim Start, nach der Familie: Rechte- und
        # Limit-Fehler zeigen sich sofort, der Hot-Path kennt keine Kette
        # (Review MR 10, P1-2). Ein Fehler je Stufe ist nicht fatal.
        for tier in delay_tiers(queue_name):
            if tier not in self._ready_delay_tiers and tier not in self._unavailable_delay_tiers:
                await self._declare_delay_tier(queue_name, tier, delay)

    def _delay_target(self, queue_name: str, delay_ms: int) -> str:
        """Ziel einer Rueckstellung um ``delay_ms`` (BZ-01, MR 10): die
        kleinste Delay-Stufe ``{queue}.DQ.<stufe>``, deren Obergrenze
        ``delay_ms`` erreicht, sofern dieser Prozess sie beim Start deklariert
        hat (Review MR 10, P1-2); nur ein Prozess, der die Familie selbst
        deklariert hat (der Worker), nutzt Stufen. Sonst, ueber 1 h und fuer
        eine nicht verfuegbare Stufe gilt ``{queue}.DQ`` wie bis 0.11."""
        fallback = delay_queue_for(queue_name, delay_ms)
        tier = delay_tier_for(queue_name, delay_ms)
        if tier == fallback or queue_name not in self._declared_arguments:
            return fallback
        return tier if tier in self._ready_delay_tiers else fallback

    def due_delay_tier(self, queue_name: str, delay_ms: int) -> str | None:
        """Die Stufe einer Rueckstellung um ``delay_ms``, wenn ihr Neu-Deklarieren
        voruebergehend gescheitert und die Pause vorbei ist (PF-21): Die
        Rueckstellung versucht es erneut, erst Neu-Deklarieren, dann Publish."""
        tier = delay_tier_for(queue_name, delay_ms)
        if tier not in self._tier_retries.pending or self._delay_tier_paused(tier):
            return None
        return tier

    def _delay_tier_paused(self, tier: str) -> bool:
        retry_at = self._tier_retries.pending.get(tier)
        return retry_at is not None and self.monotonic() < retry_at

    def delay_tiers_unavailable(self, queue: str) -> int:
        """Zahl der Delay-Stufen von ``queue``, deren Deklaration der Broker in
        diesem Prozess abgelehnt hat (BZ-01, MR 10; Gauge des Workers)."""
        return sum(1 for tier in delay_tiers(queue) if tier in self._unavailable_delay_tiers)

    async def _declare_delay_tier(self, queue_name: str, tier: str, fallback: str) -> bool:
        """Deklariert die Stufe ``tier`` mit den Argumenten von ``{queue}.DQ``
        ueber die Kette (beim Start und beim Neu-Deklarieren nach einem
        unroutbaren Publish); ``False``, wenn der Broker sie ablehnt."""
        delay_args = delay_queue_arguments(queue_name, self._queue_type)
        wants_at_least_once = self._queue_type == "quorum" and self.dead_letter_strategy != "legacy"
        target = at_least_once(delay_args) if wants_at_least_once else None
        pub = await self._ensure_publish_channel()
        try:
            await self._declare_adopting(pub, tier, delay_args, target)
        except (aio_pika.exceptions.AMQPChannelError, QueueArgumentsMismatch) as exc:
            if isinstance(exc, QueueArgumentsMismatch) and exc.argument in AT_LEAST_ONCE_ARGUMENTS:
                # Der strenge Modus ``at-least-once`` (``_require_at_least_once``):
                # eine at-most-once-Stufe ist derselbe Konfigurationsfehler wie
                # eine at-most-once-Bestandsqueue, Exit 78 (Review MR 10, P2-4).
                raise
            # Nicht fatal (Plan MR 10): Queue-Limit, Rechte, ein Konflikt mit
            # einer vorhandenen Queue. Die Rueckstellung geht an ``{queue}.DQ``;
            # die Stufe bleibt fuer diesen Prozess aus, damit nicht jede
            # Rueckstellung erneut einen 406 kostet.
            self._refuse_delay_tier(queue_name, tier, fallback, exc)
            return False
        self._ready_delay_tiers.add(tier)
        if self._tier_retries.pending.pop(tier, None) is not None:
            # Auch die Familien-Deklaration heilt eine ausstehende Stufe (PF-21).
            logger.info("delay_tier_restored", queue=queue_name, tier=tier)
        return True

    def _refuse_delay_tier(
        self, queue_name: str, tier: str, fallback: str, exc: BaseException, *, hint: str | None = None
    ) -> None:
        """Die Stufe bleibt fuer diesen Prozess aus (E-026): WARNING, Gauge."""
        self._unavailable_delay_tiers.add(tier)
        self._tier_retries.pending.pop(tier, None)
        logger.warning(
            "delay_tier_unavailable",
            queue=queue_name,
            tier=tier,
            fallback=fallback,
            **_error_fields(exc),
            hint=hint
            or (
                "the delay tier could not be declared; delayed messages of this tier go to the "
                ".DQ, where a long delay at its head holds back shorter ones. Check queue limits "
                "and permissions for the pattern \\.DQ(\\.\\w+)?$, then restart the worker"
            ),
        )

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
        if self._queue_type != "quorum" or arguments is None or main_queue_of(queue) != queue:
            return None
        limit = arguments.get("x-delivery-limit", _BROKER_DEFAULT_DELIVERY_LIMIT)
        if not isinstance(limit, int) or limit < 0:
            return None
        return limit

    def invalidate_declaration(self, name: str) -> None:
        """Verwirft den Cache-Eintrag der Familie von ``name`` (Haupt-, Delay-
        oder Dead-Letter-Queue): bei ``UnroutableMessage`` und NOT_FOUND ist
        die Queue weg, die naechste Deklaration legt sie neu an (Rev. 3 F5).
        Eine Delay-Stufe (BZ-01, MR 10) verliert nur ihren eigenen Eintrag;
        die Familie bleibt deklariert (Review MR 10, P2-3)."""
        if is_delay_tier(name):
            self._ready_delay_tiers.discard(name)
            return
        self._declaration_cache.discard(main_queue_of(name))

    async def restore_delay_tier(self, tier: str) -> bool:
        """Deklariert nur die Stufe ``tier`` neu, nach einem unroutbaren
        Publish an sie (Review MR 10, P2-3); ``True``, wenn sie danach Ziel
        ist. Lehnt der Broker sie ab, gilt ``{queue}.DQ`` fuer den Rest des
        Prozesses (E-026). Ein voruebergehender Fehler ist im Betrieb nicht
        fatal (PF-21): WARNING, die Rueckstellung geht an ``{queue}.DQ``."""
        if tier in self._tier_retries.running or self._delay_tier_paused(tier):
            # Hoechstens ein Versuch je Stufe gleichzeitig und keiner in der
            # Pause (PF-21, Review P3-2): die Rueckstellung wartet nicht, sie
            # geht an ``{queue}.DQ``.
            return False
        self._tier_retries.running.add(tier)
        try:
            return await self._restore_delay_tier(tier)
        finally:
            self._tier_retries.running.discard(tier)

    async def _restore_delay_tier(self, tier: str) -> bool:
        queue_name = main_queue_of(tier)
        fallback = delay_queue_for(queue_name, 0)
        try:
            return await self._declare_delay_tier_bounded(queue_name, tier, fallback)
        except QueueArgumentsMismatch as exc:
            # Strenger Modus (E-017): beim Start fatal, im Betrieb wie eine
            # Ablehnung (PF-21; wie E-083 kein Herunterfahren im Betrieb).
            # Exit 78 kommt beim naechsten Start.
            self._refuse_delay_tier(
                queue_name,
                tier,
                fallback,
                exc,
                hint=(
                    "the existing delay tier dead-letters at-most-once, which the strict mode "
                    "at-least-once refuses; delayed messages of this tier go to the .DQ. Apply the "
                    "at-least-once policy or delete the empty tier: otherwise the next start of the "
                    "worker exits with 78"
                ),
            )
            return False
        except Exception as exc:  # noqa: BLE001 - jeder andere Fehler gilt als voruebergehend (PF-21), wird geloggt
            self._tier_retries.pending[tier] = self.monotonic() + DELAY_TIER_RETRY_PAUSE_S
            logger.warning(
                "delay_tier_redeclare_failed",
                queue=queue_name,
                tier=tier,
                fallback=fallback,
                **_error_fields(exc),
                retry_in_s=DELAY_TIER_RETRY_PAUSE_S,
                hint=(
                    "declaring the deleted delay tier again failed; delayed messages of this tier go to "
                    "the .DQ until a later attempt succeeds"
                ),
            )
            return False

    async def _declare_delay_tier_bounded(self, queue_name: str, tier: str, fallback: str) -> bool:
        """``_declare_delay_tier`` mit eigener kurzer Frist statt der
        Publish-Frist (PF-21, Review-Punkt P2), beim Herunterfahren mit der
        kuerzeren Restfrist; ein abgebrochenes Anlegen laeuft beim Broker
        weiter. Ohne offene Verbindung ``BrokerConnectionError``: sonst
        deklarierte ``_declare_adopting`` auf dem Publish-Channel."""
        if self._connection is None or self._connection.is_closed:
            raise BrokerConnectionError("broker is not connected")
        async with asyncio.timeout(min(self.declare_probe_timeout_s, self._effective_publish_timeout())):
            return await self._declare_delay_tier(queue_name, tier, fallback)

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
