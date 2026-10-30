"""Consumer-Wächter: verlorene Consumer selbst neu anmelden (Plan
2026-10-08-1541, U6, KTD3).

Beendet der Broker einen Consumer (``basic.cancel`` ab RabbitMQ 4.3 nach
``consumer-timeout``, Channel-Close bis 4.2), merkt aio-pika davon nichts
(seine eigene Wiederanmeldung aus 10.2 ist für swarmqs Kanäle abgeschaltet,
E-089, ``underlay.ReleasingRobustChannel``):
Der Tag bleibt in ``RobustQueue._consumers`` und der Worker bekäme ohne
Neustart nie wieder eine Nachricht. Der Wächter vergleicht deshalb in jedem
Takt die angemeldeten Tags mit ``consumers`` des aktuellen aiormq-Channels.

* Fehlt ein Tag auf einem offenen, unveränderten Channel (4.3,
  ``basic.cancel``), meldet der Wächter ihn sofort selbst ab. Auf 4.3.6
  belegt: Das beendet den Zombie am Broker, räumt ``RobustQueue._consumers``
  auf und macht ein spätes Ack unschädlich. Erst danach meldet er neu an.
* Fehlt ein Tag, weil der Channel geschlossen oder ersetzt wurde (bis 4.2),
  wartet er einen Takt, ob aio-pikas Restore ihn wieder anlegt (auf 4.2.9
  belegt: gleicher Tag auf neuem Channel), und greift erst im zweiten ein.
* Neu angemeldet wird über dieselbe ``RobustQueue`` mit neuem Tag, nicht über
  ``broker.consume()``: ``_ensure_consume_channel`` könnte im Restore-Fenster
  einen zweiten Channel öffnen.

Die Queue-Objekte sind aio-pika-``RobustQueue``; benutzt werden nur
``channel.get_underlay_channel()``, ``cancel`` und ``consume``.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from swarmq.broker.rabbitmq.robust_channels import readmit_queue, release_queue
from swarmq.logging import _error_fields, get_logger

logger = get_logger(__name__)

#: Hook für die Metrik ``swarmq_consumer_reregistrations_total``: (Queue, Grund).
ReregisteredHook = Callable[[str, str], None]
#: Passive Prüfung, ob eine Queue beim Broker existiert (PF-20, E-083).
QueueExists = Callable[[str], Awaitable[bool]]

# ``RobustChannel.get_underlay_channel`` wartet auf ``connection.ready()`` und
# kehrt bei getrennter Verbindung nie zurück. Der Takt gibt dann auf; die
# Wiederverbindung ist Sache von aio-pika, ``/ready`` meldet sie über den
# Broker-Ping. Begrenzt wird nur dieser Kanalbezug (im Takt und vor jedem
# RPC, ``_channel_ready``), nicht das RPC selbst.
_RPC_TIMEOUT_S = 2.0
# Notbremse für ``basic.cancel``/``basic.consume`` (FS-09, KTD16). Ein
# abgebrochenes RPC beantwortet aiormq mit ``Channel.Close(504)`` -- auf diesem
# Kanal liegen alle Consumer des Workers (E-019), alle unbestätigten
# Zustellungen kämen zurück, während sie hier noch laufen. Deshalb kein
# ``wait_for`` um das RPC, sondern aiormqs eigene Frist: Sie schließt den Kanal
# erst, wenn der Broker so lange stumm war, dass der Kanal ohnehin verloren ist.
# Eine Frist, die noch vor dem Senden (Kanalsperre) abläuft, schließt nichts.
_RPC_EMERGENCY_TIMEOUT_S = 60.0
# So lange wartet ``stop()`` auf einen laufenden Takt. Danach koppelt es ihn
# ab, statt ihn abzubrechen (FS-09): Ein mitten im RPC abgebrochener Cancel
# oder Consume schlösse den Consume-Kanal und hinterließe einen Tag, von dem
# niemand weiß, ob der Broker ihn noch kennt.
_STOP_TIMEOUT_S = 5.0
# Fehlt ein Tag auf geschlossenem oder ersetztem Channel so viele Takte, meldet
# der Waechter ihn selbst neu an (aio-pikas Restore bekommt den ersten Takt).
_REREGISTER_AFTER_MISSING_TICKS = 2


@dataclass(eq=False)  # Identität: ``cancel_all`` vergisst genau diese Einträge
class _Watched:
    queue_name: str
    queue: Any
    callback: Any
    # ``None``: selbst abgemeldet, Neuanmeldung steht noch aus.
    tag: str | None
    seen_on: Any = None
    missing_ticks: int = 0
    lost_tag: str | None = None
    # Zeitpunkt (``clock``), seit dem der Consumer vermisst wird.
    missing_since: float | None = None
    reason: str = ""
    # Die Queue fehlt beim Broker (PF-20, E-083): kein ``basic.consume``.
    queue_missing: bool = False


class ConsumerWatch:
    """Registrierung der Consumer eines Brokers plus Takt (KTD3).

    Schnittstelle für die Settle-Regeln (U7, KTD4): ``is_watched(tag)``,
    ``self_cancelled_tags``, ``request_check()`` und
    ``wait_cancelled(tag, timeout)``.
    """

    def __init__(
        self,
        *,
        timeout_s: float = _RPC_TIMEOUT_S,
        rpc_timeout_s: float = _RPC_EMERGENCY_TIMEOUT_S,
        clock: Callable[[], float] = time.monotonic,
        queue_exists: QueueExists | None = None,
    ) -> None:
        self._timeout_s = timeout_s
        self._queue_exists = queue_exists
        self._rpc_timeout_s = rpc_timeout_s
        self._clock = clock
        self._entries: list[_Watched] = []
        self.on_reregistered: ReregisteredHook | None = None
        self._self_cancelled: set[str] = set()
        # Wird bei jedem eigenen Cancel gesetzt und ersetzt.
        self._cancelled_event = asyncio.Event()
        self._wake = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        # Abgekoppelte Takte (``stop()``), bis ihr RPC endet: Antwort, Notbremse
        # oder Schließen des Kanals (Kanalbezug: ``timeout_s``). Die Referenz
        # hält sie bis dahin am Leben.
        self._detached: set[asyncio.Task[None]] = set()
        self._stopping = False

    def add(self, queue_name: str, queue: Any, callback: Any, tag: str) -> None:
        """Merkt sich einen frisch angemeldeten Consumer (aus ``consume``)."""
        self._entries.append(_Watched(queue_name, queue, callback, tag))

    def tags(self) -> list[tuple[str, str]]:
        """Die aktuell angemeldeten (Queue, Tag)-Paare — nach einer
        Neuanmeldung nicht mehr die aus ``consume``."""
        return [(entry.queue_name, entry.tag) for entry in self._entries if entry.tag is not None]

    def is_watched(self, tag: str) -> bool:
        """Ob *tag* der aktuell angemeldete Tag eines geführten Consumers ist
        (je Settle geprüft, daher ohne Zwischenliste)."""
        return any(entry.tag == tag for entry in self._entries)

    def start(self, interval_s: float) -> None:
        """Startet den Takt alle ``interval_s`` Sekunden; idempotent."""
        if self._task is None:
            self._stopping = False
            self._task = asyncio.create_task(self._run(interval_s))

    async def stop(self) -> None:
        """Beendet den Takt; danach meldet der Wächter nichts mehr neu an.

        Ein laufender Takt darf seinen Cancel noch abschließen (bis
        ``_STOP_TIMEOUT_S``), damit ``cancel_consume`` danach mit bekannten
        Tags arbeitet. Hängt sein RPC länger, wird der Takt abgekoppelt, nicht
        abgebrochen (FS-09, KTD16): Sein RPC endet mit der Antwort, der
        Notbremse (``rpc_timeout_s``) oder dem Schließen des Kanals
        (``broker.close()``); ein Kanalbezug davor endet nach ``timeout_s``.
        Danach meldet er keinen weiteren Eintrag mehr an.
        """
        self._stopping = True
        task, self._task = self._task, None
        if task is None:
            return
        self._wake.set()
        _done, pending = await asyncio.wait({task}, timeout=_STOP_TIMEOUT_S)
        if pending:
            self._detached.add(task)
            task.add_done_callback(self._detached.discard)
            logger.warning("consumer_watch_detached", timeout_s=_STOP_TIMEOUT_S)
            return
        with contextlib.suppress(asyncio.CancelledError):
            await task

    async def cancel_all(self) -> list[tuple[str, Exception]]:
        """Stoppt den Wächter und meldet jeden aktuellen Tag ab (``cancel_consume``).

        Fehler je Consumer isoliert: Ein scheiternder Cancel hält die übrigen
        nicht auf. Der Kanalbezug endet nach ``timeout_s``, das RPC spätestens
        mit der Notbremse ``rpc_timeout_s``. Danach ist die Registrierung leer; zurück kommen die
        gescheiterten Tags mit ihrem Fehler.
        """
        await self.stop()
        errors: list[tuple[str, Exception]] = []
        for entry in self._entries:
            if entry.tag is None:
                continue
            try:
                await self._cancel(entry.queue, entry.tag)
            except Exception as exc:  # noqa: BLE001 - Fehler je Tag sammeln und zurueckgeben
                errors.append((entry.tag, exc))
                continue
            self._mark_cancelled(entry.tag)
        self._entries.clear()
        return errors

    def request_check(self) -> None:
        """Stößt sofort einen Takt an (ein Settle auf einen toten Tag, KTD4)."""
        self._wake.set()

    async def _run(self, interval_s: float) -> None:
        while not self._stopping:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._wake.wait(), timeout=interval_s)
            self._wake.clear()
            if self._stopping:
                return  # type: ignore[unreachable]  # stop() setzt _stopping waehrend des await
            try:
                await self.tick()
            except Exception as exc:  # noqa: BLE001 - gescheiterter Takt darf Waechter nicht beenden
                # Ein gescheiterter Takt darf den Wächter nicht beenden — sonst
                # wäre der nächste Consumer-Verlust wieder ein stiller Stillstand.
                logger.warning("consumer_watch_tick_failed", **_error_fields(exc))

    @property
    def self_cancelled_tags(self) -> frozenset[str]:
        """Tags, die swarmq selbst abgemeldet hat (Wächter oder ``cancel_consume``).

        Auf 4.3 ist ein spätes Ack auf so einen Tag unschädlich; auf einen
        nur vom Broker beendeten Tag weckte es den Zombie (KTD4).
        """
        return frozenset(self._self_cancelled)

    async def wait_cancelled(self, tag: str, timeout: float) -> bool:
        """Wartet, bis swarmq ``tag`` selbst abgemeldet hat; ``False`` nach ``timeout``."""
        try:
            async with asyncio.timeout(timeout):
                while tag not in self._self_cancelled:
                    await self._cancelled_event.wait()
        except TimeoutError:
            return False
        return True

    def _mark_cancelled(self, tag: str) -> None:
        self._self_cancelled.add(tag)
        event, self._cancelled_event = self._cancelled_event, asyncio.Event()
        event.set()

    def gaps(self) -> dict[str, float]:
        """Je angemeldeter Queue: Sekunden ohne aktiven Consumer (0.0 = aktiv).

        Grundlage des Consumer-Checks von ``/ready``. Gemessen ab dem Takt,
        der den Verlust zuerst bemerkt hat.
        """
        now = self._clock()
        return {
            entry.queue_name: 0.0 if entry.missing_since is None else now - entry.missing_since
            for entry in self._entries
        }

    def missing_queues(self) -> list[str]:
        """Konsumierte Queues, die beim Broker fehlen (PF-20, E-083); ``/ready``
        nennt sie, bis es sie wieder gibt."""
        return [entry.queue_name for entry in self._entries if entry.queue_missing]

    async def tick(self) -> None:
        """Ein Prüfdurchgang über alle angemeldeten Consumer."""
        for entry in self._entries:
            if self._stopping:
                # Ein abgekoppelter Takt (``stop()``) meldet keinen weiteren
                # Eintrag mehr an: der Worker drainiert schon (Review U10).
                return
            try:
                underlay = await asyncio.wait_for(entry.queue.channel.get_underlay_channel(), timeout=self._timeout_s)
            except TimeoutError:
                logger.warning(
                    "consumer_watch_channel_timeout",
                    queue=entry.queue_name,
                    timeout_s=self._timeout_s,
                )
                return
            if entry.tag is None:
                await self._consume(entry, underlay)
                continue
            if entry.tag in underlay.consumers:
                entry.seen_on = underlay
                entry.missing_ticks = 0
                entry.missing_since = None
                continue
            if entry.missing_since is None:
                entry.missing_since = self._clock()
            if underlay is entry.seen_on and not underlay.is_closed:
                # Offener, unveränderter Channel: der Broker hat den Consumer
                # beendet (``basic.cancel`` ab 4.3). Sofort selbst abmelden.
                await self._reregister(entry, underlay, "broker_cancel")
                continue
            # Channel geschlossen oder ersetzt (bis 4.2): aio-pikas Restore legt
            # den Tag meist selbst wieder an. Erst im zweiten Takt eingreifen.
            entry.missing_ticks += 1
            if entry.missing_ticks >= _REREGISTER_AFTER_MISSING_TICKS:
                await self._reregister(entry, underlay, "channel_replaced")

    async def _channel_ready(self, queue: Any) -> None:
        """Kanalbezug begrenzt auf ``timeout_s`` (Review U10).

        aio-pikas ``cancel``/``consume`` holen den Kanal vor dem RPC selbst
        über ``get_underlay_channel`` -- ohne Frist; bei getrennter Verbindung
        (auch nach ``broker.close()``) kehrt das nie zurück. Ist der Kanal hier
        bereit, kehrt der innere Bezug ohne Suspendierung zurück, die
        Verbindung kann dazwischen nicht wegfallen. Ein Abbruch an dieser
        Stelle schließt nichts: gesendet ist noch nichts.
        """
        await asyncio.wait_for(queue.channel.get_underlay_channel(), timeout=self._timeout_s)

    async def _cancel(self, queue: Any, tag: str) -> None:
        await self._channel_ready(queue)
        await queue.cancel(tag, timeout=self._rpc_timeout_s)

    async def _reregister(self, entry: _Watched, underlay: Any, reason: str) -> None:
        assert entry.tag is not None
        try:
            await self._cancel(entry.queue, entry.tag)
        except Exception as exc:  # noqa: BLE001 - geloggt, naechster Takt versucht erneut
            # Ohne erfolgreichen Cancel bliebe der tote Tag in
            # ``RobustQueue._consumers`` und ein Restore legte ihn
            # wieder an; der nächste Takt versucht es erneut.
            logger.warning(
                "consumer_cancel_failed",
                queue=entry.queue_name,
                consumer_tag=entry.tag,
                **_error_fields(exc),
            )
            return
        self._mark_cancelled(entry.tag)
        entry.lost_tag, entry.tag, entry.reason = entry.tag, None, reason
        if self._stopping:
            return
        await self._consume(entry, underlay)

    async def _consume(self, entry: _Watched, underlay: Any) -> None:
        try:
            if not await self._queue_present(entry):
                return
            await self._channel_ready(entry.queue)
            tag = await entry.queue.consume(entry.callback, timeout=self._rpc_timeout_s)
        except Exception as exc:  # noqa: BLE001 - geloggt, Waechter laeuft weiter
            logger.warning(
                "consumer_reregister_failed",
                queue=entry.queue_name,
                reason=entry.reason,
                **_error_fields(exc),
            )
            return
        if self._stopping or entry not in self._entries:
            # FS-09: Die Antwort eines abgekoppelten Takts kam nach ``stop()``
            # bzw. ``cancel_all``. Den Tag kennt sonst niemand mehr, er bekäme
            # im Drain neue Zustellungen -- sofort wieder abmelden.
            await self._cancel_late(entry, tag)
            return
        entry.tag = tag
        entry.seen_on = underlay
        entry.missing_ticks = 0
        entry.missing_since = None
        logger.warning(
            "consumer_reregistered",
            queue=entry.queue_name,
            reason=entry.reason,
            old_consumer_tag=entry.lost_tag,
            consumer_tag=tag,
        )
        if self.on_reregistered is not None:
            self.on_reregistered(entry.queue_name, entry.reason)

    async def _queue_present(self, entry: _Watched) -> bool:
        """Ob die Queue beim Broker existiert, passiv geprueft (PF-20, E-083).

        Fehlt sie, unterbleibt ``basic.consume``: Es scheiterte an NOT_FOUND und
        schloesse den gemeinsamen Consume-Kanal (E-019). Beim ersten Fehlen
        nimmt der Waechter die Queue aus aio-pikas Restore-Liste, sonst
        scheiterte jeder Reconnect an ihr, und meldet ERROR. Gibt es sie
        wieder, kommt sie zurueck in die Liste."""
        if self._queue_exists is None:
            return True
        # Begrenzt wie der Kanalbezug: Die Pruefung laeuft auf einem
        # Wegwerf-Kanal, ein Abbruch schliesst nie den Consume-Kanal (E-053).
        if await asyncio.wait_for(self._queue_exists(entry.queue_name), timeout=self._timeout_s):
            if entry.queue_missing:
                # Jemand hat die Queue neu angelegt: wieder in die Restore-Liste
                # und neu anmelden.
                entry.queue_missing = False
                entry.reason = "queue_recreated"
                readmit_queue(entry.queue, entry.queue_name)
            return True
        if not entry.queue_missing:
            entry.queue_missing = True
            release_queue(entry.queue, entry.queue_name)
            logger.error("consumer_queue_missing", queue=entry.queue_name)
        return False

    async def _cancel_late(self, entry: _Watched, tag: str) -> None:
        try:
            await self._cancel(entry.queue, tag)
        except Exception as exc:  # noqa: BLE001 - geloggt; der Verbindungsschluss beendet den Consumer
            logger.warning(
                "consumer_cancel_failed",
                queue=entry.queue_name,
                consumer_tag=tag,
                **_error_fields(exc),
            )
            return
        self._mark_cancelled(tag)
