"""Consumer-Wächter: verlorene Consumer selbst neu anmelden (Plan
2026-10-08-1541, U6, KTD3).

Beendet der Broker einen Consumer (``basic.cancel`` ab RabbitMQ 4.3 nach
``consumer-timeout``, Channel-Close bis 4.2), merkt aio-pika davon nichts:
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
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from swarmq.logging import _error_fields, get_logger

logger = get_logger(__name__)

#: Hook für die Metrik ``swarmq_consumer_reregistrations_total``: (Queue, Grund).
ReregisteredHook = Callable[[str, str], None]

# ``RobustChannel.get_underlay_channel`` wartet auf ``connection.ready()`` und
# kehrt bei getrennter Verbindung nie zurück. Der Takt gibt dann auf; die
# Wiederverbindung ist Sache von aio-pika, ``/ready`` meldet sie über den
# Broker-Ping.
_RPC_TIMEOUT_S = 2.0
# So lange darf ein laufender Takt beim Stoppen noch zu Ende kommen. Ein
# mitten im RPC abgebrochener Cancel oder Consume hinterließe einen Tag, von
# dem niemand weiß, ob der Broker ihn noch kennt.
_STOP_TIMEOUT_S = 5.0


@dataclass
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
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._timeout_s = timeout_s
        self._clock = clock
        self._entries: list[_Watched] = []
        self.on_reregistered: ReregisteredHook | None = None
        self._self_cancelled: set[str] = set()
        # Wird bei jedem eigenen Cancel gesetzt und ersetzt.
        self._cancelled_event = asyncio.Event()
        self._wake = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
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

        Ein laufender Takt darf seinen Cancel noch abschließen (gebunden
        durch ``_STOP_TIMEOUT_S``), damit ``cancel_consume`` danach mit
        bekannten Tags arbeitet.
        """
        self._stopping = True
        task, self._task = self._task, None
        if task is None:
            return
        self._wake.set()
        _done, pending = await asyncio.wait({task}, timeout=_STOP_TIMEOUT_S)
        if pending:
            task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    async def cancel_all(self) -> list[tuple[str, Exception]]:
        """Stoppt den Wächter und meldet jeden aktuellen Tag ab (``cancel_consume``).

        Fehler je Consumer isoliert: Ein scheiternder Cancel hält die übrigen
        nicht auf. Danach ist die Registrierung leer; zurück kommen die
        gescheiterten Tags mit ihrem Fehler.
        """
        await self.stop()
        errors: list[tuple[str, Exception]] = []
        for entry in self._entries:
            if entry.tag is None:
                continue
            try:
                await entry.queue.cancel(entry.tag)
            except Exception as exc:
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
                return
            try:
                await self.tick()
            except Exception as exc:
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

    async def tick(self) -> None:
        """Ein Prüfdurchgang über alle angemeldeten Consumer."""
        for entry in self._entries:
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
            if entry.missing_ticks >= 2:
                await self._reregister(entry, underlay, "channel_replaced")

    async def _reregister(self, entry: _Watched, underlay: Any, reason: str) -> None:
        assert entry.tag is not None
        try:
            await asyncio.wait_for(entry.queue.cancel(entry.tag), timeout=self._timeout_s)
        except Exception as exc:
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
            tag = await asyncio.wait_for(entry.queue.consume(entry.callback), timeout=self._timeout_s)
        except Exception as exc:
            logger.warning(
                "consumer_reregister_failed",
                queue=entry.queue_name,
                reason=entry.reason,
                **_error_fields(exc),
            )
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
