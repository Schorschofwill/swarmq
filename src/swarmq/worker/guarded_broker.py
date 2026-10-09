"""Protokollierender Broker-Wrapper des Ausfuehrungsschutzes (Plan
2026-10-08-1541, U8, KTD5).

Die Marker folgen dem Broker-Verb, das fuer eine Zustellung tatsaechlich
ausgefuehrt wurde -- nicht dem ``DeliveryOutcome``: Dead-Letter-Wege melden
``Redelivered``, manche Neuzustellungen ``Terminal``, und der Standard im
``finally`` des Runners ist ``Terminal``.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from swarmq.exceptions import BrokerConnectionError, SettleFailedAfterPublish
from swarmq.worker.gates.execution_guard import ExecutionGuard, retry_count_of


class GuardedBroker:
    def __init__(self, inner: Any, guard: ExecutionGuard) -> None:
        self._inner = inner
        self._guard = guard

    def __getattr__(self, name: str) -> Any:
        # Publish, Consume-Lebenszyklus, Admin: unveraendert vom inneren Broker.
        # ``route_via_dlx`` ist optional (``BrokerAdmin``): fehlt es innen,
        # fehlt es auch hier, und ``gates.route_via_dlx`` nimmt ``dead_letter``.
        attr = getattr(self._inner, name)
        if name == "route_via_dlx":
            return self._route_via_dlx
        return attr

    async def _mark_done(self, delivery: Any) -> None:
        lease = self._guard.take_lease(delivery)
        if lease is not None:
            await self._guard.finish(lease)

    async def ack(self, delivery: Any) -> None:
        """Nach einem Claim folgt ``ack`` nur einer abgeschlossenen Ausfuehrung
        (Ergebnis gespeichert). Auch ein am Kanal gescheitertes Ack (bis 4.2
        nach dem Consumer-Timeout) setzt deshalb den Done-Marker: die
        Neuzustellung wird uebersprungen statt erneut ausgefuehrt."""
        try:
            await self._inner.ack(delivery)
        except BrokerConnectionError:
            await self._mark_done(delivery)
            raise
        await self._mark_done(delivery)

    async def dead_letter(self, delivery: Any, *, headers: Mapping[str, Any]) -> None:
        """Done, sobald die Kopie in der DLQ liegt -- auch wenn danach nur das
        Ack des Originals scheiterte. Scheitert der Publish, bleibt die Lease
        stehen (der Aufrufer stellt per ``redeliver`` zurueck)."""
        try:
            await self._inner.dead_letter(delivery, headers=headers)
        except SettleFailedAfterPublish:
            await self._mark_done(delivery)
            raise
        await self._mark_done(delivery)

    async def _route_via_dlx(self, delivery: Any) -> None:
        """Done nur nach Erfolg: scheitert das ``nack``, kommt die Nachricht
        zurueck und muss dieselbe terminale Entscheidung erneut treffen."""
        await self._inner.route_via_dlx(delivery)
        await self._mark_done(delivery)

    async def redeliver(self, delivery: Any) -> None:
        """Running VOR dem ``nack`` freigeben -- sonst saehe die sofortige
        Neuzustellung den eigenen Marker und stellte sich zurueck."""
        await self._release(delivery)
        await self._inner.redeliver(delivery)

    async def retry_later(self, delivery: Any, *, delay_ms: int, headers: Mapping[str, Any]) -> None:
        """Mit erhoehtem Retry-Zaehler (Retry): Superseded-Marker nach dem
        Publish. Sonst (Zurueckstellung, Requeue-Kopie): dieselbe Ausfuehrung
        kommt wieder -- Running VOR dem Publish freigeben."""
        lease = self._guard.lease_of(delivery)
        retry_count = retry_count_of(headers)
        if lease is not None and retry_count <= lease.retry_count:
            await self._release(delivery)
        try:
            await self._inner.retry_later(delivery, delay_ms=delay_ms, headers=headers)
        except SettleFailedAfterPublish:
            await self._mark_superseded(delivery, retry_count)
            raise
        await self._mark_superseded(delivery, retry_count)

    async def _mark_superseded(self, delivery: Any, retry_count: int) -> None:
        lease = self._guard.take_lease(delivery)
        if lease is not None:
            await self._guard.supersede(lease, retry_count)

    async def _release(self, delivery: Any) -> None:
        lease = self._guard.take_lease(delivery)
        if lease is not None:
            await self._guard.release(lease)
