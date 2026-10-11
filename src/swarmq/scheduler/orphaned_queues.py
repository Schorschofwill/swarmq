"""Verwaiste Queues melden (Plan 0.12.0, MR 18, BZ-14).

Wird ein Task in eine andere Queue verschoben oder ``--queues`` geaendert,
bleibt der Rueckstand der alten Queue ohne Consumer liegen. Jeder Worker
meldet darum die Queues, die er konsumiert, im Meldetakt mit Valkey-Zeit
(``sq:{sched}:report:queues``).

Der Leader prueft im Uebernahme-Takt die Queues, die kein lebender Worker
mehr meldet (letzte Meldung aelter als drei Meldetakte, gemessen an der
Valkey-Zeit), per passivem Declare: ``consumer_count == 0`` und
``message_count > 0`` heisst verwaist. Log und Signal nur beim Uebergang
(``SCHEDULER_QUEUE_ORPHANED``, Gauge ``swarmq_scheduler_queues_orphaned``).
Keine Management-API. Mixin ueber dem ``SchedulerLoop``-Zustand; Logger wie
in ``fire.py`` ueber ``self._log`` (``swarmq.scheduler.loop``).
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from swarmq.scheduler.reports import REPORT_MAX_AGE_MS, live_window_ms
from swarmq.signals import Signal

if TYPE_CHECKING:
    from swarmq.config import SwarmQConfig
    from swarmq.contracts.broker import QueueStatus


#: Zeitlimit einer Pruefung (Lesen, passive Declares, Aufraeumen; Q6). Der
#: Takt laeuft danach weiter, der naechste prueft erneut.
QUEUE_CHECK_TIMEOUT_S = 30.0


def is_orphaned(status: QueueStatus | None, *, last_seen_ms: int, live_since_ms: int) -> bool:
    """Verwaist (BZ-14): Rueckstand, kein Consumer, und die letzte Meldung
    liegt vor dem Lebendigkeitsfenster. Rein; auch fuer ``swarmq check``."""
    return (
        status is not None and last_seen_ms < live_since_ms and status.consumer_count == 0 and status.message_count > 0
    )


class SchedulerOrphanedQueues:
    # Mixin ueber dem Loop-Zustand: die Attribute setzt ``SchedulerLoop.__init__``.
    _backend: Any
    _broker: Any
    _consumed_queues: Sequence[str] = ()

    if TYPE_CHECKING:
        _config: SwarmQConfig
        _term: int

        @property
        def _log(self) -> Any: ...

        async def _emit(self, signal: Signal, **extra: Any) -> None: ...

    #: Zurzeit verwaiste Queues mit ihrem Rueckstand (fuer Log und Signal nur
    #: beim Uebergang).
    _orphaned_queues: dict[str, int] | None = None
    _orphaned_term: int = -1

    async def _report_queues(self) -> None:
        """Die eigenen Queues melden, in jeder Rolle; die Meldungen verfallen
        7 d nach der letzten Meldung irgendeines Workers (Q5)."""
        if not self._consumed_queues:
            return
        try:
            await self._backend.schedules.report_queues(self._consumed_queues, ttl_ms=REPORT_MAX_AGE_MS)
        except Exception as exc:  # noqa: BLE001 - Worker arbeitet weiter, nächster Takt meldet erneut
            # Der Worker arbeitet weiter; der naechste Takt meldet erneut.
            self._log.warning("scheduler_queue_report_failed", error_type=type(exc).__name__, error=str(exc))

    async def check_orphaned_queues(self) -> None:
        """Leader-Takt: verwaiste Queues melden (nur beim Uebergang); wirft nie."""
        try:
            await asyncio.wait_for(self._check_orphaned_queues(), timeout=QUEUE_CHECK_TIMEOUT_S)
        except Exception as exc:  # noqa: BLE001 - Leader-Takt wirft nie, Fehler wird geloggt
            self._log_check_failed(exc)

    async def _check_orphaned_queues(self) -> None:
        snapshot = await self._backend.schedules.queue_reports()
        # Kandidaten: Queues, die kein lebender Worker mehr meldet (Valkey-Zeit).
        live_since = snapshot.now_ms - live_window_ms(self._config.scheduler_seen_interval_s)
        candidates = sorted(queue for queue, last in snapshot.last_seen_ms.items() if last < live_since)
        statuses: dict[str, QueueStatus | None] = await self._broker.queue_statuses(candidates) if candidates else {}
        # Neue Amtszeit: die Uebergaenge der alten gelten nicht mehr (die
        # Gauge faellt beim Standby auf 0).
        previous = (self._orphaned_queues or {}) if self._orphaned_term == self._term else {}
        self._orphaned_term = self._term
        current = {
            queue: status.message_count
            for queue, status in statuses.items()
            if status is not None
            and is_orphaned(status, last_seen_ms=snapshot.last_seen_ms[queue], live_since_ms=live_since)
        }
        self._orphaned_queues = current
        for queue in sorted(current.keys() - previous.keys()):
            self._log.warning(
                "scheduler_queue_orphaned",
                queue=queue,
                message_count=current[queue],
                hint=(
                    "no consumer and no live worker reports this queue any more; typical cause: "
                    "a task moved to another queue or --queues changed. Consume or move the "
                    "backlog, or delete the queue"
                ),
            )
            await self._emit_orphaned(queue, current[queue], orphaned=True)
        for queue in sorted(previous.keys() - current.keys()):
            status = statuses.get(queue)
            self._log.info("scheduler_queue_orphan_resolved", queue=queue)
            await self._emit_orphaned(queue, 0 if status is None else status.message_count, orphaned=False)
        # Aufraeumen (Q5): fehlend oder leer und 7 d nicht gemeldet.
        forget_before = snapshot.now_ms - REPORT_MAX_AGE_MS
        forgotten = [
            queue
            for queue in candidates
            if snapshot.last_seen_ms[queue] < forget_before
            and ((status := statuses.get(queue)) is None or status.message_count == 0)
        ]
        if forgotten:
            await self._backend.schedules.forget_queue_reports(forgotten)

    def _log_check_failed(self, exc: Exception) -> None:
        """Fehlt eine Faehigkeit (Fremd-Store, Fremd-Broker, ``MagicMock``),
        bleibt die Pruefung still aus (Q15); andere Fehler warnen, der naechste
        Takt prueft erneut."""
        fields = {"error_type": type(exc).__name__, "error": str(exc)}
        if isinstance(exc, (AttributeError, TypeError)):
            self._log.debug("scheduler_queue_check_unavailable", **fields)
        else:
            self._log.warning("scheduler_queue_check_failed", **fields)

    async def _emit_orphaned(self, queue: str, message_count: int, *, orphaned: bool) -> None:
        await self._emit(
            Signal.SCHEDULER_QUEUE_ORPHANED,
            queue=queue,
            orphaned=orphaned,
            message_count=message_count,
            orphaned_count=len(self._orphaned_queues or {}),
        )
