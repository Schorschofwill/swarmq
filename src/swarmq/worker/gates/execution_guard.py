"""Ausführungsschutz (Plan 2026-10-08-1541, U8, KTD5).

Eine erneut zugestellte Nachricht läuft nicht neben dem Original, nicht nach
dessen Abschluss und nicht neben einem Retry. Die Identität einer Ausführung
ist die Task-ID plus ``x-swarmq-replay-id``.

Das Gate liegt im ``try`` des Runners nach dem Registry-Lookup und vor dem
Expiry-Gate: so deckt dessen ``finally`` jeden Ausgang ab, das
Delivery-Count-Gate (hängende Worker) bleibt davor, und ``max_age`` schickt
keine Kopie in die DLQ, während das Original läuft. FIRE_AND_FORGET-Tasks
sind bei der Ausführung schon bestätigt und werden übersprungen.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any

from swarmq.backoff import backend_defer_delay
from swarmq.contracts import ExecutionGuardStore
from swarmq.exceptions import BackendConnectionError
from swarmq.headers import HEADER_GUARD_DEFER_COUNT, HEADER_REPLAY_ID, HEADER_RETRY_COUNT
from swarmq.logging import _error_fields, get_logger
from swarmq.task import DeliveryMode
from swarmq.worker.gates import GateContext, GateDeps, prior_deferrals, redeliver_in_place, republish_headers
from swarmq.worker.gates.backend_defer import bounded_backend_call, defer_backend_unavailable
from swarmq.worker.gates.delivery_count import WORKER_HUNG_ERROR_TYPE, DeadLetter
from swarmq.worker.outcome import Continue, DeliveryOutcome, Redelivered, Terminal

logger = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class GuardLease:
    """Der Running-Marker, den eine Zustellung hält."""

    identity: str
    owner: str
    retry_count: int
    task_id: str


def retry_count_of(headers: Mapping[str, Any] | None) -> int:
    """Retry-Zaehler aus den Headern; ein kaputter Header zaehlt wie keiner."""
    raw = (headers or {}).get(HEADER_RETRY_COUNT, 0)
    return raw if isinstance(raw, int) and raw > 0 else 0


# Laenger ist kein von swarmq erzeugter Wert (uuid4().hex); ein laengerer oder
# kaputter Header zaehlt wie keiner und wird nicht ungedeckelt zum Valkey-Key.
_MAX_REPLAY_ID_LEN = 64


def execution_identity(delivery: Any, task_id: str) -> str:
    """Task-ID plus ``x-swarmq-replay-id``, falls vorhanden."""
    replay = (delivery.headers or {}).get(HEADER_REPLAY_ID)
    if isinstance(replay, str) and 0 < len(replay) <= _MAX_REPLAY_ID_LEN:
        return f"{task_id}|{replay}"
    return task_id


class ExecutionGuard:
    def __init__(
        self,
        deps: GateDeps,
        *,
        store: Callable[[], ExecutionGuardStore | None],
        dead_letter: DeadLetter,
        on_event: Callable[[str], None] | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._deps = deps
        self._store = store
        self._dead_letter = dead_letter
        # Metrik-Hook (``swarmq_execution_guard_events_total``).
        self._on_event = on_event
        self._sleep = sleep
        # Gesammelter Verlaengerungstakt: laeuft nur, solange Leases bestehen.
        self._renewal: asyncio.Task[None] | None = None
        # Gehaltene Running-Marker je Zustellung (``id(delivery)``); der
        # Broker-Wrapper schliesst sie mit dem ausgefuehrten Verb ab.
        self._leases: dict[int, GuardLease] = {}
        # Owner-Tokens verlorener Marker: einmal melden, nicht weiter verlaengern.
        self._lost: set[str] = set()
        # Freigaben nach gescheitertem Claim (gehalten, damit sie nicht
        # vorzeitig eingesammelt werden).
        self._background: set[asyncio.Task[None]] = set()

    async def apply(self, delivery: Any, ctx: GateContext) -> DeliveryOutcome:
        assert ctx.task_info is not None
        config = self._deps.config
        if not config.execution_guard or ctx.task_info.delivery is DeliveryMode.FIRE_AND_FORGET:
            # FIRE_AND_FORGET ist vor der Ausfuehrung bestaetigt: eine
            # Neuzustellung gibt es nicht.
            return Continue
        store = self._store()
        if store is None:
            return Continue
        lease = GuardLease(
            execution_identity(delivery, ctx.task_id), uuid.uuid4().hex, retry_count_of(delivery.headers), ctx.task_id,
        )
        try:
            claim = await bounded_backend_call(
                self._deps,
                store.claim(
                    lease.identity,
                    owner=lease.owner,
                    retry_count=lease.retry_count,
                    ttl_ms=int(config.execution_guard_ttl_s * 1000),
                    state_ttl_ms=self._state_ttl_ms(),
                    max_orphans=config.execution_guard_max_orphans,
                ),
                what="execution guard claim",
            )
        except BackendConnectionError as exc:
            # Lief der Claim im Store und ging nur die Antwort verloren
            # (Timeout), steht der eigene Marker: im Hintergrund freigeben
            # (owner-gebunden, begrenzt), sonst zaehlte er spaeter als Waise.
            self._release_in_background(lease)
            return await defer_backend_unavailable(self._deps, delivery, ctx, exc, gate="execution_guard")
        if claim.status in ("done", "superseded"):
            # Abgeschlossen (das Original hat bestaetigt bzw. seine Kopie
            # liegt in der DLQ) oder von einer Retry-Kopie ueberholt. Nicht
            # freigeben -- Unique-Flag und Schedfire-Marker gehoeren dem
            # Original bzw. der Retry-Kopie.
            logger.info(
                "duplicate_delivery_skipped", task_id=ctx.task_id, task_name=ctx.task_name, reason=claim.status,
            )
            self._event("duplicate_skipped")
            await self._deps.broker.ack(delivery)
            return Terminal.never_release()
        if claim.status == "running":
            return await self._defer(delivery, ctx)
        # Ab hier haelt die Zustellung den Running-Marker; das ausgefuehrte
        # Verb schliesst ihn ab (Broker-Wrapper), sonst der ``finally``.
        self._leases[id(delivery)] = lease
        if self._renewal is None or self._renewal.done():
            self._renewal = asyncio.create_task(self._renew_loop())
        if claim.status == "orphan_limit":
            return await self._dead_letter_orphan(delivery, ctx, claim.orphans)
        return Continue

    async def _dead_letter_orphan(self, delivery: Any, ctx: GateContext, orphans: int) -> DeliveryOutcome:
        """Absturzschleife: dieselbe Ausfuehrung ist wiederholt ohne Freigabe
        abgelaufen. Jede Zurueckstellung waere eine neue Nachricht und
        haebelte das Zustelllimit aus -- also in die DLQ (die Zustellung haelt
        den Marker, ihr Dead-Letter setzt den Done-Marker)."""
        limit = self._deps.config.execution_guard_max_orphans
        logger.error(
            "execution_guard_orphan_limit", task_id=ctx.task_id, task_name=ctx.task_name,
            orphans=orphans, max_orphans=limit,
        )
        message = (
            f"{WORKER_HUNG_ERROR_TYPE}: execution expired {orphans} times without release "
            f"(execution_guard_max_orphans {limit}); the worker running it died or hung"
        )
        self._event("orphan_limit")
        await self._dead_letter(delivery, ctx, message, WORKER_HUNG_ERROR_TYPE)
        return Terminal.already_released()

    async def _defer(self, delivery: Any, ctx: GateContext) -> DeliveryOutcome:
        """Dieselbe Ausfuehrung laeuft anderswo: als neue Nachricht ueber die
        Delay-Queue zurueckstellen, mit wachsender Wartezeit. Zaehlt nicht
        gegen den Retry-Zaehler."""
        config = self._deps.config
        headers = republish_headers(delivery, ctx)
        attempt = prior_deferrals(delivery.headers or {}, HEADER_GUARD_DEFER_COUNT) + 1
        headers[HEADER_GUARD_DEFER_COUNT] = attempt
        delay_s = backend_defer_delay(attempt, config.backend_defer_base_delay, config.backend_defer_max_delay)
        delay_ms = int(delay_s * 1000)
        try:
            await self._deps.broker.retry_later(delivery, delay_ms=delay_ms, headers=headers)
        except Exception as exc:
            # Broker gestoert: der alte Weg, Requeue an derselben Stelle.
            logger.warning(
                "execution_guard_defer_failed_redelivered", task_id=ctx.task_id, task_name=ctx.task_name,
                **_error_fields(exc),
            )
            await redeliver_in_place(self._deps, delivery, ctx)
            return Redelivered
        self._event("deferred")
        logger.info(
            "execution_guard_deferred", task_id=ctx.task_id, task_name=ctx.task_name,
            defer_count=attempt, delay_ms=delay_ms,
        )
        return Redelivered

    def _release_in_background(self, lease: GuardLease) -> None:
        task = asyncio.create_task(self.release(lease))
        self._background.add(task)
        task.add_done_callback(self._background.discard)

    def lease_of(self, delivery: Any) -> GuardLease | None:
        """Der Running-Marker der Zustellung, ohne ihn abzuschliessen."""
        return self._leases.get(id(delivery))

    def take_lease(self, delivery: Any) -> GuardLease | None:
        """Nimmt den Running-Marker der Zustellung aus dem Register (einmal)."""
        lease = self._leases.pop(id(delivery), None)
        if lease is not None:
            self._lost.discard(lease.owner)
        if not self._leases and self._renewal is not None:
            self._renewal.cancel()
            self._renewal = None
        return lease

    def _state_ttl_ms(self) -> int:
        ttl = self._deps.config.execution_guard_done_ttl_s
        assert ttl is not None  # Default in SwarmQConfig.__post_init__
        return int(ttl * 1000)

    async def _mark(self, lease: GuardLease, marker: str, call: Awaitable[None]) -> None:
        """Begrenzt und fehlertolerant: ein gescheiterter Marker darf das Verb
        nicht aufhalten. Der Running-Marker laeuft dann nach seiner TTL ab
        (die naechste Zustellung zaehlt ihn als Waise)."""
        try:
            await bounded_backend_call(self._deps, call, what=f"execution guard {marker}")
        except Exception as exc:
            logger.warning(
                "execution_guard_finish_failed", identity=lease.identity, marker=marker, **_error_fields(exc),
            )

    async def finish(self, lease: GuardLease) -> None:
        """Done-Marker setzen (terminales Verb ausgefuehrt)."""
        store = self._store()
        assert store is not None
        await self._mark(
            lease, "done", store.mark_done(lease.identity, owner=lease.owner, state_ttl_ms=self._state_ttl_ms()),
        )

    async def release(self, lease: GuardLease) -> None:
        """Nur den eigenen Running-Marker loeschen (nicht-terminaler Ausgang)."""
        store = self._store()
        assert store is not None
        await self._mark(lease, "release", store.release(lease.identity, owner=lease.owner))

    async def supersede(self, lease: GuardLease, retry_count: int) -> None:
        """Superseded-Marker mit dem Zaehler der Retry-Kopie setzen."""
        store = self._store()
        assert store is not None
        await self._mark(
            lease,
            "superseded",
            store.mark_superseded(
                lease.identity, owner=lease.owner, retry_count=retry_count, state_ttl_ms=self._state_ttl_ms(),
            ),
        )

    async def settle(self, delivery: Any) -> None:
        """``finally`` des Runners: hat kein Verb die Lease abgeschlossen (kein
        Verb, Ausnahme, Abbruch), nur den Running-Marker freigeben."""
        lease = self.take_lease(delivery)
        if lease is not None:
            await self.release(lease)

    async def _renew_loop(self) -> None:
        # Drei Takte je TTL: ein ausgefallener Takt laesst den Marker nicht ablaufen.
        interval = self._deps.config.execution_guard_ttl_s / 3
        while self._leases:
            await self._sleep(interval)
            await self.renew_once()

    async def renew_once(self) -> None:
        """Gesammelter Takt: verlaengert die eigenen Running-Marker per
        Compare-and-PEXPIRE (im CPU-Modus aus dem Hauptprozess -- die
        Ausfuehrung im Kind blockiert diesen Event-Loop nicht)."""
        store = self._store()
        if store is None:
            return
        ttl_ms = int(self._deps.config.execution_guard_ttl_s * 1000)
        leases = [lease for lease in self._leases.values() if lease.owner not in self._lost]
        # Nebenlaeufig: die ScriptPipeline schickt die Verlaengerungen eines
        # Takts als einen Stapel.
        results = await asyncio.gather(
            *(
                bounded_backend_call(
                    self._deps, store.renew(lease.identity, owner=lease.owner, ttl_ms=ttl_ms),
                    what="execution guard renew",
                )
                for lease in leases
            ),
            return_exceptions=True,
        )
        for lease, renewed in zip(leases, results, strict=True):
            if isinstance(renewed, BaseException):
                if not isinstance(renewed, Exception):
                    raise renewed
                # Der naechste Takt versucht es erneut; die TTL deckt drei Takte.
                logger.warning(
                    "execution_guard_renew_failed", task_id=lease.task_id, identity=lease.identity,
                    **_error_fields(renewed),
                )
                continue
            if not renewed and lease in self._leases.values():
                # Weg oder fremd (und nicht eben vom Verb abgeschlossen): eine andere Zustellung kann jetzt parallel
                # laufen (Event-Loop hing, Valkey-Ausfall laenger als die TTL).
                logger.warning(
                    "execution_guard_lease_lost", task_id=lease.task_id, identity=lease.identity,
                    hint="running marker expired or taken over; a redelivery may run in parallel",
                )
                self._lost.add(lease.owner)
                self._event("lease_lost")

    def _event(self, event: str) -> None:
        if self._on_event is not None:
            self._on_event(event)
