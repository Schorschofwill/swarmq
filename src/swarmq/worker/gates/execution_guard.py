"""Ausführungsschutz (Plan 2026-10-08-1541, U8, KTD5).

Eine erneut zugestellte Nachricht läuft nicht neben dem Original, nicht nach
dessen Abschluss und nicht neben einem Retry. Die Identität einer Ausführung
ist die Task-ID plus ``x-swarmq-replay-id`` plus der Zahl der Broker-Tode
ausserhalb der Delay-Queues (``swarmq.identity``, Plan 0.12.0, MR 12).

Das Gate liegt im ``try`` des Runners nach dem Registry-Lookup und vor dem
Expiry-Gate: so deckt dessen ``finally`` jeden Ausgang ab, das
Delivery-Count-Gate (hängende Worker) bleibt davor, und ``max_age`` schickt
keine Kopie in die DLQ, während das Original läuft. FIRE_AND_FORGET-Tasks
sind bei der Ausführung schon bestätigt und werden übersprungen.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Awaitable, Callable, Coroutine, Mapping
from dataclasses import dataclass
from typing import Any

from swarmq import identity
from swarmq.backoff import backend_defer_delay, nominal_defer_total
from swarmq.contracts import ExecutionGuardStore
from swarmq.exceptions import BackendConnectionError
from swarmq.headers import HEADER_GUARD_DEFER_COUNT, HEADER_RETRY_COUNT
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
    # TTL des Zustands (Done-/Superseded-Marker) dieser Ausfuehrung in ms;
    # None = ``execution_guard_done_ttl_s`` (MR 12: Replay-Identitaeten leben
    # mindestens die DLQ-Frist, A4; mit ``max_age`` mindestens
    # ``max_age + 300``, Review MR 12 P1-1).
    state_ttl_ms: int | None = None
    # Rueckstand (MR 12, BZ-12): so lange lebt der Zustand mindestens, sobald
    # eine Kopie dieser Ausfuehrung zurueckgestellt wurde oder ihr Abschluss
    # eine Neuzustellung erwartet (gescheitertes Ack); None = kein Rueckstand.
    backlog_ttl_ms: int | None = None


def retry_count_of(headers: Mapping[str, Any] | None) -> int:
    """Retry-Zaehler aus den Headern; ein kaputter Header zaehlt wie keiner."""
    raw = (headers or {}).get(HEADER_RETRY_COUNT, 0)
    return raw if isinstance(raw, int) and raw > 0 else 0


def execution_identity(delivery: Any, task_id: str) -> str:
    """Task-ID | ``x-swarmq-replay-id`` | #n (Plan 0.12.0, MR 12): siehe
    ``swarmq.identity.execution_identity``."""
    return identity.execution_identity(delivery.headers, task_id)


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
        # Shutdown (``drain``): Nachholversuche warten nicht mehr, sondern
        # versuchen es sofort ein letztes Mal.
        self._closing = asyncio.Event()

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
            execution_identity(delivery, ctx.task_id),
            uuid.uuid4().hex,
            retry_count_of(delivery.headers),
            ctx.task_id,
            state_ttl_ms=self._lease_state_ttl_ms(delivery.headers, ctx.task_info.max_age),
            backlog_ttl_ms=self._backlog_ttl_ms(ctx.task_info.max_age),
        )
        try:
            claim = await bounded_backend_call(
                self._deps,
                store.claim(
                    lease.identity,
                    owner=lease.owner,
                    retry_count=lease.retry_count,
                    ttl_ms=int(config.execution_guard_ttl_s * 1000),
                    state_ttl_ms=self._state_ttl_ms(lease),
                    max_orphans=config.execution_guard_max_orphans,
                    backlog_ttl_ms=lease.backlog_ttl_ms,
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
                "duplicate_delivery_skipped",
                task_id=ctx.task_id,
                task_name=ctx.task_name,
                reason=claim.status,
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
            "execution_guard_orphan_limit",
            task_id=ctx.task_id,
            task_name=ctx.task_name,
            orphans=orphans,
            max_orphans=limit,
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
        except Exception as exc:  # noqa: BLE001 - Broker gestoert, Requeue an derselben Stelle
            # Broker gestoert: der alte Weg, Requeue an derselben Stelle.
            logger.warning(
                "execution_guard_defer_failed_redelivered",
                task_id=ctx.task_id,
                task_name=ctx.task_name,
                **_error_fields(exc),
            )
            await redeliver_in_place(self._deps, delivery, ctx)
            return Redelivered
        self._event("deferred")
        logger.info(
            "execution_guard_deferred",
            task_id=ctx.task_id,
            task_name=ctx.task_name,
            defer_count=attempt,
            delay_ms=delay_ms,
        )
        self._report_stall(ctx, attempt)
        return Redelivered

    def _report_stall(self, ctx: GateContext, attempt: int) -> None:
        """Einmal je Nachricht, wenn die Zurueckstellungen zusammen
        ``consumer_timeout_s`` ueberschreiten: das Original laeuft
        ungewoehnlich lange oder haengt (verlaengert seinen Marker aber
        weiter). Weiter zurueckstellen -- keine DLQ."""
        config = self._deps.config
        base, cap = config.backend_defer_base_delay, config.backend_defer_max_delay
        limit = config.consumer_timeout_s
        if not nominal_defer_total(attempt - 1, base, cap) < limit <= nominal_defer_total(attempt, base, cap):
            return
        logger.warning(
            "execution_guard_defer_stalled",
            task_id=ctx.task_id,
            task_name=ctx.task_name,
            defer_count=attempt,
            consumer_timeout_s=limit,
            hint=(
                "the same execution has been running elsewhere for longer than consumer_timeout_s; "
                "the redelivery keeps being deferred -- check for a hung task"
            ),
        )
        self._event("defer_stalled")

    async def _pause(self, delay: float) -> None:
        """Wartet ``delay``, beim Shutdown nicht mehr."""
        if self._closing.is_set():
            return
        waits = {asyncio.ensure_future(self._sleep(delay)), asyncio.ensure_future(self._closing.wait())}
        try:
            await asyncio.wait(waits, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for wait in waits:
                wait.cancel()

    async def drain(self) -> None:
        """Shutdown, vor dem Schliessen des Backends: laufende Nachholversuche
        versuchen es sofort ein letztes Mal."""
        self._closing.set()
        if self._background:
            await asyncio.wait(set(self._background))

    def _release_in_background(self, lease: GuardLease) -> None:
        self._in_background(self.release(lease))

    def _in_background(self, call: Coroutine[Any, Any, None]) -> None:
        task = asyncio.create_task(call)
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

    def _done_ttl_s(self) -> float:
        ttl = self._deps.config.execution_guard_done_ttl_s
        assert ttl is not None  # Default in SwarmQConfig.__post_init__
        return ttl

    def _replay_state_ttl_ms(self) -> int:
        """Zustands-TTL einer Replay-Identitaet (MR 12, A4): mindestens die
        DLQ-Frist, denn ein erneuter ``dlq retry`` derselben XQ-Nachricht
        traegt dieselbe Identitaet; ohne Frist (``None``) der Rueckstand."""
        config = self._deps.config
        floor = config.dead_letter_ttl_s
        if floor is None:
            floor = config.execution_guard_backlog_ttl_s
        return int(max(self._done_ttl_s(), floor) * 1000)

    def _lease_state_ttl_ms(self, headers: Mapping[str, Any] | None, max_age: int | None) -> int | None:
        """Zustands-TTL der Ausfuehrung: Replay-Identitaeten mindestens die
        DLQ-Frist (A4); mit ``max_age`` mindestens ``max_age + 300`` (Review
        MR 12, P1-1) -- ein Duplikat kann so lange in der Queue warten, auch
        wenn das Original sofort lief. None = ``execution_guard_done_ttl_s``."""
        ttl = self._replay_state_ttl_ms() if identity.is_replay(headers) else None
        if max_age is None:
            return ttl
        floor = int((max_age + 300.0) * 1000)
        return max(int(self._done_ttl_s() * 1000) if ttl is None else ttl, floor)

    def _backlog_ttl_ms(self, max_age: int | None) -> int:
        """Rueckstand (BZ-12): ``max(execution_guard_backlog_ttl_s, max_age + 300)``
        -- eine zurueckgestellte Kopie kommt spaetestens mit dem Ablauf des
        Tasks zurueck, sonst irgendwann hinter dem Rueckstand der Queue."""
        backlog = self._deps.config.execution_guard_backlog_ttl_s
        if max_age is not None:
            backlog = max(backlog, max_age + 300.0)
        return int(backlog * 1000)

    def _state_ttl_ms(self, lease: GuardLease) -> int:
        """Zustands-TTL der Lease: ihre eigene (Replay) oder die Konfiguration."""
        if lease.state_ttl_ms is not None:
            return lease.state_ttl_ms
        return int(self._done_ttl_s() * 1000)

    async def _mark(self, lease: GuardLease, marker: str, call: Callable[[], Awaitable[None]]) -> None:
        """Begrenzt und fehlertolerant: ein gescheiterter Marker darf das Verb
        nicht aufhalten. Er wird im Hintergrund nachgeholt (owner-gebunden,
        ``guard_finish.lua``), sonst bliebe der Running-Marker bis zu seiner
        TTL stehen und zaehlte bei der Neuzustellung als Waise."""
        try:
            await bounded_backend_call(self._deps, call(), what=f"execution guard {marker}")
        except Exception as exc:  # noqa: BLE001 - Marker darf das Verb nicht aufhalten
            logger.warning(
                "execution_guard_finish_failed",
                identity=lease.identity,
                marker=marker,
                **_error_fields(exc),
            )
            self._in_background(self._retry_mark(lease, marker, call))

    async def _retry_mark(self, lease: GuardLease, marker: str, call: Callable[[], Awaitable[None]]) -> None:
        config = self._deps.config
        base, cap = config.backend_defer_base_delay, config.backend_defer_max_delay
        attempt = 0
        while True:
            attempt += 1
            await self._pause(backend_defer_delay(attempt, base, cap))
            # Begrenzt auf etwa eine Marker-TTL (nominelle Wartezeiten): danach
            # ist der Marker ohnehin abgelaufen.
            last = self._closing.is_set() or nominal_defer_total(attempt, base, cap) >= config.execution_guard_ttl_s
            try:
                await bounded_backend_call(self._deps, call(), what=f"execution guard {marker}")
            except Exception as exc:  # noqa: BLE001 - Nachholversuch, naechste Runde oder Log
                if not last:
                    continue
                logger.warning(
                    "execution_guard_finish_gave_up",
                    identity=lease.identity,
                    marker=marker,
                    attempts=attempt,
                    hint=(
                        "the running marker expires by its TTL and counts as an orphan; "
                        "without the done marker a redelivery runs the task again"
                    ),
                    **_error_fields(exc),
                )
                return
            logger.info("execution_guard_finish_recovered", identity=lease.identity, marker=marker, attempts=attempt)
            return

    def _finish_ttl_ms(self, lease: GuardLease, extended: bool) -> int:
        """``extended`` (BZ-12): der Abschluss weiss, dass eine Neuzustellung
        kommt (gescheitertes Ack, ``SettleFailedAfterPublish``) -- sie kann
        hinter einem Rueckstand zurueckkommen, der Marker lebt den Rueckstand."""
        ttl = self._state_ttl_ms(lease)
        if extended and lease.backlog_ttl_ms is not None:
            ttl = max(ttl, lease.backlog_ttl_ms)
        return ttl

    async def finish(self, lease: GuardLease, *, extended: bool = False) -> None:
        """Done-Marker setzen (terminales Verb ausgefuehrt)."""
        store = self._store()
        assert store is not None
        ttl_ms = self._finish_ttl_ms(lease, extended)
        await self._mark(
            lease,
            "done",
            lambda: store.mark_done(lease.identity, owner=lease.owner, state_ttl_ms=ttl_ms),
        )

    async def release(self, lease: GuardLease) -> None:
        """Nur den eigenen Running-Marker loeschen (nicht-terminaler Ausgang)."""
        store = self._store()
        assert store is not None
        await self._mark(lease, "release", lambda: store.release(lease.identity, owner=lease.owner))

    async def supersede(self, lease: GuardLease, retry_count: int, *, extended: bool = False) -> None:
        """Superseded-Marker mit dem Zaehler der Retry-Kopie setzen."""
        store = self._store()
        assert store is not None
        ttl_ms = self._finish_ttl_ms(lease, extended)
        await self._mark(
            lease,
            "superseded",
            lambda: store.mark_superseded(
                lease.identity,
                owner=lease.owner,
                retry_count=retry_count,
                state_ttl_ms=ttl_ms,
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
                    self._deps,
                    store.renew(lease.identity, owner=lease.owner, ttl_ms=ttl_ms),
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
                    "execution_guard_renew_failed",
                    task_id=lease.task_id,
                    identity=lease.identity,
                    **_error_fields(renewed),
                )
                continue
            if not renewed and lease in self._leases.values():
                # Weg oder fremd (und nicht eben vom Verb abgeschlossen): eine andere Zustellung kann jetzt parallel
                # laufen (Event-Loop hing, Valkey-Ausfall laenger als die TTL).
                logger.warning(
                    "execution_guard_lease_lost",
                    task_id=lease.task_id,
                    identity=lease.identity,
                    hint="running marker expired or taken over; a redelivery may run in parallel",
                )
                self._lost.add(lease.owner)
                self._event("lease_lost")

    def _event(self, event: str) -> None:
        if self._on_event is not None:
            self._on_event(event)
