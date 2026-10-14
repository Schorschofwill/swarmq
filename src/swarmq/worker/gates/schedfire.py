"""Schedfire-Dedup-Gate (Phase 3c Unit 8), Plan U7 — verbatim
aus ``pipeline.py``. Läuft als erstes Gate, vor Cancel-Check, Semaphore,
Rate-Limit und Lock.

Zwei Marker: Completed (lange TTL, nach dem Lauf) und In-Flight (kurze TTL,
Claim vor dem Lauf). Duplikate werden bestätigt und übersprungen
(``Terminal.never_release()`` — kein Claim, keine Freigabe); Backend-Fehler
→ Zurueckstellung ueber die Delay-Queue (``backend_defer``, ``Redelivered``).
Der Runner behandelt dieses Gate wie heute *vor* seinem ``try/finally``
(Plan-Fortschrittsprotokoll U19).

Neuzustellung mit fremdem Claim (FS-03, E-046): Stirbt der Worker, der den
In-Flight-Claim haelt (SIGKILL, OOM), stellt der Broker die einzige Kopie neu
zu -- der Claim gehoert noch dem Toten. Eine Broker-Neuzustellung
(``x-delivery-count``/``x-acquired-count`` > 0) oder eine schon
zurueckgestellte Kopie (``x-swarmq-schedfire-defer-count``) wird deshalb
nicht bestaetigt, sondern mit wachsender Wartezeit zurueckgestellt, bis der
Completed-Marker steht (Ack) oder der Claim ablaeuft (Lauf). Ein frisches
Doppel-Fire des Leaders traegt beides nicht und bleibt ein Duplikat (E-047).
Gedeckelt auf ``max(completed_ttl, inflight_ttl + backend_defer_max_delay)``
ueber die Summe der Wartezeiten aus dem Defer-Zaehler (untere Jitter-Grenze);
danach Stall-Log und Ack. Klassische Queues fuehren keine Broker-Zaehler --
dort ist die erste Neuzustellung nicht erkennbar und wird wie bisher
bestaetigt.

Claim-Token (0.9.0): Der In-Flight-Claim speichert ein Owner-Token je
Zustellung. Jede Kopie (Zurueckstellung, Retry, Rueckstellung) traegt es als
``x-swarmq-schedfire-claim`` und bietet es beim Claim zur Uebernahme an --
scheiterte die Freigabe am Backend-Ausfall, gilt der Claim fuer die Kopie
als ihrer statt als Duplikat. Die Uebernahme rotiert das Token; eine
frische Zustellung ohne oder mit altem Token bleibt ein Duplikat, eine
Neuzustellung wird zurueckgestellt (s. o.).

Das Token liegt schon VOR dem Claim-Aufruf am Kontext: geht dessen Antwort
verloren (Timeout, Reset), kann Valkey den Claim geschrieben haben oder
nicht. Die Kopie traegt dann das neue Token und die angebotenen (komma-
getrennt, neuestes zuerst, gedeckelt) -- sie uebernimmt den Claim in beiden
Faellen. Ein Requeue an derselben Stelle (``nack``) kann kein Token tragen;
dort wird der Claim vorher freigegeben (``redeliver_in_place``) -- scheitert
das, geht statt des ``nack`` eine Kopie mit dem Token.
"""

from __future__ import annotations

import time
import uuid
from itertools import islice
from typing import Any

from swarmq.backoff import MIN_DEFER_JITTER, backend_defer_delay, nominal_defer_total
from swarmq.exceptions import BackendConnectionError, TaskNotFoundError
from swarmq.headers import HEADER_ENQUEUED_AT, HEADER_SCHEDFIRE_CLAIM, HEADER_SCHEDFIRE_DEFER_COUNT
from swarmq.identity import identity_suffix
from swarmq.logging import _error_fields, get_logger
from swarmq.worker.gates import (
    MAX_SCHEDFIRE_CLAIM_TOKENS,
    GateContext,
    GateDeps,
    SchedfireClaim,
    broker_redelivered,
    prior_deferrals,
    redeliver_in_place,
    republish_headers,
)
from swarmq.worker.gates.backend_defer import bounded_backend_call, defer_backend_unavailable
from swarmq.worker.outcome import Continue, DeliveryOutcome, Redelivered, Terminal

logger = get_logger(__name__)


def _claim_tokens(raw: Any) -> tuple[str, ...]:
    """Tokens aus ``x-swarmq-schedfire-claim`` (kommagetrennt, neuestes
    zuerst); ein fehlender oder kaputter Header bietet keins an. Gedeckelt
    auf ``MAX_SCHEDFIRE_CLAIM_TOKENS``: ein gefaelschter Header geht nicht
    ungedeckelt als Lua-ARGV an Valkey."""
    if not isinstance(raw, str):
        return ()
    tokens = (token for token in (part.strip() for part in raw.split(",")) if token)
    return tuple(islice(tokens, MAX_SCHEDFIRE_CLAIM_TOKENS))


def _is_redelivery(headers: dict[str, Any]) -> bool:
    """Neuzustellung (FS-03, E-046): vom Broker zurueckgegeben (Zustell-
    zaehler) oder eine schon zurueckgestellte Kopie (Defer-Header). Ein
    frisches Doppel-Fire des Leaders traegt beides nicht (E-047)."""
    return broker_redelivered(headers) or prior_deferrals(headers, HEADER_SCHEDFIRE_DEFER_COUNT) > 0


def schedfire_key(fire_key: str, headers: Any) -> str:
    """Key der Marker eines Fires (Plan 0.12.0, MR 12, BZ-08 Cron): bei einer
    Replay-Identitaet (``x-swarmq-replay-id`` gesetzt oder Broker-Tode
    ausserhalb der Delay-Queues) ``K|<Identitaet ohne task_id>``, damit der
    Completed-Marker des ersten Laufs das bewusste Replay nicht verwirft.
    Normale Doppel-Fires (n = 0, kein H) behalten K wie 0.11."""
    suffix = identity_suffix(headers)
    return fire_key if not suffix else f"{fire_key}|{suffix.lstrip('|')}"


def completed_marker_ttl_ms(config: Any, headers: Any, max_age: int | None) -> int:
    """TTL des Completed-Markers (Plan 0.12.0, MR 12, BZ-12): ``max(base,
    2 * Wartezeit + 300)`` mit der Wartezeit des Fires in der Queue (ab
    ``x-swarmq-enqueued-at``), gedeckelt bei ``max(base, max_age + 300)`` und
    ``execution_guard_backlog_ttl_s``, nie unter ``scheduler_dedup_ttl``
    (base). Ein Duplikat desselben Fires wartet hinter demselben Rueckstand.

    Mit ``max_age`` gilt ``max_age + 300`` zugleich als Untergrenze (Review
    MR 12, P1-1, von Georg am 2026-10-10 entschieden): ein Duplikat kann bis
    ``max_age`` warten, auch wenn das Fire selbst sofort lief. Der Deckel
    ``execution_guard_backlog_ttl_s`` bleibt."""
    base = float(config.scheduler_dedup_ttl)
    header_map: dict[str, Any] = headers or {}
    enqueued_at = header_map.get(HEADER_ENQUEUED_AT)
    try:
        waited = max(0.0, time.time() - int(enqueued_at)) if enqueued_at is not None else 0.0
    except TypeError, ValueError:
        waited = 0.0
    ttl = max(base, 2 * waited + 300.0)
    cap = float(config.execution_guard_backlog_ttl_s)
    if max_age is not None:
        cap = min(cap, max(base, max_age + 300.0))
        ttl = max(ttl, max_age + 300.0)
    return int(max(base, min(ttl, cap)) * 1000)


class SchedfireGate:
    def __init__(self, deps: GateDeps) -> None:
        self._deps = deps

    async def apply(self, delivery: Any, ctx: GateContext) -> DeliveryOutcome:
        """Three-state check:

        1. **Completed marker present** → a previous delivery of this fire
           already finished. Ack + skip.
        2. **Completed absent, in-flight SETNX succeeded** → we own the run.
           The runner's finally writes the completed marker on exit.
        3. **Completed absent, in-flight SETNX lost** → another worker
           holds the fire. A fresh delivery is a duplicate: ack + skip
           (E-047). A redelivery (broker counters or the schedfire defer
           header) may have lost its holder to a crash: it is deferred via
           the delay queue until the completed marker appears or the claim
           expires (FS-03, E-046), capped as described in the module
           docstring.
        """
        headers: dict[str, Any] = delivery.headers or {}
        header_value = headers.get("x-swarmq-schedfire")
        if not header_value:
            return Continue
        header_value = schedfire_key(header_value, delivery.headers)
        deps = self._deps

        try:
            already_completed = await bounded_backend_call(
                deps,
                deps.backend.schedules.is_fire_completed(header_value),
                what="schedfire completed check",
            )
        except BackendConnectionError as exc:
            # KTD11: die Zurueckstellung erholt sich selbst -> eine WARNING-Zeile.
            return await defer_backend_unavailable(
                deps,
                delivery,
                ctx,
                exc,
                gate="schedfire",
                stage="completed_check",
                schedfire_key=header_value,
            )

        if already_completed:
            logger.info("schedfire_completed_duplicate_acked", schedfire_key=header_value)
            await deps.broker.ack(delivery)
            return Terminal.never_release()

        # Couple the in-flight TTL to the task's maximum
        # legal runtime (same pattern as compute_lock_ttl_ms) — a fixed TTL
        # could expire DURING a long run and let a stale-leader duplicate
        # win the freed SETNX. Tasks without hard_timeout keep the
        # configured TTL (documented residual).
        inflight_ttl_s = deps.config.scheduler_inflight_ttl
        hard_timeout: int | None = None
        max_age: int | None = None
        try:
            task_info = deps.registry.get(ctx.task_name).task_info
            hard_timeout, max_age = task_info.hard_timeout, task_info.max_age
        except TaskNotFoundError:
            pass  # unknown task fails later in the pipeline anyway
        if hard_timeout is not None:
            inflight_ttl_s = max(inflight_ttl_s, hard_timeout + 30)
        inflight_ttl_ms = int(inflight_ttl_s * 1000)
        completed_ttl_ms = completed_marker_ttl_ms(deps.config, delivery.headers, max_age)
        # Frisches Owner-Token je Zustellung; eine Kopie bietet das Token
        # ihres Vorgaengers zur Uebernahme an (der Claim rotiert dabei; eine
        # Broker-Redelivery derselben Kopie trifft einen fremden Claim und
        # wird zurueckgestellt, FS-03).
        owner = uuid.uuid4().hex
        claim_headers: dict[str, Any] = delivery.headers or {}
        offered = _claim_tokens(claim_headers.get(HEADER_SCHEDFIRE_CLAIM))
        # Das Token liegt VOR dem Aufruf am Kontext: geht die Antwort
        # verloren, traegt der Claim ``owner`` (geschrieben) oder noch eines
        # der angebotenen Tokens -- die Kopie bietet alle an.
        ctx.schedfire_claim = SchedfireClaim(
            key=header_value,
            owner=owner,
            candidates=offered,
            completed_ttl_ms=completed_ttl_ms,
        )
        try:
            claimed = await bounded_backend_call(
                deps,
                deps.backend.schedules.claim_fire(header_value, inflight_ttl_ms, owner=owner, takeover=offered),
                what="schedfire inflight claim",
            )
        except BackendConnectionError as exc:
            return await defer_backend_unavailable(
                deps,
                delivery,
                ctx,
                exc,
                gate="schedfire",
                stage="inflight_claim",
                schedfire_key=header_value,
            )

        if claimed:
            # This worker owns the run; the runner's finally finalizes the
            # claim (completed marker on terminal, in-flight clear otherwise).
            # Der Claim traegt jetzt sicher nur noch ``owner``.
            ctx.schedfire_claim = SchedfireClaim(key=header_value, owner=owner, completed_ttl_ms=completed_ttl_ms)
            return Continue

        # Another worker is running it right now.
        ctx.schedfire_claim = None
        if _is_redelivery(claim_headers):
            # Deckel (F5): bis dahin ist der fremde Claim sicher abgelaufen
            # (oder die Kopie hat ihn spaetestens ``backend_defer_max_delay``
            # danach geprueft), und der Completed-Marker eines inzwischen
            # gelaufenen Fires steht noch.
            cap_s = max(completed_ttl_ms / 1000, inflight_ttl_s + deps.config.backend_defer_max_delay)
            return await self._defer(delivery, ctx, header_value, cap_s)
        logger.info("schedfire_inflight_duplicate_acked", schedfire_key=header_value)
        await deps.broker.ack(delivery)
        return Terminal.never_release()

    async def _defer(self, delivery: Any, ctx: GateContext, key: str, cap_s: float) -> DeliveryOutcome:
        """Fremder Claim bei einer Neuzustellung: als neue Nachricht ueber
        die Delay-Queue zurueckstellen, mit wachsender Wartezeit."""
        config = self._deps.config
        prior = prior_deferrals(delivery.headers or {}, HEADER_SCHEDFIRE_DEFER_COUNT)
        waited_s = nominal_defer_total(prior, config.backend_defer_base_delay, config.backend_defer_max_delay)
        if waited_s * MIN_DEFER_JITTER >= cap_s:
            logger.warning(
                "schedfire_defer_stalled",
                task_id=ctx.task_id,
                task_name=ctx.task_name,
                schedfire_key=key,
                defer_count=prior,
                cap_s=cap_s,
                hint=(
                    "the in-flight claim of this fire stayed foreign for longer than its ttl; "
                    "another delivery holds a fresh claim -- the redelivery is acked without running"
                ),
            )
            await self._deps.broker.ack(delivery)
            return Terminal.never_release()
        headers = republish_headers(delivery, ctx)
        attempt = prior + 1
        headers[HEADER_SCHEDFIRE_DEFER_COUNT] = attempt
        delay_s = backend_defer_delay(attempt, config.backend_defer_base_delay, config.backend_defer_max_delay)
        delay_ms = int(delay_s * 1000)
        try:
            await self._deps.broker.retry_later(delivery, delay_ms=delay_ms, headers=headers)
        except Exception as exc:  # noqa: BLE001 - Broker gestoert, Requeue an derselben Stelle
            # Broker gestoert: Requeue an derselben Stelle. Die Zustellung
            # haelt keinen Claim (``ctx.schedfire_claim`` ist None) -- der
            # fremde bleibt unangetastet.
            logger.warning(
                "schedfire_defer_failed_redelivered",
                task_id=ctx.task_id,
                task_name=ctx.task_name,
                schedfire_key=key,
                **_error_fields(exc),
            )
            await redeliver_in_place(self._deps, delivery, ctx)
            return Redelivered
        logger.info(
            "schedfire_redelivery_deferred",
            task_id=ctx.task_id,
            task_name=ctx.task_name,
            schedfire_key=key,
            defer_count=attempt,
            delay_ms=delay_ms,
        )
        return Redelivered
