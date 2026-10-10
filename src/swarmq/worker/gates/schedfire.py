"""Schedfire-Dedup-Gate (Phase 3c Unit 8), Plan U7 — verbatim
aus ``pipeline.py``. Läuft als erstes Gate, vor Cancel-Check, Semaphore,
Rate-Limit und Lock.

Zwei Marker: Completed (lange TTL, nach dem Lauf) und In-Flight (kurze TTL,
Claim vor dem Lauf). Duplikate werden bestätigt und übersprungen
(``Terminal.never_release()`` — kein Claim, keine Freigabe); Backend-Fehler
→ Zurueckstellung ueber die Delay-Queue (``backend_defer``, ``Redelivered``).
Der Runner behandelt dieses Gate wie heute *vor* seinem ``try/finally``
(Plan-Fortschrittsprotokoll U19).

Claim-Token (0.9.0): Der In-Flight-Claim speichert ein Owner-Token je
Zustellung. Jede Kopie (Zurueckstellung, Retry, Rueckstellung) traegt es als
``x-swarmq-schedfire-claim`` und bietet es beim Claim zur Uebernahme an --
scheiterte die Freigabe am Backend-Ausfall, gilt der Claim fuer die Kopie
als ihrer statt als Duplikat. Die Uebernahme rotiert das Token; eine
Zustellung ohne oder mit altem Token bleibt ein Duplikat.

Das Token liegt schon VOR dem Claim-Aufruf am Kontext: geht dessen Antwort
verloren (Timeout, Reset), kann Valkey den Claim geschrieben haben oder
nicht. Die Kopie traegt dann das neue Token und die angebotenen (komma-
getrennt, neuestes zuerst, gedeckelt) -- sie uebernimmt den Claim in beiden
Faellen. Ein Requeue an derselben Stelle (``nack``) kann kein Token tragen;
dort wird der Claim vorher freigegeben (``redeliver_in_place``) -- scheitert
das, geht statt des ``nack`` eine Kopie mit dem Token.
"""

from __future__ import annotations

import uuid
from itertools import islice
from typing import Any

from swarmq.exceptions import BackendConnectionError, TaskNotFoundError
from swarmq.headers import HEADER_SCHEDFIRE_CLAIM
from swarmq.identity import identity_suffix
from swarmq.logging import get_logger
from swarmq.worker.gates import MAX_SCHEDFIRE_CLAIM_TOKENS, GateContext, GateDeps, SchedfireClaim
from swarmq.worker.gates.backend_defer import bounded_backend_call, defer_backend_unavailable
from swarmq.worker.outcome import Continue, DeliveryOutcome, Terminal

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


def schedfire_key(fire_key: str, headers: Any) -> str:
    """Key der Marker eines Fires (Plan 0.12.0, MR 12, BZ-08 Cron): bei einer
    Replay-Identitaet (``x-swarmq-replay-id`` gesetzt oder Broker-Tode
    ausserhalb der Delay-Queues) ``K|<Identitaet ohne task_id>``, damit der
    Completed-Marker des ersten Laufs das bewusste Replay nicht verwirft.
    Normale Doppel-Fires (n = 0, kein H) behalten K wie 0.11."""
    suffix = identity_suffix(headers)
    return fire_key if not suffix else f"{fire_key}|{suffix.lstrip('|')}"


class SchedfireGate:
    def __init__(self, deps: GateDeps) -> None:
        self._deps = deps

    async def apply(self, delivery: Any, ctx: GateContext) -> DeliveryOutcome:
        """Three-state check:

        1. **Completed marker present** → a previous delivery of this fire
           already finished. Ack + skip.
        2. **Completed absent, in-flight SETNX succeeded** → we own the run.
           The runner's finally writes the completed marker on exit.
        3. **Completed absent, in-flight SETNX lost** → another worker is
           running this fire. Ack + skip; if that worker crashes before the
           completed marker, the in-flight key expires and the next
           redelivery re-runs (closes the one-marker silent-drop hole).
        """
        header_value = (delivery.headers or {}).get("x-swarmq-schedfire")
        if not header_value:
            return Continue
        header_value = schedfire_key(header_value, delivery.headers)
        deps = self._deps

        try:
            already_completed = await bounded_backend_call(
                deps, deps.backend.schedules.is_fire_completed(header_value), what="schedfire completed check",
            )
        except BackendConnectionError as exc:
            # KTD11: die Zurueckstellung erholt sich selbst -> eine WARNING-Zeile.
            return await defer_backend_unavailable(
                deps, delivery, ctx, exc, gate="schedfire", stage="completed_check", schedfire_key=header_value,
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
        try:
            hard_timeout = deps.registry.get(ctx.task_name).task_info.hard_timeout
        except TaskNotFoundError:
            pass  # unknown task fails later in the pipeline anyway
        if hard_timeout is not None:
            inflight_ttl_s = max(inflight_ttl_s, hard_timeout + 30)
        inflight_ttl_ms = int(inflight_ttl_s * 1000)
        # Frisches Owner-Token je Zustellung; eine Kopie bietet das Token
        # ihres Vorgaengers zur Uebernahme an (der Claim rotiert dabei, eine
        # Broker-Redelivery derselben Kopie bleibt ein Duplikat).
        owner = uuid.uuid4().hex
        offered = _claim_tokens((delivery.headers or {}).get(HEADER_SCHEDFIRE_CLAIM))
        # Das Token liegt VOR dem Aufruf am Kontext: geht die Antwort
        # verloren, traegt der Claim ``owner`` (geschrieben) oder noch eines
        # der angebotenen Tokens -- die Kopie bietet alle an.
        ctx.schedfire_claim = SchedfireClaim(key=header_value, owner=owner, candidates=offered)
        try:
            claimed = await bounded_backend_call(
                deps,
                deps.backend.schedules.claim_fire(header_value, inflight_ttl_ms, owner=owner, takeover=offered),
                what="schedfire inflight claim",
            )
        except BackendConnectionError as exc:
            return await defer_backend_unavailable(
                deps, delivery, ctx, exc, gate="schedfire", stage="inflight_claim", schedfire_key=header_value,
            )

        if claimed:
            # This worker owns the run; the runner's finally finalizes the
            # claim (completed marker on terminal, in-flight clear otherwise).
            # Der Claim traegt jetzt sicher nur noch ``owner``.
            ctx.schedfire_claim = SchedfireClaim(key=header_value, owner=owner)
            return Continue

        # Another worker is running it right now.
        ctx.schedfire_claim = None
        logger.info("schedfire_inflight_duplicate_acked", schedfire_key=header_value)
        await deps.broker.ack(delivery)
        return Terminal.never_release()
