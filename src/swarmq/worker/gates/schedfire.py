"""Schedfire-Dedup-Gate (Phase 3c Unit 8), Plan U7 — verbatim
aus ``pipeline.py``. Läuft als erstes Gate, vor Cancel-Check, Semaphore,
Rate-Limit und Lock.

Zwei Marker: Completed (lange TTL, nach dem Lauf) und In-Flight (kurze TTL,
Claim vor dem Lauf). Duplikate werden bestätigt und übersprungen
(``Terminal.never_release()`` — kein Claim, keine Freigabe); Backend-Fehler
→ ``nack(requeue=True)`` (``Redelivered``). Der Runner behandelt dieses Gate
wie heute *vor* seinem ``try/finally`` (Plan-Fortschrittsprotokoll U19).
"""

from __future__ import annotations

from typing import Any

import structlog

from swarmq.exceptions import BackendConnectionError, TaskNotFoundError
from swarmq.worker.gates import GateContext, GateDeps
from swarmq.worker.outcome import Continue, DeliveryOutcome, Redelivered, Terminal

logger = structlog.get_logger()


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
        deps = self._deps

        try:
            already_completed = await deps.backend.schedules.is_fire_completed(header_value)
        except BackendConnectionError:
            logger.error("schedfire_dedup_backend_error", schedfire_key=header_value, stage="completed_check")
            await deps.broker.redeliver(delivery)
            structlog.contextvars.clear_contextvars()
            return Redelivered

        if already_completed:
            logger.info("schedfire_completed_duplicate_acked", schedfire_key=header_value)
            await deps.broker.ack(delivery)
            structlog.contextvars.clear_contextvars()
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
        try:
            claimed = await deps.backend.schedules.claim_fire(header_value, inflight_ttl_ms)
        except BackendConnectionError:
            logger.error("schedfire_dedup_backend_error", schedfire_key=header_value, stage="inflight_claim")
            await deps.broker.redeliver(delivery)
            structlog.contextvars.clear_contextvars()
            return Redelivered

        if claimed:
            # This worker owns the run; the runner's finally finalizes the
            # claim (completed marker on terminal, in-flight clear otherwise).
            deps.claim_schedfire(delivery, header_value)
            return Continue

        # Another worker is running it right now.
        logger.info("schedfire_inflight_duplicate_acked", schedfire_key=header_value)
        await deps.broker.ack(delivery)
        structlog.contextvars.clear_contextvars()
        return Terminal.never_release()
