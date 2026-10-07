"""Rate-Limit-Gate, Plan U7 — verbatim aus ``pipeline.py``.

Limitiert → Zähler-Header ``x-swarmq-rate-limit-count`` erhöhen, Signal
``TASK_RATE_LIMITED``, Rückstellung über ``retry_later`` (Verzögerungsziel der
Zustellungs-Queue, KTD2); Outcome ``Redelivered``. Backend-Fehler des Limiters
→ Zurueckstellung mit Backoff (``backend_defer``); andere Limiter- oder
Publish-Fehler → ``redeliver`` (KTD3), ebenfalls ``Redelivered``.
"""

from __future__ import annotations

from typing import Any

from swarmq.exceptions import BackendConnectionError
from swarmq.limits import parse_rate_limit
from swarmq.logging import _error_fields, get_logger
from swarmq.signals import Signal
from swarmq.worker.gates import GateContext, GateDeps
from swarmq.worker.gates.backend_defer import defer_backend_unavailable
from swarmq.worker.outcome import Continue, DeliveryOutcome, Redelivered

logger = get_logger(__name__)


class RateLimitGate:
    def __init__(self, deps: GateDeps) -> None:
        self._deps = deps

    async def apply(self, delivery: Any, ctx: GateContext) -> DeliveryOutcome:
        """The whole sequence — Valkey check, signal/log, broker republish,
        ack — runs inside a single ``try`` so that a Valkey or broker
        failure cannot leave the message in a half-processed state.
        Mirrors the retry path's try/except contract around
        ``publish_to_delay_queue``."""
        task_info = ctx.task_info
        assert task_info is not None
        deps = self._deps
        limiter = deps.rate_limiter()
        if task_info.rate_limit is None or limiter is None:
            return Continue

        config = parse_rate_limit(task_info.rate_limit)
        try:
            # RateLimitStore-Vertrag (U15): eine Entscheidung statt Tupel.
            decision = await limiter.check(
                ctx.task_name,
                max_requests=config.max_requests,
                window_ms=config.window_size_ms,
            )
            allowed, remaining, retry_after_ms = (
                decision.allowed, decision.remaining, decision.retry_after_ms,
            )
        except Exception as exc:
            # The limiter is the only side-effect-free decision-point we
            # have. If Valkey is unreachable we cannot decide safely;
            # nack(requeue=True) so the broker hands the message to the
            # next worker that may have a healthier client. Important:
            # this runs BEFORE any FIRE_AND_FORGET ack — no data loss.
            logger.error("rate_limit_check_failed", task_id=ctx.task_id, task_name=ctx.task_name, **_error_fields(exc))
            if isinstance(exc, BackendConnectionError):
                # Backend weg: zurueckstellen statt das Zustellbudget per
                # Requeue zu verbrauchen. Andere Fehler bleiben beim Requeue.
                return await defer_backend_unavailable(deps, delivery, ctx, exc, gate="rate_limit")
            await deps.broker.redeliver(delivery)
            return Redelivered

        if allowed:
            return Continue

        # Rate-limited. Track how often this exact message has bounced
        # so operators (and TASK_RATE_LIMITED handlers) can see when a
        # task has been stuck in the limiter loop. We don't auto-DLQ
        # here — that decision belongs to the operator and is documented
        # as a Non-Goal in plan 005. The header is the visible knob.
        original_headers = dict(delivery.headers) if delivery.headers else {}
        rl_count = int(original_headers.get("x-swarmq-rate-limit-count", 0)) + 1
        original_headers["x-swarmq-rate-limit-count"] = rl_count

        logger.info(
            "task_rate_limited",
            task_id=ctx.task_id,
            task_name=ctx.task_name,
            retry_after_ms=retry_after_ms,
            remaining=remaining,
            rate_limit_count=rl_count,
        )
        await deps.signals.dispatch(
            Signal.TASK_RATE_LIMITED,
            task_id=ctx.task_id,
            task_name=ctx.task_name,
            retry_after_ms=retry_after_ms,
            remaining=remaining,
            rate_limit_count=rl_count,
        )

        try:
            await deps.broker.retry_later(delivery, delay_ms=retry_after_ms, headers=original_headers)
        except Exception as exc:
            # Broker failure between publish and ack would create a
            # duplicate (republished + redelivered original). nack with
            # requeue=True keeps the original in flight for retry; the
            # delay-queue copy may still arrive but the rate-limit check
            # on the next consume cycle will naturally serialize them.
            logger.error(
                "rate_limit_requeue_failed", task_id=ctx.task_id, task_name=ctx.task_name, **_error_fields(exc)
            )
            await deps.broker.redeliver(delivery)

        return Redelivered
