"""Expiry-Gate (R4.4 ``max_age``), Plan U7 — verbatim aus ``pipeline.py``.

Abgelaufen → Fehler-Ergebnis speichern, Signal ``TASK_EXPIRED``, an das
Dead-Letter-Ziel veröffentlichen, bestätigen (``dead_letter``); Outcome
``Terminal.never_release()`` (das Dedup-Flag bleibt bewusst stehen — heutiges
Verhalten). Publish-Fehler → ``redeliver`` (Fallback laut KTD3).
"""

from __future__ import annotations

import time
from typing import Any

from swarmq.cancellation import build_result_dict
from swarmq.exceptions import BackendConnectionError, BrokerConnectionError
from swarmq.headers import HEADER_ENQUEUED_AT
from swarmq.logging import _error_fields, get_logger
from swarmq.serialization import encode_result
from swarmq.signals import Signal
from swarmq.worker.gates import GateContext, GateDeps, redeliver_in_place
from swarmq.worker.outcome import Continue, DeliveryOutcome, Terminal

logger = get_logger(__name__)


class ExpiryGate:
    def __init__(self, deps: GateDeps) -> None:
        self._deps = deps

    async def apply(self, delivery: Any, ctx: GateContext) -> DeliveryOutcome:
        """``max_age=None`` disables the check entirely. A missing
        ``x-swarmq-enqueued-at`` header is treated as 'not expired'
        (defensive — pre-R4.4 producers + external producers)."""
        task_info = ctx.task_info
        assert task_info is not None
        if task_info.max_age is None:
            return Continue
        headers = delivery.headers or {}
        enqueued_at = headers.get(HEADER_ENQUEUED_AT)
        if enqueued_at is None:
            return Continue
        try:
            enqueued_ts = int(enqueued_at)
        except (TypeError, ValueError):
            # Malformed header — treat as non-expiring rather than DLQ
            # on a header-shape bug. An expired task with a corrupt
            # header still gets MAX_RETRIES redeliveries + x-delivery-
            # limit as the fallback safety net.
            return Continue
        now = int(time.time())
        age = now - enqueued_ts
        if age < task_info.max_age:
            return Continue

        deps = self._deps
        logger.warning(
            "task_expired",
            task_id=ctx.task_id,
            task_name=ctx.task_name,
            queue=ctx.queue,
            age_s=age,
            max_age_s=task_info.max_age,
        )
        await deps.signals.dispatch(
            Signal.TASK_EXPIRED,
            task_id=ctx.task_id,
            task_name=ctx.task_name,
            queue=ctx.queue,
            age_s=age,
            max_age_s=task_info.max_age,
        )
        # Store an error result so callers waiting on get_result() see
        # a clean TaskError("expired") instead of an indefinite timeout.
        # result_ttl=0 means fire-and-forget — skip the
        # store entirely (mirrors the main-path semantics).
        ttl_ms = (task_info.result_ttl or deps.config.result_ttl) * 1000
        if ttl_ms > 0:
            try:
                await deps.backend.store_result(
                    ctx.task_id,
                    encode_result(build_result_dict(
                        task_id=ctx.task_id,
                        queue=ctx.queue,
                        task_name=ctx.task_name,
                        is_error=True,
                        is_cancelled=False,
                        error_message=f"task expired: age {age}s exceeds max_age {task_info.max_age}s",
                    )),
                    ttl_ms=ttl_ms,
                    op_timeout=deps.config.backend_op_timeout,
                )
            except BackendConnectionError as exc:
                # Best-effort: if the backend is down, the message still
                # routes to the DLQ for operator inspection.
                logger.warning("task_expired_result_store_failed", **_error_fields(exc))

        # Route to DLQ + ack. Same pattern as the dead-letter path but
        # inline because the DLQ semantics of 'expired' differ from
        # 'error' (different signal, no retry-count bump).
        #
        # Guard the DLQ publish + ack pair. Without
        # this, a broker hiccup between store_result (succeeded above)
        # and the publish call leaves the TaskError('expired') result in
        # Valkey while the original message cycles through redelivery,
        # generating duplicate TASK_EXPIRED signals. nack(requeue=True)
        # preserves the message so a subsequent delivery retries the
        # terminal path.
        try:
            await deps.broker.dead_letter(delivery, headers=dict(delivery.headers or {}))
        except BrokerConnectionError as exc:
            logger.warning("task_expired_dlq_publish_failed", **_error_fields(exc))
            try:
                await redeliver_in_place(deps, delivery, ctx)
            except BrokerConnectionError as nack_exc:
                # Double-failure — let the message TTL / broker
                # redeliver-limit be the safety net.
                logger.error("task_expired_nack_also_failed", **_error_fields(nack_exc))
        return Terminal.never_release()
