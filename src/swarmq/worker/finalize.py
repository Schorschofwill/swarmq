"""Abschluss einer Zustellung (Plan 0.12.0, MR 0; verbatim aus ``pipeline.py``).

Schedfire-Claim abschliessen oder freigeben und das Dedup-Flag (R3.5) an den
terminalen Pfaden freigeben. ``Worker`` delegiert hierher und reicht den
Logger von ``swarmq.worker.pipeline`` durch (Q17), damit Logger-Name und
Patch-Pfad unveraendert bleiben.
"""

from __future__ import annotations

from collections.abc import Awaitable
from typing import TYPE_CHECKING, Any

from swarmq.headers import HEADER_UNIQUE_UNTIL
from swarmq.logging import _error_fields
from swarmq.worker.gates.backend_defer import bounded_backend_call

if TYPE_CHECKING:
    from swarmq.config import SwarmQConfig
    from swarmq.task import TaskInfo
    from swarmq.worker.gates import GateContext, GateDeps, WorkerBackend


async def finalize_schedfire(
    ctx: GateContext,
    *,
    terminal: bool,
    backend: WorkerBackend,
    config: SwarmQConfig,
    gate_deps: GateDeps,
    log: Any,
) -> bool:
    """Finalize the in-flight schedfire claim of this delivery.

    Called from the ``_handle_message`` finally block (and the
    pre-start-cancel terminal path). No-ops when the delivery holds no
    claim (``ctx.schedfire_claim`` is set only by the schedfire gate);
    the claim is taken off the context first, so it is finalized once.

    * ``terminal=True`` (success, error→DLQ, pre-start cancel): write
      the long-TTL ``sq:sched:fired:<key>`` completed marker so any
      redelivery of this fire is ack+skipped.
    * ``terminal=False`` (retry republish, rate-limit / lock-conflict
      / shutdown / result-write requeue): the fire is being
      re-delivered, so DELETE the short-TTL in-flight key instead of
      writing the completed marker. Writing
      the completed marker here would make the redelivered/retry copy
      fail ``is_schedfire_completed`` and get silently dropped.

    Failures are logged and swallowed — an orphan in-flight marker
    self-heals via its short TTL. Returns ``False`` only when a held
    claim could not be finalized (``redeliver_in_place`` then copies
    instead of nacking)."""
    claim, ctx.schedfire_claim = ctx.schedfire_claim, None
    if claim is None:
        return True
    schedfire_key, owner = claim.key, claim.owner
    try:
        finalize: Awaitable[object]
        if terminal:
            assert config.scheduler_dedup_ttl is not None  # Config-Default
            # MR 12 (BZ-12): die TTL aus dem Gate folgt der Wartezeit des Fires.
            ttl_ms = claim.completed_ttl_ms
            if ttl_ms is None:
                ttl_ms = int(config.scheduler_dedup_ttl * 1000)
            finalize = backend.schedules.mark_fire_completed(schedfire_key, ttl_ms)
        else:
            # Owner-geprueft: hat eine Kopie den Claim schon uebernommen
            # (verspaetete Freigabe), bleibt er ihr.
            finalize = backend.schedules.release_fire_claim(schedfire_key, owner=owner)
        # Begrenzt: ein Blackhole darf weder Gate noch finally blockieren.
        await bounded_backend_call(gate_deps, finalize, what="schedfire finalize")
    except Exception as exc:  # noqa: BLE001 - darf weder nack noch finally ueberspringen
        # Auch eine fremde Ausnahme (Dritt-Store): sie darf weder den
        # ``nack``/die Kopie eines Requeues noch das finally ueberspringen.
        log.warning(
            "schedfire_finalize_failed",
            schedfire_key=schedfire_key,
            terminal=terminal,
            **_error_fields(exc),
        )
        return False
    return True


async def maybe_release_unique(
    message: Any,
    task_id: str,
    task_info: TaskInfo | None = None,
    *,
    unique_hash: str | None,
    backend: WorkerBackend,
    config: SwarmQConfig,
) -> None:
    """Release the R3.5 dedup flag if this message carries one.

    Contract:
      * No-op when the message lacks a valid ``x-swarmq-unique-hash``
        header (missing, wrong type, not a 64-char lowercase hex
        digest).
      * No-op in ``start``-mode at terminal paths — Unit 10 already
        released at pickup (owner-check would fail here anyway).
      * ``completion``-mode (default): call ``backend.release_unique``;
        owner-check protects against TTL-expiry-race (first task's
        flag was reclaimed by a second task before first finished).
      * When ``task_info`` is None (task not registered) → treat as
        completion mode. Better to attempt release than leave an
        orphan flag until TTL.

    Backend errors are swallowed inside ``release_unique`` — an
    orphan flag self-heals via TTL and a release failure must not
    break the worker's terminal-path pipeline (same discipline as
    ``clear_cancel_flag``). ``op_timeout`` bounds the Valkey call
    so the finally block can't wedge on a slow backend.
    """
    if unique_hash is None:
        return
    # Prefer the header over TaskInfo — per-call overrides from
    # Client.schedule set the header, and we want those overrides
    # to flow end-to-end. TaskInfo is the fallback when the header
    # is absent or malformed.
    headers: dict[str, Any] = message.headers or {}
    header_mode = headers.get(HEADER_UNIQUE_UNTIL)
    mode = header_mode or (task_info.unique_until if task_info is not None else "completion")
    if mode == "start":
        return  # Release already happened at pickup (Unit 10).
    await backend.release_unique(
        unique_hash,
        task_id,
        op_timeout=config.backend_op_timeout,
    )
