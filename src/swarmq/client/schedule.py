"""Client-Schedule-Seite (Plan U10) — verbatim aus ``client.py``: schedule,
schedule_many, Dedup-Claim/-Rollback, deferred (delay/eta) und die
Queue-/Delivery-Auflösung (seit Plan 0.12.0, MR 0, in ``schedule_resolve.py``).
``_MISSING`` wird hier und von der Fassade re-exportiert (app.py importiert es
unverändert)."""

from __future__ import annotations

import asyncio
import time
import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Any, Literal, Protocol

if TYPE_CHECKING:
    from swarmq.middleware import MiddlewareChain
    from swarmq.registry import TaskRegistry
    from swarmq.signals import SignalDispatcher

from swarmq.client.schedule_resolve import _MISSING as _MISSING  # Re-Export (Fassade, app.py)
from swarmq.client.schedule_resolve import ClientScheduleResolve
from swarmq.client.schedule_resolve import _MissingType as _MissingType
from swarmq.contracts import Publisher, ResultBackend
from swarmq.contracts.coordination import ScheduleStore as ContractScheduleStore
from swarmq.exceptions import (
    PublishIndeterminate,
    TaskNotFoundError,
)
from swarmq.headers import (
    HEADER_ENQUEUED_AT,
    HEADER_UNIQUE_HASH,
    HEADER_UNIQUE_UNTIL,
)
from swarmq.logging import _error_fields, get_logger
from swarmq.middleware import (
    MessageContext,
)
from swarmq.scheduler import ScheduleDefinition, ScheduleStore
from swarmq.serialization import (
    compute_dedup_hash,
    encode_json,
)
from swarmq.signals import Signal
from swarmq.task import UniqueUntil

logger = get_logger(__name__)


class ClientBroker(Publisher, Protocol):
    """Broker-Bedarf des Clients (U15): publish + Verbindungs-Lifecycle."""


class ClientBackend(ResultBackend, Protocol):
    """Backend-Bedarf des Clients (U15): ``ResultBackend`` plus der
    ``schedules``-Store der Fassade (Delay/ETA, existiert nach ``connect()``)."""

    @property
    def schedules(self) -> ContractScheduleStore: ...


class ClientScheduling(ClientScheduleResolve):
    # Mixin über dem Client-Zustand (U10); die Attribute setzt ``Client.__init__``.
    _schedule_store: ScheduleStore | None
    if TYPE_CHECKING:
        _broker: ClientBroker
        _backend: ClientBackend
        _registry: TaskRegistry | None
        _backend_op_timeout: float | None
        _signal_dispatcher: SignalDispatcher

        def _get_chain(self) -> MiddlewareChain: ...

    async def _claim_unique_flag(
        self,
        task_name: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        task_id: str,
        ttl_ms: int,
        source: str,
        **extra_signal_payload: Any,
    ) -> tuple[str, str | None]:
        """Attempt SETNX for the given (task_name, args, kwargs) tuple.

        Returns ``(dedup_hash, existing_task_id)``:
          * ``existing_task_id is None`` → flag claimed; caller proceeds
            to publish and tag the message headers.
          * ``existing_task_id is str`` → duplicate detected; caller
            short-circuits and returns the existing owner's task_id.
            TASK_DUPLICATE_DETECTED has already been dispatched with
            the standard payload (existing_task_id, task_name,
            dedup_hash, source) plus any ``extra_signal_payload`` keys
            (e.g. batch_index for schedule_many).

        Shared helper — single source of truth for the SETNX + dup-
        signal shape used by ``Client.schedule``, ``_schedule_deferred``,
        and ``schedule_many``. Changing the signal payload now requires
        exactly one edit.
        """
        dedup_hash = compute_dedup_hash(task_name, args, kwargs)
        existing = await self._backend.claim_unique(
            dedup_hash,
            task_id,
            ttl_ms,
            op_timeout=self._backend_op_timeout,
        )
        if existing is None:
            return dedup_hash, None

        payload: dict[str, Any] = {
            "existing_task_id": existing,
            "task_name": task_name,
            "dedup_hash": dedup_hash,
            "source": source,
            **extra_signal_payload,
        }
        await self._signal_dispatcher.dispatch(
            Signal.TASK_DUPLICATE_DETECTED,
            **payload,
        )
        logger.info("dedup_detected", **payload)
        return dedup_hash, existing

    async def _rollback_unique_claim(
        self,
        dedup_hash: str,
        task_id: str,
    ) -> None:
        """Best-effort release of a dedup flag whose enqueue failed.

        Called by P1 #1 / #2 rollback paths when SETNX succeeded but the
        subsequent publish / store insert raised. The release uses the
        owner-checked Lua script — if a racing second schedule somehow
        claimed the same cell, the DEL is a no-op. Failures are logged
        and swallowed so the original enqueue exception stays the
        visible error to the caller (TTL-backstop self-heals any leak).
        """
        try:
            await self._backend.release_unique(
                dedup_hash,
                task_id,
                op_timeout=self._backend_op_timeout,
            )
        except Exception as exc:  # noqa: BLE001 - Rollback best effort, Fehler wird geloggt
            logger.warning(
                "dedup_rollback_failed",
                dedup_hash=dedup_hash,
                task_id=task_id,
                **_error_fields(exc),
            )

    def _ensure_schedule_store(self) -> ScheduleStore:
        """Lazy ``ScheduleStore``. Seit U18 kommt die
        Valkey-Seite über ``backend.schedules`` — vor ``connect()`` wirft die
        Fassade ``BackendConnectionError``, hier als ``RuntimeError`` mit
        Hinweis auf ``async with SwarmQ(...)`` / ``backend.connect()``."""
        if self._schedule_store is None:
            try:
                self._schedule_store = ScheduleStore(self._backend)
            except RuntimeError as exc:
                raise RuntimeError(
                    "Client.schedule(delay=/eta=) requires a connected backend — "
                    "use `async with SwarmQ(...)` or await backend.connect() first"
                ) from exc
        return self._schedule_store

    async def schedule(
        self,
        task_name: str,
        *args: Any,
        queue: str | None = None,
        delay: float | None = None,
        eta: datetime | None = None,
        unique_for: int | _MissingType | None = _MISSING,
        unique_until: Literal["start", "completion"] | _MissingType = _MISSING,
        **kwargs: Any,
    ) -> str:
        """Submit a task for execution and return its task_id.

        Reserved keyword-only args ``delay``, ``eta``, ``unique_for``,
        ``unique_until`` cannot collide with user payloads because they
        are reserved at ``@app.task`` registration.

        Dedup (R3.5) is silent-by-default: a duplicate ``schedule`` call
        within ``unique_for`` seconds returns the *existing* task_id
        instead of a new one, and emits ``TASK_DUPLICATE_DETECTED``
        for observability. No exception is raised — the API shape
        stays ``schedule() -> str``.
        """
        # --- validation (registry mode only) ---
        if self._registry is not None and not self._registry.has(task_name):
            raise TaskNotFoundError(task_name)

        # --- queue resolution chain + delivery_mode resolution ---
        resolved_queue, delivery_mode = self._resolve_queue_and_delivery_mode(task_name, queue)

        # --- deferred path: delay= / eta= ---
        if delay is not None and eta is not None:
            raise ValueError("Specify delay OR eta, not both")
        if delay is not None and delay < 0:
            raise ValueError(f"delay must be >= 0, got {delay!r}")
        # delay=0 is a normal immediate publish; only non-zero delay and
        # eta take the scheduler-store detour.
        if (delay is not None and delay > 0) or eta is not None:
            # Resolve R3.5 dedup up here so deferred path + immediate path
            # apply the same precedence rules (_MISSING → TaskInfo default,
            # None → dedup off, int → explicit).
            resolved_unique_for, resolved_unique_until = self._resolve_unique(task_name, unique_for, unique_until)
            return await self._schedule_deferred(
                task_name,
                list(args),
                kwargs,
                resolved_queue=resolved_queue,
                delay=delay,
                eta=eta,
                unique_for=resolved_unique_for,
                unique_until=resolved_unique_until,
            )

        # --- build and publish message ---
        task_id = str(uuid.uuid7())

        # --- R3.5 dedup resolution ---
        resolved_unique_for, resolved_unique_until = self._resolve_unique(task_name, unique_for, unique_until)
        dedup_hash: str | None = None
        headers: dict[str, Any] = {}

        if resolved_unique_for is not None:
            dedup_hash, existing = await self._claim_unique_flag(
                task_name,
                args,
                kwargs,
                task_id,
                ttl_ms=resolved_unique_for * 1000,
                source="client.schedule",
            )
            if existing is not None:
                return existing  # Silent dedup — signal already emitted.
            # Flag acquired — tag the message so the worker can release
            # at the right point (start-mode: pickup; completion-mode: terminal).
            headers[HEADER_UNIQUE_HASH] = dedup_hash
            headers[HEADER_UNIQUE_UNTIL] = resolved_unique_until

        # R4.4 Task Expiry: always stamp the enqueue time. Worker
        # compares (now - enqueued_at) to task_info.max_age at pickup.
        # Timestamp is unconditional so tasks with task_info.max_age=None
        # can still be debugged (header is useful even without expiry).
        headers[HEADER_ENQUEUED_AT] = int(time.time())

        message = {
            "task": {
                "id": task_id,
                "queue": resolved_queue,
                "name": task_name,
            },
            "args": list(args),
            "kwargs": kwargs,
        }

        # Canonical message_dict for middleware hooks (worker-agnostic schema).
        message_dict: MessageContext = {
            "task_id": task_id,
            "task_name": task_name,
            "queue": resolved_queue,
            "args": list(args),
            "kwargs": kwargs,
            "delivery_mode": delivery_mode,
        }

        # Resolve the middleware chain once per schedule() call — the factory
        # sees any middlewares registered after Client construction.
        chain = self._get_chain()

        # If SETNX claimed the flag,
        # rollback on any downstream failure (pre_enqueue hook raises,
        # encode_json raises, broker.publish raises). Without rollback
        # the flag zombies until TTL while no message exists in the
        # broker — any subsequent same-args schedule() gets the orphan
        # task_id as "existing". Owner-check in unique_release makes
        # the rollback safe against a racing second schedule that
        # somehow won SETNX in the meantime (cannot happen atomically
        # but defense-in-depth).
        try:
            # pre_enqueue exceptions PROPAGATE — they can prevent the enqueue.
            await chain.run_pre_enqueue(message_dict)

            # broker.publish forwards **properties to aio_pika.Message(**),
            # which accepts ``headers=`` natively. Empty dict is harmless.
            await self._broker.publish(
                resolved_queue,
                encode_json(message),
                headers=headers or None,
            )
        except PublishIndeterminate:
            # Die Frist riss beim Warten auf das Publisher-Confirm — der
            # Broker kann die Nachricht bereits haben. Die Dedup-Sperre
            # bleibt darum stehen: Ein Wiederholversuch des Aufrufers laeuft
            # dagegen und wird abgewiesen, statt den Task ein zweites Mal
            # auszufuehren. Fuer ein Feature, dessen einziger Zweck
            # Einmaligkeit ist, ist Blockieren die richtige Richtung; die
            # Sperre loest sich nach ihrer eigenen Lebensdauer von selbst
            # (Plan 2026-08-28-001, R11).
            raise
        except BaseException:
            if dedup_hash is not None:
                await self._rollback_unique_claim(dedup_hash, task_id)
            raise

        # post_enqueue + TASK_ENQUEUED signal only after successful publish.
        await chain.run_post_enqueue(message_dict)
        await self._signal_dispatcher.dispatch(
            Signal.TASK_ENQUEUED,
            task_id=task_id,
            task_name=task_name,
            queue=resolved_queue,
        )
        return task_id

    async def _batch_claim_flags(
        self,
        prepped: list[dict[str, Any]],
    ) -> tuple[dict[int, asyncio.Task[str | None]], dict[int, str | None], dict[int, BaseException]]:
        """Pass 1 (parallel): fire all SETNX calls concurrently. A sequential
        loop was the main per-batch latency cost for dedup-tagged batches —
        gather amortises N * Valkey_RTT to ~1 * RTT."""
        setnx_tasks: dict[int, asyncio.Task[str | None]] = {}
        for i, entry in enumerate(prepped):
            if entry["dedup_hash"] is None:
                continue
            setnx_tasks[i] = asyncio.create_task(
                self._backend.claim_unique(
                    entry["dedup_hash"],
                    entry["task_id"],
                    entry["dedup_ttl_ms"],
                    op_timeout=self._backend_op_timeout,
                ),
            )
        setnx_results: dict[int, str | None] = {}
        setnx_errors: dict[int, BaseException] = {}
        if setnx_tasks:
            await asyncio.gather(*setnx_tasks.values(), return_exceptions=True)
            for i, t in setnx_tasks.items():
                exc = t.exception()
                if exc is None:
                    setnx_results[i] = t.result()
                else:
                    setnx_errors[i] = exc
        return setnx_tasks, setnx_results, setnx_errors

    async def _batch_rollback_claims(
        self,
        prepped: list[dict[str, Any]],
        setnx_tasks: dict[int, asyncio.Task[str | None]],
        setnx_results: dict[int, str | None],
    ) -> None:
        """Release every flag claimed in Pass 1 (all claims are equally
        orphaned since publish was never reached)."""
        for i, entry in enumerate(prepped):
            if entry["dedup_hash"] is None:
                continue
            if setnx_results.get(i) is None and i in setnx_tasks:
                # Claim succeeded in Pass 1.
                await self._rollback_unique_claim(
                    entry["dedup_hash"],
                    entry["task_id"],
                )

    async def _batch_emit_duplicate(
        self,
        i: int,
        entry: dict[str, Any],
        existing: str,
    ) -> None:
        payload = {
            "existing_task_id": existing,
            "task_name": entry["task_name"],
            "dedup_hash": entry["dedup_hash"],
            "source": "client.schedule_many",
            "batch_index": i,
        }
        await self._signal_dispatcher.dispatch(
            Signal.TASK_DUPLICATE_DETECTED,
            **payload,
        )
        logger.info("dedup_detected", **payload)

    async def _batch_publish_and_finalize(
        self,
        state: dict[str, Any],
        chain: Any,
    ) -> None:
        """Publish concurrently (non-dup positions), rollback flags of
        failed publishes, and emit post_enqueue + TASK_ENQUEUED for the
        successful ones — also on partial failure, so observers aren't
        left with a half-emitted state."""
        publish_indices = state["publish_indices"]
        task_ids = state["task_ids"]
        coros = [
            self._broker.publish(
                state["resolved_queues"][i],
                state["encoded_messages"][i],
                headers=state["dedup_headers"][i] or None,
            )
            for i in publish_indices
        ]
        results = await asyncio.gather(*coros, return_exceptions=True)
        errors = [(publish_indices[k], r) for k, r in enumerate(results) if isinstance(r, BaseException)]
        ok_indices = [publish_indices[k] for k, r in enumerate(results) if not isinstance(r, BaseException)]
        if errors:
            # Log partially-submitted task_ids so they can be recovered.
            submitted = [task_ids[i] for i in ok_indices]
            if submitted:
                logger.warning(
                    "schedule_many_partial_failure",
                    submitted_task_ids=submitted,
                    failed_count=len(errors),
                )
            # Rollback flags for elements whose publish failed (the
            # successful ones still have matching messages in the broker).
            for idx, _ in errors:
                claim = state["claimed_flags"].get(idx)
                if claim is not None:
                    await self._rollback_unique_claim(*claim)
        for i in ok_indices:
            await chain.run_post_enqueue(state["message_dicts"][i])
            await self._signal_dispatcher.dispatch(
                Signal.TASK_ENQUEUED,
                task_id=task_ids[i],
                task_name=state["task_names"][i],
                queue=state["resolved_queues"][i],
            )
        if errors:
            raise errors[0][1]

    async def schedule_many(
        self,
        tasks: list[tuple[str, tuple[Any, ...], dict[str, Any]]],
        queue: str | None = None,
        *,
        unique_for: int | _MissingType | None = _MISSING,
        unique_until: Literal["start", "completion"] | _MissingType = _MISSING,
    ) -> list[str]:
        """Submit multiple tasks for execution and return their task_ids.

        Each element of *tasks* is a (task_name, args, kwargs) tuple.
        All messages are published concurrently via asyncio.gather.

        R3.5 dedup (Unit 8): the batch-level ``unique_for`` /
        ``unique_until`` kwargs override TaskInfo defaults for every
        element (same precedence as the immediate path — _MISSING falls
        through to TaskInfo when a registry is available, None disables
        dedup, int sets the TTL). Duplicates emit
        ``TASK_DUPLICATE_DETECTED`` with ``batch_index=i`` in the
        payload and the returned list places the existing owner's
        task_id at index ``i`` — list length always equals
        ``len(tasks)``.
        """
        if not tasks:
            return []

        # Resolve the middleware chain once for this whole batch so that
        # every pre_enqueue/post_enqueue hook sees the same chain instance.
        chain = self._get_chain()

        prepped = self._batch_prepare(tasks, queue, unique_for, unique_until)
        setnx_tasks, setnx_results, setnx_errors = await self._batch_claim_flags(prepped)

        # Positional result slots so dup-indices and success-indices stay
        # aligned with the input list; ``claimed_flags`` tracks Pass-1-claims
        # for rollback on any downstream failure (orphan flags would block
        # re-scheduling those args for the full unique_for window).
        task_ids: list[str] = [""] * len(tasks)
        state: dict[str, Any] = {
            "task_ids": task_ids,
            "publish_indices": [],
            "message_dicts": {},
            "encoded_messages": {},
            "resolved_queues": {},
            "task_names": {},
            "dedup_headers": {},
            "claimed_flags": {},
        }

        # Pass 2 (sequential): iterate in input order, emit per-element
        # signals / build headers / register pre_enqueue. Ordering matters:
        # TASK_DUPLICATE_DETECTED handlers see events in batch-index order.
        try:
            if setnx_errors:
                raise next(iter(setnx_errors.values()))  # noqa: TRY301 - erster SETNX-Fehler laeuft bewusst durch den Rollback-Pfad

            for i, entry in enumerate(prepped):
                headers: dict[str, Any] = {}
                if entry["dedup_hash"] is not None:
                    existing = setnx_results.get(i)
                    if existing is not None:
                        # Duplicate — emit signal, fill result slot, skip.
                        await self._batch_emit_duplicate(i, entry, existing)
                        state["task_ids"][i] = existing
                        continue
                    # Flag claimed — record for rollback + headers.
                    headers[HEADER_UNIQUE_HASH] = entry["dedup_hash"]
                    headers[HEADER_UNIQUE_UNTIL] = entry["unique_until"]
                    state["claimed_flags"][i] = (entry["dedup_hash"], entry["task_id"])

                # R4.4: always stamp enqueue time (see schedule()).
                headers[HEADER_ENQUEUED_AT] = int(time.time())

                state["task_ids"][i] = entry["task_id"]
                state["publish_indices"].append(i)
                state["task_names"][i] = entry["task_name"]
                state["resolved_queues"][i] = entry["queue"]
                state["dedup_headers"][i] = headers
                state["encoded_messages"][i] = encode_json(
                    {
                        "task": {
                            "id": entry["task_id"],
                            "queue": entry["queue"],
                            "name": entry["task_name"],
                        },
                        "args": list(entry["args"]),
                        "kwargs": entry["kwargs"],
                    }
                )
                message_dict: MessageContext = {
                    "task_id": entry["task_id"],
                    "task_name": entry["task_name"],
                    "queue": entry["queue"],
                    "args": list(entry["args"]),
                    "kwargs": entry["kwargs"],
                    "delivery_mode": entry["delivery_mode"],
                }
                state["message_dicts"][i] = message_dict

                # pre_enqueue: exception aborts the whole batch.
                await chain.run_pre_enqueue(message_dict)
        except BaseException:
            await self._batch_rollback_claims(prepped, setnx_tasks, setnx_results)
            raise

        await self._batch_publish_and_finalize(state, chain)
        return task_ids

    async def _schedule_deferred(
        self,
        task_name: str,
        args: list[Any],
        kwargs: dict[str, Any],
        *,
        resolved_queue: str,
        delay: float | None,
        eta: datetime | None,
        unique_for: int | None = None,
        unique_until: UniqueUntil = "completion",
    ) -> str:
        """Write a one-shot entry to the ScheduleStore and return task_id.

        The scheduler-loop in Unit 10 poll's, publishes with
        ``x-swarmq-schedfire`` header, and commits. The worker dedupes on
        the header (Unit 8), then runs the task like any other message.

        R3.5 (Unit 7): when ``unique_for`` is set, claim the dedup flag
        BEFORE the ScheduleStore insert with TTL ``(unique_for +
        fire_offset_s) * 1000`` so the flag outlives the fire delay plus
        the whole terminal window. The producer here is authoritative —
        Scheduler._fire_one observes ``source="cli_unique"`` and skips
        its own SETNX, avoiding the double-claim race. On duplicate we
        emit ``TASK_DUPLICATE_DETECTED`` and return the existing task_id
        silently, same API shape as the immediate path.
        """
        now_ms = int(time.time() * 1000)
        if eta is not None:
            fire_ts_ms = int(eta.timestamp() * 1000)
            # Allow a 1s lenience for clock skew / scheduling latency.
            if fire_ts_ms < now_ms - 1_000:
                raise ValueError(f"eta is in the past: {eta.isoformat()}. Use delay=0 for immediate execution.")
        else:
            assert delay is not None  # caller-side validated
            fire_ts_ms = now_ms + int(delay * 1000)

        task_id = str(uuid.uuid7())

        # --- R3.5 Unit 7: producer-side SETNX before ScheduleStore insert ---
        dedup_hash: str | None = None
        if unique_for is not None:
            # Fire offset: how long we wait before publish. Use ceiling
            # division so sub-second delays (e.g. delay=0.9) still contribute
            # a full second of TTL padding — floor would truncate to 0 and
            # let the flag expire before the task finishes running. Clamp
            # to 0 so late-past eta does not shrink TTL below unique_for.
            fire_offset_ms = max(0, fire_ts_ms - now_ms)
            fire_offset_s = (fire_offset_ms + 999) // 1000  # ceiling division
            ttl_ms = (unique_for + int(fire_offset_s)) * 1000
            dedup_hash, existing = await self._claim_unique_flag(
                task_name,
                tuple(args),
                kwargs,
                task_id,
                ttl_ms=ttl_ms,
                source="client.schedule_deferred",
            )
            if existing is not None:
                return existing

        schedule_name = f"oneshot:{task_id}"
        definition = ScheduleDefinition(
            schedule_name=schedule_name,
            task_name=task_name,
            args=tuple(args),
            kwargs=kwargs,
            queue=resolved_queue,
            cron=None,
            fire_ts_ms=fire_ts_ms,
            source="cli_unique" if dedup_hash is not None else "cli",
            dedup_hash=dedup_hash,
            unique_until=unique_until,
            task_id=task_id,
        )

        # Rollback the dedup flag if the
        # ScheduleStore insert fails — for large delays the TTL is
        # (unique_for + fire_offset) seconds, potentially hours of
        # orphan window blocking re-schedule of the same args.
        store = self._ensure_schedule_store()
        try:
            await store.add_schedule(definition)
        except BaseException:
            if dedup_hash is not None:
                await self._rollback_unique_claim(dedup_hash, task_id)
            raise
        return task_id
