"""Scheduler-Loop (Plan U11) — verbatim aus ``scheduler.py``: Election
(Leader-Lock über den ``LockStore``-Vertrag), Poll/Fire mit Schedfire-Dedup
und Handover-Marker. Seit U12 (Plan 2026-10-08-1541) meldet jeder Loop die
statischen Crons seines Workers; der Leader übernimmt die Meldungen aller
Worker im Meldetakt. Der Cron-Sync lebt als Mixin in ``sync.py``."""
from __future__ import annotations

import asyncio
import os
import socket
import time
import uuid
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Literal, Protocol

import msgspec

from swarmq.config import SwarmQConfig
from swarmq.exceptions import BackendConnectionError
from swarmq.locking import SCHEDULER_LOCK_NAME
from swarmq.logging import _error_fields, get_logger
from swarmq.serialization import compute_dedup_hash, encode_json
from swarmq.signals import Signal, SignalDispatcher
from swarmq.task import Task, TaskInfo

if TYPE_CHECKING:
    from swarmq.contracts import Publisher
    from swarmq.contracts.coordination import LockStore

from swarmq.contracts.coordination import LockToken
from swarmq.contracts.coordination import ScheduleStore as ContractScheduleStore
from swarmq.scheduler.cron_time import iter_fire_ms, next_fire_ms
from swarmq.scheduler.definition import ScheduleDefinition, ScheduleStore, make_schedfire_key
from swarmq.scheduler.reports import cron_fingerprint, live_window_ms
from swarmq.scheduler.sync import SchedulerCronSync

logger = get_logger(__name__)


class SchedulerBackend(Protocol):
    """Struktureller Backend-Ausschnitt des Loops (U11/U15): Schedfire-Dedup
    über ``claim_unique`` (ResultBackend) plus der ``schedules``-Store der
    Fassade (Handover-Marker, rohe Definitionen für den Cron-Sync)."""

    async def claim_unique(
        self, dedup_hash: str, owner: str, ttl_ms: int, *, op_timeout: float | None = None
    ) -> str | None: ...

    @property
    def schedules(self) -> ContractScheduleStore: ...

_HANDOVER_MARKER_TTL_MS_FLOOR = 5_000
"""Minimum TTL for the handover marker (5s). The actual TTL used is
``max(floor, heartbeat_interval*2*1000)`` so a slow standby still
observes the marker on its next election tick — otherwise every
clean handover would time out before the successor polls and emit
FAILOVER instead of LEADER."""

_SchedulerState = Literal["standby", "leader", "stopped"]


def _make_owner_token() -> str:
    """Human-readable owner identity for the leader lock.

    Format: ``{hostname}:{pid}:{uuid4_short}``. Exposed via
    ``swarmq schedule status`` so operators can tell which worker is
    currently leader, rather than seeing an opaque 32-char hex blob.
    """
    host = socket.gethostname()
    pid = os.getpid()
    suffix = uuid.uuid4().hex[:8]
    return f"{host}:{pid}:{suffix}"


class SchedulerLoop(SchedulerCronSync):
    """Embedded scheduler — one instance per worker.

    Runs four cooperating coroutines:

    - ``_election_loop``: at each ``heartbeat_interval`` tick either
      attempts ``acquire`` (standby) or ``renew`` (leader). On acquire
      success, consults the transient handover marker to decide
      whether to emit :class:`~swarmq.signals.Signal.SCHEDULER_LEADER`
      (clean handover or first-ever start) or
      :class:`~swarmq.signals.Signal.SCHEDULER_LEADER_FAILOVER` (lock
      TTL expired with no marker). On renew success, emits
      :class:`~swarmq.signals.Signal.SCHEDULER_HEARTBEAT`.
    - ``_poll_loop``: at each ``poll_interval`` tick, standbys no-op;
      the leader reads due schedules, publishes to the main queue
      with an ``x-swarmq-schedfire`` header, and commits via the
      owner-checked Lua script.
    - ``_report_loop``: every ``scheduler_seen_interval_s`` (and once in
      :meth:`start`) reports this worker's static crons, in any role.
    - ``_adopt_loop``: the leader adopts the youngest live report per
      cron right after acquiring the lock and then every
      ``scheduler_seen_interval_s`` (Plan 2026-10-08-1541, KTD10).

    :meth:`stop` is the graceful path: emits HANDOVER, writes a
    short-TTL marker under the outgoing token, releases the lock, and
    cancels the sub-coroutines. A non-graceful exit (crash/SIGKILL)
    leaves no marker, and the next standby to acquire observes the
    missing marker and emits FAILOVER.
    """

    def __init__(
        self,
        *,
        backend: SchedulerBackend,
        broker: Publisher,
        config: SwarmQConfig,
        lock_manager: LockStore,
        store: ScheduleStore,
        signal_dispatcher: SignalDispatcher,
        owner_token: str | None = None,
        worker_id: str | None = None,
        static_crons: Sequence[ScheduleDefinition] = (),
    ) -> None:
        self._backend = backend
        self._broker = broker
        self._config = config
        self._lock_manager = lock_manager
        self._store = store
        self._signals = signal_dispatcher
        self._owner_token = owner_token or _make_owner_token()
        # LockStore-Vertrag (U11): Token deterministisch aus Name+Owner —
        # acquire liefert denselben Token, renew/release nehmen ihn.
        self._lock_token = LockToken(
            name=SCHEDULER_LOCK_NAME, owner=self._owner_token, max_concurrency=1
        )
        self._worker_id = worker_id or self._owner_token
        # Statische Crons dieses Workers (eigene Registry, Zone aufgelöst);
        # gemeldet beim Start und danach im Meldetakt (KTD10).
        self._static_crons = tuple(static_crons)

        self._state: _SchedulerState = "standby"
        self._prev_leader_seen = False
        """Set to True when election sees the lock held by *someone*
        (acquire returned False). Used together with the
        handover-marker check to distinguish first-ever start from
        TTL-expiry failover."""

        self._election_task: asyncio.Task[None] | None = None
        self._poll_task: asyncio.Task[None] | None = None
        self._report_task: asyncio.Task[None] | None = None
        self._adopt_task: asyncio.Task[None] | None = None
        self._adopt_now = asyncio.Event()
        self._stop_event = asyncio.Event()

        # Consecutive renew-ERROR counter. A renew that
        # returns False (lock observably gone) already steps down, but a
        # renew that RAISES (Valkey unreachable) previously kept
        # state=="leader" indefinitely — the poll loop fired through the
        # entire outage while a standby took over after TTL expiry
        # (split-brain for the outage duration). After `limit`
        # consecutive failures the leader proactively steps down, one
        # heartbeat BEFORE the lock TTL can have expired.
        self._renew_failures = 0
        self._renew_failure_limit = max(
            1,
            int(
                config.scheduler_lock_ttl
                / config.scheduler_heartbeat_interval
            )
            - 1,
        )

    # ---- lifecycle ------------------------------------------------------

    @property
    def state(self) -> _SchedulerState:
        return self._state

    @property
    def owner_token(self) -> str:
        return self._owner_token

    async def start(self) -> None:
        """Launch election + poll coroutines. Idempotent."""
        if self._election_task is not None:
            return
        self._stop_event.clear()
        await self._report_crons()
        self._election_task = asyncio.create_task(self._election_loop())
        self._poll_task = asyncio.create_task(self._poll_loop())
        self._report_task = asyncio.create_task(self._report_loop())
        self._adopt_task = asyncio.create_task(self._adopt_loop())

    async def stop(self) -> None:
        """Graceful shutdown. Releases the leader lock if held, sets
        the handover marker so the next acquirer emits LEADER instead
        of FAILOVER, then cancels both sub-coroutines.

        Ordering: state transition and
        stop_event are set BEFORE lock release. Otherwise a window
        exists where the lock is already released (another worker can
        acquire it) while our own poll_loop still believes it is the
        leader — one spurious publish batch can fire inside that
        window. Setting state first prevents our poll_loop from
        starting a new tick after the release.
        """
        if self._state == "leader":
            await self._emit(Signal.SCHEDULER_LEADER_HANDOVER)
            try:
                # 2x heartbeat converted to ms — explicit factor-of-2
                # safety margin over the seconds-to-ms conversion.
                marker_ttl_ms = max(
                    _HANDOVER_MARKER_TTL_MS_FLOOR,
                    int(self._config.scheduler_heartbeat_interval * 2 * 1000),
                )
                await self._backend.schedules.set_handover_marker(
                    self._owner_token, marker_ttl_ms
                )
            except BackendConnectionError as exc:
                logger.warning(
                    "scheduler_handover_marker_failed",
                    **_error_fields(exc),
                )
            # Transition state BEFORE release — closes the window
            # where poll_loop runs as leader while the lock is gone.
            self._state = "standby"
            self._stop_event.set()
            try:
                await self._lock_manager.release(self._lock_token)
            except BackendConnectionError as exc:
                logger.warning(
                    "scheduler_lock_release_failed", **_error_fields(exc)
                )

        # Idempotent — the leader path above already set this; this
        # call covers the standby path where the
        # early-transition block was skipped entirely. Do not remove
        # either call; they belong to different control-flow branches.
        self._stop_event.set()
        await self._cancel_sub_tasks()
        self._state = "stopped"

    async def abort(self) -> None:
        """Crash-style teardown: cancel the election + poll sub-coroutines
        WITHOUT the graceful handover/lock-release that :meth:`stop` performs.

        For the cancel path — process crash, ``start_worker()`` task
        cancellation, ProcessManager reload — where a graceful release is
        neither possible nor desirable: a real crash leaves no chance to emit
        HANDOVER, and the leader lock simply expires by TTL so a successor
        takes over via failover. Idempotent; safe to call after ``stop`` or a
        prior ``abort`` (tasks already None/done). This is the sub-task cleanup
        tail of ``stop`` minus the leader-release block.
        """
        self._stop_event.set()
        self._state = "stopped"
        await self._cancel_sub_tasks()

    async def _cancel_sub_tasks(self) -> None:
        tasks = (
            self._election_task, self._poll_task, self._report_task,
            self._adopt_task,
        )
        for task in tasks:
            if task is not None and not task.done():
                task.cancel()
        for task in tasks:
            if task is not None:
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass
        self._election_task = None
        self._poll_task = None
        self._report_task = None
        self._adopt_task = None

    # ---- cron reports (KTD10) -------------------------------------------

    async def _report_loop(self) -> None:
        interval = self._config.scheduler_seen_interval_s
        try:
            while not self._stop_event.is_set():
                await self._sleep_until_stop(interval)
                if not self._stop_event.is_set():
                    await self._report_crons()
        except asyncio.CancelledError:
            return

    async def _report_crons(self) -> None:
        """Jeden eigenen statischen Cron melden — unabhängig von der Rolle."""
        live_window = live_window_ms(self._config.scheduler_seen_interval_s)
        try:
            for definition in self._static_crons:
                await self._backend.schedules.report_cron(
                    definition.schedule_name,
                    cron_fingerprint(definition),
                    msgspec.json.encode(definition),
                    live_window_ms=live_window,
                )
        except Exception as exc:
            # Der Worker arbeitet weiter; der nächste Takt meldet erneut.
            logger.warning("scheduler_cron_report_failed", **_error_fields(exc))

    async def _adopt_loop(self) -> None:
        """Leader-Takt der Übernahme (KTD10): sofort nach dem Erwerb der
        Leader-Rolle, danach alle ``scheduler_seen_interval_s``."""
        interval = self._config.scheduler_seen_interval_s
        try:
            while not self._stop_event.is_set():
                try:
                    await asyncio.wait_for(self._adopt_now.wait(), timeout=interval)
                except TimeoutError:
                    pass
                self._adopt_now.clear()
                if self._state == "leader":
                    try:
                        await self.adopt_reported_crons()
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        logger.warning(
                            "scheduler_cron_adopt_failed", **_error_fields(exc)
                        )
        except asyncio.CancelledError:
            return

    # ---- election loop --------------------------------------------------

    async def _election_loop(self) -> None:
        interval = self._config.scheduler_heartbeat_interval
        try:
            while not self._stop_event.is_set():
                try:
                    await self._election_tick()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning(
                        "scheduler_election_tick_error", **_error_fields(exc)
                    )
                await self._sleep_until_stop(interval)
        except asyncio.CancelledError:
            return

    async def _election_tick(self) -> None:
        if self._state == "leader":
            try:
                still_leader = await self._lock_manager.renew(
                    self._lock_token,
                    int(self._config.scheduler_lock_ttl * 1000),
                )
            except BackendConnectionError as exc:
                # A renew ERROR must not keep the
                # leader in office indefinitely. After `limit`
                # consecutive failures (≈ one heartbeat before the lock
                # TTL is provably expired) step down proactively so the
                # old leader stops firing BEFORE a standby can take
                # over — closing the unbounded split-brain window.
                self._renew_failures += 1
                logger.warning(
                    "scheduler_renew_error",
                    **_error_fields(exc),
                    consecutive_failures=self._renew_failures,
                    step_down_at=self._renew_failure_limit,
                )
                if self._renew_failures >= self._renew_failure_limit:
                    logger.warning(
                        "scheduler_renew_failures_stepdown",
                        consecutive_failures=self._renew_failures,
                    )
                    self._renew_failures = 0
                    self._state = "standby"
                    self._prev_leader_seen = True
                    await self._emit(Signal.SCHEDULER_STANDBY)
                return
            self._renew_failures = 0
            if still_leader:
                await self._emit(Signal.SCHEDULER_HEARTBEAT)
            else:
                logger.warning("scheduler_lock_lost")
                self._state = "standby"
                self._prev_leader_seen = True
                # Revive SCHEDULER_STANDBY (declared but never emitted):
                # a genuine leader->standby transition, the natural point
                # for it. Emitted once per transition, not per tick, so it
                # carries signal without noise.
                await self._emit(Signal.SCHEDULER_STANDBY)
            return

        # state == "standby"
        try:
            acquired = (
                await self._lock_manager.acquire(
                    SCHEDULER_LOCK_NAME,
                    owner=self._owner_token,
                    ttl_ms=int(self._config.scheduler_lock_ttl * 1000),
                    max_concurrency=1,  # mutex
                )
                is not None
            )
        except BackendConnectionError as exc:
            logger.warning("scheduler_acquire_error", **_error_fields(exc))
            return

        if not acquired:
            self._prev_leader_seen = True
            return

        # Acquired the lock — decide which signal to emit.
        try:
            had_handover = (
                await self._backend.schedules.take_handover_marker()
            )
        except BackendConnectionError as exc:
            logger.warning(
                "scheduler_handover_marker_read_failed", **_error_fields(exc)
            )
            had_handover = False

        self._state = "leader"
        self._renew_failures = 0  # fresh term, fresh renew budget
        if had_handover or not self._prev_leader_seen:
            await self._emit(Signal.SCHEDULER_LEADER)
        else:
            await self._emit(Signal.SCHEDULER_LEADER_FAILOVER)

        # KTD10: die Cron-Meldungen aller Worker sofort übernehmen, nicht erst
        # nach einem Meldetakt.
        self._adopt_now.set()

    # ---- poll loop ------------------------------------------------------

    async def _poll_loop(self) -> None:
        interval = self._config.scheduler_poll_interval
        try:
            while not self._stop_event.is_set():
                if self._state == "leader":
                    try:
                        await self._poll_tick()
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        logger.warning(
                            "scheduler_poll_tick_error", **_error_fields(exc)
                        )
                await self._sleep_until_stop(interval)
        except asyncio.CancelledError:
            return

    async def _poll_tick(self) -> None:
        now_ms = int(time.time() * 1000)
        try:
            due = await self._store.poll_due(now_ms, limit=100)
        except BackendConnectionError as exc:
            logger.warning("scheduler_poll_due_failed", **_error_fields(exc))
            return

        for schedule_name, score_ms, definition in due:
            if self._state != "leader":
                # Lost leadership mid-batch — abort remaining items so
                # the new leader handles them (Unit 10 'lock lost
                # mid-batch' scenario).
                return
            if definition.disabled:
                continue
            await self._fire_one(schedule_name, score_ms, definition)

    async def _fire_one(
        self,
        schedule_name: str,
        score_ms: int,
        definition: ScheduleDefinition,
    ) -> None:
        # Use the authoritative zset score, not definition.fire_ts_ms
        # (Unit 7). The two can diverge briefly during a static-
        # cron update where update_static_schedule writes a new
        # fire_ts_ms into defs before the ZADD lands, or if an operator
        # hand-edits the zset. The dedup key must match what will
        # actually be published so a redelivered message hits the
        # same SETNX cell.
        schedfire_key = make_schedfire_key(schedule_name, score_ms)

        now_ms = int(time.time() * 1000)
        next_fire_ts_ms = await self._plan_next_fire(
            schedule_name, score_ms, definition, now_ms,
        )
        if next_fire_ts_ms is None:
            return

        # Build the task message body — same shape as Client.schedule.
        task_id = str(uuid.uuid7())
        body = encode_json({
            "task": {
                "id": task_id,
                "queue": definition.queue,
                "name": definition.task_name,
            },
            "args": list(definition.args),
            "kwargs": definition.kwargs,
        })

        # --- R3.5 Unit 9: pre-publish SETNX for static crons, header
        # propagation for client-originated deferred (cli_unique) ---
        # Stamp HEADER_ENQUEUED_AT so R4.4 max_age
        # works for scheduler-fired tasks (cron + delay + eta). Without
        # this the worker's _check_max_age_expired saw no header and
        # treated every scheduled message as non-expiring.
        headers: dict[str, Any] = {
            "x-swarmq-schedfire": schedfire_key,
            "x-swarmq-enqueued-at": int(time.time()),
        }
        dup_detected = False

        if definition.source == "cli_unique" and definition.dedup_hash:
            # Client already SETNX'd this task at schedule(delay=/eta=) time.
            # Propagate the hash + intended mode so the worker releases
            # at the right point. unique_until is persisted on the
            # definition so post-deploy TaskInfo
            # changes cannot silently flip release semantics.
            headers["x-swarmq-unique-hash"] = definition.dedup_hash
            headers["x-swarmq-unique-until"] = definition.unique_until
        else:
            # Static cron or plain CLI — resolve TaskInfo and SETNX here.
            task_info = self._resolve_task_info(definition.task_name)
            if task_info is None and schedule_name not in self._reported_names:
                # A schedule whose task was renamed or
                # removed previously published into the void with no
                # trace. Warn-only, deliberately NOT a skip: in multi-
                # module deployments the task may live in ANOTHER
                # worker's registry — a hard skip would break those.
                # Crons a live worker reports (U12, KTD10) belong to such
                # a registry and therefore do not warn.
                logger.warning(
                    "scheduler_task_not_in_local_registry",
                    schedule_name=schedule_name,
                    task_name=definition.task_name,
                    hint=(
                        "the leader's registry does not know this task; "
                        "if no other worker fleet registers it, the "
                        "message lands unconsumed — remove the schedule "
                        "or fix the task name"
                    ),
                )
            if task_info is not None and task_info.unique_for is not None:
                dedup_hash = compute_dedup_hash(
                    definition.task_name,
                    tuple(definition.args),
                    dict(definition.kwargs),
                )
                try:
                    existing = await self._backend.claim_unique(
                        dedup_hash, task_id, task_info.unique_for * 1000,
                        op_timeout=self._config.backend_op_timeout,
                    )
                except BackendConnectionError as exc:
                    logger.warning(
                        "scheduler_unique_setnx_failed",
                        schedule_name=schedule_name,
                        **_error_fields(exc),
                    )
                    return  # Retry on next poll; commit not called.

                if existing is not None:
                    await self._emit_duplicate_detected(
                        existing_task_id=existing,
                        task_name=definition.task_name,
                        dedup_hash=dedup_hash,
                    )
                    logger.info(
                        "dedup_detected",
                        existing_task_id=existing,
                        task_name=definition.task_name,
                        dedup_hash=dedup_hash,
                        source="scheduler.fire",
                    )
                    dup_detected = True
                else:
                    headers["x-swarmq-unique-hash"] = dedup_hash
                    headers["x-swarmq-unique-until"] = task_info.unique_until

        if not dup_detected:
            try:
                await self._broker.publish(
                    definition.queue, body, headers=headers,
                )
            except Exception as exc:
                logger.warning(
                    "scheduler_publish_failed",
                    schedule_name=schedule_name,
                    **_error_fields(exc),
                )
                return  # Do NOT commit — retry on the next poll tick.

        # The commit runs even on dup-skip: commit_fire MUST advance the
        # zset (otherwise the same schedule re-fires on every poll tick
        # forever — see plan Unit 9 Risks "Double-SETNX-Race"
        # commit-on-fail rationale).
        updated_def = await self._advanced_definition(
            schedule_name, score_ms, definition, next_fire_ts_ms, now_ms,
        )

        # fired_ts_ms: the commit only lands while the zset still holds the
        # score we fired. A `schedule remove` (disable/permanent) or a
        # re-plan that raced this fire keeps its change instead of being
        # overwritten with the pre-poll definition (0.9.0).
        try:
            committed = await self._store.commit_fire(
                schedule_name,
                next_fire_ts_ms,
                updated_def,
                self._owner_token,
                fired_ts_ms=score_ms,
            )
        except BackendConnectionError as exc:
            logger.warning(
                "scheduler_commit_failed",
                schedule_name=schedule_name,
                **_error_fields(exc),
                dup_detected=dup_detected,
            )
            # On dup: next poll will re-observe the same score and re-emit
            # the dup signal. Self-heals when Valkey recovers — signal
            # noise is the accepted v1 trade-off for operator simplicity.
            return

        if not committed:
            # Owner-check rejected — we lost the lock between publish and
            # commit. The new leader will handle the next poll.
            logger.warning(
                "scheduler_commit_owner_mismatch",
                schedule_name=schedule_name,
            )
            self._state = "standby"

    async def _advanced_definition(
        self,
        schedule_name: str,
        score_ms: int,
        definition: ScheduleDefinition,
        next_fire_ts_ms: int,
        now_ms: int,
    ) -> ScheduleDefinition | None:
        """Definition to commit after a fire (``None`` removes a one-shot);
        reports cron occurrences lost to catch-up-skip."""
        if definition.cron is None:
            return None
        # Catch-up-skip was correct but invisible —
        # a skipped daily report went unnoticed. Count the cron
        # occurrences that fell between the due score and now (they
        # will never fire) and surface them as a signal.
        skipped_count = self._count_skipped_fires(
            definition.cron, definition.timezone, score_ms, now_ms,
        )
        if skipped_count > 0:
            logger.warning(
                "scheduler_fires_skipped",
                schedule_name=schedule_name,
                skipped_count=skipped_count,
            )
            await self._emit(
                Signal.SCHEDULER_FIRE_SKIPPED,
                schedule_name=schedule_name,
                skipped_count=skipped_count,
            )
        return msgspec.structs.replace(
            definition, fire_ts_ms=next_fire_ts_ms, last_fire_at=now_ms,
        )

    async def _plan_next_fire(
        self,
        schedule_name: str,
        score_ms: int,
        definition: ScheduleDefinition,
        now_ms: int,
    ) -> int | None:
        """Next fire for a cron, ``-1`` for one-shots — computed BEFORE the
        publish (KTD12, R18). A definition whose next fire cannot be
        computed in this leader (unknown zone, broken expression) is logged
        and leaves the fire index (``-1``, definition kept); ``None`` tells
        the caller to skip it without blocking the rest of the batch.

        Cron semantics: expressions are evaluated in the definition's
        timezone (None = UTC) against the leader clock, with the DST rule
        of ``cron_time`` (Plan 2026-10-08-1541, KTD2). Catch-up after
        downtime is SKIP: the next fire after "now" yields exactly one
        catch-up fire, never a backlog storm.
        """
        if definition.cron is None:
            return -1
        try:
            return next_fire_ms(definition.cron, definition.timezone, now_ms)
        except Exception as exc:
            logger.error(
                "scheduler_next_fire_failed",
                schedule_name=schedule_name,
                cron=definition.cron,
                timezone=definition.timezone,
                **_error_fields(exc),
            )
        # Without a next score the entry would be due again on every tick.
        await self._store.commit_fire(
            schedule_name, -1, definition, self._owner_token,
            fired_ts_ms=score_ms,
        )
        return None

    def _resolve_task_info(self, task_name: str) -> TaskInfo | None:
        """Find the registered TaskInfo for *task_name* (R3.5 Unit 9).

        Scheduler has no direct registry reference — it walks the global
        ``Task._registered_tasks`` list. Linear scan is O(n); with n typically < 100 tasks this
        is well below the per-fire ScheduleStore + broker round-trip
        cost, so no index is introduced.
        """
        for task_class in Task._registered_tasks:
            ti: TaskInfo | None = getattr(task_class, "task_info", None)
            if ti is not None and ti.name == task_name:
                return ti
        return None

    async def _emit_duplicate_detected(
        self,
        *,
        existing_task_id: str,
        task_name: str,
        dedup_hash: str,
    ) -> None:
        """Dispatch TASK_DUPLICATE_DETECTED with source='scheduler.fire'.

        Wrapped here because :meth:`_emit` is specialised for
        scheduler-state signals (accepts only ``worker_id``). Handler
        exceptions are swallowed via the same SignalDispatcher contract.
        """
        try:
            await self._signals.dispatch(
                Signal.TASK_DUPLICATE_DETECTED,
                existing_task_id=existing_task_id,
                task_name=task_name,
                dedup_hash=dedup_hash,
                source="scheduler.fire",
            )
        except Exception as exc:
            logger.warning(
                "scheduler_duplicate_signal_dispatch_error",
                task_name=task_name,
                **_error_fields(exc),
            )

    # ---- static-cron sync (Fork 1) -------------------------------------


    # ---- helpers --------------------------------------------------------

    async def _sleep_until_stop(self, seconds: float) -> None:
        """Sleep for *seconds*, but short-circuit on stop."""
        try:
            await asyncio.wait_for(
                self._stop_event.wait(), timeout=seconds
            )
        except TimeoutError:
            return

    async def _emit(self, signal: Signal, **extra: Any) -> None:
        try:
            await self._signals.dispatch(
                signal, worker_id=self._worker_id, **extra
            )
        except Exception as exc:
            logger.warning(
                "scheduler_signal_dispatch_error",
                signal=signal.value,
                **_error_fields(exc),
            )

    @staticmethod
    def _count_skipped_fires(
        cron: str, timezone: str | None, score_ms: int, now_ms: int,
    ) -> int:
        """Count cron occurrences in ``(score_ms, now_ms]`` — fires that
        catch-up-skip will never run.

        The due score itself is the fire we ARE running; everything
        between it and ``now_ms`` is lost to the skip semantics. Iteration
        is capped at 1000 — a week-long outage on a minutely cron does
        not need an exact count to alert on. Occurrences are computed in
        the definition's *timezone* with the same DST rule as the score.
        """
        skipped = 0
        for fire_ms in iter_fire_ms(cron, timezone, score_ms):
            if fire_ms > now_ms or skipped >= 1000:
                break
            skipped += 1
        return skipped
