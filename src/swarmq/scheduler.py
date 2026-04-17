"""SwarmQ embedded scheduler — ScheduleDefinition, ScheduleStore, SchedulerLoop.

The scheduler owns two Valkey datastructures:

- ``sq:sched:defs`` — global hash keyed by schedule_name holding the
  JSON-encoded :class:`ScheduleDefinition` (inline ``disabled`` flag,
  inline ``source``, etc. — see plan Persistence-Layout).
- ``sq:sched:zset`` — sorted-set where score is the next fire-ts_ms and
  member is the schedule_name. Poll reads a batch (sched_poll.lua),
  the leader publishes + commits (sched_commit.lua with owner-check).

Three top-level pieces:

- :class:`ScheduleDefinition` — msgspec.Struct persisted per schedule.
- :class:`ScheduleStore` — CRUD + poll/commit wrapper around the
  two Valkey keys, holding the registered Lua scripts.
- :class:`SchedulerLoop` — embedded-in-each-worker coroutine. Two
  sub-loops: ``_election_loop`` drives leader-election + heartbeat,
  ``_poll_loop`` drives the publish-before-ZREM firing. Only the
  leader runs the poll body; standbys short-circuit.
"""

from __future__ import annotations

import asyncio
import os
import socket
import time
import uuid
from datetime import UTC, datetime
from typing import Any, Literal

import msgspec
import structlog
import valkey.asyncio as valkey_asyncio
from croniter import croniter

from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.config import SwarmQConfig
from swarmq.exceptions import BackendConnectionError
from swarmq.locking import LockManager
from swarmq.lua import load_script
from swarmq.serialization import encode_json
from swarmq.signals import Signal, SignalDispatcher
from swarmq.task import Task

logger = structlog.get_logger()


SCHEDULER_LOCK_NAME = "scheduler:leader"
"""Logical lock name — LockManager's automatic ``sq:lock:`` prefix
yields the final Valkey key ``sq:lock:scheduler:leader``."""

SCHEDULER_LOCK_KEY = "sq:lock:scheduler:leader"
"""Final Valkey key for the leader lock. Single source of truth for
the owner-check in sched_commit.lua and the CLI status view. Must match
``locking._LOCK_KEY_PREFIX + SCHEDULER_LOCK_NAME``."""

_HANDOVER_MARKER_TTL_MS_FLOOR = 5_000
"""Minimum TTL for the handover marker (5s). The actual TTL used is
``max(floor, heartbeat_interval*2*1000)`` so a slow standby still
observes the marker on its next election tick — otherwise every
clean handover would time out before the successor polls and emit
FAILOVER instead of LEADER (review-finding reliability R-001)."""


# ---------------------------------------------------------------------------
# Schedule metadata
# ---------------------------------------------------------------------------


class ScheduleDefinition(msgspec.Struct, kw_only=True, frozen=True):
    """Persisted schedule metadata.

    Stored JSON-encoded in ``sq:sched:defs`` under the schedule_name.
    Three source types produce a definition:

    * ``source="static"`` — declared via ``TaskInfo(cron=...)`` in code.
      The leader-sync may overwrite it on redeploy when the cron-string
      has changed (Fork 1).
    * ``source="cli"`` — added via ``swarmq schedule add``. Treated as
      insert-if-absent; ``TaskInfo``-sync will never overwrite a CLI
      schedule (CD11).
    * One-shot entries (delay/eta) — ``cron=None``, ``schedule_name``
      is ``"oneshot:<uuid>"``, ``source="cli"``. Discriminated by
      ``cron is None`` at commit-time: next_fire_ts = -1 signals
      one-shot, which skips ZADD and triggers HDEL.
    """

    schedule_name: str
    task_name: str
    args: tuple
    kwargs: dict
    queue: str
    cron: str | None
    fire_ts_ms: int | None
    source: Literal["static", "cli"]
    disabled: bool = False
    last_fire_at: int | None = None


# ---------------------------------------------------------------------------
# Schedfire-key (consumer-side dedup identity)
# ---------------------------------------------------------------------------


def make_schedfire_key(schedule_name: str, fire_ts_ms: int) -> str:
    """Deterministic key for a single scheduled fire event.

    Published as the ``x-swarmq-schedfire`` message header and used as the
    SETNX identity for consumer-side dedup.
    """
    return f"{schedule_name}:{fire_ts_ms}"


def parse_schedfire_key(value: str) -> tuple[str, int]:
    """Inverse of :func:`make_schedfire_key`.

    Uses ``rsplit(":", 1)`` so that schedule_names containing colons
    (e.g. ``oneshot:<uuid>``) round-trip correctly (P1-4).
    """
    if ":" not in value:
        raise ValueError(f"schedfire key missing ':' separator: {value!r}")
    name, ts = value.rsplit(":", 1)
    try:
        return name, int(ts)
    except ValueError as exc:
        raise ValueError(
            f"schedfire key has non-integer timestamp: {value!r}"
        ) from exc


# ---------------------------------------------------------------------------
# ScheduleStore — the CRUD + poll/commit wrapper
# ---------------------------------------------------------------------------


class ScheduleStore:
    """Valkey-facing surface for scheduler persistence.

    Constructed after the backend connects (mirrors ``LockManager`` /
    ``RateLimiter`` lifecycle). Holds two registered Lua scripts
    (sched_poll, sched_commit) so the NOSCRIPT-recovery loop inside
    ``valkey.asyncio.AsyncScript`` is the recovery path — no
    reimplementation here.

    Insert semantics encode the plan's Fork-1 decision:

    - :meth:`add_schedule` is insert-if-absent (CD11). CLI writes and
      static-cron-first-insert both go through this path.
    - :meth:`update_static_schedule` unconditionally overwrites; the
      leader sync loop calls this when it detects a static cron-string
      change between code and stored def.
    """

    def __init__(self, backend: ValkeyBackend) -> None:
        self._backend = backend
        client = backend._client
        if client is None:
            raise RuntimeError(
                "ScheduleStore requires a connected ValkeyBackend "
                "(backend.connect() must have been awaited first)"
            )
        self._client: valkey_asyncio.Valkey = client
        self._poll_script = client.register_script(load_script("sched_poll"))
        self._commit_script = client.register_script(
            load_script("sched_commit")
        )

    # ---- CRUD -----------------------------------------------------------

    async def add_schedule(self, definition: ScheduleDefinition) -> bool:
        """Insert a new schedule. Returns True on insert, False on no-op.

        Insert-if-absent (CD11): when an entry already exists under
        ``definition.schedule_name``, this method does nothing. Static
        cron updates go through :meth:`update_static_schedule` (Fork 1).
        """
        existing = await self._backend.hget_schedule_def(
            definition.schedule_name
        )
        if existing is not None:
            return False
        payload = msgspec.json.encode(definition)
        await self._backend.hset_schedule_def(
            definition.schedule_name, payload
        )
        if definition.fire_ts_ms is not None:
            await self._backend.zadd_schedule(
                definition.schedule_name, definition.fire_ts_ms
            )
        return True

    async def update_static_schedule(
        self, definition: ScheduleDefinition
    ) -> None:
        """Unconditionally write a static-sourced definition (Fork 1).

        The caller is responsible for verifying that the existing entry
        was ``source="static"`` and that its cron-string actually
        changed. CLI-sourced entries never go through this path.
        """
        payload = msgspec.json.encode(definition)
        await self._backend.hset_schedule_def(
            definition.schedule_name, payload
        )
        if definition.fire_ts_ms is not None:
            await self._backend.zadd_schedule(
                definition.schedule_name, definition.fire_ts_ms
            )

    async def remove_schedule(self, schedule_name: str) -> None:
        """Permanently remove a schedule (ZREM + HDEL)."""
        await self._backend.zrem_schedule(schedule_name)
        await self._backend.hdel_schedule_def(schedule_name)

    async def disable_schedule(self, schedule_name: str) -> None:
        """Mark an existing schedule disabled AND remove it from the
        due-set so the poll loop no longer touches it.

        Missing schedule is a no-op. Insert-if-absent (CD11) then
        prevents a later static-sync from resurrecting the entry
        because the disabled flag stays in the def JSON. The zset
        removal (review-finding correctness C-02) stops the poll
        loop from repeatedly pulling the disabled entry every tick
        and burning HGET bandwidth for work it cannot do.
        """
        existing = await self._backend.hget_schedule_def(schedule_name)
        if existing is None:
            return
        current = msgspec.json.decode(existing, type=ScheduleDefinition)
        updated = msgspec.structs.replace(current, disabled=True)
        await self._backend.hset_schedule_def(
            schedule_name, msgspec.json.encode(updated)
        )
        await self._backend.zrem_schedule(schedule_name)

    async def list_schedules(self) -> list[ScheduleDefinition]:
        raw = await self._backend.hgetall_schedule_defs()
        return [
            msgspec.json.decode(payload, type=ScheduleDefinition)
            for payload in raw.values()
        ]

    # ---- Poll / commit --------------------------------------------------

    async def poll_due(
        self, now_ms: int, limit: int = 100
    ) -> list[tuple[str, int, ScheduleDefinition]]:
        """Return schedules whose fire-ts is <= ``now_ms``.

        Runs sched_poll.lua which is read-only. The return list is
        decoded into (schedule_name, score_ms, ScheduleDefinition) for
        the leader's publish loop. Orphan zset entries (def missing)
        are skipped inside the Lua script.
        """
        try:
            rows = await self._poll_script(
                keys=[
                    self._backend._sched_zset_key(),
                    self._backend._sched_defs_key(),
                ],
                args=[now_ms, limit],
            )
        except BackendConnectionError:
            raise
        except Exception as exc:  # noqa: BLE001 — wrap at boundary
            raise BackendConnectionError(
                f"sched_poll failed: {exc}"
            ) from exc
        result: list[tuple[str, int, ScheduleDefinition]] = []
        for row in rows:
            member_raw, score_raw, def_raw = row
            name = _decode_bytes(member_raw)
            score = int(_decode_bytes(score_raw))
            definition = msgspec.json.decode(def_raw, type=ScheduleDefinition)
            result.append((name, score, definition))
        return result

    async def commit_fire(
        self,
        schedule_name: str,
        next_fire_ts_ms: int,
        updated_def: ScheduleDefinition | None,
        owner_token: str,
    ) -> bool:
        """Atomically commit a fire event.

        ``next_fire_ts_ms=-1`` + ``updated_def=None`` signals a one-shot
        that should be removed entirely (zset-rem + defs-del). Returns
        True when the script confirmed the commit, False when the
        owner-check rejected it (stale leader, P1-2).
        """
        def_payload = (
            msgspec.json.encode(updated_def) if updated_def is not None else b""
        )
        try:
            result = await self._commit_script(
                keys=[
                    self._backend._sched_zset_key(),
                    self._backend._sched_defs_key(),
                    SCHEDULER_LOCK_KEY,
                ],
                args=[
                    schedule_name,
                    next_fire_ts_ms,
                    def_payload,
                    owner_token,
                ],
            )
        except BackendConnectionError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise BackendConnectionError(
                f"sched_commit failed for {schedule_name!r}: {exc}"
            ) from exc
        return int(result) == 1


def _decode_bytes(value: bytes | str) -> str:
    return value.decode() if isinstance(value, bytes) else value


# ---------------------------------------------------------------------------
# SchedulerLoop — leader-election + poll (Unit 10)
# ---------------------------------------------------------------------------


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


class SchedulerLoop:
    """Embedded scheduler — one instance per worker.

    Runs two cooperating coroutines:

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

    :meth:`stop` is the graceful path: emits HANDOVER, writes a
    short-TTL marker under the outgoing token, releases the lock, and
    cancels both sub-coroutines. A non-graceful exit (crash/SIGKILL)
    leaves no marker, and the next standby to acquire observes the
    missing marker and emits FAILOVER.
    """

    def __init__(
        self,
        *,
        backend: ValkeyBackend,
        broker: RabbitMQBroker,
        config: SwarmQConfig,
        lock_manager: LockManager,
        store: ScheduleStore,
        signal_dispatcher: SignalDispatcher,
        owner_token: str | None = None,
        worker_id: str | None = None,
    ) -> None:
        self._backend = backend
        self._broker = broker
        self._config = config
        self._lock_manager = lock_manager
        self._store = store
        self._signals = signal_dispatcher
        self._owner_token = owner_token or _make_owner_token()
        self._worker_id = worker_id or self._owner_token

        self._state: _SchedulerState = "standby"
        self._prev_leader_seen = False
        """Set to True when election sees the lock held by *someone*
        (acquire returned False). Used together with the
        handover-marker check to distinguish first-ever start from
        TTL-expiry failover."""

        self._election_task: asyncio.Task[None] | None = None
        self._poll_task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()

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
        self._election_task = asyncio.create_task(self._election_loop())
        self._poll_task = asyncio.create_task(self._poll_loop())

    async def stop(self) -> None:
        """Graceful shutdown. Releases the leader lock if held, sets
        the handover marker so the next acquirer emits LEADER instead
        of FAILOVER, then cancels both sub-coroutines.

        Ordering (review-finding R-005): state transition and
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
                await self._backend.set_schedfire_handover_marker(
                    self._owner_token, marker_ttl_ms
                )
            except BackendConnectionError as exc:
                logger.warning(
                    "scheduler_handover_marker_failed",
                    error=str(exc),
                )
            # Transition state BEFORE release — closes the window
            # where poll_loop runs as leader while the lock is gone.
            self._state = "standby"
            self._stop_event.set()
            try:
                await self._lock_manager.release(
                    SCHEDULER_LOCK_NAME, self._owner_token, 1
                )
            except BackendConnectionError as exc:
                logger.warning(
                    "scheduler_lock_release_failed", error=str(exc)
                )

        # Idempotent — the leader path above already set this (R-005
        # ordering fix); this call covers the standby path where the
        # early-transition block was skipped entirely. Do not remove
        # either call; they belong to different control-flow branches.
        self._stop_event.set()
        for task in (self._election_task, self._poll_task):
            if task is not None and not task.done():
                task.cancel()
        for task in (self._election_task, self._poll_task):
            if task is not None:
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass
        self._election_task = None
        self._poll_task = None
        self._state = "stopped"

    # ---- election loop --------------------------------------------------

    async def _election_loop(self) -> None:
        interval = self._config.scheduler_heartbeat_interval
        try:
            while not self._stop_event.is_set():
                try:
                    await self._election_tick()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "scheduler_election_tick_error", error=str(exc)
                    )
                await self._sleep_until_stop(interval)
        except asyncio.CancelledError:
            return

    async def _election_tick(self) -> None:
        if self._state == "leader":
            try:
                still_leader = await self._lock_manager.renew(
                    SCHEDULER_LOCK_NAME,
                    self._owner_token,
                    int(self._config.scheduler_lock_ttl * 1000),
                )
            except BackendConnectionError as exc:
                logger.warning("scheduler_renew_error", error=str(exc))
                return
            if still_leader:
                await self._emit(Signal.SCHEDULER_HEARTBEAT)
            else:
                logger.warning("scheduler_lock_lost")
                self._state = "standby"
                self._prev_leader_seen = True
            return

        # state == "standby"
        try:
            acquired = await self._lock_manager.acquire(
                SCHEDULER_LOCK_NAME,
                self._owner_token,
                int(self._config.scheduler_lock_ttl * 1000),
                1,  # mutex
            )
        except BackendConnectionError as exc:
            logger.warning("scheduler_acquire_error", error=str(exc))
            return

        if not acquired:
            self._prev_leader_seen = True
            return

        # Acquired the lock — decide which signal to emit.
        try:
            had_handover = (
                await self._backend.get_and_del_schedfire_handover_marker()
            )
        except BackendConnectionError as exc:
            logger.warning(
                "scheduler_handover_marker_read_failed", error=str(exc)
            )
            had_handover = False

        self._state = "leader"
        if had_handover or not self._prev_leader_seen:
            await self._emit(Signal.SCHEDULER_LEADER)
        else:
            await self._emit(Signal.SCHEDULER_LEADER_FAILOVER)

        # Sync static TaskInfo(cron=...) declarations (Fork 1).
        try:
            await self.sync_taskinfo_crons()
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "scheduler_sync_taskinfo_crons_failed", error=str(exc)
            )

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
                    except Exception as exc:  # noqa: BLE001
                        logger.warning(
                            "scheduler_poll_tick_error", error=str(exc)
                        )
                await self._sleep_until_stop(interval)
        except asyncio.CancelledError:
            return

    async def _poll_tick(self) -> None:
        now_ms = int(time.time() * 1000)
        try:
            due = await self._store.poll_due(now_ms, limit=100)
        except BackendConnectionError as exc:
            logger.warning("scheduler_poll_due_failed", error=str(exc))
            return

        for schedule_name, _score_ms, definition in due:
            if self._state != "leader":
                # Lost leadership mid-batch — abort remaining items so
                # the new leader handles them (Review-Finding Unit 10
                # 'lock lost mid-batch' scenario).
                return
            if definition.disabled:
                continue
            await self._fire_one(schedule_name, definition)

    async def _fire_one(
        self, schedule_name: str, definition: ScheduleDefinition
    ) -> None:
        fire_ts_ms = definition.fire_ts_ms
        if fire_ts_ms is None:
            # Defensive — every entry in zset must have a score.
            logger.warning(
                "scheduler_entry_without_fire_ts",
                schedule_name=schedule_name,
            )
            return

        schedfire_key = make_schedfire_key(schedule_name, fire_ts_ms)

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

        try:
            await self._broker.publish(
                definition.queue,
                body,
                headers={"x-swarmq-schedfire": schedfire_key},
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "scheduler_publish_failed",
                schedule_name=schedule_name,
                error=str(exc),
            )
            return  # Do NOT commit — retry on the next poll tick.

        # Compute next_fire for cron, -1 for one-shots.
        if definition.cron is None:
            next_fire_ts_ms = -1
            updated_def: ScheduleDefinition | None = None
        else:
            now = datetime.now(UTC)
            next_dt = croniter(definition.cron, now).get_next(datetime)
            next_fire_ts_ms = int(next_dt.timestamp() * 1000)
            updated_def = msgspec.structs.replace(
                definition,
                fire_ts_ms=next_fire_ts_ms,
                last_fire_at=int(time.time() * 1000),
            )

        try:
            committed = await self._store.commit_fire(
                schedule_name,
                next_fire_ts_ms,
                updated_def,
                self._owner_token,
            )
        except BackendConnectionError as exc:
            logger.warning(
                "scheduler_commit_failed",
                schedule_name=schedule_name,
                error=str(exc),
            )
            return

        if not committed:
            # Owner-check rejected — we lost the lock between publish and
            # commit. The new leader will handle the next poll.
            logger.warning(
                "scheduler_commit_owner_mismatch",
                schedule_name=schedule_name,
            )
            self._state = "standby"

    # ---- static-cron sync (Fork 1) -------------------------------------

    async def sync_taskinfo_crons(self) -> None:
        """Reconcile static ``TaskInfo(cron=...)`` declarations into the
        ``sq:sched:defs`` hash.

        Six resolution branches (in check order):

        1. ``hget_schedule_def`` raises ``BackendConnectionError`` →
           log and skip this schedule, continue iteration.
        2. No existing entry → ``add_schedule`` (insert).
        3. Existing entry fails msgspec decode → best-effort
           ``update_static_schedule`` overwrite with the valid
           definition from code.
        4. Existing ``source="cli"`` entry → no-op with warning log
           (CD11 — CLI ownership beats code, but the shadow is
           surfaced so operators can discover it).
        5. Existing ``source="static"`` entry with the same
           cron-string → no-op (stable).
        6. Existing ``source="static"`` entry with a **different**
           cron-string → ``update_static_schedule`` (Fork 1). The
           existing ``disabled`` flag is preserved so an operator's
           ``schedule remove`` is not silently reversed
           (correctness C-01).
        """
        for task_class in Task._registered_tasks:
            task_info = getattr(task_class, "task_info", None)
            if task_info is None or not task_info.cron:
                continue

            schedule_name = task_info.name
            queue = task_info.queues[0]
            now = datetime.now(UTC)
            next_dt = croniter(task_info.cron, now).get_next(datetime)
            fire_ts_ms = int(next_dt.timestamp() * 1000)

            definition = ScheduleDefinition(
                schedule_name=schedule_name,
                task_name=task_info.name,
                args=(),
                kwargs={},
                queue=queue,
                cron=task_info.cron,
                fire_ts_ms=fire_ts_ms,
                source="static",
            )

            try:
                existing_raw = await self._backend.hget_schedule_def(
                    schedule_name
                )
            except BackendConnectionError as exc:
                logger.warning(
                    "scheduler_sync_hget_failed",
                    schedule_name=schedule_name,
                    error=str(exc),
                )
                continue

            if existing_raw is None:
                await self._store.add_schedule(definition)
                continue

            try:
                existing = msgspec.json.decode(
                    existing_raw, type=ScheduleDefinition
                )
            except msgspec.DecodeError:
                # Malformed entry — best-effort overwrite with the
                # valid one from code.
                await self._store.update_static_schedule(definition)
                continue

            if existing.source == "cli":
                # CD11 protection: CLI ownership beats code. Log so
                # the silent shadow is discoverable (reliability R-006).
                logger.warning(
                    "scheduler_static_cron_shadowed_by_cli",
                    schedule_name=schedule_name,
                    static_cron=task_info.cron,
                    cli_cron=existing.cron,
                )
                continue
            if existing.cron == task_info.cron:
                continue  # Unchanged static
            # Static cron changed — propagate (Fork 1).
            # Preserve the disabled flag from the existing entry so
            # an operator's `schedule remove` intent is not silently
            # reversed by the next redeploy (correctness C-01).
            propagated = msgspec.structs.replace(
                definition, disabled=existing.disabled
            )
            logger.info(
                "scheduler_static_cron_updated",
                schedule_name=schedule_name,
                old_cron=existing.cron,
                new_cron=task_info.cron,
                disabled_preserved=existing.disabled,
            )
            await self._store.update_static_schedule(propagated)

    # ---- helpers --------------------------------------------------------

    async def _sleep_until_stop(self, seconds: float) -> None:
        """Sleep for *seconds*, but short-circuit on stop."""
        try:
            await asyncio.wait_for(
                self._stop_event.wait(), timeout=seconds
            )
        except TimeoutError:
            return

    async def _emit(self, signal: Signal) -> None:
        try:
            await self._signals.dispatch(
                signal, worker_id=self._worker_id
            )
        except Exception as exc:  # noqa: BLE001 — never abort on handler
            logger.warning(
                "scheduler_signal_dispatch_error",
                signal=signal.value,
                error=str(exc),
            )
