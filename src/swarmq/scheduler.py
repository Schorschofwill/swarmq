"""SwarmQ embedded scheduler — ScheduleDefinition, ScheduleStore helpers.

The scheduler owns two Valkey datastructures:

- ``sq:sched:defs`` — global hash keyed by schedule_name holding the
  JSON-encoded :class:`ScheduleDefinition` (inline ``disabled`` flag,
  inline ``source``, etc. — see plan Persistence-Layout).
- ``sq:sched:zset`` — sorted-set where score is the next fire-ts_ms and
  member is the schedule_name. Poll reads a batch (sched_poll.lua),
  the leader publishes + commits (sched_commit.lua with owner-check).

This module provides the Python surface: msgspec-encoded definitions,
insert-if-absent vs. static-cron-update semantics (CD11 + Fork 1), and
schedfire-key (de)serialization for the consumer-side dedup header.
``SchedulerLoop`` itself (election + poll + heartbeat) lives further
down in Unit 10.
"""

from __future__ import annotations

from typing import Literal

import msgspec
import structlog
import valkey.asyncio as valkey_asyncio

from swarmq.backend.valkey import ValkeyBackend
from swarmq.exceptions import BackendConnectionError
from swarmq.lua import load_script

logger = structlog.get_logger()


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
        """Mark an existing schedule disabled without removing it.

        Missing schedule is a no-op. Insert-if-absent (CD11) then
        prevents a later static-sync from resurrecting the entry
        because the disabled flag stays in the def JSON.
        """
        existing = await self._backend.hget_schedule_def(schedule_name)
        if existing is None:
            return
        current = msgspec.json.decode(existing, type=ScheduleDefinition)
        updated = msgspec.structs.replace(current, disabled=True)
        await self._backend.hset_schedule_def(
            schedule_name, msgspec.json.encode(updated)
        )

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
                    # Leader lock key — must match LockManager's prefix
                    # ("sq:lock:") + the scheduler's lock name ("scheduler:leader")
                    "sq:lock:scheduler:leader",
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
