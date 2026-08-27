"""Schedule-Definitionen (Plan U11) — verbatim aus ``scheduler.py``:
``ScheduleDefinition`` (msgspec-Kodierung), Schedfire-Keys und die
definitions-typisierte ``ScheduleStore``-Klasse, die seit U18 an den
Bytes-Vertrag ``backend.schedules`` delegiert (KTD4, Entscheidung U18)."""

from __future__ import annotations

from typing import Any, Literal

import msgspec
import structlog

from swarmq.exceptions import BackendConnectionError
from swarmq.locking import SCHEDULER_LOCK_KEY, SCHEDULER_LOCK_NAME, lock_key  # noqa: F401  # Re-Export (KTD10)

logger = structlog.get_logger()



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
    * ``source="cli_unique"`` — one-shot deferred schedule
      (``Client.schedule(delay=|eta=)``) whose producer already claimed
      the R3.5 dedup flag at enqueue-time. Scheduler._fire_one skips
      the pre-publish SETNX for this source and reuses
      ``dedup_hash`` directly as the message header, partitioning
      SETNX responsibility between client (deferred) and scheduler
      (static cron). Origin plan Unit 7 / Risks "Double-SETNX-Race".
    * One-shot entries (delay/eta) — ``cron=None``, ``schedule_name``
      is ``"oneshot:<uuid>"``, ``source="cli"`` or ``"cli_unique"``.
      Discriminated by ``cron is None`` at commit-time: next_fire_ts =
      -1 signals one-shot, which skips ZADD and triggers HDEL.
    """

    schedule_name: str
    task_name: str
    args: tuple
    kwargs: dict
    queue: str
    cron: str | None
    fire_ts_ms: int | None
    source: Literal["static", "cli", "cli_unique"]
    disabled: bool = False
    last_fire_at: int | None = None
    # R3.5 Unit 7: persisted dedup hash for source="cli_unique" entries.
    # Optional with default None so msgpack/json roundtrip stays
    # backwards-compatible with pre-R3.5 ScheduleDefinition entries in
    # sq:sched:defs (new field at the end of the struct).
    dedup_hash: str | None = None
    # R3.5 ce-review P1 #5 / ADV-007: persist unique_until alongside
    # dedup_hash. Previously the scheduler re-resolved unique_until from
    # TaskInfo at fire-time, which silently flipped release semantics when
    # TaskInfo changed between enqueue and fire. The client knows the
    # intended mode at enqueue time; persist it so fire-time is
    # deterministic. Optional default "completion" matches the producer
    # default used when the Client sees _MISSING.
    unique_until: Literal["start", "completion"] = "completion"


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


class ScheduleStore:
    """Definitions-Sicht der Scheduler-Persistenz.

    Seit Plan U18 liegt die Valkey-Seite (Lua-Skripte, Keys, Marker) in
    ``ValkeyBackend.schedules`` (``ValkeyScheduleStore``, Vertrag
    ``ScheduleStore`` auf ``bytes``); diese Klasse kodiert/dekodiert die
    ``ScheduleDefinition`` (msgspec) und delegiert. Konstruiert nach dem
    ``connect()`` des Backends — davor wirft ``backend.schedules``
    ``BackendConnectionError``.

    Insert semantics encode the plan's Fork-1 decision:

    - :meth:`add_schedule` is insert-if-absent (CD11). CLI writes and
      static-cron-first-insert both go through this path.
    - :meth:`update_static_schedule` unconditionally overwrites; the
      leader sync loop calls this when it detects a static cron-string
      change between code and stored def.
    """

    def __init__(self, backend: Any) -> None:
        # ``Any``: erwartet die verbundene Backend-Fassade (Property
        # ``schedules`` → ``contracts.ScheduleStore``); ein eigener
        # Fassaden-Vertrag existiert nicht (Reinheits-Guard, KTD7).
        self._backend = backend
        try:
            self._store = backend.schedules
        except BackendConnectionError as exc:
            raise RuntimeError(
                "ScheduleStore requires a connected ValkeyBackend (backend.connect() must have been awaited first)"
            ) from exc

    # ---- CRUD -----------------------------------------------------------

    async def add_schedule(self, definition: ScheduleDefinition) -> bool:
        """Insert a new schedule. Returns True on insert, False on no-op
        (insert-if-absent via HSETNX, Unit 8 / M-14)."""
        return await self._store.add(definition.schedule_name, msgspec.json.encode(definition), definition.fire_ts_ms)

    async def update_static_schedule(self, definition: ScheduleDefinition) -> None:
        """Unconditionally write a static-sourced definition (Fork 1)."""
        await self._store.put(definition.schedule_name, msgspec.json.encode(definition), definition.fire_ts_ms)

    async def remove_schedule(self, schedule_name: str) -> bool:
        """Permanently remove a schedule (ZREM + HDEL). True only when an
        entry existed (Unit 4 / M-2)."""
        return await self._store.remove(schedule_name)

    async def disable_schedule(self, schedule_name: str) -> bool:
        """Mark an existing schedule disabled AND remove it from the
        due-set. False when no entry was present OR the existing entry was
        malformed (logged + parked for operator inspection, Unit 5 / M-3).
        The write-pair runs atomically in ``sched_disable.lua`` (Unit 8 /
        M-14); the HGET+decode stays in Python because Lua does not share our
        msgspec encoding."""
        existing = await self._store.get(schedule_name)
        if existing is None:
            return False
        try:
            current = msgspec.json.decode(existing, type=ScheduleDefinition)
        except msgspec.DecodeError as exc:
            logger.warning(
                "schedule_def_malformed_skipped",
                schedule_name=schedule_name,
                error=str(exc),
                hint="run 'swarmq schedule remove --permanent <name>' to clear the malformed entry",
            )
            return False
        updated = msgspec.structs.replace(current, disabled=True)
        return await self._store.disable(schedule_name, msgspec.json.encode(updated))

    async def list_schedules(self) -> list[ScheduleDefinition]:
        """Every decodable ScheduleDefinition; malformed entries are logged
        and skipped so one bad row cannot break ``swarmq schedule list``
        (Unit 5 / M-5)."""
        raw = await self._store.list_all()
        result: list[ScheduleDefinition] = []
        for name, payload in raw.items():
            try:
                result.append(msgspec.json.decode(payload, type=ScheduleDefinition))
            except msgspec.DecodeError as exc:
                logger.warning("schedule_def_malformed_skipped", schedule_name=name, error=str(exc))
        return result

    # ---- Poll / commit --------------------------------------------------

    async def poll_due(self, now_ms: int, limit: int = 100) -> list[tuple[str, int, ScheduleDefinition]]:
        """Schedules whose fire-ts is <= ``now_ms`` as
        ``(schedule_name, score_ms, ScheduleDefinition)``."""
        rows = await self._store.poll_due(now_ms, limit=limit)
        return [(name, score, msgspec.json.decode(payload, type=ScheduleDefinition)) for name, score, payload in rows]

    async def commit_fire(
        self,
        schedule_name: str,
        next_fire_ts_ms: int,
        updated_def: ScheduleDefinition | None,
        owner_token: str,
    ) -> bool:
        """Atomically commit a fire event. ``next_fire_ts_ms=-1`` +
        ``updated_def=None`` removes a one-shot entirely. False when the
        owner-check rejected it (stale leader, P1-2)."""
        return await self._store.commit_fire(
            schedule_name,
            next_fire_ts_ms=None if next_fire_ts_ms < 0 else next_fire_ts_ms,
            payload=msgspec.json.encode(updated_def) if updated_def is not None else None,
            owner=owner_token,
        )


def _decode_bytes(value: bytes | str) -> str:
    return value.decode() if isinstance(value, bytes) else value
