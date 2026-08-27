"""Valkey-Backend (Paket seit Plan U17; Koordinationsverträge seit U18).

``ValkeyBackend`` erfüllt ``swarmq.contracts.ResultBackend`` und bietet die
Koordinationsobjekte als nicht-optionale Properties ``.locks``,
``.rate_limit``, ``.chords``, ``.schedules`` (KTD4) — vor ``connect()``
werfen sie ``BackendConnectionError``. Der Importpfad
``swarmq.backend.valkey.ValkeyBackend`` bleibt gültig; die Fassade
re-exportiert weder ``valkey`` noch ``logger`` (Tests patchen die Submodule).

Die heutigen Schedule-/Schedfire-Methodennamen bleiben als Aliase, die an
``.schedules`` delegieren (KTD10; entfallen in U15).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from swarmq.backend.valkey.chords import ChordManager
from swarmq.backend.valkey.locks import ValkeyLockStore
from swarmq.backend.valkey.rate_limit import ValkeyRateLimitStore
from swarmq.backend.valkey.results import ValkeyResults
from swarmq.backend.valkey.schedules import (
    DEFS_KEY,
    ZSET_KEY,
    ValkeyScheduleStore,
    fired_key,
    handover_key,
    inflight_key,
)
from swarmq.locking import SCHEDULER_LOCK_NAME


class ValkeyBackend(ValkeyResults):
    _locks: ValkeyLockStore | None = None
    _rate_limit: ValkeyRateLimitStore | None = None
    _chords: ChordManager | None = None
    _schedules: ValkeyScheduleStore | None = None

    # ------------------------------------------------------------------
    # Koordinationsobjekte (KTD4)
    # ------------------------------------------------------------------

    @property
    def locks(self) -> ValkeyLockStore:
        if self._locks is None:
            self._locks = ValkeyLockStore(self.client)
        return self._locks

    @property
    def rate_limit(self) -> ValkeyRateLimitStore:
        if self._rate_limit is None:
            self._rate_limit = ValkeyRateLimitStore(self.client)
        return self._rate_limit

    @property
    def chords(self) -> ChordManager:
        if self._chords is None:
            self._chords = ChordManager(self.client)
        return self._chords

    @property
    def schedules(self) -> ValkeyScheduleStore:
        if self._schedules is None:
            self._schedules = ValkeyScheduleStore(self.client)
        return self._schedules

    async def close(self) -> None:
        await super().close()
        # Die Stores halten den alten Client; nach close() neu aufbauen.
        self._locks = self._rate_limit = self._chords = self._schedules = None

    # ------------------------------------------------------------------
    # Aliase der heutigen Namen (KTD10; entfallen in U15)
    # ------------------------------------------------------------------

    def _sched_zset_key(self) -> str:
        return ZSET_KEY

    def _sched_defs_key(self) -> str:
        return DEFS_KEY

    def _sched_fired_key(self, schedfire_key: str) -> str:
        return fired_key(schedfire_key)

    def _sched_inflight_key(self, schedfire_key: str) -> str:
        return inflight_key(schedfire_key)

    def _sched_handover_key(self, owner_token: str) -> str:
        return handover_key(owner_token)

    async def zadd_schedule(self, schedule_name: str, fire_ts_ms: int) -> None:
        await self.schedules.zadd_schedule(schedule_name, fire_ts_ms)

    async def zrem_schedule(self, schedule_name: str) -> None:
        await self.schedules.zrem_schedule(schedule_name)

    async def zrangebyscore_schedules(self, max_score_ms: int, limit: int) -> list[bytes]:
        return await self.schedules.zrangebyscore_schedules(max_score_ms, limit)

    async def hset_schedule_def(self, schedule_name: str, def_json: bytes) -> None:
        await self.schedules.hset_schedule_def(schedule_name, def_json)

    async def hsetnx_schedule_def(self, schedule_name: str, def_json: bytes) -> bool:
        return await self.schedules.hsetnx_schedule_def(schedule_name, def_json)

    async def hget_schedule_def(self, schedule_name: str) -> bytes | None:
        return await self.schedules.hget_schedule_def(schedule_name)

    async def hdel_schedule_def(self, schedule_name: str) -> None:
        await self.schedules.hdel_schedule_def(schedule_name)

    async def hgetall_schedule_defs(self) -> dict[bytes, bytes]:
        return await self.schedules.hgetall_schedule_defs()

    async def set_schedfire_dedup(self, schedfire_key: str, ttl_ms: int) -> bool:
        return await self.schedules.set_schedfire_dedup(schedfire_key, ttl_ms)

    async def set_schedfire_inflight(self, schedfire_key: str, ttl_ms: int) -> bool:
        return await self.schedules.set_schedfire_inflight(schedfire_key, ttl_ms)

    async def clear_schedfire_inflight(self, schedfire_key: str) -> None:
        await self.schedules.clear_schedfire_inflight(schedfire_key)

    async def is_schedfire_completed(self, schedfire_key: str) -> bool:
        return await self.schedules.is_schedfire_completed(schedfire_key)

    async def set_schedfire_handover_marker(self, owner_token: str, ttl_ms: int) -> None:
        await self.schedules.set_schedfire_handover_marker(owner_token, ttl_ms)

    async def get_and_del_schedfire_handover_marker(self) -> bool:
        return await self.schedules.get_and_del_schedfire_handover_marker()

    async def get_scheduler_leader(self) -> str | None:
        """Return the current scheduler-lock holder, or ``None`` if unlocked.
        Does NOT swallow connection errors (Decision E4)."""
        return await self.locks.current_owner(SCHEDULER_LOCK_NAME)


__all__ = ["ValkeyBackend"]

if TYPE_CHECKING:  # Konformitäts-Anker (KTD1)
    from swarmq.contracts import ResultBackend

    _conforms_backend: type[ResultBackend] = ValkeyBackend
