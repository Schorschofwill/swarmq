"""Valkey-Backend (Paket seit Plan U17).

``ValkeyBackend`` erfüllt ``swarmq.contracts.ResultBackend``; der Importpfad
``swarmq.backend.valkey.ValkeyBackend`` bleibt gültig. Die Fassade
re-exportiert weder ``valkey`` noch ``logger`` — Tests patchen die Submodule
(``swarmq.backend.valkey.connection.valkey.asyncio.Valkey.from_url``).

Die Schedule-/Schedfire-Primitive bleiben in diesem Schritt hier und ziehen
in U18 nach ``schedules.py`` (KTD4).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from swarmq.backend.valkey.results import ValkeyResults
from swarmq.exceptions import BackendConnectionError


class ValkeyBackend(ValkeyResults):
    # ---- Scheduler key helpers (Unit 5) ------------------------------------
    # Härtung R2 P1-7: the {sched} hash tag co-locates zset, defs and
    # the leader lock (sq:lock:{sched}:leader) in ONE Valkey-Cluster
    # hash slot — sched_poll.lua/sched_commit.lua touch them in a
    # single EVAL and would otherwise die with CROSSSLOT on a sharded
    # cluster. Single-key markers (fired/handover/unique) deliberately
    # carry no tag: they never share an EVAL with another key, and
    # spreading them across slots is preferable.
    # BREAKING (pre-1.0): existing sq:sched:* keys are not migrated —
    # re-create schedules after upgrading (or RENAME the keys).
    def _sched_zset_key(self) -> str:
        return "sq:{sched}:zset"

    def _sched_defs_key(self) -> str:
        return "sq:{sched}:defs"

    def _sched_fired_key(self, schedfire_key: str) -> str:
        return f"sq:sched:fired:{schedfire_key}"

    def _sched_handover_key(self, owner_token: str) -> str:
        return f"sq:sched:handover:{owner_token}"

    def _sched_inflight_key(self, schedfire_key: str) -> str:
        """Separate namespace from the completed marker so the two
        TTLs can coexist without collision. A task in-flight has
        ``sq:sched:inflight:<key>`` set; once completed, that expires
        naturally and ``sq:sched:fired:<key>`` takes over as the
        long-lived block."""
        return f"sq:sched:inflight:{schedfire_key}"

    # -----------------------------------------------------------------------
    # Scheduler persistence operations (Unit 5) — ziehen in U18 nach schedules.py
    # -----------------------------------------------------------------------

    async def zadd_schedule(self, schedule_name: str, fire_ts_ms: int) -> None:
        """Insert / update the fire-time of a schedule in the sorted-set."""
        await self._run("zadd_schedule", self.client.zadd(self._sched_zset_key(), {schedule_name: fire_ts_ms}))

    async def zrem_schedule(self, schedule_name: str) -> None:
        """Drop a schedule from the zset. No-op when it is not present."""
        await self._run("zrem_schedule", self.client.zrem(self._sched_zset_key(), schedule_name))

    async def zrangebyscore_schedules(self, max_score_ms: int, limit: int) -> list[bytes]:
        """Return schedule names whose fire-ts is <= *max_score_ms*.
        Read-only; the Lua commit-script is the only mutation path."""
        result: list[bytes] = await self._run(
            "zrangebyscore_schedules",
            self.client.zrangebyscore(self._sched_zset_key(), "-inf", max_score_ms, start=0, num=limit),
        )
        return result

    async def hset_schedule_def(self, schedule_name: str, def_json: bytes) -> None:
        await self._run("hset_schedule_def", self.client.hset(self._sched_defs_key(), schedule_name, def_json))  # type: ignore[arg-type]  # bytes ok

    async def hsetnx_schedule_def(self, schedule_name: str, def_json: bytes) -> bool:
        """Atomic insert-if-absent. Returns True when the field was
        newly created (Unit 8 / M-14)."""
        result = await self._run(
            "hsetnx_schedule_def", self.client.hsetnx(self._sched_defs_key(), schedule_name, def_json)  # type: ignore[arg-type]
        )
        return int(result) == 1

    async def hget_schedule_def(self, schedule_name: str) -> bytes | None:
        result: bytes | None = await self._run(
            "hget_schedule_def", self.client.hget(self._sched_defs_key(), schedule_name)
        )
        return result

    async def hdel_schedule_def(self, schedule_name: str) -> None:
        await self._run("hdel_schedule_def", self.client.hdel(self._sched_defs_key(), schedule_name))

    async def hgetall_schedule_defs(self) -> dict[bytes, bytes]:
        result: dict[bytes, bytes] = await self._run(
            "hgetall_schedule_defs", self.client.hgetall(self._sched_defs_key())
        )
        return result

    async def set_schedfire_dedup(self, schedfire_key: str, ttl_ms: int) -> bool:
        """Consumer-side "completed" dedup marker (long TTL, written AFTER the
        task finished). True when this was the first write."""
        result = await self._run(
            "set_schedfire_dedup", self.client.set(self._sched_fired_key(schedfire_key), "1", nx=True, px=ttl_ms)
        )
        return bool(result)

    async def set_schedfire_inflight(self, schedfire_key: str, ttl_ms: int) -> bool:
        """Claim the in-flight slot for a scheduled-fire message (short TTL).
        True when this caller is the first to claim the slot."""
        result = await self._run(
            "set_schedfire_inflight", self.client.set(self._sched_inflight_key(schedfire_key), "1", nx=True, px=ttl_ms)
        )
        return bool(result)

    async def clear_schedfire_inflight(self, schedfire_key: str) -> None:
        """Release the in-flight claim WITHOUT writing the completed marker
        (P0-B, 2026-06-02 audit): the fire is being re-delivered."""
        await self._run("clear_schedfire_inflight", self.client.delete(self._sched_inflight_key(schedfire_key)))

    async def is_schedfire_completed(self, schedfire_key: str) -> bool:
        result = await self._run("is_schedfire_completed", self.client.exists(self._sched_fired_key(schedfire_key)))
        return bool(result == 1)

    async def set_schedfire_handover_marker(self, owner_token: str, ttl_ms: int) -> None:
        """Leader marker set right before graceful release (Review P2-1)."""
        await self._run(
            "set_schedfire_handover_marker", self.client.set(self._sched_handover_key(owner_token), "1", px=ttl_ms)
        )

    async def get_and_del_schedfire_handover_marker(self) -> bool:
        """Consume any handover marker set by a previous leader."""
        found_any = False
        try:
            async for key in self.client.scan_iter(match="sq:sched:handover:*", count=100):
                await self.client.delete(key)
                found_any = True
        except BackendConnectionError:
            raise
        except Exception as exc:
            raise BackendConnectionError(str(exc)) from exc
        return found_any

    async def get_scheduler_leader(self) -> str | None:
        """Return the current scheduler-lock holder, or ``None`` if unlocked.

        Does NOT swallow connection errors (Decision E4): callers can
        distinguish "no leader elected" (``None``) from "backend unreachable"
        (``ValkeyConnectionError`` / ``ValkeyTimeoutError`` propagates).
        """
        from swarmq.scheduler import SCHEDULER_LOCK_KEY

        raw = await self.client.get(SCHEDULER_LOCK_KEY)
        if raw is None:
            return None
        # ``errors='replace'`` keeps ``schedule status`` working even if a
        # rogue writer stored non-UTF8 bytes under SCHEDULER_LOCK_KEY.
        result = raw.decode(errors="replace") if isinstance(raw, bytes) else str(raw)
        # Treat empty string the same as "no leader".
        return result if result else None


__all__ = ["ValkeyBackend"]

if TYPE_CHECKING:  # Konformitäts-Anker (KTD1)
    from swarmq.contracts import ResultBackend

    _conforms_backend: type[ResultBackend] = ValkeyBackend
