"""Valkey-Schedules (Plan U18, KTD4): ``sched_poll/commit/disable.lua`` und
die Schedule-/Schedfire-Primitive der bisherigen Fassade.

``ValkeyScheduleStore`` erfüllt den Vertrag ``ScheduleStore`` auf ``bytes``
plus Fire-Zeitpunkt; die msgspec-Kodierung der ``ScheduleDefinition`` bleibt
in ``swarmq.scheduler`` (dort delegiert ``ScheduleStore`` hierher).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

import valkey.asyncio as valkey_asyncio
from valkey.exceptions import ValkeyError

from swarmq.exceptions import BackendConnectionError
from swarmq.locking import SCHEDULER_LOCK_KEY
from swarmq.lua import load_script

# The {sched} hash tag co-locates zset, defs and the leader
# lock (sq:lock:{sched}:leader) in ONE Valkey-Cluster hash slot —
# sched_poll.lua/sched_commit.lua touch them in a single EVAL and would
# otherwise die with CROSSSLOT on a sharded cluster. Single-key markers
# (fired/inflight/handover) deliberately carry no tag.
ZSET_KEY = "sq:{sched}:zset"
DEFS_KEY = "sq:{sched}:defs"


def fired_key(schedfire_key: str) -> str:
    return f"sq:sched:fired:{schedfire_key}"


def inflight_key(schedfire_key: str) -> str:
    """Separate namespace from the completed marker so the two TTLs can
    coexist without collision."""
    return f"sq:sched:inflight:{schedfire_key}"


def handover_key(owner_token: str) -> str:
    return f"sq:sched:handover:{owner_token}"


def _decode(value: bytes | str) -> str:
    return value.decode() if isinstance(value, bytes) else value


class ValkeyScheduleStore:
    def __init__(self, client: valkey_asyncio.Valkey) -> None:
        self._client = client
        self._poll_script = client.register_script(load_script("sched_poll"))
        self._commit_script = client.register_script(load_script("sched_commit"))
        self._disable_script = client.register_script(load_script("sched_disable"))

    async def _run(self, what: str, coro: Any) -> Any:
        try:
            return await coro
        except BackendConnectionError:
            raise
        except (ValkeyError, OSError) as exc:
            raise BackendConnectionError(str(exc)) from exc
        except Exception as exc:
            raise BackendConnectionError(f"{what} failed: {exc}") from exc

    # ------------------------------------------------------------------
    # Vertrag ``ScheduleStore``
    # ------------------------------------------------------------------

    async def add(self, name: str, payload: bytes, fire_ts_ms: int | None) -> bool:
        """Insert-if-absent via HSETNX (Unit 8: kein TOCTOU zwischen
        HGET und HSET); danach der Fire-Zeitpunkt im Index."""
        if not await self.hsetnx_schedule_def(name, payload):
            return False
        if fire_ts_ms is not None:
            await self.zadd_schedule(name, fire_ts_ms)
        return True

    async def put(self, name: str, payload: bytes, fire_ts_ms: int | None) -> None:
        await self.hset_schedule_def(name, payload)
        if fire_ts_ms is not None:
            await self.zadd_schedule(name, fire_ts_ms)

    async def exists(self, name: str) -> bool:
        result = await self._run("hexists", self._client.hexists(DEFS_KEY, name))
        return bool(result)

    async def remove(self, name: str) -> bool:
        """ZREM + HDEL; ``True`` nur, wenn ein Eintrag existierte (Unit 4)."""
        existed = await self.exists(name)
        await self.zrem_schedule(name)
        await self.hdel_schedule_def(name)
        return existed

    async def get(self, name: str) -> bytes | None:
        return await self.hget_schedule_def(name)

    async def list_all(self) -> Mapping[str, bytes]:
        raw = await self.hgetall_schedule_defs()
        return {_decode(k): v for k, v in raw.items()}

    async def disable(self, name: str, payload: bytes) -> bool:
        """``sched_disable.lua``: HEXISTS-Guard, dann HSET + ZREM atomar."""
        result = await self._run(
            f"sched_disable for {name!r}", self._disable_script(keys=[DEFS_KEY, ZSET_KEY], args=[name, payload])
        )
        return int(result) == 1

    async def poll_due(self, now_ms: int, *, limit: int = 100) -> list[tuple[str, int, bytes]]:
        """``sched_poll.lua`` (read-only): ``(name, fire_ts_ms, payload)`` der
        fälligen Einträge; Waisen ohne Definition überspringt das Skript."""
        rows = await self._run("sched_poll", self._poll_script(keys=[ZSET_KEY, DEFS_KEY], args=[now_ms, limit]))
        result: list[tuple[str, int, bytes]] = []
        for member_raw, score_raw, def_raw in rows:
            result.append((_decode(member_raw), int(_decode(score_raw)), def_raw))
        return result

    async def commit_fire(
        self,
        name: str,
        *,
        next_fire_ts_ms: int | None,
        payload: bytes | None,
        owner: str,
        fired_ts_ms: int | None = None,
    ) -> bool:
        """``sched_commit.lua``: Owner-Check gegen den Leader-Lock im selben
        EVAL; ``next_fire_ts_ms=None`` + ``payload=None`` entfernt einen
        One-Shot. ``False``, wenn der Owner-Check ablehnt (stale leader).
        Mit ``fired_ts_ms`` verwirft das Skript den Commit (Ergebnis 2,
        hier ``True``), wenn der Fire-Index-Score inzwischen ein anderer ist."""
        args: list[Any] = [
            name,
            -1 if next_fire_ts_ms is None else next_fire_ts_ms,
            payload if payload is not None else b"",
            owner,
        ]
        if fired_ts_ms is not None:
            args.append(fired_ts_ms)
        result = await self._run(
            f"sched_commit for {name!r}",
            self._commit_script(keys=[ZSET_KEY, DEFS_KEY, SCHEDULER_LOCK_KEY], args=args),
        )
        return int(result) in (1, 2)

    async def mark_fire_completed(self, fire_key: str, ttl_ms: int) -> bool:
        return await self.set_schedfire_dedup(fire_key, ttl_ms)

    async def claim_fire(self, fire_key: str, ttl_ms: int) -> bool:
        return await self.set_schedfire_inflight(fire_key, ttl_ms)

    async def release_fire_claim(self, fire_key: str) -> None:
        await self.clear_schedfire_inflight(fire_key)

    async def is_fire_completed(self, fire_key: str) -> bool:
        return await self.is_schedfire_completed(fire_key)

    async def set_handover_marker(self, owner: str, ttl_ms: int) -> None:
        await self.set_schedfire_handover_marker(owner, ttl_ms)

    async def take_handover_marker(self) -> bool:
        return await self.get_and_del_schedfire_handover_marker()

    # ------------------------------------------------------------------
    # Primitive (heutige Fassaden-Namen; die Fassade delegiert hierher)
    # ------------------------------------------------------------------

    async def zadd_schedule(self, schedule_name: str, fire_ts_ms: int) -> None:
        await self._run("zadd_schedule", self._client.zadd(ZSET_KEY, {schedule_name: fire_ts_ms}))

    async def zrem_schedule(self, schedule_name: str) -> None:
        await self._run("zrem_schedule", self._client.zrem(ZSET_KEY, schedule_name))

    async def zrangebyscore_schedules(self, max_score_ms: int, limit: int) -> list[bytes]:
        result: list[bytes] = await self._run(
            "zrangebyscore_schedules", self._client.zrangebyscore(ZSET_KEY, "-inf", max_score_ms, start=0, num=limit)
        )
        return result

    async def hset_schedule_def(self, schedule_name: str, def_json: bytes) -> None:
        await self._run("hset_schedule_def", self._client.hset(DEFS_KEY, schedule_name, def_json))  # type: ignore[arg-type]

    async def hsetnx_schedule_def(self, schedule_name: str, def_json: bytes) -> bool:
        result = await self._run("hsetnx_schedule_def", self._client.hsetnx(DEFS_KEY, schedule_name, def_json))  # type: ignore[arg-type]
        return int(result) == 1

    async def hget_schedule_def(self, schedule_name: str) -> bytes | None:
        result: bytes | None = await self._run("hget_schedule_def", self._client.hget(DEFS_KEY, schedule_name))
        return result

    async def hdel_schedule_def(self, schedule_name: str) -> None:
        await self._run("hdel_schedule_def", self._client.hdel(DEFS_KEY, schedule_name))

    async def hgetall_schedule_defs(self) -> dict[bytes, bytes]:
        result: dict[bytes, bytes] = await self._run("hgetall_schedule_defs", self._client.hgetall(DEFS_KEY))
        return result

    async def set_schedfire_dedup(self, schedfire_key: str, ttl_ms: int) -> bool:
        """Consumer-side "completed" marker (long TTL, written AFTER the task
        finished; Unit 1). True when this was the first write."""
        result = await self._run(
            "set_schedfire_dedup", self._client.set(fired_key(schedfire_key), "1", nx=True, px=ttl_ms)
        )
        return bool(result)

    async def set_schedfire_inflight(self, schedfire_key: str, ttl_ms: int) -> bool:
        """Pre-run claim with a short TTL; a crashed claimant lets the next
        redelivery re-run (Unit 1)."""
        result = await self._run(
            "set_schedfire_inflight", self._client.set(inflight_key(schedfire_key), "1", nx=True, px=ttl_ms)
        )
        return bool(result)

    async def clear_schedfire_inflight(self, schedfire_key: str) -> None:
        """Release the claim WITHOUT the completed marker:
        the fire is being re-delivered, so the long-TTL marker must not make
        the redelivered copy get ack+skipped."""
        await self._run("clear_schedfire_inflight", self._client.delete(inflight_key(schedfire_key)))

    async def is_schedfire_completed(self, schedfire_key: str) -> bool:
        result = await self._run("is_schedfire_completed", self._client.exists(fired_key(schedfire_key)))
        return bool(result == 1)

    async def set_schedfire_handover_marker(self, owner_token: str, ttl_ms: int) -> None:
        """Leader marker set right before graceful release."""
        await self._run("set_schedfire_handover_marker", self._client.set(handover_key(owner_token), "1", px=ttl_ms))

    async def get_and_del_schedfire_handover_marker(self) -> bool:
        """Consume any handover marker set by a previous leader (scan + delete)."""
        found_any = False
        try:
            async for key in self._client.scan_iter(match="sq:sched:handover:*", count=100):
                await self._client.delete(key)
                found_any = True
        except (ValkeyError, OSError) as exc:
            raise BackendConnectionError(str(exc)) from exc
        return found_any


if TYPE_CHECKING:  # Konformitäts-Anker (KTD1)
    from swarmq.contracts import ScheduleStore

    _conforms_schedules: type[ScheduleStore] = ValkeyScheduleStore
