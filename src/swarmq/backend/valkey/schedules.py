"""Valkey-Schedules (Plan U18, KTD4): ``sched_poll/commit/disable/replan.lua`` und
die Schedule-/Schedfire-Primitive der bisherigen Fassade.

``ValkeyScheduleStore`` erfüllt den Vertrag ``ScheduleStore`` auf ``bytes``
plus Fire-Zeitpunkt; die msgspec-Kodierung der ``ScheduleDefinition`` bleibt
in ``swarmq.scheduler`` (dort delegiert ``ScheduleStore`` hierher).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

import valkey.asyncio as valkey_asyncio
from valkey.exceptions import ValkeyError

from swarmq.backend.valkey.connection import as_backend_error
from swarmq.contracts.coordination import CronReport, CronReports, LeaderInfo
from swarmq.exceptions import BackendConnectionError
from swarmq.keyspace import KeySpace
from swarmq.lua import load_script

_DEFAULT_KEYS = KeySpace()

# The {sched} hash tag co-locates zset, defs and the leader
# lock (sq:lock:{sched}:leader) in ONE Valkey-Cluster hash slot —
# sched_poll.lua/sched_commit.lua touch them in a single EVAL and would
# otherwise die with CROSSSLOT on a sharded cluster. Single-key markers
# (fired/inflight/handover) deliberately carry no tag.
# Die Namen bildet ``swarmq.keyspace`` (Plan 0.12.0, Q4); die Konstanten
# bleiben als Aliase der Default-Keys.
ZSET_KEY = _DEFAULT_KEYS.sched_zset()
DEFS_KEY = _DEFAULT_KEYS.sched_defs()
# Cron-Meldungen der Worker (Plan 2026-10-08-1541, KTD10): drei Hashes mit
# demselben Hash-Tag, Feld jeweils ``name|fingerprint``.
REPORT_FIRST_SEEN_KEY = _DEFAULT_KEYS.sched_report_first_seen()
REPORT_LAST_SEEN_KEY = _DEFAULT_KEYS.sched_report_last_seen()
REPORT_DEF_KEY = _DEFAULT_KEYS.sched_report_def()
# Version des Leaders neben dem Leader-Lock (KTD11), Wert ``owner|version``
# mit der Lock-TTL; 0.9.0-Leader schreiben ihn nie.
LEADER_VERSION_KEY = _DEFAULT_KEYS.sched_leader_version()
# Versionen der meldenden Worker (KTD11): ein String-Key je Worker mit der
# Lebendigkeits-TTL der Cron-Meldungen; abgelaufene verschwinden von selbst.
WORKER_VERSION_PREFIX = _DEFAULT_KEYS.sched_worker_version_prefix
# Beim Aufraeumen deaktivierte Crons (KTD8): ein Set mit den Namen; das
# Wiederanlegen nach einer neuen Meldung uebernimmt ``disabled``.
PRUNED_DISABLED_KEY = _DEFAULT_KEYS.sched_disabled()


def fired_key(schedfire_key: str) -> str:
    return _DEFAULT_KEYS.sched_fired(schedfire_key)


def inflight_key(schedfire_key: str) -> str:
    """Separate namespace from the completed marker so the two TTLs can
    coexist without collision."""
    return _DEFAULT_KEYS.sched_inflight(schedfire_key)


def handover_key(owner_token: str) -> str:
    return _DEFAULT_KEYS.sched_handover(owner_token)


def _decode(value: bytes | str) -> str:
    return value.decode() if isinstance(value, bytes) else value


class ValkeyScheduleStore:
    def __init__(self, client: valkey_asyncio.Valkey, *, keys: KeySpace | None = None) -> None:
        self._client = client
        self._keys = keys if keys is not None else _DEFAULT_KEYS
        k = self._keys
        self._zset = k.sched_zset()
        self._defs = k.sched_defs()
        self._report_last_seen = k.sched_report_last_seen()
        self._report_keys = [k.sched_report_first_seen(), self._report_last_seen, k.sched_report_def()]
        self._report_known = k.sched_report_known()
        self._disabled = k.sched_disabled()
        self._leader_version = k.sched_leader_version()
        self._scheduler_lock = k.scheduler_lock()
        self._poll_script = client.register_script(load_script("sched_poll"))
        self._commit_script = client.register_script(load_script("sched_commit"))
        self._disable_script = client.register_script(load_script("sched_disable"))
        self._replan_script = client.register_script(load_script("sched_replan"))
        self._fire_claim_script = client.register_script(load_script("sched_fire_claim"))
        # Compare-and-delete mit Owner-Check: dieselbe Logik wie die
        # Unique-Freigabe (GET == Token -> DEL).
        self._owner_release_script = client.register_script(load_script("unique_release"))
        self._report_script = client.register_script(load_script("sched_report"))
        self._report_prune_script = client.register_script(load_script("sched_report_prune"))
        self._prune_script = client.register_script(load_script("sched_prune"))

    def _fired(self, schedfire_key: str) -> str:
        return self._keys.sched_fired(schedfire_key)

    def _inflight(self, schedfire_key: str) -> str:
        return self._keys.sched_inflight(schedfire_key)

    async def _run(self, what: str, coro: Any) -> Any:
        """Transportfehler -> ``BackendConnectionError``, Fehlerantworten und
        andere Fehler -> ``BackendResponseError`` (``as_backend_error``)."""
        try:
            return await coro
        except BackendConnectionError:
            raise
        except (ValkeyError, OSError) as exc:
            raise as_backend_error(str(exc), exc) from exc
        except Exception as exc:
            raise as_backend_error(f"{what} failed: {exc}", exc) from exc

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
        result = await self._run("hexists", self._client.hexists(self._defs, name))
        return bool(result)

    async def remove(self, name: str) -> bool:
        """ZREM + HDEL; ``True`` nur, wenn ein Eintrag existierte (Unit 4)."""
        existed = await self.exists(name)
        await self.zrem_schedule(name)
        await self.hdel_schedule_def(name)
        return existed

    async def forget_known_cron(self, name: str) -> None:
        await self._run(f"forget_known_cron for {name!r}", self._client.srem(self._report_known, name))

    async def get(self, name: str) -> bytes | None:
        return await self.hget_schedule_def(name)

    async def fire_ts(self, name: str) -> int | None:
        """ZSCORE im Fire-Index; ``None`` ohne Eintrag."""
        score = await self._run("fire_ts", self._client.zscore(self._zset, name))
        return None if score is None else int(score)

    async def list_all(self) -> Mapping[str, bytes]:
        raw = await self.hgetall_schedule_defs()
        return {_decode(k): v for k, v in raw.items()}

    async def replan_if_unchanged(
        self, name: str, expected: bytes, payload: bytes, fire_ts_ms: int,
    ) -> bool:
        """``sched_replan.lua``: HGET-Vergleich, Score-Prüfung, HSET + ZADD
        in einem EVAL (Review #4)."""
        result = await self._run(
            f"sched_replan for {name!r}",
            self._replan_script(keys=[self._defs, self._zset], args=[name, expected, payload, fire_ts_ms]),
        )
        return int(result) == 1

    async def disable(self, name: str, payload: bytes) -> bool:
        """``sched_disable.lua``: HEXISTS-Guard, dann HSET + ZREM atomar."""
        result = await self._run(
            f"sched_disable for {name!r}", self._disable_script(keys=[self._defs, self._zset], args=[name, payload])
        )
        return int(result) == 1

    async def poll_due(self, now_ms: int, *, limit: int = 100) -> list[tuple[str, int, bytes]]:
        """``sched_poll.lua``: ``(name, fire_ts_ms, payload)`` der fälligen
        Einträge. Waisen ohne Definition und deaktivierte Definitionen nimmt
        das Skript aus dem Fire-Index, bevor das Limit greift (BZ-28)."""
        rows = await self._run("sched_poll", self._poll_script(keys=[self._zset, self._defs], args=[now_ms, limit]))
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
        fired_payload: bytes | None = None,
    ) -> bool:
        """``sched_commit.lua``: Owner-Check gegen den Leader-Lock im selben
        EVAL; ``next_fire_ts_ms=None`` + ``payload=None`` entfernt einen
        One-Shot. ``False``, wenn der Owner-Check ablehnt (stale leader).
        Mit ``fired_ts_ms`` verwirft das Skript den Commit (Ergebnis 2,
        hier ``True``), wenn der Fire-Index-Score inzwischen ein anderer ist.
        Mit ``fired_payload`` schreibt es die Definition nur zurück, wenn sie
        seit dem Poll unverändert ist (KTD12)."""
        args: list[Any] = [
            name,
            -1 if next_fire_ts_ms is None else next_fire_ts_ms,
            payload if payload is not None else b"",
            owner,
        ]
        if fired_ts_ms is not None or fired_payload is not None:
            args.append("" if fired_ts_ms is None else fired_ts_ms)
        if fired_payload is not None:
            args.append(fired_payload)
        result = await self._run(
            f"sched_commit for {name!r}",
            self._commit_script(keys=[self._zset, self._defs, self._scheduler_lock], args=args),
        )
        return int(result) in (1, 2)

    async def mark_fire_completed(self, fire_key: str, ttl_ms: int) -> bool:
        return await self.set_schedfire_dedup(fire_key, ttl_ms)

    async def claim_fire(
        self, fire_key: str, ttl_ms: int, *, owner: str | None = None,
        takeover: str | Sequence[str] | None = None,
    ) -> bool:
        """``sched_fire_claim.lua``: frei oder Wert in ``takeover`` -> Owner
        ``owner`` mit neuer TTL, sonst ``False`` (Duplikat)."""
        offered = [takeover] if isinstance(takeover, str) else list(takeover or ())
        result = await self._run(
            "sched_fire_claim",
            self._fire_claim_script(
                keys=[self._inflight(fire_key)], args=[owner or "1", ttl_ms, *(offered or [""])],
            ),
        )
        return int(result) == 1

    async def release_fire_claim(self, fire_key: str, *, owner: str | None = None) -> None:
        if owner is None:
            await self.clear_schedfire_inflight(fire_key)
            return
        await self._run(
            "release_fire_claim", self._owner_release_script(keys=[self._inflight(fire_key)], args=[owner]),
        )

    async def is_fire_completed(self, fire_key: str) -> bool:
        return await self.is_schedfire_completed(fire_key)

    async def set_handover_marker(self, owner: str, ttl_ms: int) -> None:
        await self.set_schedfire_handover_marker(owner, ttl_ms)

    async def take_handover_marker(self, previous_owner: str | None = None) -> bool:
        """``GETDEL`` auf den festen Key, kein SCAN über die DB (BZ-07); der
        Legacy-Key des gefundenen Owners wird mit entfernt. Ein Leader vor
        0.12 schreibt nur den Legacy-Key: ihn findet ``previous_owner``."""
        owner = await self._run("take_handover_marker", self._client.getdel(self._keys.sched_handover_latest()))
        if owner is not None:
            await self._run("take_handover_marker", self._client.delete(self._keys.sched_handover(_decode(owner))))
            return True
        if previous_owner is None:
            return False
        legacy = await self._run("take_handover_marker", self._client.getdel(self._keys.sched_handover(previous_owner)))
        return legacy is not None

    async def report_cron(
        self, name: str, fingerprint: str, payload: bytes, *, live_window_ms: int,
    ) -> None:
        """``sched_report.lua``: Meldung mit Server-Zeit (``TIME``); danach der
        Name ins Set der je gemeldeten Crons (BZ-11, eigener Befehl: die
        Arität des Skripts bleibt, Q3)."""
        await self._run(
            f"sched_report for {name!r}",
            self._report_script(
                keys=self._report_keys, args=[f"{name}|{fingerprint}", payload, live_window_ms],
            ),
        )
        await self._run(f"sched_report_known for {name!r}", self._client.sadd(self._report_known, name))

    async def known_cron_names(self) -> frozenset[str]:
        raw = await self._run("known_cron_names", self._client.smembers(self._report_known))
        return frozenset(_decode(name) for name in raw)

    async def cron_reports(self) -> CronReports:
        """``TIME`` und die drei Meldungs-Hashes in einer Transaktion."""
        async with self._client.pipeline(transaction=True) as pipe:
            pipe.time()
            for key in self._report_keys:
                pipe.hgetall(key)
            (seconds, micros), first, last, defs = await self._run("cron_reports", pipe.execute())
        reports: list[CronReport] = []
        for raw_field, raw_first in first.items():
            field = _decode(raw_field)
            raw_last, payload = last.get(raw_field), defs.get(raw_field)
            if raw_last is None or payload is None:
                continue
            name, fingerprint = field.rsplit("|", 1)
            reports.append(CronReport(name, fingerprint, int(raw_first), int(raw_last), payload))
        return CronReports(now_ms=int(seconds) * 1000 + int(micros) // 1000, reports=tuple(reports))

    async def prune_cron_reports(self, *, max_age_ms: int) -> int:
        """``sched_report_prune.lua``: Prüfen und Löschen in einem Schritt."""
        result = await self._run(
            "sched_report_prune", self._report_prune_script(keys=self._report_keys, args=[max_age_ms]),
        )
        return int(result)

    async def prune_cron(
        self, name: str, expected: bytes, *, stale_after_ms: int | None, remember_disabled: bool,
    ) -> bool:
        """``sched_prune.lua``: Prüfen und Entfernen in einem Schritt (KTD8)."""
        result = await self._run(
            f"sched_prune for {name!r}",
            self._prune_script(
                keys=[self._defs, self._zset, self._report_last_seen, self._disabled],
                args=[
                    name, expected, "" if stale_after_ms is None else stale_after_ms,
                    "1" if remember_disabled else "0",
                ],
            ),
        )
        if int(result) != 1:
            return False
        # BZ-11: der Name verlaesst das Set der je gemeldeten Crons nur mit
        # der Definition (eigener Befehl, Aritaet des Skripts bleibt, Q3).
        await self._run(f"sched_prune_known for {name!r}", self._client.srem(self._report_known, name))
        return True

    async def pruned_disabled(self, name: str) -> bool:
        result = await self._run("pruned_disabled", self._client.sismember(self._disabled, name))
        return bool(result)

    async def forget_pruned_disabled(self, name: str) -> None:
        await self._run("forget_pruned_disabled", self._client.srem(self._disabled, name))

    async def report_worker_version(self, worker_id: str, version: str, *, ttl_ms: int) -> None:
        await self._run(
            "report_worker_version",
            self._client.set(self._keys.sched_worker_version(worker_id), version, px=ttl_ms),
        )

    async def worker_versions(self) -> Mapping[str, str]:
        """SCAN über die Worker-Keys (nur für ``schedule status``), dann MGET;
        zwischendurch abgelaufene Keys fehlen im Ergebnis."""
        try:
            pattern = f"{self._keys.sched_worker_version_prefix}*"
            keys = [key async for key in self._client.scan_iter(match=pattern, count=100)]
        except (ValkeyError, OSError) as exc:
            raise as_backend_error(str(exc), exc) from exc
        if not keys:
            return {}
        values = await self._run("worker_versions", self._client.mget(keys))
        return {
            _decode(key).removeprefix(self._keys.sched_worker_version_prefix): _decode(value)
            for key, value in zip(keys, values, strict=True)
            if value is not None
        }

    async def set_leader_version(self, owner: str, version: str, *, ttl_ms: int) -> None:
        await self._run(
            "set_leader_version", self._client.set(self._leader_version, f"{owner}|{version}", px=ttl_ms),
        )

    async def leader_info(self) -> LeaderInfo:
        """Lock-Owner und Versions-Key in einer Transaktion."""
        async with self._client.pipeline(transaction=True) as pipe:
            pipe.get(self._scheduler_lock)
            pipe.get(self._leader_version)
            raw_owner, raw_version = await self._run("leader_info", pipe.execute())
        owner = None if raw_owner is None else _decode(raw_owner)
        version = None
        if raw_version is not None:
            writer, _, written = _decode(raw_version).rpartition("|")
            version = written if owner is not None and writer == owner else None
        return LeaderInfo(owner=owner, version=version)

    # ------------------------------------------------------------------
    # Primitive (heutige Fassaden-Namen; die Fassade delegiert hierher)
    # ------------------------------------------------------------------

    async def zadd_schedule(self, schedule_name: str, fire_ts_ms: int) -> None:
        await self._run("zadd_schedule", self._client.zadd(self._zset, {schedule_name: fire_ts_ms}))

    async def zrem_schedule(self, schedule_name: str) -> None:
        await self._run("zrem_schedule", self._client.zrem(self._zset, schedule_name))

    async def zrangebyscore_schedules(self, max_score_ms: int, limit: int) -> list[bytes]:
        result: list[bytes] = await self._run(
            "zrangebyscore_schedules", self._client.zrangebyscore(self._zset, "-inf", max_score_ms, start=0, num=limit)
        )
        return result

    async def hset_schedule_def(self, schedule_name: str, def_json: bytes) -> None:
        await self._run("hset_schedule_def", self._client.hset(self._defs, schedule_name, def_json))  # type: ignore[arg-type]

    async def hsetnx_schedule_def(self, schedule_name: str, def_json: bytes) -> bool:
        result = await self._run("hsetnx_schedule_def", self._client.hsetnx(self._defs, schedule_name, def_json))  # type: ignore[arg-type]
        return int(result) == 1

    async def hget_schedule_def(self, schedule_name: str) -> bytes | None:
        result: bytes | None = await self._run("hget_schedule_def", self._client.hget(self._defs, schedule_name))
        return result

    async def hdel_schedule_def(self, schedule_name: str) -> None:
        await self._run("hdel_schedule_def", self._client.hdel(self._defs, schedule_name))

    async def hgetall_schedule_defs(self) -> dict[bytes, bytes]:
        result: dict[bytes, bytes] = await self._run("hgetall_schedule_defs", self._client.hgetall(self._defs))
        return result

    async def set_schedfire_dedup(self, schedfire_key: str, ttl_ms: int) -> bool:
        """Consumer-side "completed" marker (long TTL, written AFTER the task
        finished; Unit 1). True when this was the first write."""
        result = await self._run(
            "set_schedfire_dedup", self._client.set(self._fired(schedfire_key), "1", nx=True, px=ttl_ms)
        )
        return bool(result)

    async def set_schedfire_inflight(self, schedfire_key: str, ttl_ms: int) -> bool:
        """Pre-run claim with a short TTL; a crashed claimant lets the next
        redelivery re-run (Unit 1)."""
        result = await self._run(
            "set_schedfire_inflight", self._client.set(self._inflight(schedfire_key), "1", nx=True, px=ttl_ms)
        )
        return bool(result)

    async def clear_schedfire_inflight(self, schedfire_key: str) -> None:
        """Release the claim WITHOUT the completed marker:
        the fire is being re-delivered, so the long-TTL marker must not make
        the redelivered copy get ack+skipped."""
        await self._run("clear_schedfire_inflight", self._client.delete(self._inflight(schedfire_key)))

    async def is_schedfire_completed(self, schedfire_key: str) -> bool:
        result = await self._run("is_schedfire_completed", self._client.exists(self._fired(schedfire_key)))
        return bool(result == 1)

    async def set_schedfire_handover_marker(self, owner_token: str, ttl_ms: int) -> None:
        """Leader marker set right before graceful release: the fixed key for
        successors from 0.12 on (BZ-07, no SCAN) plus the per-owner key that
        successors before 0.12 find by SCAN."""
        await self._run(
            "set_schedfire_handover_marker",
            self._client.set(self._keys.sched_handover_latest(), owner_token, px=ttl_ms),
        )
        key = self._keys.sched_handover(owner_token)
        await self._run("set_schedfire_handover_marker", self._client.set(key, "1", px=ttl_ms))

    async def get_and_del_schedfire_handover_marker(self, previous_owner: str | None = None) -> bool:
        """Alias von :meth:`take_handover_marker` (Name der früheren Fassade)."""
        return await self.take_handover_marker(previous_owner)


if TYPE_CHECKING:  # Konformitäts-Anker (KTD1)
    from swarmq.contracts import ScheduleStore

    _conforms_schedules: type[ScheduleStore] = ValkeyScheduleStore
