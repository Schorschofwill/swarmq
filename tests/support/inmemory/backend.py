"""In-Memory-``ResultBackend`` samt den vier Koordinationsverträgen (R2, KTD4).

Dictionaries mit TTL gegen die Fake-Clock; Owner-Checks über die Token-
Typen; Sliding-Window nach derselben Näherung wie ``sliding_window_counter.lua``.
Die Fassade bietet ``.locks``, ``.rate_limit``, ``.chords``, ``.schedules``
wie die ``ValkeyBackend``-Fassade ab U18.
"""

from __future__ import annotations

import asyncio
import json
import math
from collections.abc import AsyncIterator, Mapping, Sequence
from typing import TYPE_CHECKING

from swarmq.contracts import (
    ChordStore,
    CronReport,
    CronReports,
    LeaderInfo,
    LockStore,
    LockToken,
    RateLimitDecision,
    RateLimitStore,
    ResultBackend,
    ScheduleStore,
)
from swarmq.exceptions import BackendConnectionError
from swarmq.locking import SCHEDULER_LOCK_NAME
from tests.support.inmemory.clock import FakeClock

_POLL_S = 0.05


class _TTLDict[V]:
    """Dictionary mit Ablaufzeit pro Schlüssel (lazy gegen die Fake-Clock)."""

    def __init__(self, clock: FakeClock) -> None:
        self._clock = clock
        self._data: dict[str, tuple[V, int | None]] = {}

    def _purge(self) -> None:
        now = self._clock.now_ms()
        for key in [k for k, (_, exp) in self._data.items() if exp is not None and exp <= now]:
            del self._data[key]

    def set(self, key: str, value: V, ttl_ms: int | None) -> None:
        self._data[key] = (value, None if ttl_ms is None else self._clock.now_ms() + ttl_ms)

    def set_if_absent(self, key: str, value: V, ttl_ms: int | None) -> bool:
        if key in self:
            return False
        self.set(key, value, ttl_ms)
        return True

    def get(self, key: str) -> V | None:
        self._purge()
        entry = self._data.get(key)
        return None if entry is None else entry[0]

    def expire(self, key: str, ttl_ms: int) -> bool:
        if key not in self:
            return False
        value, _ = self._data[key]
        self.set(key, value, ttl_ms)
        return True

    def pop(self, key: str) -> V | None:
        self._purge()
        entry = self._data.pop(key, None)
        return None if entry is None else entry[0]

    def keys(self) -> list[str]:
        self._purge()
        return list(self._data)

    def __contains__(self, key: object) -> bool:
        self._purge()
        return key in self._data


class InMemoryBackend:
    def __init__(self, clock: FakeClock, *, unreachable: bool = False) -> None:
        self._clock = clock
        self._unreachable = unreachable
        self._connected = False
        self._results: _TTLDict[bytes] = _TTLDict(clock)
        self._progress: _TTLDict[dict[bytes, bytes]] = _TTLDict(clock)
        self._cancel: _TTLDict[str] = _TTLDict(clock)
        self._wf_state: _TTLDict[bytes] = _TTLDict(clock)
        self._wf_cancel: _TTLDict[str] = _TTLDict(clock)
        self._unique: _TTLDict[str] = _TTLDict(clock)
        self._result_events: dict[str, asyncio.Event] = {}
        self._wf_events: dict[str, asyncio.Event] = {}
        self._progress_subscribers: dict[str, list[asyncio.Queue[bytes]]] = {}
        self._locks = InMemoryLockStore(clock)
        self._rate_limit = InMemoryRateLimitStore(clock)
        self._chords = InMemoryChordStore(clock)
        self._schedules = InMemoryScheduleStore(clock, self._locks)

    # --- Fassaden-Properties (KTD4) -------------------------------------------

    @property
    def locks(self) -> InMemoryLockStore:
        self._require_connected()
        return self._locks

    @property
    def rate_limit(self) -> InMemoryRateLimitStore:
        self._require_connected()
        return self._rate_limit

    @property
    def chords(self) -> InMemoryChordStore:
        self._require_connected()
        return self._chords

    @property
    def schedules(self) -> InMemoryScheduleStore:
        self._require_connected()
        return self._schedules

    # --- Lebenszyklus ---------------------------------------------------------

    async def connect(self) -> None:
        if self._unreachable:
            raise BackendConnectionError("in-memory backend configured unreachable")
        self._connected = True

    async def close(self) -> None:
        self._connected = False

    async def ping(self) -> None:
        self._require_connected()

    def _require_connected(self) -> None:
        if not self._connected:
            raise BackendConnectionError("not connected")

    # --- Ergebnisse -------------------------------------------------------------

    async def store_result(self, task_id: str, data: bytes, ttl_ms: int, *, op_timeout: float | None = None) -> None:
        self._results.set(task_id, data, ttl_ms)

    async def get_result(self, task_id: str) -> bytes | None:
        return self._results.get(task_id)

    async def get_results(self, task_ids: Sequence[str]) -> list[bytes | None]:
        return [self._results.get(t) for t in task_ids]

    async def delete_result(self, task_id: str) -> None:
        self._results.pop(task_id)

    async def notify_result(self, task_id: str) -> None:
        self._result_events.setdefault(task_id, asyncio.Event()).set()

    async def wait_for_result(self, task_id: str, *, timeout: float) -> bytes | None:
        event = self._result_events.setdefault(task_id, asyncio.Event())
        return await _wait_until(lambda: self._results.get(task_id), event, timeout)

    # --- Fortschritt ------------------------------------------------------------

    async def set_progress(
        self, task_id: str, fields: Mapping[str, str], ttl_ms: int, *, op_timeout: float | None = None
    ) -> None:
        current = self._progress.get(task_id) or {}
        current.update({k.encode(): v.encode() for k, v in fields.items()})
        self._progress.set(task_id, current, ttl_ms)

    async def get_progress(self, task_id: str, *, op_timeout: float | None = None) -> Mapping[bytes, bytes] | None:
        value = self._progress.get(task_id)
        return dict(value) if value else None

    async def publish_progress(self, task_id: str, payload: bytes) -> None:
        for queue in self._progress_subscribers.get(task_id, []):
            queue.put_nowait(payload)

    async def watch_progress(self, task_id: str) -> AsyncIterator[bytes]:
        queue: asyncio.Queue[bytes] = asyncio.Queue()
        self._progress_subscribers.setdefault(task_id, []).append(queue)
        try:
            while True:
                yield await queue.get()
        finally:
            self._progress_subscribers[task_id].remove(queue)

    # --- Cancel -----------------------------------------------------------------

    async def set_cancel_flag(self, task_id: str, ttl_ms: int, *, op_timeout: float | None = None) -> bool:
        return self._cancel.set_if_absent(task_id, "1", ttl_ms)

    async def is_cancelled(self, task_id: str, *, op_timeout: float | None = None) -> bool:
        return task_id in self._cancel

    async def clear_cancel_flag(self, task_id: str, *, op_timeout: float | None = None) -> None:
        self._cancel.pop(task_id)

    # --- Workflow ---------------------------------------------------------------

    async def store_workflow_state(
        self, wf_id: str, data: bytes, ttl_ms: int, *, op_timeout: float | None = None
    ) -> None:
        self._wf_state.set(wf_id, data, ttl_ms)

    async def compare_and_store_workflow_state(
        self, wf_id: str, data: bytes, ttl_ms: int, *, expected_version: int, op_timeout: float | None = None
    ) -> bool:
        current = self._wf_state.get(wf_id)
        if current is None:
            return False
        if int(json.loads(current).get("version", 0)) != expected_version:
            return False
        self._wf_state.set(wf_id, data, ttl_ms)
        return True

    async def get_workflow_state(self, wf_id: str, *, op_timeout: float | None = None) -> bytes | None:
        return self._wf_state.get(wf_id)

    async def set_workflow_cancel_flag(self, wf_id: str, ttl_ms: int, *, op_timeout: float | None = None) -> bool:
        return self._wf_cancel.set_if_absent(wf_id, "1", ttl_ms)

    async def is_workflow_cancelled(self, wf_id: str, *, op_timeout: float | None = None) -> bool:
        return wf_id in self._wf_cancel

    async def notify_workflow(self, wf_id: str) -> None:
        self._wf_events.setdefault(wf_id, asyncio.Event()).set()

    async def wait_for_workflow(self, wf_id: str, *, timeout: float) -> bool:
        event = self._wf_events.setdefault(wf_id, asyncio.Event())
        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
        except TimeoutError:
            return False
        event.clear()
        return True

    # --- Dedup ------------------------------------------------------------------

    async def claim_unique(
        self, dedup_hash: str, owner: str, ttl_ms: int, *, op_timeout: float | None = None
    ) -> str | None:
        if self._unique.set_if_absent(dedup_hash, owner, ttl_ms):
            return None
        return self._unique.get(dedup_hash)

    async def release_unique(self, dedup_hash: str, owner: str, *, op_timeout: float | None = None) -> bool:
        if self._unique.get(dedup_hash) != owner:
            return False
        self._unique.pop(dedup_hash)
        return True


async def _wait_until(read: object, event: asyncio.Event, timeout: float) -> bytes | None:
    """Liest sofort, wartet dann auf ``event`` oder pollt, bis ``timeout`` abläuft."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        value = read()  # type: ignore[operator]
        if value is not None:
            return value  # type: ignore[no-any-return]
        remaining = deadline - loop.time()
        if remaining <= 0:
            return None
        try:
            await asyncio.wait_for(event.wait(), timeout=min(remaining, _POLL_S))
            event.clear()
        except TimeoutError:
            continue


class InMemoryLockStore:
    def __init__(self, clock: FakeClock) -> None:
        self._clock = clock
        self._mutex: _TTLDict[str] = _TTLDict(clock)  # name → owner
        self._semaphore: dict[str, _TTLDict[str]] = {}  # name → owner → "1"

    async def acquire(self, name: str, *, owner: str, ttl_ms: int, max_concurrency: int) -> LockToken | None:
        if ttl_ms <= 0 or max_concurrency < 1:
            return None
        if max_concurrency == 1:
            if not self._mutex.set_if_absent(name, owner, ttl_ms):
                return None
        else:
            slots = self._semaphore.setdefault(name, _TTLDict(self._clock))
            if len(slots.keys()) >= max_concurrency:
                return None
            slots.set(owner, "1", ttl_ms)
        return LockToken(name=name, owner=owner, max_concurrency=max_concurrency)

    async def release(self, token: LockToken) -> bool:
        if token.max_concurrency == 1:
            if self._mutex.get(token.name) != token.owner:
                return False
            self._mutex.pop(token.name)
            return True
        slots = self._semaphore.get(token.name)
        return slots is not None and slots.pop(token.owner) is not None

    async def renew(self, token: LockToken, ttl_ms: int) -> bool:
        if token.max_concurrency != 1:
            raise NotImplementedError("renew is only defined for mutex locks")
        if self._mutex.get(token.name) != token.owner:
            return False
        return self._mutex.expire(token.name, ttl_ms)

    async def force_release(self, name: str, *, max_concurrency: int) -> bool:
        if max_concurrency == 1:
            return self._mutex.pop(name) is not None
        slots = self._semaphore.pop(name, None)
        return slots is not None and bool(slots.keys())

    async def current_owner(self, name: str) -> str | None:
        return self._mutex.get(name)


class InMemoryRateLimitStore:
    def __init__(self, clock: FakeClock) -> None:
        self._clock = clock
        self._counts: _TTLDict[int] = _TTLDict(clock)

    async def check(self, name: str, *, max_requests: int, window_ms: int) -> RateLimitDecision:
        if window_ms <= 0:
            return RateLimitDecision(False, 0, 0)
        now = self._clock.now_ms()
        window_ts = now - now % window_ms
        prev_ts = window_ts - window_ms
        current = self._counts.get(f"{name}:{window_ts}") or 0
        previous = self._counts.get(f"{name}:{prev_ts}") or 0
        elapsed = now - window_ts
        estimated = previous * (1 - elapsed / window_ms) + current
        if estimated >= max_requests:
            retry_after = max(1, math.ceil(window_ms - elapsed))
            return RateLimitDecision(False, 0, retry_after)
        self._counts.set(f"{name}:{window_ts}", current + 1, 2 * window_ms)
        remaining = max(0, max_requests - int(estimated) - 1)
        return RateLimitDecision(True, remaining, 0)


class InMemoryChordStore:
    def __init__(self, clock: FakeClock) -> None:
        self._sets: _TTLDict[set[str]] = _TTLDict(clock)

    async def record_member(self, wf_id: str, chord_id: str, member_id: str, *, expected: int, ttl_ms: int) -> bool:
        key = f"{wf_id}:{chord_id}"
        members = self._sets.get(key) or set()
        members.add(member_id)
        self._sets.set(key, members, ttl_ms)
        return len(members) >= expected

    # Alias der heutigen ChordManager-Signatur (KTD10; entfällt in U15).
    async def record_and_check_complete(
        self, wf_id: str, chord_id: str, member_id: str, expected_n: int, ttl_ms: int
    ) -> bool:
        return await self.record_member(wf_id, chord_id, member_id, expected=expected_n, ttl_ms=ttl_ms)


class InMemoryScheduleStore:
    def __init__(self, clock: FakeClock, locks: InMemoryLockStore) -> None:
        self._clock = clock
        self._locks = locks
        self._defs: dict[str, bytes] = {}
        self._fires: dict[str, int] = {}
        self._completed: _TTLDict[str] = _TTLDict(clock)
        self._inflight: _TTLDict[str] = _TTLDict(clock)
        self._handover: _TTLDict[str] = _TTLDict(clock)
        # Cron-Meldungen (KTD10): (name, fingerprint) -> [first, last, payload]
        self._reports: dict[tuple[str, str], tuple[int, int, bytes]] = {}
        # Leader-Version (KTD11): "leader" -> (owner, version)
        self._leader_version: _TTLDict[tuple[str, str]] = _TTLDict(clock)
        self._worker_versions: _TTLDict[str] = _TTLDict(clock)

    async def add(self, name: str, payload: bytes, fire_ts_ms: int | None) -> bool:
        if name in self._defs:
            return False
        await self.put(name, payload, fire_ts_ms)
        return True

    async def put(self, name: str, payload: bytes, fire_ts_ms: int | None) -> None:
        self._defs[name] = payload
        if fire_ts_ms is not None:
            self._fires[name] = fire_ts_ms

    async def exists(self, name: str) -> bool:
        return name in self._defs

    async def remove(self, name: str) -> bool:
        existed = name in self._defs
        self._defs.pop(name, None)
        self._fires.pop(name, None)
        return existed

    async def get(self, name: str) -> bytes | None:
        return self._defs.get(name)

    async def fire_ts(self, name: str) -> int | None:
        return self._fires.get(name)

    async def list_all(self) -> Mapping[str, bytes]:
        return dict(self._defs)

    async def replan_if_unchanged(
        self, name: str, expected: bytes, payload: bytes, fire_ts_ms: int,
    ) -> bool:
        if self._defs.get(name) != expected or name in self._fires:
            return False
        self._defs[name] = payload
        self._fires[name] = fire_ts_ms
        return True

    async def disable(self, name: str, payload: bytes) -> bool:
        if name not in self._defs:
            return False
        self._defs[name] = payload
        self._fires.pop(name, None)
        return True

    async def poll_due(self, now_ms: int, *, limit: int = 100) -> list[tuple[str, int, bytes]]:
        due = sorted(((ts, n) for n, ts in self._fires.items() if ts <= now_ms and n in self._defs))
        return [(n, ts, self._defs[n]) for ts, n in due[:limit]]

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
        if await self._locks.current_owner(SCHEDULER_LOCK_NAME) != owner:
            return False
        if fired_ts_ms is not None and self._fires.get(name) != fired_ts_ms:
            return True
        self._fires.pop(name, None)
        if next_fire_ts_ms is not None and next_fire_ts_ms >= 0:
            self._fires[name] = next_fire_ts_ms
        if payload is None:
            self._defs.pop(name, None)
        elif fired_payload is None or self._defs.get(name) == fired_payload:
            self._defs[name] = payload
        return True

    async def mark_fire_completed(self, fire_key: str, ttl_ms: int) -> bool:
        return self._completed.set_if_absent(fire_key, "1", ttl_ms)

    async def claim_fire(
        self, fire_key: str, ttl_ms: int, *, owner: str | None = None,
        takeover: str | Sequence[str] | None = None,
    ) -> bool:
        current = self._inflight.get(fire_key)
        offered = (takeover,) if isinstance(takeover, str) else tuple(takeover or ())
        if current is not None and current not in offered:
            return False
        self._inflight.set(fire_key, owner or "1", ttl_ms)
        return True

    async def release_fire_claim(self, fire_key: str, *, owner: str | None = None) -> None:
        if owner is None or self._inflight.get(fire_key) == owner:
            self._inflight.pop(fire_key)

    async def is_fire_completed(self, fire_key: str) -> bool:
        return fire_key in self._completed

    async def set_handover_marker(self, owner: str, ttl_ms: int) -> None:
        self._handover.set(owner, "1", ttl_ms)

    async def take_handover_marker(self) -> bool:
        keys = self._handover.keys()
        for key in keys:
            self._handover.pop(key)
        return bool(keys)

    async def report_cron(
        self, name: str, fingerprint: str, payload: bytes, *, live_window_ms: int,
    ) -> None:
        now = self._clock.now_ms()
        previous = self._reports.get((name, fingerprint))
        # first_seen bleibt nur, solange der Abdruck lebt (KTD10).
        alive = previous is not None and previous[1] >= now - live_window_ms
        first = previous[0] if previous is not None and alive else now
        self._reports[(name, fingerprint)] = (first, now, payload)

    async def prune_cron_reports(self, *, max_age_ms: int) -> int:
        cutoff = self._clock.now_ms() - max_age_ms
        stale = [key for key, (_, last, _) in self._reports.items() if last < cutoff]
        for key in stale:
            del self._reports[key]
        return len(stale)

    async def cron_reports(self) -> CronReports:
        reports = tuple(
            CronReport(name, fp, first, last, payload)
            for (name, fp), (first, last, payload) in self._reports.items()
        )
        return CronReports(now_ms=self._clock.now_ms(), reports=reports)

    async def report_worker_version(self, worker_id: str, version: str, *, ttl_ms: int) -> None:
        self._worker_versions.set(worker_id, version, ttl_ms)

    async def worker_versions(self) -> Mapping[str, str]:
        live = self._worker_versions.keys()  # purgt abgelaufene Meldungen
        return {worker_id: version for worker_id in live if (version := self._worker_versions.get(worker_id))}

    async def set_leader_version(self, owner: str, version: str, *, ttl_ms: int) -> None:
        self._leader_version.set("leader", (owner, version), ttl_ms)

    async def leader_info(self) -> LeaderInfo:
        owner = await self._locks.current_owner(SCHEDULER_LOCK_NAME)
        written = self._leader_version.get("leader")
        version = written[1] if written is not None and written[0] == owner else None
        return LeaderInfo(owner=owner, version=version)


if TYPE_CHECKING:  # Konformitäts-Anker (KTD1)
    _conforms_backend: type[ResultBackend] = InMemoryBackend
    _conforms_locks: type[LockStore] = InMemoryLockStore
    _conforms_rate_limit: type[RateLimitStore] = InMemoryRateLimitStore
    _conforms_chords: type[ChordStore] = InMemoryChordStore
    _conforms_schedules: type[ScheduleStore] = InMemoryScheduleStore
