"""Contract-Klassen für ``LockStore``, ``RateLimitStore``, ``ChordStore`` und
``ScheduleStore`` (Entwurf §2.4).

Fixtures eines Bindings: ``impl`` (der jeweilige Store, verbunden), ``clock``;
für ``ScheduleStoreContract`` zusätzlich ``leader``: ``async (owner) -> None``
macht ``owner`` zum Scheduler-Leader (das Binding erwirbt den Leader-Lock
über seinen ``LockStore``).
"""

from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable

from swarmq.contracts import ChordStore, LockStore, LockToken, RateLimitStore, ScheduleStore
from tests.contracts._support import Clock

Leader = Callable[[str], Awaitable[None]]


def _name(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


class LockStoreContract:
    async def test_acquire_returns_token_and_blocks_second_owner(self, impl: LockStore) -> None:
        name = _name("lock")
        token = await impl.acquire(name, owner="a", ttl_ms=60_000, max_concurrency=1)
        assert token == LockToken(name=name, owner="a", max_concurrency=1)
        assert await impl.acquire(name, owner="b", ttl_ms=60_000, max_concurrency=1) is None
        assert await impl.current_owner(name) == "a"

    async def test_release_enforces_owner_via_token(self, impl: LockStore) -> None:
        name = _name("lock")
        token = await impl.acquire(name, owner="a", ttl_ms=60_000, max_concurrency=1)
        assert token is not None
        forged = LockToken(name=name, owner="b", max_concurrency=1)
        assert await impl.release(forged) is False
        assert await impl.current_owner(name) == "a"
        assert await impl.release(token) is True
        assert await impl.current_owner(name) is None
        assert await impl.release(token) is False  # bereits freigegeben

    async def test_renew_extends_only_for_owner(self, impl: LockStore, clock: Clock) -> None:
        name = _name("lock")
        token = await impl.acquire(name, owner="a", ttl_ms=600, max_concurrency=1)
        assert token is not None
        forged = LockToken(name=name, owner="b", max_concurrency=1)
        assert await impl.renew(forged, ttl_ms=60_000) is False
        assert await impl.renew(token, ttl_ms=60_000) is True
        await clock.advance(1.0)
        assert await impl.current_owner(name) == "a"

    async def test_lock_expires_after_ttl(self, impl: LockStore, clock: Clock) -> None:
        name = _name("lock")
        await impl.acquire(name, owner="a", ttl_ms=300, max_concurrency=1)
        await clock.advance(0.8)
        assert await impl.acquire(name, owner="b", ttl_ms=60_000, max_concurrency=1) is not None

    async def test_force_release_ignores_owner(self, impl: LockStore) -> None:
        name = _name("lock")
        await impl.acquire(name, owner="a", ttl_ms=60_000, max_concurrency=1)
        assert await impl.force_release(name, max_concurrency=1) is True
        assert await impl.current_owner(name) is None
        assert await impl.force_release(name, max_concurrency=1) is False

    async def test_semaphore_admits_up_to_max_concurrency(self, impl: LockStore) -> None:
        name = _name("sem")
        t1 = await impl.acquire(name, owner="a", ttl_ms=60_000, max_concurrency=2)
        t2 = await impl.acquire(name, owner="b", ttl_ms=60_000, max_concurrency=2)
        assert t1 is not None and t2 is not None
        assert await impl.acquire(name, owner="c", ttl_ms=60_000, max_concurrency=2) is None
        assert await impl.release(t1) is True
        assert await impl.acquire(name, owner="c", ttl_ms=60_000, max_concurrency=2) is not None

    async def test_semaphore_slots_expire_after_ttl(self, impl: LockStore, clock: Clock) -> None:
        """Slots, deren Halter länger als ``ttl_ms`` inaktiv sind, werden beim
        nächsten ``acquire`` mit derselben TTL freigegeben. (Kein Anspruch
        auf slot-individuelle TTLs: ``lock_acquire.lua`` räumt mit der TTL
        des Aufrufers auf.)"""
        name = _name("sem")
        await impl.acquire(name, owner="a", ttl_ms=300, max_concurrency=2)
        await impl.acquire(name, owner="b", ttl_ms=300, max_concurrency=2)
        assert await impl.acquire(name, owner="c", ttl_ms=300, max_concurrency=2) is None
        await clock.advance(0.8)
        assert await impl.acquire(name, owner="c", ttl_ms=300, max_concurrency=2) is not None

    async def test_current_owner_of_unknown_lock_is_none(self, impl: LockStore) -> None:
        assert await impl.current_owner(_name("lock")) is None


class RateLimitStoreContract:
    async def test_allows_up_to_limit_then_reports_retry_after(self, impl: RateLimitStore) -> None:
        name = _name("rl")
        for _ in range(3):
            decision = await impl.check(name, max_requests=3, window_ms=60_000)
            assert decision.allowed is True
            assert decision.retry_after_ms == 0
        denied = await impl.check(name, max_requests=3, window_ms=60_000)
        assert denied.allowed is False
        assert denied.remaining == 0
        assert 1 <= denied.retry_after_ms <= 60_000

    async def test_remaining_counts_down(self, impl: RateLimitStore) -> None:
        name = _name("rl")
        first = await impl.check(name, max_requests=2, window_ms=60_000)
        second = await impl.check(name, max_requests=2, window_ms=60_000)
        assert first.remaining > second.remaining >= 0

    async def test_window_rolls_over(self, impl: RateLimitStore, clock: Clock) -> None:
        name = _name("rl")
        await impl.check(name, max_requests=1, window_ms=500)
        assert (await impl.check(name, max_requests=1, window_ms=500)).allowed is False
        await clock.advance(1.2)
        assert (await impl.check(name, max_requests=1, window_ms=500)).allowed is True

    async def test_names_are_independent(self, impl: RateLimitStore) -> None:
        a, b = _name("rl"), _name("rl")
        await impl.check(a, max_requests=1, window_ms=60_000)
        assert (await impl.check(b, max_requests=1, window_ms=60_000)).allowed is True


class ChordStoreContract:
    async def test_completes_when_expected_distinct_members_recorded(self, impl: ChordStore) -> None:
        wf, chord = _name("wf"), "c1"
        assert await impl.record_member(wf, chord, "m1", expected=3, ttl_ms=60_000) is False
        assert await impl.record_member(wf, chord, "m2", expected=3, ttl_ms=60_000) is False
        assert await impl.record_member(wf, chord, "m3", expected=3, ttl_ms=60_000) is True

    async def test_duplicate_member_is_idempotent(self, impl: ChordStore) -> None:
        wf, chord = _name("wf"), "c1"
        assert await impl.record_member(wf, chord, "m1", expected=2, ttl_ms=60_000) is False
        assert await impl.record_member(wf, chord, "m1", expected=2, ttl_ms=60_000) is False
        assert await impl.record_member(wf, chord, "m2", expected=2, ttl_ms=60_000) is True
        assert await impl.record_member(wf, chord, "m2", expected=2, ttl_ms=60_000) is True

    async def test_chords_are_isolated_per_workflow(self, impl: ChordStore) -> None:
        a, b = _name("wf"), _name("wf")
        await impl.record_member(a, "c", "m1", expected=1, ttl_ms=60_000)
        assert await impl.record_member(b, "c", "m2", expected=2, ttl_ms=60_000) is False


class ScheduleStoreContract:
    async def test_add_is_insert_if_absent(self, impl: ScheduleStore) -> None:
        name = _name("sched")
        assert await impl.exists(name) is False
        assert await impl.add(name, b"v1", fire_ts_ms=1_000) is True
        assert await impl.add(name, b"v2", fire_ts_ms=2_000) is False
        assert await impl.exists(name) is True
        assert await impl.get(name) == b"v1"

    async def test_put_overwrites(self, impl: ScheduleStore) -> None:
        name = _name("sched")
        await impl.add(name, b"v1", fire_ts_ms=1_000)
        await impl.put(name, b"v2", fire_ts_ms=2_000)
        assert await impl.get(name) == b"v2"
        assert [n for n, _, _ in await impl.poll_due(2_000)] .count(name) == 1

    async def test_remove_reports_prior_existence(self, impl: ScheduleStore) -> None:
        name = _name("sched")
        assert await impl.remove(name) is False
        await impl.add(name, b"v1", fire_ts_ms=1_000)
        assert await impl.remove(name) is True
        assert await impl.exists(name) is False
        assert await impl.get(name) is None
        assert all(n != name for n, _, _ in await impl.poll_due(10_000))

    async def test_list_all_contains_added_definitions(self, impl: ScheduleStore) -> None:
        a, b = _name("sched"), _name("sched")
        await impl.add(a, b"A", fire_ts_ms=None)
        await impl.add(b, b"B", fire_ts_ms=5_000)
        listing = await impl.list_all()
        assert listing[a] == b"A" and listing[b] == b"B"

    async def test_poll_due_returns_only_due_entries_with_fire_time(self, impl: ScheduleStore) -> None:
        early, late = _name("sched"), _name("sched")
        await impl.add(early, b"E", fire_ts_ms=1_000)
        await impl.add(late, b"L", fire_ts_ms=9_000)
        due = {n: (ts, p) for n, ts, p in await impl.poll_due(5_000)}
        assert due.get(early) == (1_000, b"E")
        assert late not in due

    async def test_poll_due_respects_limit(self, impl: ScheduleStore) -> None:
        names = [_name("sched") for _ in range(3)]
        for i, n in enumerate(names):
            await impl.add(n, b"x", fire_ts_ms=100 + i)
        assert len(await impl.poll_due(1_000, limit=2)) <= 2

    async def test_disable_replaces_definition_and_removes_from_index(self, impl: ScheduleStore) -> None:
        name = _name("sched")
        await impl.add(name, b"on", fire_ts_ms=1_000)
        assert await impl.disable(name, b"off") is True
        assert await impl.get(name) == b"off"
        assert all(n != name for n, _, _ in await impl.poll_due(10_000))
        assert await impl.disable(_name("sched"), b"x") is False

    async def test_commit_fire_requires_leader(self, impl: ScheduleStore, leader: Leader) -> None:
        name = _name("sched")
        await impl.add(name, b"v1", fire_ts_ms=1_000)
        assert await impl.commit_fire(name, next_fire_ts_ms=2_000, payload=b"v2", owner="impostor") is False
        assert await impl.get(name) == b"v1"
        await leader("chief")
        assert await impl.commit_fire(name, next_fire_ts_ms=2_000, payload=b"v2", owner="chief") is True
        assert await impl.get(name) == b"v2"
        due = {n: ts for n, ts, _ in await impl.poll_due(2_000)}
        assert due[name] == 2_000

    async def test_commit_fire_with_none_payload_removes_one_shot(self, impl: ScheduleStore, leader: Leader) -> None:
        name = _name("oneshot")
        await impl.add(name, b"once", fire_ts_ms=1_000)
        await leader("chief")
        assert await impl.commit_fire(name, next_fire_ts_ms=None, payload=None, owner="chief") is True
        assert await impl.exists(name) is False
        assert all(n != name for n, _, _ in await impl.poll_due(10_000))

    async def test_fire_markers(self, impl: ScheduleStore) -> None:
        key = _name("fire")
        assert await impl.is_fire_completed(key) is False
        assert await impl.claim_fire(key, ttl_ms=60_000) is True
        assert await impl.claim_fire(key, ttl_ms=60_000) is False
        await impl.release_fire_claim(key)
        assert await impl.claim_fire(key, ttl_ms=60_000) is True
        assert await impl.mark_fire_completed(key, ttl_ms=60_000) is True
        assert await impl.mark_fire_completed(key, ttl_ms=60_000) is False
        assert await impl.is_fire_completed(key) is True

    async def test_fire_claim_expires(self, impl: ScheduleStore, clock: Clock) -> None:
        key = _name("fire")
        await impl.claim_fire(key, ttl_ms=300)
        await clock.advance(0.8)
        assert await impl.claim_fire(key, ttl_ms=60_000) is True

    async def test_handover_marker_is_taken_once(self, impl: ScheduleStore) -> None:
        assert await impl.take_handover_marker() is False
        await impl.set_handover_marker(_name("owner"), ttl_ms=60_000)
        assert await impl.take_handover_marker() is True
        assert await impl.take_handover_marker() is False
