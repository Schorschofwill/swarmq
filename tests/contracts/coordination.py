"""Contract-Klassen für ``LockStore``, ``RateLimitStore``, ``ChordStore`` und
``ScheduleStore`` (Entwurf §2.4).

Fixtures eines Bindings: ``impl`` (der jeweilige Store, verbunden), ``clock``;
für ``ScheduleStoreContract`` zusätzlich ``leader``: ``async (owner) -> None``
macht ``owner`` zum Scheduler-Leader (das Binding erwirbt den Leader-Lock
über seinen ``LockStore``).
"""

# Test list (0.9.0, Scheduler-Commit-Wettlauf) -- ScheduleStoreContract:
# 1. commit_fire mit ``fired_ts_ms`` verwirft den Commit, wenn der Eintrag
#    inzwischen deaktiviert wurde (Fire-Index leer, Definition bleibt).
# 2. ... wenn der Eintrag inzwischen dauerhaft entfernt wurde.
# 3. ... wenn der Fire-Index-Score inzwischen ein anderer ist (``put``).
# 4. commit_fire mit passendem ``fired_ts_ms`` wendet den Commit an.
# 4b. (Vertrag, dokumentierte Grenze) Entfernt und mit DEMSELBEN Score neu
#     angelegt (ABA): der Commit greift -- der Score ist das einzige
#     Merkmal, die Neuanlage ist vom gefeuerten Eintrag nicht zu
#     unterscheiden; ihre Definition wird ueberschrieben.
#
# Test list (Plan 2026-10-08-1541, U3, KTD12) -- ScheduleStoreContract:
# 11. ``fire_ts`` liefert den Score im Fire-Index, ``None`` ohne Eintrag.
# 12. commit_fire mit ``fired_payload``: hat sich die Definition seit dem
#     Poll geaendert, rueckt der Score vor, die Definition bleibt.
# 13. ... ist sie unveraendert, wird sie zurueckgeschrieben.
#
# Test list (0.9.0, Schedfire-Claim-Token) -- ScheduleStoreContract:
# 5. claim_fire mit ``owner``: ein zweiter Claim ohne Uebernahme verliert.
# 6. Uebernahme mit dem gespeicherten Token gewinnt und rotiert den Owner;
#    das alte Token gewinnt danach nicht mehr (echte Duplikate bleiben
#    Duplikate).
# 7. Uebernahme mit fremdem Token verliert.
# 8. release_fire_claim mit fremdem ``owner`` laesst den Claim stehen, mit
#    eigenem gibt es ihn frei.
# 9. Die Uebernahme setzt die TTL neu.
# 10. Uebernahme mit mehreren angebotenen Tokens (Kopie nach unklarem
#     Claim-Ausgang): jedes davon gewinnt, der Owner rotiert.
#
# Test list (Plan 2026-10-14-0748, U6, KTD7, FS-06a) -- ScheduleStoreContract:
# 10a. Wiederholter claim_fire mit demselben ``owner`` (Transport-Retry nach
#      verlorener Antwort) gewinnt.
# 10b. ... auch nach einer Uebernahme (Wiederholung mit denselben Argumenten).
# 10c. Eine Neuzustellung mit neuem ``owner`` bleibt nach dem Selbsttreffer
#      fremd (Duplikat).
# 10d. Ohne ``owner`` gewinnt nur der erste Aufruf (``test_fire_markers``).

# Test list (Plan 2026-10-14-0748, U11, KTD7, FS-06b) -- LockStoreContract:
# L1. Wiederholter Mutex-``acquire`` mit demselben ``owner`` (Transport-Retry
#     nach verlorener Antwort) liefert das Token; die PTTL bleibt (E-022).
# L2. ... ebenso beim Semaphor: Token, PTTL unveraendert.
# L3. Semaphor voll inklusive eigenem Token: die Wiederholung liefert das Token.
# L4. Ein anderer ``owner`` bleibt nach dem Selbsttreffer ausgeschlossen
#     (Mutex) bzw. abgewiesen (voller Semaphor).

# Test list (Plan 2026-10-08-1541, U12, KTD10) -- ScheduleStoreContract:
# 14. ``report_cron`` legt eine Meldung an: ``first_seen == last_seen`` in
#     Backend-Zeit, Payload abrufbar; ``cron_reports`` liefert die
#     Backend-Zeit des Lesens mit.
# 15. Eine erneute Meldung im Lebendigkeitsfenster rückt nur ``last_seen``
#     vor und ersetzt den Payload.
# 16. Eine Meldung nach Ablauf des Fensters (Abdruck war tot) setzt
#     ``first_seen`` neu (Revert A→B→A gilt wieder als neu).
# 17. Zwei Abdrücke eines Namens sind getrennte Meldungen.
# 18. ``prune_cron_reports`` löscht nur Meldungen, deren ``last_seen``
#     älter als die Frist ist.

# Test list (Plan 2026-10-08-1541, U13, KTD11) -- ScheduleStoreContract:
# 19. ``leader_info`` liefert den Lock-Owner; ohne Versions-Key (0.9.0-
#     Leader) ist ``version`` ``None``.
# 20. ``set_leader_version`` des Lock-Owners erscheint in ``leader_info``.
# 21. Der Versions-Key eines anderen Owners (Vorgänger) gilt nicht.
# 22. Der Versions-Key läuft nach seiner TTL ab.
# 23. ``report_worker_version`` erscheint in ``worker_versions`` und läuft
#     nach seiner TTL ab.

# Test list (Plan 0.12.0, MR 4, BZ-28) -- ScheduleStoreContract:
# 33. ``poll_due`` nimmt deaktivierte Definitionen mit Score aus dem
#     Fire-Index, bevor das Limit greift; die Definition bleibt.

# Test list (Plan 0.12.0, MR 11, BZ-11) -- ScheduleStoreContract:
# 34. ``report_cron`` merkt den Namen in ``known_cron_names`` (je gemeldet).
# 35. ``prune_cron_reports`` laesst ``known_cron_names`` unberuehrt.
# 36. ``prune_cron`` entfernt den Namen zusammen mit der Definition.
# 37. ``forget_known_cron`` (CLI ``schedule remove --permanent``) entfernt ihn
#     ebenso.

# Test list (Plan 0.12.0, MR 14, BZ-04) -- ScheduleStoreContract:
# 38. ``ensure_epoch`` legt den Epoch nur an, wenn er fehlt, und liefert
#     danach immer den ersten Wert (``None`` nur beim Anlegen).

# Test list (Plan 0.12.0, MR 18, BZ-14) -- ScheduleStoreContract:
# 39. ``report_queues`` meldet jede Queue mit Backend-Zeit (``queue_reports``).
# 40. eine spaetere Meldung rueckt ``last_seen`` vor.
# 41. ``forget_queue_reports`` entfernt nur die genannten Queues.
# 42. meldet niemand innerhalb von ``ttl_ms``, verfallen alle Meldungen (Q5).

# Test list (Review #4, atomare Heilung) -- ScheduleStoreContract:
# 24. ``replan_if_unchanged`` schreibt Definition und Score, wenn die
#     gespeicherte Definition die gelesene ist und kein Score existiert.
# 25. ... ändert nichts, wenn sie inzwischen deaktiviert/geändert wurde.
# 26. ... ändert nichts, wenn inzwischen ein Score existiert.
# 27. ... ändert nichts, wenn der Eintrag inzwischen entfernt wurde.

# Test list (Plan 2026-10-08-1541, U10, KTD8) -- ScheduleStoreContract:
# 28. ``prune_cron`` mit Frist entfernt Definition und Fire-Index-Eintrag,
#     wenn jede Meldung des Namens älter als die Frist ist (Backend-Zeit).
# 29. ... lässt sie stehen, wenn eine Meldung des Namens frischer ist.
# 30. ... lässt sie stehen, wenn sich die Definition seit dem Lesen geändert
#     hat oder es keine Meldung gibt.
# 31. Ohne Frist (nie gemeldet) entfernt ``prune_cron`` nur, wenn es keine
#     Meldung des Namens gibt; Meldungen anderer Namen (auch mit ``|``)
#     zählen nicht.
# 32. ``remember_disabled`` merkt den Namen (``pruned_disabled``);
#     ``forget_pruned_disabled`` vergisst ihn.

from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable

import pytest

from swarmq.contracts import ChordStore, CronReport, LeaderInfo, LockStore, LockToken, RateLimitStore, ScheduleStore
from tests.contracts._support import Clock

Leader = Callable[[str], Awaitable[None]]


def _name(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


@pytest.mark.timeout(5)
class LockStoreContract:
    async def test_acquire_returns_token_and_blocks_second_owner(self, impl: LockStore) -> None:
        name = _name("lock")
        token = await impl.acquire(name, owner="a", ttl_ms=60_000, max_concurrency=1)
        assert token == LockToken(name=name, owner="a", max_concurrency=1)
        assert await impl.acquire(name, owner="b", ttl_ms=60_000, max_concurrency=1) is None
        assert await impl.current_owner(name) == "a"

    @pytest.mark.entscheidung("E-022")
    async def test_acquire_repeated_by_same_owner_returns_token_and_keeps_pttl(
        self,
        impl: LockStore,
        clock: Clock,
    ) -> None:
        """FS-06b (KTD7): die Wiederholung nach verlorener Antwort trifft den
        eigenen Mutex und gewinnt; die Frist wird nicht neu gesetzt (E-022)."""
        name = _name("lock")
        token = await impl.acquire(name, owner="a", ttl_ms=60_000, max_concurrency=1)
        await clock.advance(0.05)
        before = (await impl.observe(name, max_concurrency=1)).pttl_ms

        again = await impl.acquire(name, owner="a", ttl_ms=60_000, max_concurrency=1)

        assert again == token
        assert (await impl.observe(name, max_concurrency=1)).pttl_ms <= before
        assert await impl.acquire(name, owner="b", ttl_ms=60_000, max_concurrency=1) is None

    @pytest.mark.entscheidung("E-022")
    async def test_semaphore_acquire_repeated_by_same_owner_returns_token_and_keeps_pttl(
        self,
        impl: LockStore,
        clock: Clock,
    ) -> None:
        """FS-06b (KTD7): wie beim Mutex, der eigene Slot gilt als erworben,
        ohne die Frist des Keys neu zu setzen (E-022)."""
        name = _name("sem")
        token = await impl.acquire(name, owner="a", ttl_ms=60_000, max_concurrency=2)
        await clock.advance(0.05)
        before = (await impl.observe(name, max_concurrency=2)).pttl_ms

        again = await impl.acquire(name, owner="a", ttl_ms=60_000, max_concurrency=2)

        assert again == token
        assert (await impl.observe(name, max_concurrency=2)).pttl_ms <= before

    @pytest.mark.entscheidung("E-022")
    async def test_full_semaphore_including_own_token_returns_token_on_repeat(self, impl: LockStore) -> None:
        """FS-06b: ist der Semaphor samt eigenem Slot voll, liefert die
        Wiederholung des eigenen Aufrufs trotzdem das Token; ein fremder
        Owner bleibt abgewiesen."""
        name = _name("sem")
        token = await impl.acquire(name, owner="a", ttl_ms=60_000, max_concurrency=2)
        assert await impl.acquire(name, owner="b", ttl_ms=60_000, max_concurrency=2) is not None

        assert await impl.acquire(name, owner="a", ttl_ms=60_000, max_concurrency=2) == token
        assert await impl.acquire(name, owner="c", ttl_ms=60_000, max_concurrency=2) is None

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

    async def test_extend_keeps_the_mutex_for_the_owner_only(self, impl: LockStore, clock: Clock) -> None:
        """BZ-25 (Plan 0.12.0, MR 13): ``extend`` verlaengert einen gehaltenen
        Mutex nur fuer seinen Halter."""
        name = _name("lock")
        token = await impl.acquire(name, owner="a", ttl_ms=300, max_concurrency=1)
        assert token is not None
        forged = LockToken(name=name, owner="b", max_concurrency=1)
        assert await impl.extend(forged, ttl_ms=60_000) is False
        assert await impl.extend(token, ttl_ms=60_000) is True
        await clock.advance(0.8)
        assert await impl.current_owner(name) == "a"

    async def test_extend_keeps_a_semaphore_slot_and_never_shortens(self, impl: LockStore, clock: Clock) -> None:
        name = _name("sem")
        a = await impl.acquire(name, owner="a", ttl_ms=300, max_concurrency=2)
        assert a is not None
        assert await impl.extend(LockToken(name=name, owner="x", max_concurrency=2), ttl_ms=60_000) is False
        assert await impl.extend(a, ttl_ms=60_000) is True
        assert await impl.extend(a, ttl_ms=100) is True  # kuerzer: bleibt bei 60 s
        await clock.advance(0.8)
        assert await impl.acquire(name, owner="b", ttl_ms=60_000, max_concurrency=2) is not None
        assert await impl.acquire(name, owner="c", ttl_ms=60_000, max_concurrency=2) is None  # a haelt noch
        assert await impl.release(a) is True

    async def test_extend_after_expiry_fails(self, impl: LockStore, clock: Clock) -> None:
        name = _name("lock")
        token = await impl.acquire(name, owner="a", ttl_ms=300, max_concurrency=1)
        assert token is not None
        await clock.advance(0.8)
        assert await impl.extend(token, ttl_ms=60_000) is False

    async def test_acquire_with_a_shorter_ttl_keeps_the_longer_semaphore_holder(
        self,
        impl: LockStore,
        clock: Clock,
    ) -> None:
        """Rev. 3 F6 (MR 13): ein Erwerber mit kurzer TTL verkuerzt den Key
        nicht unter einem Langlaeufer weg."""
        name = _name("sem")
        a = await impl.acquire(name, owner="a", ttl_ms=3_600_000, max_concurrency=2)
        assert a is not None
        assert await impl.acquire(name, owner="b", ttl_ms=300, max_concurrency=2) is not None
        await clock.advance(0.8)
        assert await impl.release(a) is True, "a ist nach dem kurzen Erwerber b nicht mehr Halter"

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

    async def test_force_release_of_an_expired_mutex_is_false(self, impl: LockStore, clock: Clock) -> None:
        """Plan 0.14, U1 (KTD1): ``True`` heisst „ein Lock existierte und ist weg“."""
        name = _name("lock")
        await impl.acquire(name, owner="a", ttl_ms=300, max_concurrency=1)
        await clock.advance(0.8)
        assert await impl.force_release(name, max_concurrency=1) is False

    async def test_force_release_clears_a_held_semaphore(self, impl: LockStore) -> None:
        name = _name("sem")
        await impl.acquire(name, owner="a", ttl_ms=60_000, max_concurrency=2)
        await impl.acquire(name, owner="b", ttl_ms=60_000, max_concurrency=2)
        assert await impl.force_release(name, max_concurrency=2) is True
        assert (await impl.observe(name, max_concurrency=2)).owners == frozenset()
        assert await impl.force_release(name, max_concurrency=2) is False

    async def test_semaphore_admits_up_to_max_concurrency(self, impl: LockStore) -> None:
        name = _name("sem")
        t1 = await impl.acquire(name, owner="a", ttl_ms=60_000, max_concurrency=2)
        t2 = await impl.acquire(name, owner="b", ttl_ms=60_000, max_concurrency=2)
        assert t1 is not None
        assert t2 is not None
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

    async def test_observe_reports_holders_and_remaining_ttl(self, impl: LockStore) -> None:
        """BZ-05 (Plan 0.12.0, MR 13): ein Wartender liest die Halter (Fingerprint)
        und die Rest-TTL des Keys; -2 ohne Key."""
        mutex, sem = _name("lock"), _name("sem")
        await impl.acquire(mutex, owner="a", ttl_ms=60_000, max_concurrency=1)
        await impl.acquire(sem, owner="a", ttl_ms=60_000, max_concurrency=3)
        await impl.acquire(sem, owner="b", ttl_ms=60_000, max_concurrency=3)

        seen = await impl.observe(mutex, max_concurrency=1)
        assert seen.owners == frozenset({"a"})
        assert 0 < seen.pttl_ms <= 60_000
        seen = await impl.observe(sem, max_concurrency=3)
        assert seen.owners == frozenset({"a", "b"})
        assert 0 < seen.pttl_ms <= 60_000
        gone = await impl.observe(_name("lock"), max_concurrency=1)
        assert gone.owners == frozenset()
        assert gone.pttl_ms == -2


@pytest.mark.timeout(5)
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


@pytest.mark.timeout(5)
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


@pytest.mark.timeout(5)
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
        assert [n for n, _, _ in await impl.poll_due(2_000)].count(name) == 1

    async def test_fire_ts_reports_index_score(self, impl: ScheduleStore) -> None:
        planned, disabled = _name("sched"), _name("sched")
        await impl.add(planned, b"v1", fire_ts_ms=1_000)
        await impl.add(disabled, b"off", fire_ts_ms=None)
        assert await impl.fire_ts(planned) == 1_000
        assert await impl.fire_ts(disabled) is None
        assert await impl.fire_ts(_name("sched")) is None

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
        assert listing[a] == b"A"
        assert listing[b] == b"B"

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

    async def test_poll_due_drops_disabled_entries_before_the_limit(self, impl: ScheduleStore) -> None:
        """Liste 33 (BZ-28)."""
        off, on = _name("sched"), _name("sched")
        await impl.put(off, b'{"disabled":true}', fire_ts_ms=100)
        await impl.put(on, b'{"disabled":false}', fire_ts_ms=200)
        assert [n for n, _, _ in await impl.poll_due(1_000, limit=1)] == [on]
        assert await impl.fire_ts(off) is None
        assert await impl.get(off) == b'{"disabled":true}'

    async def test_disable_replaces_definition_and_removes_from_index(self, impl: ScheduleStore) -> None:
        name = _name("sched")
        await impl.add(name, b"on", fire_ts_ms=1_000)
        assert await impl.disable(name, b"off") is True
        assert await impl.get(name) == b"off"
        assert all(n != name for n, _, _ in await impl.poll_due(10_000))
        assert await impl.disable(_name("sched"), b"x") is False

    async def test_replan_if_unchanged_plans_an_unchanged_unscored_entry(self, impl: ScheduleStore) -> None:
        name = _name("sched")
        await impl.add(name, b"stuck", fire_ts_ms=None)
        assert await impl.replan_if_unchanged(name, b"stuck", b"healed", 4_000) is True
        assert await impl.get(name) == b"healed"
        assert await impl.fire_ts(name) == 4_000

    async def test_replan_if_unchanged_keeps_a_disable_since_the_read(self, impl: ScheduleStore) -> None:
        name = _name("sched")
        await impl.add(name, b"stuck", fire_ts_ms=None)
        await impl.disable(name, b"off")
        assert await impl.replan_if_unchanged(name, b"stuck", b"healed", 4_000) is False
        assert await impl.get(name) == b"off"
        assert await impl.fire_ts(name) is None

    async def test_replan_if_unchanged_keeps_a_score_set_since_the_read(self, impl: ScheduleStore) -> None:
        name = _name("sched")
        await impl.add(name, b"stuck", fire_ts_ms=None)
        await impl.put(name, b"stuck", fire_ts_ms=2_000)
        assert await impl.replan_if_unchanged(name, b"stuck", b"healed", 4_000) is False
        assert await impl.get(name) == b"stuck"
        assert await impl.fire_ts(name) == 2_000

    async def test_replan_if_unchanged_does_not_recreate_a_removed_entry(self, impl: ScheduleStore) -> None:
        name = _name("sched")
        await impl.add(name, b"stuck", fire_ts_ms=None)
        await impl.remove(name)
        assert await impl.replan_if_unchanged(name, b"stuck", b"healed", 4_000) is False
        assert await impl.exists(name) is False
        assert await impl.fire_ts(name) is None

    async def test_prune_cron_removes_a_cron_whose_reports_are_all_stale(
        self,
        impl: ScheduleStore,
        clock: Clock,
    ) -> None:
        name = _name("cron")
        await impl.add(name, b"def", fire_ts_ms=1_000)
        await impl.report_cron(name, "fp1", b"def", live_window_ms=60_000)
        await impl.report_cron(name, "fp2", b"def", live_window_ms=60_000)
        await clock.advance(0.2)
        assert await impl.prune_cron(name, b"def", stale_after_ms=100, remember_disabled=False) is True
        assert await impl.exists(name) is False
        assert await impl.fire_ts(name) is None

    async def test_prune_cron_keeps_a_cron_with_one_fresh_report(self, impl: ScheduleStore, clock: Clock) -> None:
        """Rolling Deploy: der alte Abdruck ist verstummt, der neue meldet."""
        name = _name("cron")
        await impl.add(name, b"def", fire_ts_ms=1_000)
        await impl.report_cron(name, "old", b"def", live_window_ms=60_000)
        await clock.advance(0.2)
        await impl.report_cron(name, "new", b"def", live_window_ms=60_000)
        assert await impl.prune_cron(name, b"def", stale_after_ms=100, remember_disabled=False) is False
        assert await impl.get(name) == b"def"
        assert await impl.fire_ts(name) == 1_000

    async def test_prune_cron_keeps_a_changed_or_unreported_cron(self, impl: ScheduleStore, clock: Clock) -> None:
        changed, unreported = _name("cron"), _name("cron")
        await impl.add(changed, b"new", fire_ts_ms=1_000)
        await impl.add(unreported, b"def", fire_ts_ms=1_000)
        await impl.report_cron(changed, "fp", b"def", live_window_ms=60_000)
        await clock.advance(0.2)
        assert await impl.prune_cron(changed, b"old", stale_after_ms=100, remember_disabled=False) is False
        assert await impl.prune_cron(unreported, b"def", stale_after_ms=100, remember_disabled=False) is False
        assert await impl.get(changed) == b"new"
        assert await impl.get(unreported) == b"def"

    async def test_prune_cron_without_period_removes_only_unreported_crons(self, impl: ScheduleStore) -> None:
        orphan, reported = _name("cron"), _name("cron")
        await impl.add(orphan, b"def", fire_ts_ms=1_000)
        await impl.add(reported, b"def", fire_ts_ms=1_000)
        await impl.report_cron(reported, "fp", b"def", live_window_ms=60_000)
        # Ein anderer Name, der mit ``<orphan>|`` beginnt, ist keine Meldung von orphan.
        await impl.report_cron(f"{orphan}|x", "fp", b"def", live_window_ms=60_000)
        assert await impl.prune_cron(reported, b"def", stale_after_ms=None, remember_disabled=False) is False
        assert await impl.prune_cron(orphan, b"def", stale_after_ms=None, remember_disabled=False) is True
        assert await impl.exists(orphan) is False
        assert await impl.fire_ts(orphan) is None
        assert await impl.get(reported) == b"def"

    async def test_prune_cron_remembers_a_disabled_cron(self, impl: ScheduleStore) -> None:
        disabled, enabled, kept = _name("cron"), _name("cron"), _name("cron")
        for name in (disabled, enabled, kept):
            await impl.add(name, b"def", fire_ts_ms=None)
        await impl.report_cron(kept, "fp", b"def", live_window_ms=60_000)
        assert await impl.prune_cron(disabled, b"def", stale_after_ms=None, remember_disabled=True) is True
        assert await impl.prune_cron(enabled, b"def", stale_after_ms=None, remember_disabled=False) is True
        assert await impl.prune_cron(kept, b"def", stale_after_ms=None, remember_disabled=True) is False
        assert await impl.pruned_disabled(disabled) is True
        assert await impl.pruned_disabled(enabled) is False
        assert await impl.pruned_disabled(kept) is False
        await impl.forget_pruned_disabled(disabled)
        assert await impl.pruned_disabled(disabled) is False

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

    async def test_commit_fire_discards_when_disabled_since_poll(self, impl: ScheduleStore, leader: Leader) -> None:
        name = _name("sched")
        await impl.add(name, b"v1", fire_ts_ms=1_000)
        await leader("chief")
        await impl.disable(name, b"off")
        assert (
            await impl.commit_fire(name, next_fire_ts_ms=2_000, payload=b"v2", owner="chief", fired_ts_ms=1_000) is True
        )
        assert await impl.get(name) == b"off"
        assert all(n != name for n, _, _ in await impl.poll_due(10_000))

    async def test_commit_fire_discards_when_removed_since_poll(self, impl: ScheduleStore, leader: Leader) -> None:
        name = _name("sched")
        await impl.add(name, b"v1", fire_ts_ms=1_000)
        await leader("chief")
        await impl.remove(name)
        assert (
            await impl.commit_fire(name, next_fire_ts_ms=2_000, payload=b"v2", owner="chief", fired_ts_ms=1_000) is True
        )
        assert await impl.exists(name) is False
        assert all(n != name for n, _, _ in await impl.poll_due(10_000))

    async def test_commit_fire_discards_when_replanned_since_poll(self, impl: ScheduleStore, leader: Leader) -> None:
        name = _name("sched")
        await impl.add(name, b"v1", fire_ts_ms=1_000)
        await leader("chief")
        await impl.put(name, b"replanned", fire_ts_ms=5_000)
        assert (
            await impl.commit_fire(name, next_fire_ts_ms=2_000, payload=b"v2", owner="chief", fired_ts_ms=1_000) is True
        )
        assert await impl.get(name) == b"replanned"
        assert {n: ts for n, ts, _ in await impl.poll_due(10_000)}[name] == 5_000

    async def test_commit_fire_applies_when_fired_score_unchanged(self, impl: ScheduleStore, leader: Leader) -> None:
        name = _name("sched")
        await impl.add(name, b"v1", fire_ts_ms=1_000)
        await leader("chief")
        assert (
            await impl.commit_fire(name, next_fire_ts_ms=2_000, payload=b"v2", owner="chief", fired_ts_ms=1_000) is True
        )
        assert await impl.get(name) == b"v2"
        assert {n: ts for n, ts, _ in await impl.poll_due(10_000)}[name] == 2_000

    async def test_commit_fire_keeps_definition_changed_since_poll(
        self,
        impl: ScheduleStore,
        leader: Leader,
    ) -> None:
        """Liste 12: eine Uebernahme zwischen Poll und Commit (Score blieb
        faellig) wird nicht zurueckgesetzt; der Score rueckt trotzdem vor,
        sonst feuerte der Termin ein zweites Mal."""
        name = _name("sched")
        await impl.add(name, b"v1", fire_ts_ms=1_000)
        await leader("chief")
        await impl.put(name, b"taken-over", fire_ts_ms=None)
        assert (
            await impl.commit_fire(
                name,
                next_fire_ts_ms=2_000,
                payload=b"v2",
                owner="chief",
                fired_ts_ms=1_000,
                fired_payload=b"v1",
            )
            is True
        )
        assert await impl.get(name) == b"taken-over"
        assert await impl.fire_ts(name) == 2_000

    async def test_commit_fire_writes_back_unchanged_polled_definition(
        self,
        impl: ScheduleStore,
        leader: Leader,
    ) -> None:
        """Liste 13."""
        name = _name("sched")
        await impl.add(name, b"v1", fire_ts_ms=1_000)
        await leader("chief")
        await impl.commit_fire(
            name,
            next_fire_ts_ms=2_000,
            payload=b"v2",
            owner="chief",
            fired_ts_ms=1_000,
            fired_payload=b"v1",
        )
        assert await impl.get(name) == b"v2"
        assert await impl.fire_ts(name) == 2_000

    async def test_commit_fire_owner_check_precedes_superseded_check(self, impl: ScheduleStore, leader: Leader) -> None:
        name = _name("sched")
        await impl.add(name, b"v1", fire_ts_ms=1_000)
        await leader("chief")
        await impl.remove(name)
        assert (
            await impl.commit_fire(name, next_fire_ts_ms=2_000, payload=b"v2", owner="impostor", fired_ts_ms=1_000)
            is False
        )

    async def test_commit_fire_cannot_tell_a_recreation_with_the_same_score(
        self,
        impl: ScheduleStore,
        leader: Leader,
    ) -> None:
        """Liste 4b: dokumentierte Grenze des Verwerf-Schutzes (ABA)."""
        name = _name("sched")
        await impl.add(name, b"v1", fire_ts_ms=1_000)
        await leader("chief")
        await impl.remove(name)
        await impl.add(name, b"recreated", fire_ts_ms=1_000)
        assert (
            await impl.commit_fire(name, next_fire_ts_ms=2_000, payload=b"v2", owner="chief", fired_ts_ms=1_000) is True
        )
        assert await impl.get(name) == b"v2"
        assert {n: ts for n, ts, _ in await impl.poll_due(2_000)}[name] == 2_000

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

    async def test_fire_claim_with_owner_blocks_second_claim(self, impl: ScheduleStore) -> None:
        key = _name("fire")
        assert await impl.claim_fire(key, ttl_ms=60_000, owner="a") is True
        assert await impl.claim_fire(key, ttl_ms=60_000, owner="b") is False

    async def test_fire_claim_repeated_by_same_owner_wins(self, impl: ScheduleStore) -> None:
        """Liste 10a (FS-06a): valkey-py wiederholt den Claim nach verlorener
        Antwort; der zweite Lauf trifft den eigenen Claim."""
        key = _name("fire")
        assert await impl.claim_fire(key, ttl_ms=60_000, owner="a") is True
        assert await impl.claim_fire(key, ttl_ms=60_000, owner="a") is True

    async def test_fire_claim_repeated_takeover_wins(self, impl: ScheduleStore) -> None:
        """Liste 10b: die Wiederholung einer Uebernahme trifft den schon
        rotierten Owner."""
        key = _name("fire")
        assert await impl.claim_fire(key, ttl_ms=60_000, owner="a") is True
        assert await impl.claim_fire(key, ttl_ms=60_000, owner="b", takeover="a") is True
        assert await impl.claim_fire(key, ttl_ms=60_000, owner="b", takeover="a") is True

    async def test_fire_claim_redelivery_with_new_owner_stays_duplicate(self, impl: ScheduleStore) -> None:
        """Liste 10c: der Selbsttreffer gilt nur fuer denselben Owner; eine
        Neuzustellung (frischer Owner) bleibt Duplikat."""
        key = _name("fire")
        assert await impl.claim_fire(key, ttl_ms=60_000, owner="a") is True
        assert await impl.claim_fire(key, ttl_ms=60_000, owner="a") is True
        assert await impl.claim_fire(key, ttl_ms=60_000, owner="b") is False

    async def test_fire_claim_takeover_with_stored_token_rotates_owner(self, impl: ScheduleStore) -> None:
        """Eine Kopie, die das Token ihres Vorgaengers traegt, uebernimmt den
        Claim; eine Zustellung mit dem alten Token (Broker-Redelivery der
        Kopie) ist danach wieder ein Duplikat."""
        key = _name("fire")
        assert await impl.claim_fire(key, ttl_ms=60_000, owner="a") is True
        assert await impl.claim_fire(key, ttl_ms=60_000, owner="b", takeover="a") is True
        assert await impl.claim_fire(key, ttl_ms=60_000, owner="c", takeover="a") is False

    async def test_fire_claim_takeover_accepts_any_offered_token(self, impl: ScheduleStore) -> None:
        key = _name("fire")
        assert await impl.claim_fire(key, ttl_ms=60_000, owner="a") is True
        assert await impl.claim_fire(key, ttl_ms=60_000, owner="c", takeover=("b", "a")) is True
        assert await impl.claim_fire(key, ttl_ms=60_000, owner="d", takeover=("b", "a")) is False
        assert await impl.claim_fire(key, ttl_ms=60_000, owner="e", takeover=("c",)) is True

    async def test_fire_claim_takeover_with_foreign_token_loses(self, impl: ScheduleStore) -> None:
        key = _name("fire")
        assert await impl.claim_fire(key, ttl_ms=60_000, owner="a") is True
        assert await impl.claim_fire(key, ttl_ms=60_000, owner="b", takeover="x") is False

    async def test_fire_claim_takeover_renews_ttl(self, impl: ScheduleStore, clock: Clock) -> None:
        key = _name("fire")
        await impl.claim_fire(key, ttl_ms=300, owner="a")
        assert await impl.claim_fire(key, ttl_ms=60_000, owner="b", takeover="a") is True
        await clock.advance(0.8)
        assert await impl.claim_fire(key, ttl_ms=60_000, owner="c") is False

    async def test_release_fire_claim_checks_owner(self, impl: ScheduleStore) -> None:
        key = _name("fire")
        await impl.claim_fire(key, ttl_ms=60_000, owner="a")
        await impl.release_fire_claim(key, owner="x")
        assert await impl.claim_fire(key, ttl_ms=60_000, owner="b") is False
        await impl.release_fire_claim(key, owner="a")
        assert await impl.claim_fire(key, ttl_ms=60_000, owner="b") is True

    async def test_fire_claim_expires(self, impl: ScheduleStore, clock: Clock) -> None:
        key = _name("fire")
        await impl.claim_fire(key, ttl_ms=300)
        await clock.advance(0.8)
        assert await impl.claim_fire(key, ttl_ms=60_000) is True

    @staticmethod
    async def _reports_of(impl: ScheduleStore, name: str) -> list[CronReport]:
        snapshot = await impl.cron_reports()
        return [r for r in snapshot.reports if r.name == name]

    async def test_report_cron_records_a_report_in_backend_time(self, impl: ScheduleStore) -> None:
        name = _name("cron")
        await impl.report_cron(name, "fp1", b"def-1", live_window_ms=60_000)
        snapshot = await impl.cron_reports()
        [report] = [r for r in snapshot.reports if r.name == name]
        assert (report.fingerprint, report.payload) == ("fp1", b"def-1")
        assert report.first_seen_ms == report.last_seen_ms <= snapshot.now_ms

    async def test_report_within_live_window_keeps_first_seen(self, impl: ScheduleStore, clock: Clock) -> None:
        name = _name("cron")
        await impl.report_cron(name, "fp1", b"def-1", live_window_ms=60_000)
        [first] = await self._reports_of(impl, name)
        await clock.advance(0.05)
        await impl.report_cron(name, "fp1", b"def-2", live_window_ms=60_000)
        [again] = await self._reports_of(impl, name)
        assert again.first_seen_ms == first.first_seen_ms
        assert again.last_seen_ms > first.last_seen_ms
        assert again.payload == b"def-2"

    async def test_report_after_dead_window_resets_first_seen(self, impl: ScheduleStore, clock: Clock) -> None:
        name = _name("cron")
        await impl.report_cron(name, "fp1", b"def", live_window_ms=100)
        [first] = await self._reports_of(impl, name)
        await clock.advance(0.2)
        await impl.report_cron(name, "fp1", b"def", live_window_ms=100)
        [revived] = await self._reports_of(impl, name)
        assert revived.first_seen_ms == revived.last_seen_ms > first.first_seen_ms

    async def test_fingerprints_of_one_name_are_separate_reports(self, impl: ScheduleStore) -> None:
        name = _name("cron|with-bar")
        await impl.report_cron(name, "fp1", b"old", live_window_ms=60_000)
        await impl.report_cron(name, "fp2", b"new", live_window_ms=60_000)
        reports = await self._reports_of(impl, name)
        assert sorted((r.fingerprint, r.payload) for r in reports) == [("fp1", b"old"), ("fp2", b"new")]

    async def test_prune_cron_reports_deletes_only_stale_reports(self, impl: ScheduleStore, clock: Clock) -> None:
        stale, fresh = _name("cron"), _name("cron")
        await impl.report_cron(stale, "fp", b"s", live_window_ms=60_000)
        await clock.advance(0.2)
        await impl.report_cron(fresh, "fp", b"f", live_window_ms=60_000)
        await impl.prune_cron_reports(max_age_ms=100)
        assert await self._reports_of(impl, stale) == []
        assert [r.payload for r in await self._reports_of(impl, fresh)] == [b"f"]

    async def test_epoch_is_created_once_and_then_kept(self, impl: ScheduleStore) -> None:
        # Der Epoch ist ein einzelner Key je Valkey: ein frueherer Test oder
        # Worker kann ihn schon angelegt haben.
        candidate = _name("epoch")
        previous = await impl.ensure_epoch(candidate)
        current = candidate if previous is None else previous
        assert await impl.ensure_epoch(_name("epoch")) == current
        assert await impl.ensure_epoch(_name("epoch")) == current

    async def test_report_queues_records_each_queue_in_backend_time(self, impl: ScheduleStore) -> None:
        a, b = _name("queue"), _name("queue")
        await impl.report_queues([a, b], ttl_ms=60_000)
        snapshot = await impl.queue_reports()
        assert snapshot.last_seen_ms[a] == snapshot.last_seen_ms[b] <= snapshot.now_ms

    async def test_later_queue_report_advances_last_seen(self, impl: ScheduleStore, clock: Clock) -> None:
        queue = _name("queue")
        await impl.report_queues([queue], ttl_ms=60_000)
        first = (await impl.queue_reports()).last_seen_ms[queue]
        await clock.advance(0.05)
        await impl.report_queues([queue], ttl_ms=60_000)
        assert (await impl.queue_reports()).last_seen_ms[queue] > first

    async def test_queue_reports_expire_without_a_report_within_ttl(self, impl: ScheduleStore, clock: Clock) -> None:
        queue = _name("queue")
        await impl.report_queues([queue], ttl_ms=100)
        await clock.advance(0.2)
        assert queue not in (await impl.queue_reports()).last_seen_ms

    async def test_forget_queue_reports_removes_only_the_named_queues(self, impl: ScheduleStore) -> None:
        gone, kept = _name("queue"), _name("queue")
        await impl.report_queues([gone, kept], ttl_ms=60_000)
        await impl.forget_queue_reports([gone])
        reported = (await impl.queue_reports()).last_seen_ms
        assert gone not in reported
        assert kept in reported

    async def test_leader_info_without_version_key(self, impl: ScheduleStore, leader: Leader) -> None:
        owner = _name("old-leader")
        await leader(owner)
        assert await impl.leader_info() == LeaderInfo(owner=owner, version=None)

    async def test_leader_version_of_the_lock_owner(self, impl: ScheduleStore, leader: Leader) -> None:
        owner = _name("leader")
        await leader(owner)
        await impl.set_leader_version(owner, "0.10.0", ttl_ms=60_000)
        assert await impl.leader_info() == LeaderInfo(owner=owner, version="0.10.0")

    async def test_leader_version_of_another_owner_does_not_count(self, impl: ScheduleStore, leader: Leader) -> None:
        predecessor, owner = _name("leader"), _name("old-leader")
        await impl.set_leader_version(predecessor, "0.10.0", ttl_ms=60_000)
        await leader(owner)
        assert await impl.leader_info() == LeaderInfo(owner=owner, version=None)

    async def test_leader_version_expires(self, impl: ScheduleStore, leader: Leader, clock: Clock) -> None:
        owner = _name("leader")
        await leader(owner)
        await impl.set_leader_version(owner, "0.10.0", ttl_ms=100)
        await clock.advance(0.2)
        assert await impl.leader_info() == LeaderInfo(owner=owner, version=None)

    async def test_worker_versions_are_reported_until_their_ttl(self, impl: ScheduleStore, clock: Clock) -> None:
        stale, fresh = _name("host:1"), _name("host:2")
        await impl.report_worker_version(stale, "0.10.0", ttl_ms=100)
        await impl.report_worker_version(fresh, "0.10.1", ttl_ms=60_000)
        assert {stale: "0.10.0", fresh: "0.10.1"}.items() <= dict(await impl.worker_versions()).items()
        await clock.advance(0.2)
        versions = await impl.worker_versions()
        assert stale not in versions
        assert versions[fresh] == "0.10.1"

    async def test_handover_marker_is_taken_once(self, impl: ScheduleStore) -> None:
        assert await impl.take_handover_marker() is False
        await impl.set_handover_marker(_name("owner"), ttl_ms=60_000)
        assert await impl.take_handover_marker() is True
        assert await impl.take_handover_marker() is False

    async def test_report_cron_remembers_the_name_as_known(self, impl: ScheduleStore) -> None:
        name = _name("cron")
        await impl.report_cron(name, "fp1", b"def", live_window_ms=60_000)
        assert name in await impl.known_cron_names()

    async def test_pruning_reports_keeps_the_known_name(self, impl: ScheduleStore, clock: Clock) -> None:
        name = _name("cron")
        await impl.report_cron(name, "fp1", b"def", live_window_ms=60_000)
        await clock.advance(0.2)
        await impl.prune_cron_reports(max_age_ms=100)
        assert await self._reports_of(impl, name) == []
        assert name in await impl.known_cron_names()

    async def test_prune_cron_forgets_the_known_name_with_the_definition(
        self, impl: ScheduleStore, clock: Clock
    ) -> None:
        name = _name("cron")
        await impl.put(name, b"def", 1_000)
        await impl.report_cron(name, "fp1", b"def", live_window_ms=60_000)
        await clock.advance(0.2)
        assert await impl.prune_cron(name, b"def", stale_after_ms=100, remember_disabled=False) is True
        assert name not in await impl.known_cron_names()

    async def test_forget_known_cron_removes_the_name(self, impl: ScheduleStore) -> None:
        name, other = _name("cron"), _name("cron")
        await impl.report_cron(name, "fp1", b"def", live_window_ms=60_000)
        await impl.report_cron(other, "fp1", b"def", live_window_ms=60_000)
        await impl.forget_known_cron(name)
        known = await impl.known_cron_names()
        assert name not in known
        assert other in known
