"""Contract-Klasse für ``ExecutionGuardStore`` (Plan 2026-10-08-1541, U8, KTD5).

Fixtures eines Bindings: ``impl`` (der Store, verbunden) und ``clock``.

Test-Liste:
1. Ein Claim auf eine frische Identität gelingt.
2. Ein zweiter Claim, während der erste läuft, meldet ``running``.
3. Nach ``mark_done`` meldet jeder Claim ``done``; der Running-Marker ist weg.
4. Nach ``mark_superseded(n)`` meldet ein Claim mit kleinerem Retry-Zähler
   ``superseded``; die Retry-Kopie mit Zähler ``n`` darf laufen.
5. ``release`` des Owners gibt frei; der nächste Claim ist keine Waise.
6. ``release``/``mark_done`` eines fremden Owners lassen den Running-Marker
   stehen.
7. Läuft der Running-Marker ohne Freigabe ab, gelingt der nächste Claim und
   zählt eine Waise.
8. Ab ``max_orphans`` Waisen meldet der Claim ``orphan_limit``.
9. ``renew`` verlängert nur für den Owner; nach Ablauf liefert es ``False``.
10. Done- und Superseded-Marker laufen nach ``state_ttl_ms`` ab.
11. Der Superseded-Zähler wächst nur.
12. Ein wiederholter Claim desselben Owners (Transport-Retry nach verlorener
    Antwort) ist idempotent: ``claimed``, keine zusätzliche Waise.

Plan 0.12.0, MR 12 (BZ-12):
13. Meldet ein Claim ``running`` (die Zustellung wird zurückgestellt und kann
    hinter einem Rückstand zurückkommen), lebt der Zustand mindestens
    ``backlog_ttl_ms``.
14. ``mark_done``/``mark_superseded`` verkürzen einen längeren Zustand nie.

Review MR 12 (P2-1):
15. ``release`` nach einem ``running``-Claim behält den Rückstand: der
    Zustand verschwindet nicht mit der letzten Lease, der Done-Marker der
    nächsten Kopie lebt den Rückstand.
16. Läuft die Ausführung länger als die Zustands-TTL (Hash schon weg),
    legt der ``running``-Claim den Zustand mit dem Rückstand neu an.
"""

from __future__ import annotations

import uuid

from swarmq.contracts import ExecutionGuardStore, GuardClaim
from tests.contracts._support import Clock

TTL = 60_000
STATE_TTL = 600_000


def _identity() -> str:
    return f"ctr-guard-{uuid.uuid4().hex[:12]}"


async def _claim(
    impl: ExecutionGuardStore,
    identity: str,
    owner: str,
    *,
    retry_count: int = 0,
    ttl_ms: int = TTL,
    max_orphans: int = 3,
) -> str:
    claim = await impl.claim(
        identity,
        owner=owner,
        retry_count=retry_count,
        ttl_ms=ttl_ms,
        state_ttl_ms=STATE_TTL,
        max_orphans=max_orphans,
    )
    return claim.status


class ExecutionGuardStoreContract:
    async def test_claim_on_fresh_identity_succeeds(self, impl: ExecutionGuardStore) -> None:
        assert await _claim(impl, _identity(), "a") == "claimed"

    async def test_repeated_claim_by_the_same_owner_is_idempotent(self, impl: ExecutionGuardStore) -> None:
        identity = _identity()
        await _claim(impl, identity, "a")
        claim = await impl.claim(
            identity,
            owner="a",
            retry_count=0,
            ttl_ms=TTL,
            state_ttl_ms=STATE_TTL,
            max_orphans=3,
        )
        assert claim == GuardClaim("claimed", orphans=0)

    async def test_second_claim_while_running_reports_running(self, impl: ExecutionGuardStore) -> None:
        identity = _identity()
        await _claim(impl, identity, "a")
        assert await _claim(impl, identity, "b") == "running"

    async def test_claim_after_done_reports_done(self, impl: ExecutionGuardStore) -> None:
        identity = _identity()
        await _claim(impl, identity, "a")
        await impl.mark_done(identity, owner="a", state_ttl_ms=STATE_TTL)
        assert await _claim(impl, identity, "b") == "done"
        assert await _claim(impl, identity, "c", retry_count=5) == "done"

    async def test_superseded_skips_older_attempts_and_admits_the_retry_copy(self, impl: ExecutionGuardStore) -> None:
        identity = _identity()
        await _claim(impl, identity, "a")
        await impl.mark_superseded(identity, owner="a", retry_count=1, state_ttl_ms=STATE_TTL)
        assert await _claim(impl, identity, "b", retry_count=0) == "superseded"
        assert await _claim(impl, identity, "c", retry_count=1) == "claimed"

    async def test_superseded_counter_only_grows(self, impl: ExecutionGuardStore) -> None:
        identity = _identity()
        await impl.mark_superseded(identity, owner="a", retry_count=2, state_ttl_ms=STATE_TTL)
        await impl.mark_superseded(identity, owner="a", retry_count=1, state_ttl_ms=STATE_TTL)
        assert await _claim(impl, identity, "b", retry_count=1) == "superseded"

    async def test_release_by_owner_frees_without_counting_an_orphan(self, impl: ExecutionGuardStore) -> None:
        identity = _identity()
        await _claim(impl, identity, "a")
        await impl.release(identity, owner="a")
        claim = await impl.claim(
            identity,
            owner="b",
            retry_count=0,
            ttl_ms=TTL,
            state_ttl_ms=STATE_TTL,
            max_orphans=3,
        )
        assert claim == GuardClaim("claimed", orphans=0)

    async def test_foreign_owner_cannot_release_the_running_marker(self, impl: ExecutionGuardStore) -> None:
        identity = _identity()
        await _claim(impl, identity, "a")
        await impl.release(identity, owner="x")
        assert await _claim(impl, identity, "b") == "running"

    async def test_expired_running_marker_without_release_counts_an_orphan(
        self,
        impl: ExecutionGuardStore,
        clock: Clock,
    ) -> None:
        identity = _identity()
        await _claim(impl, identity, "a", ttl_ms=200)
        await clock.advance(0.4)
        claim = await impl.claim(
            identity,
            owner="b",
            retry_count=0,
            ttl_ms=TTL,
            state_ttl_ms=STATE_TTL,
            max_orphans=3,
        )
        assert claim == GuardClaim("claimed", orphans=1)

    async def test_orphan_limit_is_reported_from_max_orphans_on(self, impl: ExecutionGuardStore, clock: Clock) -> None:
        """Drei Abstürze (Waisen) bei ``max_orphans=3``: die vierte Zustellung
        meldet ``orphan_limit`` -- und hält den Marker, damit ihr Dead-Letter
        ihn abschließt."""
        identity = _identity()
        for owner in ("a", "b", "c"):
            await _claim(impl, identity, owner, ttl_ms=100)
            await clock.advance(0.2)
        claim = await impl.claim(
            identity,
            owner="d",
            retry_count=0,
            ttl_ms=TTL,
            state_ttl_ms=STATE_TTL,
            max_orphans=3,
        )
        assert claim == GuardClaim("orphan_limit", orphans=3)
        assert await _claim(impl, identity, "e") == "running"

    async def test_renew_extends_only_for_the_owner(self, impl: ExecutionGuardStore, clock: Clock) -> None:
        identity = _identity()
        await _claim(impl, identity, "a", ttl_ms=300)
        assert await impl.renew(identity, owner="x", ttl_ms=60_000) is False
        assert await impl.renew(identity, owner="a", ttl_ms=60_000) is True
        await clock.advance(0.5)
        assert await _claim(impl, identity, "b") == "running"

    async def test_renew_after_expiry_fails(self, impl: ExecutionGuardStore, clock: Clock) -> None:
        identity = _identity()
        await _claim(impl, identity, "a", ttl_ms=100)
        await clock.advance(0.3)
        assert await impl.renew(identity, owner="a", ttl_ms=60_000) is False

    async def test_done_and_superseded_markers_expire_after_state_ttl(
        self,
        impl: ExecutionGuardStore,
        clock: Clock,
    ) -> None:
        # MR 12: der Abschluss verkuerzt den Zustand nie -- der Claim setzt
        # hier dieselbe kurze TTL.
        done, superseded = _identity(), _identity()
        await impl.claim(done, owner="a", retry_count=0, ttl_ms=TTL, state_ttl_ms=200, max_orphans=3)
        await impl.mark_done(done, owner="a", state_ttl_ms=200)
        await impl.claim(superseded, owner="a", retry_count=0, ttl_ms=TTL, state_ttl_ms=200, max_orphans=3)
        await impl.mark_superseded(superseded, owner="a", retry_count=1, state_ttl_ms=200)
        await clock.advance(0.4)
        assert await _claim(impl, done, "b") == "claimed"
        assert await _claim(impl, superseded, "b") == "claimed"

    async def test_running_claim_extends_the_state_to_the_backlog_ttl(
        self,
        impl: ExecutionGuardStore,
        clock: Clock,
    ) -> None:
        identity = _identity()
        await impl.claim(identity, owner="a", retry_count=0, ttl_ms=100, state_ttl_ms=200, max_orphans=3)
        claim = await impl.claim(
            identity,
            owner="b",
            retry_count=0,
            ttl_ms=100,
            state_ttl_ms=200,
            max_orphans=3,
            backlog_ttl_ms=5_000,
        )
        assert claim.status == "running"
        await clock.advance(0.3)  # Running-Marker (100 ms) und Zustand (200 ms) waeren weg
        # Der Zustand lebt noch: die Lease von "a" zaehlt als Waise.
        claim = await impl.claim(identity, owner="c", retry_count=0, ttl_ms=TTL, state_ttl_ms=STATE_TTL, max_orphans=3)
        assert claim == GuardClaim("claimed", orphans=1)

    async def test_finish_never_shortens_a_longer_state(self, impl: ExecutionGuardStore, clock: Clock) -> None:
        identity = _identity()
        await impl.claim(identity, owner="a", retry_count=0, ttl_ms=TTL, state_ttl_ms=200, max_orphans=3)
        await impl.claim(
            identity,
            owner="b",
            retry_count=0,
            ttl_ms=TTL,
            state_ttl_ms=200,
            max_orphans=3,
            backlog_ttl_ms=5_000,
        )
        await impl.mark_done(identity, owner="a", state_ttl_ms=200)
        await clock.advance(0.3)  # ueber die Done-TTL des Abschlusses, unter dem Rueckstand
        assert await _claim(impl, identity, "c") == "done"

    async def test_release_after_a_running_claim_keeps_the_backlog(
        self,
        impl: ExecutionGuardStore,
        clock: Clock,
    ) -> None:
        identity = _identity()
        await impl.claim(identity, owner="a", retry_count=0, ttl_ms=TTL, state_ttl_ms=200, max_orphans=3)
        await impl.claim(
            identity,
            owner="b",
            retry_count=0,
            ttl_ms=TTL,
            state_ttl_ms=200,
            max_orphans=3,
            backlog_ttl_ms=5_000,
        )
        await impl.release(identity, owner="a")  # z. B. Lock-Konflikt: Kopie per retry_later
        claim = await impl.claim(identity, owner="a2", retry_count=0, ttl_ms=TTL, state_ttl_ms=200, max_orphans=3)
        assert claim == GuardClaim("claimed", orphans=0)
        await impl.mark_done(identity, owner="a2", state_ttl_ms=200)
        await clock.advance(0.3)  # ueber die Done-TTL, unter dem Rueckstand
        assert await _claim(impl, identity, "c") == "done"

    async def test_running_claim_recreates_an_expired_state_with_the_backlog(
        self,
        impl: ExecutionGuardStore,
        clock: Clock,
    ) -> None:
        identity = _identity()
        await impl.claim(identity, owner="a", retry_count=0, ttl_ms=TTL, state_ttl_ms=200, max_orphans=3)
        await clock.advance(0.3)  # der Lauf dauert laenger als die Zustands-TTL
        claim = await impl.claim(
            identity,
            owner="b",
            retry_count=0,
            ttl_ms=TTL,
            state_ttl_ms=200,
            max_orphans=3,
            backlog_ttl_ms=5_000,
        )
        assert claim.status == "running"
        await impl.mark_done(identity, owner="a", state_ttl_ms=200)
        await clock.advance(0.3)  # ueber die Done-TTL, unter dem Rueckstand
        assert await _claim(impl, identity, "c") == "done"
