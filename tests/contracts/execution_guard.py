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
"""

from __future__ import annotations

import uuid

from swarmq.contracts import ExecutionGuardStore, GuardClaim
from tests.contracts._support import Clock

TTL = 60_000
STATE_TTL = 600_000


def _identity() -> str:
    return f"ctr-guard-{uuid.uuid4().hex[:12]}"


async def _claim(impl: ExecutionGuardStore, identity: str, owner: str, *, retry_count: int = 0,
                 ttl_ms: int = TTL, max_orphans: int = 3) -> str:
    claim = await impl.claim(
        identity, owner=owner, retry_count=retry_count, ttl_ms=ttl_ms,
        state_ttl_ms=STATE_TTL, max_orphans=max_orphans,
    )
    return claim.status


class ExecutionGuardStoreContract:
    async def test_claim_on_fresh_identity_succeeds(self, impl: ExecutionGuardStore) -> None:
        assert await _claim(impl, _identity(), "a") == "claimed"

    async def test_repeated_claim_by_the_same_owner_is_idempotent(self, impl: ExecutionGuardStore) -> None:
        identity = _identity()
        await _claim(impl, identity, "a")
        claim = await impl.claim(
            identity, owner="a", retry_count=0, ttl_ms=TTL, state_ttl_ms=STATE_TTL, max_orphans=3,
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
            identity, owner="b", retry_count=0, ttl_ms=TTL, state_ttl_ms=STATE_TTL, max_orphans=3,
        )
        assert claim == GuardClaim("claimed", orphans=0)

    async def test_foreign_owner_cannot_release_the_running_marker(self, impl: ExecutionGuardStore) -> None:
        identity = _identity()
        await _claim(impl, identity, "a")
        await impl.release(identity, owner="x")
        assert await _claim(impl, identity, "b") == "running"

    async def test_expired_running_marker_without_release_counts_an_orphan(
        self, impl: ExecutionGuardStore, clock: Clock,
    ) -> None:
        identity = _identity()
        await _claim(impl, identity, "a", ttl_ms=200)
        await clock.advance(0.4)
        claim = await impl.claim(
            identity, owner="b", retry_count=0, ttl_ms=TTL, state_ttl_ms=STATE_TTL, max_orphans=3,
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
            identity, owner="d", retry_count=0, ttl_ms=TTL, state_ttl_ms=STATE_TTL, max_orphans=3,
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
        self, impl: ExecutionGuardStore, clock: Clock,
    ) -> None:
        done, superseded = _identity(), _identity()
        await _claim(impl, done, "a")
        await impl.mark_done(done, owner="a", state_ttl_ms=200)
        await _claim(impl, superseded, "a")
        await impl.mark_superseded(superseded, owner="a", retry_count=1, state_ttl_ms=200)
        await clock.advance(0.4)
        assert await _claim(impl, done, "b") == "claimed"
        assert await _claim(impl, superseded, "b") == "claimed"
