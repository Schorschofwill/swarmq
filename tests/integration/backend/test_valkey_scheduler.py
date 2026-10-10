"""Integration tests for scheduler-persistence ops on ValkeyBackend.

Runs the Python wrappers against a real Valkey so we see what the
scheduler actually writes/reads. Uses the shared conftest fixtures for
dev-stack env-var port awareness.

Test-Liste Plan 0.12.0, MR 4 (BZ-28, ``sched_poll.lua``):
1. Ein Poll entfernt hoechstens 1000 nie feuernde Eintraege (Waisen,
   deaktivierte) aus dem Fire-Index; der Rest folgt im naechsten Poll
   (Obergrenze der Schleife, Q6).

Test-Liste Plan 0.12.0, MR 4 (BZ-07, Handover ohne SCAN), gegen echtes
Valkey (``GETDEL``):
2. Den Legacy-Marker eines 0.11-Leaders (nur ``sq:sched:handover:<owner>``)
   findet ``take_handover_marker(previous_owner=...)`` genau einmal.
3. Die Uebernahme ueber den festen Key raeumt auch den Legacy-Key des
   Owners ab.
"""

from __future__ import annotations

import asyncio
import uuid

import pytest

from swarmq.backend.valkey import ValkeyBackend
from swarmq.backend.valkey.schedules import ZSET_KEY

pytestmark = [pytest.mark.integration, pytest.mark.timeout(5)]


@pytest.fixture
async def backend(valkey_url):
    b = ValkeyBackend(valkey_url)
    await b.connect()
    try:
        yield b
    finally:
        await b.close()


@pytest.fixture
def schedule_name() -> str:
    """Unique per test so parallel runs do not collide in the shared
    zset/defs-hash — plus we also flush_valkey between tests."""
    return f"test-{uuid.uuid4().hex[:12]}"


class TestPollIndexHygiene:
    async def test_poll_drops_at_most_1000_dead_entries_per_call(self, backend):
        """Liste 1."""
        await backend.client.zadd(ZSET_KEY, {f"orphan_{i}": 1_000 + i for i in range(1_001)})
        await backend.schedules.put("active", b"def", 5_000)

        assert await backend.schedules.poll_due(10_000, limit=100) == []
        assert await backend.client.zcard(ZSET_KEY) == 2
        assert await backend.schedules.poll_due(10_000, limit=100) == [("active", 5_000, b"def")]


class TestZsetRoundtrip:
    # Seit U15 über den ``schedules``-Store-Vertrag: ``put`` indiziert,
    # ``poll_due`` liest fällige Einträge, ``remove`` räumt zset+hash.
    async def test_put_then_poll_due_returns_member(
        self, backend, schedule_name
    ):
        await backend.schedules.put(schedule_name, b"def", 1_700_000_000_000)
        rows = await backend.schedules.poll_due(1_700_000_000_000, limit=100)
        assert schedule_name in [name for name, _, _ in rows]

    async def test_poll_due_excludes_future(self, backend, schedule_name):
        # fire_ts = 1_700_000_000_000, ask for anything due by t-1
        await backend.schedules.put(schedule_name, b"def", 1_700_000_000_000)
        rows = await backend.schedules.poll_due(1_700_000_000_000 - 1, limit=100)
        assert schedule_name not in [name for name, _, _ in rows]

    async def test_remove_clears_index(self, backend, schedule_name):
        await backend.schedules.put(schedule_name, b"def", 1_700_000_000_000)
        await backend.schedules.remove(schedule_name)
        rows = await backend.schedules.poll_due(1_700_000_000_000, limit=100)
        assert schedule_name not in [name for name, _, _ in rows]

    async def test_poll_due_respects_limit(self, backend):
        # Insert 5 schedules, ask for only 3 back
        base_ts = 1_700_000_000_000
        names = [f"lim-{i}-{uuid.uuid4().hex[:6]}" for i in range(5)]
        for i, name in enumerate(names):
            await backend.schedules.put(name, b"def", base_ts + i)
        rows = await backend.schedules.poll_due(base_ts + 10, limit=3)
        assert len(rows) == 3


class TestDefsRoundtrip:
    async def test_hset_then_hget_returns_value(self, backend, schedule_name):
        payload = b'{"cron":"* * * * *","task":"ping"}'
        await backend.schedules.put(schedule_name, payload, None)
        result = await backend.schedules.get(schedule_name)
        assert result == payload

    async def test_hget_missing_returns_none(self, backend, schedule_name):
        assert await backend.schedules.get(schedule_name) is None

    async def test_hdel_removes_key(self, backend, schedule_name):
        await backend.schedules.put(schedule_name, b"payload", None)
        await backend.schedules.remove(schedule_name)
        assert await backend.schedules.get(schedule_name) is None

    async def test_hgetall_returns_all_defs(self, backend):
        names = [f"all-{i}-{uuid.uuid4().hex[:6]}" for i in range(3)]
        for name in names:
            await backend.schedules.put(name, b"def", None)
        all_defs = await backend.schedules.list_all()
        for name in names:
            assert name in all_defs


class TestSchedfireDedup:
    async def test_first_setnx_wins(self, backend, schedule_name):
        key = f"{schedule_name}:1700000000000"
        assert await backend.schedules.mark_fire_completed(key, 60_000) is True

    async def test_second_setnx_loses(self, backend, schedule_name):
        key = f"{schedule_name}:1700000000000"
        assert await backend.schedules.mark_fire_completed(key, 60_000) is True
        assert await backend.schedules.mark_fire_completed(key, 60_000) is False

    async def test_ttl_expiry_releases_key(self, backend, schedule_name):
        key = f"{schedule_name}:1700000000000"
        # Very short TTL — within pytest-timeout, wait for expiry
        assert await backend.schedules.mark_fire_completed(key, 200) is True
        # Real-clock TTL wait, not synchronization — Valkey's expiry
        # is based on wall time and has no observable event.
        await asyncio.sleep(0.5)
        # After expiry the key is gone, SETNX succeeds again
        assert await backend.schedules.mark_fire_completed(key, 60_000) is True

    async def test_clear_inflight_lets_next_caller_reclaim(
        self, backend, schedule_name
    ):
        # P0-B: clearing the in-flight claim (non-terminal outcome) must
        # NOT write the completed marker and must let the next delivery
        # re-claim the slot.
        key = f"{schedule_name}:1700000000000"
        assert await backend.schedules.claim_fire(key, 60_000) is True
        # Second claim loses while the first is still in-flight.
        assert await backend.schedules.claim_fire(key, 60_000) is False
        # Non-terminal finalize: clear the in-flight key.
        await backend.schedules.release_fire_claim(key)
        # The completed marker was NOT written — the fire is not "done".
        assert await backend.schedules.is_fire_completed(key) is False
        # A redelivery / retry can now re-claim and run.
        assert await backend.schedules.claim_fire(key, 60_000) is True

    async def test_clear_inflight_is_idempotent_when_absent(
        self, backend, schedule_name
    ):
        # Deleting a non-existent in-flight key is a harmless no-op
        # (covers the "claim already expired via short TTL" race).
        key = f"{schedule_name}:1700000000000"
        await backend.schedules.release_fire_claim(key)  # must not raise


class TestHandoverMarker:
    async def test_get_and_del_returns_false_when_no_markers(self, backend):
        assert await backend.schedules.take_handover_marker() is False

    async def test_set_then_get_and_del_returns_true(self, backend):
        token = uuid.uuid4().hex
        await backend.schedules.set_handover_marker(token, 5_000)
        assert await backend.schedules.take_handover_marker() is True
        # Second call finds nothing — marker was DELed
        assert await backend.schedules.take_handover_marker() is False

    async def test_marker_expires_on_ttl(self, backend):
        token = uuid.uuid4().hex
        await backend.schedules.set_handover_marker(token, 200)
        # Real-clock TTL wait, not synchronization.
        await asyncio.sleep(0.5)
        # Expired marker is invisible to the standby — counts as failover
        assert await backend.schedules.take_handover_marker() is False

    async def test_takes_legacy_marker_of_previous_owner_once(self, backend):
        """Liste 2."""
        await backend.client.set("sq:sched:handover:old-leader", "1", px=5_000)
        assert await backend.schedules.take_handover_marker() is False
        assert await backend.schedules.take_handover_marker(previous_owner="old-leader") is True
        assert await backend.schedules.take_handover_marker(previous_owner="old-leader") is False

    async def test_take_clears_the_legacy_key_of_the_owner(self, backend):
        """Liste 3."""
        await backend.schedules.set_handover_marker("leader-a", 5_000)
        assert await backend.schedules.take_handover_marker() is True
        assert await backend.client.exists("sq:sched:handover:leader-a") == 0

    async def test_get_and_del_clears_multiple_markers(self, backend):
        """If two leaders left markers back-to-back, clean up all of them."""
        for _ in range(3):
            await backend.schedules.set_handover_marker(
                uuid.uuid4().hex, 5_000
            )
        assert await backend.schedules.take_handover_marker() is True
        assert await backend.schedules.take_handover_marker() is False
