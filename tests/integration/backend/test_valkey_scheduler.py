"""Integration tests for scheduler-persistence ops on ValkeyBackend.

Runs the Python wrappers against a real Valkey so we see what the
scheduler actually writes/reads. Uses the shared conftest fixtures for
dev-stack env-var port awareness.
"""

from __future__ import annotations

import asyncio
import uuid

import pytest

from swarmq.backend.valkey import ValkeyBackend

pytestmark = pytest.mark.integration


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


class TestZsetRoundtrip:
    async def test_zadd_then_zrangebyscore_returns_member(
        self, backend, schedule_name
    ):
        await backend.zadd_schedule(schedule_name, 1_700_000_000_000)
        due = await backend.zrangebyscore_schedules(1_700_000_000_000, 100)
        assert schedule_name.encode() in due

    async def test_zrangebyscore_excludes_future(self, backend, schedule_name):
        # fire_ts = 1_700_000_000_000, ask for anything due by t-1
        await backend.zadd_schedule(schedule_name, 1_700_000_000_000)
        due = await backend.zrangebyscore_schedules(
            1_700_000_000_000 - 1, 100
        )
        assert schedule_name.encode() not in due

    async def test_zrem_removes_member(self, backend, schedule_name):
        await backend.zadd_schedule(schedule_name, 1_700_000_000_000)
        await backend.zrem_schedule(schedule_name)
        due = await backend.zrangebyscore_schedules(1_700_000_000_000, 100)
        assert schedule_name.encode() not in due

    async def test_zrangebyscore_respects_limit(self, backend):
        # Insert 5 schedules, ask for only 3 back
        base_ts = 1_700_000_000_000
        names = [f"lim-{i}-{uuid.uuid4().hex[:6]}" for i in range(5)]
        for i, name in enumerate(names):
            await backend.zadd_schedule(name, base_ts + i)
        due = await backend.zrangebyscore_schedules(base_ts + 10, 3)
        assert len(due) == 3


class TestDefsRoundtrip:
    async def test_hset_then_hget_returns_value(self, backend, schedule_name):
        payload = b'{"cron":"* * * * *","task":"ping"}'
        await backend.hset_schedule_def(schedule_name, payload)
        result = await backend.hget_schedule_def(schedule_name)
        assert result == payload

    async def test_hget_missing_returns_none(self, backend, schedule_name):
        assert await backend.hget_schedule_def(schedule_name) is None

    async def test_hdel_removes_key(self, backend, schedule_name):
        await backend.hset_schedule_def(schedule_name, b"payload")
        await backend.hdel_schedule_def(schedule_name)
        assert await backend.hget_schedule_def(schedule_name) is None

    async def test_hgetall_returns_all_defs(self, backend):
        names = [f"all-{i}-{uuid.uuid4().hex[:6]}" for i in range(3)]
        for name in names:
            await backend.hset_schedule_def(name, b"def")
        all_defs = await backend.hgetall_schedule_defs()
        for name in names:
            assert name.encode() in all_defs


class TestSchedfireDedup:
    async def test_first_setnx_wins(self, backend, schedule_name):
        key = f"{schedule_name}:1700000000000"
        assert await backend.set_schedfire_dedup(key, 60_000) is True

    async def test_second_setnx_loses(self, backend, schedule_name):
        key = f"{schedule_name}:1700000000000"
        assert await backend.set_schedfire_dedup(key, 60_000) is True
        assert await backend.set_schedfire_dedup(key, 60_000) is False

    async def test_ttl_expiry_releases_key(self, backend, schedule_name):
        key = f"{schedule_name}:1700000000000"
        # Very short TTL — within pytest-timeout, wait for expiry
        assert await backend.set_schedfire_dedup(key, 200) is True
        await asyncio.sleep(0.5)
        # After expiry the key is gone, SETNX succeeds again
        assert await backend.set_schedfire_dedup(key, 60_000) is True


class TestHandoverMarker:
    async def test_get_and_del_returns_false_when_no_markers(self, backend):
        assert await backend.get_and_del_schedfire_handover_marker() is False

    async def test_set_then_get_and_del_returns_true(self, backend):
        token = uuid.uuid4().hex
        await backend.set_schedfire_handover_marker(token, 5_000)
        assert await backend.get_and_del_schedfire_handover_marker() is True
        # Second call finds nothing — marker was DELed
        assert await backend.get_and_del_schedfire_handover_marker() is False

    async def test_marker_expires_on_ttl(self, backend):
        token = uuid.uuid4().hex
        await backend.set_schedfire_handover_marker(token, 200)
        await asyncio.sleep(0.5)
        # Expired marker is invisible to the standby — counts as failover
        assert await backend.get_and_del_schedfire_handover_marker() is False

    async def test_get_and_del_clears_multiple_markers(self, backend):
        """If two leaders left markers back-to-back, clean up all of them."""
        for _ in range(3):
            await backend.set_schedfire_handover_marker(
                uuid.uuid4().hex, 5_000
            )
        assert await backend.get_and_del_schedfire_handover_marker() is True
        assert await backend.get_and_del_schedfire_handover_marker() is False
