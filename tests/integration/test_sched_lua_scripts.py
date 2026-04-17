"""Integration tests for sched_poll.lua + sched_commit.lua.

Raw-Lua tests, no ScheduleStore wrapper yet (that lands in Unit 7).
Uses the shared conftest fixtures for dev-stack env-var port awareness.
"""

from __future__ import annotations

import uuid

import pytest
import valkey.asyncio as valkey_asyncio

from swarmq.lua import load_script
from swarmq.scheduler import SCHEDULER_LOCK_KEY as LOCK_KEY

pytestmark = [pytest.mark.integration, pytest.mark.timeout(5)]

ZSET_KEY = "sq:sched:zset"
DEFS_KEY = "sq:sched:defs"


@pytest.fixture
async def valkey_client(valkey_url):
    client = valkey_asyncio.Valkey.from_url(valkey_url)
    try:
        await client.ping()
        yield client
    finally:
        await client.aclose()


@pytest.fixture
def poll_script(valkey_client):
    return valkey_client.register_script(load_script("sched_poll"))


@pytest.fixture
def commit_script(valkey_client):
    return valkey_client.register_script(load_script("sched_commit"))


@pytest.fixture
def sched_name() -> str:
    return f"sched-{uuid.uuid4().hex[:12]}"


class TestSchedPoll:
    async def test_empty_zset_returns_empty_list(self, poll_script):
        result = await poll_script(
            keys=[ZSET_KEY, DEFS_KEY], args=[1_700_000_000_000, 100]
        )
        assert result == []

    async def test_returns_due_schedules_with_score_and_def(
        self, valkey_client, poll_script, sched_name
    ):
        await valkey_client.zadd(ZSET_KEY, {sched_name: 1_000})
        await valkey_client.hset(DEFS_KEY, sched_name, b'{"task":"ping"}')

        result = await poll_script(keys=[ZSET_KEY, DEFS_KEY], args=[2_000, 100])

        assert len(result) == 1
        member, score, def_json = result[0]
        assert member == sched_name.encode()
        assert score == b"1000"
        assert def_json == b'{"task":"ping"}'

    async def test_skips_future_schedules(
        self, valkey_client, poll_script, sched_name
    ):
        await valkey_client.zadd(ZSET_KEY, {sched_name: 5_000})
        await valkey_client.hset(DEFS_KEY, sched_name, b'{"task":"ping"}')

        result = await poll_script(keys=[ZSET_KEY, DEFS_KEY], args=[1_000, 100])
        assert result == []

    async def test_skips_orphan_zset_entries(
        self, valkey_client, poll_script, sched_name
    ):
        """Member in zset but no matching def → silently skipped."""
        await valkey_client.zadd(ZSET_KEY, {sched_name: 1_000})
        # intentionally no HSET

        result = await poll_script(keys=[ZSET_KEY, DEFS_KEY], args=[2_000, 100])
        assert result == []

    async def test_respects_limit(self, valkey_client, poll_script):
        names = [f"lim-{i}-{uuid.uuid4().hex[:6]}" for i in range(5)]
        for i, name in enumerate(names):
            await valkey_client.zadd(ZSET_KEY, {name: 100 + i})
            await valkey_client.hset(DEFS_KEY, name, b'{"task":"x"}')

        result = await poll_script(keys=[ZSET_KEY, DEFS_KEY], args=[10_000, 3])
        assert len(result) == 3


class TestSchedCommitOwnerCheck:
    """Owner-check is the P1-2 guard: a paused leader must not be able
    to overwrite the new leader's state after its lock expired."""

    async def test_owner_match_applies_mutations(
        self, valkey_client, commit_script, sched_name
    ):
        owner = uuid.uuid4().hex
        await valkey_client.set(LOCK_KEY, owner)
        await valkey_client.zadd(ZSET_KEY, {sched_name: 1_000})
        await valkey_client.hset(DEFS_KEY, sched_name, b'{"last":null}')

        result = await commit_script(
            keys=[ZSET_KEY, DEFS_KEY, LOCK_KEY],
            args=[sched_name, 2_000, b'{"last":1500}', owner],
        )
        assert int(result) == 1

        # zset: member now scored 2_000
        assert float(await valkey_client.zscore(ZSET_KEY, sched_name)) == 2_000.0
        # defs: updated def written
        assert await valkey_client.hget(DEFS_KEY, sched_name) == b'{"last":1500}'

        await valkey_client.delete(LOCK_KEY)

    async def test_owner_mismatch_returns_zero_and_no_mutation(
        self, valkey_client, commit_script, sched_name
    ):
        real_owner = uuid.uuid4().hex
        stale_owner = uuid.uuid4().hex
        await valkey_client.set(LOCK_KEY, real_owner)
        await valkey_client.zadd(ZSET_KEY, {sched_name: 1_000})
        await valkey_client.hset(DEFS_KEY, sched_name, b'{"orig":1}')

        result = await commit_script(
            keys=[ZSET_KEY, DEFS_KEY, LOCK_KEY],
            args=[sched_name, 9_999, b'{"stale":1}', stale_owner],
        )
        assert int(result) == 0

        # zset score unchanged
        assert float(await valkey_client.zscore(ZSET_KEY, sched_name)) == 1_000.0
        # defs untouched
        assert await valkey_client.hget(DEFS_KEY, sched_name) == b'{"orig":1}'

        await valkey_client.delete(LOCK_KEY)

    async def test_owner_absent_returns_zero(
        self, valkey_client, commit_script, sched_name
    ):
        # No lock at all — GET returns nil, nil ~= owner
        await valkey_client.zadd(ZSET_KEY, {sched_name: 1_000})
        await valkey_client.hset(DEFS_KEY, sched_name, b'{"orig":1}')

        result = await commit_script(
            keys=[ZSET_KEY, DEFS_KEY, LOCK_KEY],
            args=[sched_name, 9_999, b'{"stale":1}', "any-token"],
        )
        assert int(result) == 0

        assert float(await valkey_client.zscore(ZSET_KEY, sched_name)) == 1_000.0


class TestSchedCommitOneShot:
    async def test_one_shot_removes_without_readd(
        self, valkey_client, commit_script, sched_name
    ):
        owner = uuid.uuid4().hex
        await valkey_client.set(LOCK_KEY, owner)
        await valkey_client.zadd(ZSET_KEY, {sched_name: 1_000})
        await valkey_client.hset(DEFS_KEY, sched_name, b'{"oneshot":true}')

        # next_fire_ts_ms = -1 signals one-shot
        result = await commit_script(
            keys=[ZSET_KEY, DEFS_KEY, LOCK_KEY],
            args=[sched_name, -1, b"", owner],
        )
        assert int(result) == 1

        # zset member gone
        assert await valkey_client.zscore(ZSET_KEY, sched_name) is None
        # def gone (empty def_json = HDEL)
        assert await valkey_client.hget(DEFS_KEY, sched_name) is None

        await valkey_client.delete(LOCK_KEY)

    async def test_empty_def_json_triggers_hdel(
        self, valkey_client, commit_script, sched_name
    ):
        owner = uuid.uuid4().hex
        await valkey_client.set(LOCK_KEY, owner)
        await valkey_client.hset(DEFS_KEY, sched_name, b'{"some":"def"}')

        await commit_script(
            keys=[ZSET_KEY, DEFS_KEY, LOCK_KEY],
            args=[sched_name, -1, b"", owner],
        )
        assert await valkey_client.hget(DEFS_KEY, sched_name) is None

        await valkey_client.delete(LOCK_KEY)
