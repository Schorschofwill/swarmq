"""Integration tests for sched_poll.lua + sched_commit.lua.

Raw-Lua tests, no ScheduleStore wrapper yet (that lands in Unit 7).
Uses the shared conftest fixtures for dev-stack env-var port awareness.
"""

from __future__ import annotations

import uuid

import pytest
import valkey.asyncio as valkey_asyncio

from swarmq.locking import SCHEDULER_LOCK_KEY as LOCK_KEY
from swarmq.lua import load_script

pytestmark = [pytest.mark.integration, pytest.mark.timeout(5)]

ZSET_KEY = "sq:{sched}:zset"
DEFS_KEY = "sq:{sched}:defs"


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


class TestSchedCommitTakeoverGuard:
    """Plan 2026-10-08-1541 (U3, KTD12): ARGV[6] = gepollte Definition.
    Der Commit schreibt die Definition nur zurueck, wenn sie seit dem Poll
    unveraendert ist; der Score rueckt in beiden Faellen vor."""

    async def _commit(self, valkey_client, commit_script, sched_name, polled):
        owner = uuid.uuid4().hex
        await valkey_client.set(LOCK_KEY, owner)
        try:
            return await commit_script(
                keys=[ZSET_KEY, DEFS_KEY, LOCK_KEY],
                args=[sched_name, 2_000, b'{"next":1}', owner, 1_000, polled],
            )
        finally:
            await valkey_client.delete(LOCK_KEY)

    async def test_changed_def_is_kept_and_score_advances(
        self, valkey_client, commit_script, sched_name
    ):
        await valkey_client.zadd(ZSET_KEY, {sched_name: 1_000})
        await valkey_client.hset(DEFS_KEY, sched_name, b'{"taken":"over"}')

        result = await self._commit(valkey_client, commit_script, sched_name, b'{"polled":1}')

        assert int(result) == 1
        assert await valkey_client.hget(DEFS_KEY, sched_name) == b'{"taken":"over"}'
        assert float(await valkey_client.zscore(ZSET_KEY, sched_name)) == 2_000.0

    async def test_unchanged_def_is_written_back(
        self, valkey_client, commit_script, sched_name
    ):
        await valkey_client.zadd(ZSET_KEY, {sched_name: 1_000})
        await valkey_client.hset(DEFS_KEY, sched_name, b'{"polled":1}')

        result = await self._commit(valkey_client, commit_script, sched_name, b'{"polled":1}')

        assert int(result) == 1
        assert await valkey_client.hget(DEFS_KEY, sched_name) == b'{"next":1}'



class TestSchedReplan:
    """``sched_replan.lua`` (Review #4): Heilung nur, solange die Definition
    die gelesene ist und kein Score existiert -- in einem EVAL."""

    @pytest.fixture
    def replan_script(self, valkey_client):
        return valkey_client.register_script(load_script("sched_replan"))

    async def _replan(self, script, sched_name):
        return await script(
            keys=[DEFS_KEY, ZSET_KEY],
            args=[sched_name, b'{"read":1}', b'{"healed":1}', 4_000],
        )

    async def test_unchanged_unscored_def_is_planned(
        self, valkey_client, replan_script, sched_name
    ):
        await valkey_client.hset(DEFS_KEY, sched_name, b'{"read":1}')

        assert int(await self._replan(replan_script, sched_name)) == 1
        assert await valkey_client.hget(DEFS_KEY, sched_name) == b'{"healed":1}'
        assert float(await valkey_client.zscore(ZSET_KEY, sched_name)) == 4_000.0

    async def test_disabled_since_read_stays_disabled(
        self, valkey_client, replan_script, sched_name
    ):
        await valkey_client.hset(DEFS_KEY, sched_name, b'{"disabled":true}')

        assert int(await self._replan(replan_script, sched_name)) == 0
        assert await valkey_client.hget(DEFS_KEY, sched_name) == b'{"disabled":true}'
        assert await valkey_client.zscore(ZSET_KEY, sched_name) is None

    async def test_scored_since_read_keeps_its_score(
        self, valkey_client, replan_script, sched_name
    ):
        await valkey_client.hset(DEFS_KEY, sched_name, b'{"read":1}')
        await valkey_client.zadd(ZSET_KEY, {sched_name: 2_000})

        assert int(await self._replan(replan_script, sched_name)) == 0
        assert await valkey_client.hget(DEFS_KEY, sched_name) == b'{"read":1}'
        assert float(await valkey_client.zscore(ZSET_KEY, sched_name)) == 2_000.0

    async def test_removed_since_read_is_not_recreated(
        self, valkey_client, replan_script, sched_name
    ):
        assert int(await self._replan(replan_script, sched_name)) == 0
        assert await valkey_client.hexists(DEFS_KEY, sched_name) == 0
        assert await valkey_client.zscore(ZSET_KEY, sched_name) is None
