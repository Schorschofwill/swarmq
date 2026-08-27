"""Integration tests for ScheduleStore against a real Valkey backend.

Covers the full add/list/poll/commit roundtrip plus the insert-if-absent
and Fork-1 static-update paths.
"""

from __future__ import annotations

import uuid

import pytest

from swarmq.backend.valkey import ValkeyBackend
from swarmq.locking import SCHEDULER_LOCK_KEY
from swarmq.scheduler import ScheduleDefinition, ScheduleStore

pytestmark = [pytest.mark.integration, pytest.mark.timeout(5)]


@pytest.fixture
async def store(valkey_url):
    backend = ValkeyBackend(valkey_url)
    await backend.connect()
    try:
        yield ScheduleStore(backend)
    finally:
        await backend.close()


@pytest.fixture
async def valkey_client(valkey_url):
    import valkey.asyncio as valkey_asyncio
    client = valkey_asyncio.Valkey.from_url(valkey_url)
    try:
        await client.ping()
        yield client
    finally:
        await client.aclose()


@pytest.fixture
def sched_name() -> str:
    return f"sched-{uuid.uuid4().hex[:12]}"


def _def(name: str, *, cron: str | None = "* * * * *",
         fire_ts_ms: int | None = 1_700_000_000_000,
         source: str = "static",
         **overrides) -> ScheduleDefinition:
    base = {
        "schedule_name": name,
        "task_name": "ping",
        "args": (),
        "kwargs": {},
        "queue": "default",
        "cron": cron,
        "fire_ts_ms": fire_ts_ms,
        "source": source,
    }
    base.update(overrides)
    return ScheduleDefinition(**base)


class TestRoundtrip:
    async def test_add_then_list_returns_definition(self, store, sched_name):
        d = _def(sched_name)
        assert await store.add_schedule(d) is True
        all_schedules = await store.list_schedules()
        assert any(s.schedule_name == sched_name for s in all_schedules)

    async def test_add_is_idempotent(self, store, sched_name):
        d = _def(sched_name)
        assert await store.add_schedule(d) is True
        # Second call with the same name is a no-op
        assert await store.add_schedule(d) is False

    async def test_add_existing_cli_not_overwritten_by_static(
        self, store, sched_name
    ):
        """CD11: CLI ownership beats static on insert-if-absent."""
        cli = _def(sched_name, source="cli", cron="*/5 * * * *")
        assert await store.add_schedule(cli) is True

        static = _def(sched_name, source="static", cron="0 * * * *")
        assert await store.add_schedule(static) is False

        schedules = {s.schedule_name: s for s in await store.list_schedules()}
        assert schedules[sched_name].cron == "*/5 * * * *"
        assert schedules[sched_name].source == "cli"


class TestUpdateStaticSchedule:
    async def test_update_overwrites_existing(self, store, sched_name):
        original = _def(sched_name, cron="* * * * *",
                        fire_ts_ms=1_700_000_000_000)
        await store.add_schedule(original)

        modified = _def(sched_name, cron="*/10 * * * *",
                        fire_ts_ms=1_700_000_500_000)
        await store.update_static_schedule(modified)

        schedules = {s.schedule_name: s for s in await store.list_schedules()}
        assert schedules[sched_name].cron == "*/10 * * * *"
        assert schedules[sched_name].fire_ts_ms == 1_700_000_500_000


class TestRemoveDisable:
    async def test_remove_deletes_both(self, store, sched_name):
        await store.add_schedule(_def(sched_name))
        await store.remove_schedule(sched_name)
        schedules = await store.list_schedules()
        assert not any(s.schedule_name == sched_name for s in schedules)

    async def test_disable_sets_flag_and_removes_from_zset(
        self, store, sched_name, valkey_client,
    ):
        """disable_schedule must (a) persist disabled=True in defs and
        (b) remove the entry from the zset so poll_due stops returning
        it every tick (review-finding correctness C-02)."""
        await store.add_schedule(_def(sched_name))
        await store.disable_schedule(sched_name)

        schedules = {s.schedule_name: s for s in await store.list_schedules()}
        assert schedules[sched_name].disabled is True

        # zset entry must be gone — otherwise poll_due returns it forever.
        zscore = await valkey_client.zscore("sq:{sched}:zset", sched_name)
        assert zscore is None


class TestPollDue:
    async def test_poll_due_returns_tuples(self, store, sched_name):
        d = _def(sched_name, fire_ts_ms=1_000)
        await store.add_schedule(d)

        rows = await store.poll_due(now_ms=2_000, limit=10)

        assert any(
            name == sched_name and score == 1_000
            and definition.schedule_name == sched_name
            for name, score, definition in rows
        )

    async def test_poll_due_excludes_future(self, store, sched_name):
        d = _def(sched_name, fire_ts_ms=5_000)
        await store.add_schedule(d)

        rows = await store.poll_due(now_ms=1_000, limit=10)
        assert all(name != sched_name for name, _, _ in rows)


class TestCommitFire:
    async def test_commit_with_valid_owner_updates_state(
        self, store, valkey_client, sched_name
    ):
        owner = uuid.uuid4().hex
        await valkey_client.set(SCHEDULER_LOCK_KEY, owner)

        d = _def(sched_name, fire_ts_ms=1_000)
        await store.add_schedule(d)

        updated = _def(sched_name, fire_ts_ms=5_000, last_fire_at=2_000)
        ok = await store.commit_fire(sched_name, 5_000, updated, owner)
        assert ok is True

        schedules = {s.schedule_name: s for s in await store.list_schedules()}
        assert schedules[sched_name].last_fire_at == 2_000

        await valkey_client.delete(SCHEDULER_LOCK_KEY)

    async def test_commit_with_stale_owner_is_rejected(
        self, store, valkey_client, sched_name
    ):
        real = uuid.uuid4().hex
        stale = uuid.uuid4().hex
        await valkey_client.set(SCHEDULER_LOCK_KEY, real)

        d = _def(sched_name, fire_ts_ms=1_000, last_fire_at=None)
        await store.add_schedule(d)

        stale_def = _def(sched_name, last_fire_at=9999)
        ok = await store.commit_fire(sched_name, 9_000, stale_def, stale)
        assert ok is False

        # State unchanged (still original last_fire_at=None)
        schedules = {s.schedule_name: s for s in await store.list_schedules()}
        assert schedules[sched_name].last_fire_at is None

        await valkey_client.delete(SCHEDULER_LOCK_KEY)

    async def test_commit_one_shot_removes_entry(
        self, store, valkey_client, sched_name
    ):
        owner = uuid.uuid4().hex
        await valkey_client.set(SCHEDULER_LOCK_KEY, owner)

        d = _def(sched_name, cron=None, fire_ts_ms=1_000, source="cli")
        await store.add_schedule(d)

        ok = await store.commit_fire(sched_name, -1, None, owner)
        assert ok is True

        schedules = await store.list_schedules()
        assert not any(s.schedule_name == sched_name for s in schedules)

        await valkey_client.delete(SCHEDULER_LOCK_KEY)
