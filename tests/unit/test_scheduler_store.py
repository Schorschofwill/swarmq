"""Unit tests for swarmq.scheduler — ScheduleDefinition, ScheduleStore,
and schedfire-key helpers.

The happy-path store methods talk to Valkey via registered Lua scripts;
here we cover the pure helpers, msgspec round-trips, and the
insert-if-absent vs. update-static branches that live in Python.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import msgspec
import pytest

from swarmq.exceptions import BackendConnectionError
from swarmq.locking import lock_key
from swarmq.scheduler import (
    SCHEDULER_LOCK_KEY,
    SCHEDULER_LOCK_NAME,
    ScheduleDefinition,
    ScheduleStore,
    make_schedfire_key,
    parse_schedfire_key,
)

# AGENTS.md §Async-Code testen rule 3: 5s per-test timeout.
pytestmark = [pytest.mark.timeout(5)]


# ---------------------------------------------------------------------------
# SCHEDULER_LOCK_KEY — live-derivation invariant (Unit 10 / Fork B1)
# ---------------------------------------------------------------------------


class TestSchedulerLockKey:
    """Regression guard for the live-derivation invariant. If
    ``locking.lock_key()`` ever becomes non-trivial (e.g. gains a tenant
    prefix), these tests catch silent drift between the scheduler's
    constant and the LockManager's actual key format."""

    def test_matches_lock_manager_derivation(self):
        """SCHEDULER_LOCK_KEY must equal lock_key(SCHEDULER_LOCK_NAME).
        This is by construction today — if it ever fails, someone
        hand-typed the constant and bypassed the derivation."""
        assert SCHEDULER_LOCK_KEY == lock_key(SCHEDULER_LOCK_NAME)

    def test_expected_format(self):
        """Snapshot of today's key format. Failure here means the
        prefix changed — update sched_commit.lua expectations and
        any operator docs at the same time."""
        assert SCHEDULER_LOCK_KEY == "sq:lock:scheduler:leader"


# ---------------------------------------------------------------------------
# schedfire_key helpers (P1-4 rsplit guarantee)
# ---------------------------------------------------------------------------


class TestMakeSchedfireKey:
    def test_returns_colon_joined(self):
        assert make_schedfire_key("cron_a", 1700000000000) == "cron_a:1700000000000"

    def test_deterministic(self):
        a = make_schedfire_key("sched", 12345)
        b = make_schedfire_key("sched", 12345)
        assert a == b


class TestParseSchedfireKey:
    def test_simple_name(self):
        assert parse_schedfire_key("sched_a:1234567890") == ("sched_a", 1234567890)

    def test_name_with_internal_colons_kept_intact(self):
        """P1-4: rsplit keeps multi-colon prefixes like oneshot:<uuid>."""
        assert parse_schedfire_key(
            "oneshot:abc-def-123:1234567890"
        ) == ("oneshot:abc-def-123", 1234567890)

    def test_no_colon_raises(self):
        with pytest.raises(ValueError):
            parse_schedfire_key("no-separator")

    def test_non_int_timestamp_raises(self):
        with pytest.raises(ValueError):
            parse_schedfire_key("sched:not-a-number")


# ---------------------------------------------------------------------------
# ScheduleDefinition msgspec roundtrip
# ---------------------------------------------------------------------------


class TestScheduleDefinition:
    def test_construct_with_cron(self):
        d = ScheduleDefinition(
            schedule_name="daily",
            task_name="backup",
            args=(),
            kwargs={},
            queue="default",
            cron="0 3 * * *",
            fire_ts_ms=1_700_000_000_000,
            source="static",
        )
        assert d.cron == "0 3 * * *"
        assert d.disabled is False
        assert d.last_fire_at is None

    def test_construct_oneshot(self):
        d = ScheduleDefinition(
            schedule_name="oneshot:abc",
            task_name="send_email",
            args=("user@example.com",),
            kwargs={"subject": "hi"},
            queue="mail",
            cron=None,
            fire_ts_ms=1_700_000_100_000,
            source="cli",
        )
        assert d.cron is None
        assert d.source == "cli"

    def test_msgspec_roundtrip(self):
        original = ScheduleDefinition(
            schedule_name="daily",
            task_name="backup",
            args=(1, "x"),
            kwargs={"k": 2},
            queue="q",
            cron="* * * * *",
            fire_ts_ms=1_700_000_000_000,
            source="static",
            disabled=False,
            last_fire_at=1_600_000_000_000,
        )
        raw = msgspec.json.encode(original)
        decoded = msgspec.json.decode(raw, type=ScheduleDefinition)
        assert decoded == original


# ---------------------------------------------------------------------------
# ScheduleStore — insert-if-absent semantics (CD11 + Fork 1)
# ---------------------------------------------------------------------------


def _make_store(hget_return=None, hsetnx_return=True, hexists_return=1):
    """Build a ScheduleStore whose backend methods are mocks."""
    backend = MagicMock()
    backend._client = MagicMock()
    # Three scripts registered now: sched_poll, sched_commit, sched_disable.
    disable_script = AsyncMock()
    backend._client.register_script = MagicMock(
        side_effect=[AsyncMock(), AsyncMock(), disable_script]
    )
    # hexists is called by remove_schedule to compute the bool return.
    backend._client.hexists = AsyncMock(return_value=hexists_return)
    backend.hget_schedule_def = AsyncMock(return_value=hget_return)
    backend.hset_schedule_def = AsyncMock()
    backend.hsetnx_schedule_def = AsyncMock(return_value=hsetnx_return)
    backend.hdel_schedule_def = AsyncMock()
    backend.hgetall_schedule_defs = AsyncMock(return_value={})
    backend.zadd_schedule = AsyncMock()
    backend.zrem_schedule = AsyncMock()
    store = ScheduleStore(backend)
    # Expose the disable script mock for assertions
    backend._disable_script = disable_script
    return store, backend


def _def(**overrides) -> ScheduleDefinition:
    base = {
        "schedule_name": "test",
        "task_name": "ping",
        "args": (),
        "kwargs": {},
        "queue": "default",
        "cron": "* * * * *",
        "fire_ts_ms": 1_700_000_000_000,
        "source": "static",
    }
    base.update(overrides)
    return ScheduleDefinition(**base)


class TestScheduleStoreAddSchedule:
    async def test_add_schedule_new_entry_writes_hsetnx_and_zadd(self):
        # hsetnx returns True → new insert
        store, backend = _make_store(hsetnx_return=True)
        d = _def(schedule_name="newly")

        result = await store.add_schedule(d)

        assert result is True
        backend.hsetnx_schedule_def.assert_awaited_once()
        call_args = backend.hsetnx_schedule_def.await_args
        assert call_args.args[0] == "newly"
        # Second arg is the JSON-encoded ScheduleDefinition
        decoded = msgspec.json.decode(
            call_args.args[1], type=ScheduleDefinition
        )
        assert decoded == d

        backend.zadd_schedule.assert_awaited_once_with(
            "newly", 1_700_000_000_000
        )

    async def test_add_schedule_existing_entry_is_noop(self):
        """CD11 insert-if-absent: HSETNX=False → skip ZADD, return False."""
        store, backend = _make_store(hsetnx_return=False)

        result = await store.add_schedule(
            _def(schedule_name="existing", cron="0 * * * *")
        )

        assert result is False
        backend.zadd_schedule.assert_not_awaited()


class TestScheduleStoreUpdateStaticSchedule:
    """Fork 1: source='static' modifications at redeploy must propagate."""

    async def test_update_static_schedule_unconditionally_writes(self):
        store, backend = _make_store(hget_return=b"anything")
        d = _def(schedule_name="modded", cron="*/10 * * * *")

        await store.update_static_schedule(d)

        backend.hset_schedule_def.assert_awaited_once()
        backend.zadd_schedule.assert_awaited_once_with(
            "modded", 1_700_000_000_000
        )

    async def test_update_static_schedule_works_without_existing_entry(self):
        """No read-first — caller is responsible for deciding when to call this."""
        store, backend = _make_store(hget_return=None)
        d = _def(schedule_name="fresh")

        await store.update_static_schedule(d)

        backend.hset_schedule_def.assert_awaited_once()
        backend.zadd_schedule.assert_awaited_once()


class TestScheduleStoreRemoveAndDisable:
    async def test_remove_schedule_calls_zrem_and_hdel_returns_true(self):
        # hexists=1 → entry existed → returns True
        store, backend = _make_store(hexists_return=1)
        result = await store.remove_schedule("drop")
        assert result is True
        backend.zrem_schedule.assert_awaited_once_with("drop")
        backend.hdel_schedule_def.assert_awaited_once_with("drop")

    async def test_remove_schedule_returns_false_when_missing(self):
        """Unit 4 / M-2: missing entry → False so CLI can report no-op."""
        store, backend = _make_store(hexists_return=0)
        result = await store.remove_schedule("ghost")
        assert result is False

    async def test_disable_schedule_returns_true_when_existed(self):
        import msgspec
        existing = msgspec.json.encode(_def(schedule_name="p", disabled=False))
        store, backend = _make_store(hget_return=existing)
        result = await store.disable_schedule("p")
        assert result is True

    async def test_disable_schedule_returns_false_when_missing(self):
        store, backend = _make_store(hget_return=None)
        result = await store.disable_schedule("ghost")
        assert result is False

    async def test_disable_schedule_returns_false_on_malformed(self):
        """Malformed-entry branch also returns False — parked for
        operator inspection, no mutation."""
        store, backend = _make_store(hget_return=b"{not valid json")
        result = await store.disable_schedule("corrupt")
        assert result is False

    async def test_disable_schedule_invokes_disable_script_with_disabled_true(self):
        """Unit 8 / M-14: disable_schedule uses sched_disable.lua for
        the atomic HSET+ZREM write-pair. The Python side still reads
        and decodes first (msgspec shape stays in Python), then calls
        the Lua script with the updated-def JSON."""
        existing = msgspec.json.encode(_def(schedule_name="p", disabled=False))
        store, backend = _make_store(hget_return=existing)

        await store.disable_schedule("p")

        backend._disable_script.assert_awaited_once()
        call = backend._disable_script.await_args
        # keys=[defs_key, zset_key], args=[name, updated_def_json]
        assert call.kwargs["args"][0] == "p"
        decoded = msgspec.json.decode(
            call.kwargs["args"][1], type=ScheduleDefinition
        )
        assert decoded.disabled is True
        # Low-level zrem/hset/hsetnx must NOT be called directly —
        # the script covers the write-pair atomically.
        backend.zrem_schedule.assert_not_awaited()
        backend.hset_schedule_def.assert_not_awaited()

    async def test_disable_schedule_missing_is_noop(self):
        store, backend = _make_store(hget_return=None)
        await store.disable_schedule("not-there")
        backend._disable_script.assert_not_awaited()
        backend.hset_schedule_def.assert_not_awaited()
        backend.zrem_schedule.assert_not_awaited()

    async def test_disable_schedule_malformed_entry_is_noop_with_log(self):
        """Mirror of list_schedules' malformed-skip path: a corrupted
        hash value must not crash disable_schedule. It logs and skips."""
        store, backend = _make_store(hget_return=b"{not valid json")
        await store.disable_schedule("corrupt")

        backend._disable_script.assert_not_awaited()


class TestScheduleStoreListSchedules:
    async def test_list_schedules_decodes_all_hashes(self):
        a = msgspec.json.encode(_def(schedule_name="a"))
        b = msgspec.json.encode(_def(schedule_name="b"))
        store, backend = _make_store()
        backend.hgetall_schedule_defs.return_value = {b"a": a, b"b": b}

        result = await store.list_schedules()

        names = {d.schedule_name for d in result}
        assert names == {"a", "b"}

    async def test_list_schedules_empty(self):
        store, backend = _make_store()
        backend.hgetall_schedule_defs.return_value = {}
        assert await store.list_schedules() == []

    async def test_list_schedules_skips_malformed_entries(self):
        """Unit 5 / M-5: one malformed entry must not break list_schedules
        for the whole hash. Well-formed entries still come through."""
        good = msgspec.json.encode(_def(schedule_name="good"))
        store, backend = _make_store()
        backend.hgetall_schedule_defs.return_value = {
            b"good": good,
            b"bad": b"{not valid json",
        }

        result = await store.list_schedules()

        names = {d.schedule_name for d in result}
        assert names == {"good"}  # malformed "bad" silently skipped

    async def test_list_schedules_skips_incompatible_schema(self):
        """An entry with a wrong schema (missing required field) also
        gets skipped rather than crashing the whole list call."""
        good = msgspec.json.encode(_def(schedule_name="good"))
        # msgspec.json.decode(type=ScheduleDefinition) requires all
        # kw_only fields — an arbitrary subset triggers DecodeError.
        incompat = b'{"schedule_name":"orphan"}'
        store, backend = _make_store()
        backend.hgetall_schedule_defs.return_value = {
            b"good": good,
            b"orphan": incompat,
        }

        result = await store.list_schedules()
        names = {d.schedule_name for d in result}
        assert names == {"good"}


class TestScheduleStoreErrorWrapping:
    async def test_add_schedule_wraps_hsetnx_error(self):
        store, backend = _make_store(hsetnx_return=True)
        backend.hsetnx_schedule_def.side_effect = BackendConnectionError("down")
        with pytest.raises(BackendConnectionError):
            await store.add_schedule(_def())
