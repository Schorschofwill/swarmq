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
from swarmq.scheduler import (
    ScheduleDefinition,
    ScheduleStore,
    make_schedfire_key,
    parse_schedfire_key,
)


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


def _make_store(hget_return=None):
    """Build a ScheduleStore whose backend methods are mocks."""
    backend = MagicMock()
    backend._client = MagicMock()
    backend._client.register_script = MagicMock(
        side_effect=[AsyncMock(), AsyncMock()]
    )
    backend.hget_schedule_def = AsyncMock(return_value=hget_return)
    backend.hset_schedule_def = AsyncMock()
    backend.hdel_schedule_def = AsyncMock()
    backend.hgetall_schedule_defs = AsyncMock(return_value={})
    backend.zadd_schedule = AsyncMock()
    backend.zrem_schedule = AsyncMock()
    return ScheduleStore(backend), backend


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
    async def test_add_schedule_new_entry_writes_both_hset_and_zadd(self):
        store, backend = _make_store(hget_return=None)
        d = _def(schedule_name="newly")

        await store.add_schedule(d)

        backend.hset_schedule_def.assert_awaited_once()
        call_args = backend.hset_schedule_def.await_args
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
        """CD11 insert-if-absent: an existing def is never overwritten by
        add_schedule — regardless of source."""
        existing = msgspec.json.encode(_def(schedule_name="existing"))
        store, backend = _make_store(hget_return=existing)

        await store.add_schedule(_def(schedule_name="existing", cron="0 * * * *"))

        backend.hset_schedule_def.assert_not_awaited()
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
    async def test_remove_schedule_calls_zrem_and_hdel(self):
        store, backend = _make_store()
        await store.remove_schedule("drop")
        backend.zrem_schedule.assert_awaited_once_with("drop")
        backend.hdel_schedule_def.assert_awaited_once_with("drop")

    async def test_disable_schedule_sets_disabled_true(self):
        existing = msgspec.json.encode(_def(schedule_name="p", disabled=False))
        store, backend = _make_store(hget_return=existing)

        await store.disable_schedule("p")

        backend.hset_schedule_def.assert_awaited_once()
        decoded = msgspec.json.decode(
            backend.hset_schedule_def.await_args.args[1],
            type=ScheduleDefinition,
        )
        assert decoded.disabled is True

    async def test_disable_schedule_missing_is_noop(self):
        store, backend = _make_store(hget_return=None)
        await store.disable_schedule("not-there")
        backend.hset_schedule_def.assert_not_awaited()


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


class TestScheduleStoreErrorWrapping:
    async def test_add_schedule_wraps_hset_error(self):
        store, backend = _make_store(hget_return=None)
        backend.hset_schedule_def.side_effect = BackendConnectionError("down")
        with pytest.raises(BackendConnectionError):
            await store.add_schedule(_def())
