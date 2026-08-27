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
        assert lock_key(SCHEDULER_LOCK_NAME) == SCHEDULER_LOCK_KEY

    def test_expected_format(self):
        """Snapshot of today's key format. Failure here means the
        prefix changed — update sched_commit.lua expectations and
        any operator docs at the same time."""
        assert SCHEDULER_LOCK_KEY == "sq:lock:{sched}:leader"


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
    """ScheduleStore über einer gemockten ``backend.schedules``-Fassade (U18):
    die Valkey-Seite (Skripte, Keys) testet ``tests/unit/backend/test_valkey_schedules.py``."""
    backend = MagicMock()
    schedules = MagicMock()
    schedules.add = AsyncMock(return_value=bool(hsetnx_return))
    schedules.put = AsyncMock()
    schedules.remove = AsyncMock(return_value=bool(hexists_return))
    schedules.get = AsyncMock(return_value=hget_return)
    schedules.list_all = AsyncMock(return_value={})
    schedules.disable = AsyncMock(return_value=True)
    schedules.poll_due = AsyncMock(return_value=[])
    schedules.commit_fire = AsyncMock(return_value=True)
    backend.schedules = schedules
    store = ScheduleStore(backend)
    return store, schedules


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


class TestScheduleStoreRequiresConnectedBackend:
    def test_unconnected_backend_raises_runtime_error(self):
        from unittest.mock import PropertyMock

        backend = MagicMock()
        type(backend).schedules = PropertyMock(side_effect=BackendConnectionError("not connected"))
        with pytest.raises(RuntimeError, match="connected"):
            ScheduleStore(backend)


class TestScheduleStoreAddSchedule:
    async def test_add_schedule_encodes_definition_and_passes_fire_time(self):
        store, schedules = _make_store(hsetnx_return=True)
        d = _def(schedule_name="newly")

        assert await store.add_schedule(d) is True

        schedules.add.assert_awaited_once()
        name, payload, fire_ts = schedules.add.await_args.args
        assert name == "newly"
        assert msgspec.json.decode(payload, type=ScheduleDefinition) == d
        assert fire_ts == 1_700_000_000_000

    async def test_add_schedule_existing_entry_is_noop(self):
        """CD11 insert-if-absent: der Store meldet False → kein Insert."""
        store, _ = _make_store(hsetnx_return=False)
        assert await store.add_schedule(_def(schedule_name="existing", cron="0 * * * *")) is False


class TestScheduleStoreUpdateStaticSchedule:
    """Fork 1: source='static' modifications at redeploy must propagate."""

    async def test_update_static_schedule_unconditionally_writes(self):
        store, schedules = _make_store(hget_return=b"anything")
        d = _def(schedule_name="modded", cron="*/10 * * * *")
        await store.update_static_schedule(d)
        name, payload, fire_ts = schedules.put.await_args.args
        assert (name, fire_ts) == ("modded", 1_700_000_000_000)
        assert msgspec.json.decode(payload, type=ScheduleDefinition) == d

    async def test_update_static_schedule_works_without_existing_entry(self):
        """No read-first — caller is responsible for deciding when to call this."""
        store, schedules = _make_store(hget_return=None)
        await store.update_static_schedule(_def(schedule_name="fresh"))
        schedules.put.assert_awaited_once()
        schedules.get.assert_not_awaited()


class TestScheduleStoreRemoveAndDisable:
    async def test_remove_schedule_delegates_and_returns_existence(self):
        store, schedules = _make_store(hexists_return=1)
        assert await store.remove_schedule("drop") is True
        schedules.remove.assert_awaited_once_with("drop")

    async def test_remove_schedule_returns_false_when_missing(self):
        """Unit 4 / M-2: missing entry → False so CLI can report no-op."""
        store, _ = _make_store(hexists_return=0)
        assert await store.remove_schedule("ghost") is False

    async def test_disable_schedule_returns_true_when_existed(self):
        existing = msgspec.json.encode(_def(schedule_name="p", disabled=False))
        store, _ = _make_store(hget_return=existing)
        assert await store.disable_schedule("p") is True

    async def test_disable_schedule_returns_false_when_missing(self):
        store, schedules = _make_store(hget_return=None)
        assert await store.disable_schedule("ghost") is False
        schedules.disable.assert_not_awaited()

    async def test_disable_schedule_returns_false_on_malformed(self):
        """Malformed-entry branch also returns False — parked for
        operator inspection, no mutation."""
        store, schedules = _make_store(hget_return=b"{not valid json")
        assert await store.disable_schedule("corrupt") is False
        schedules.disable.assert_not_awaited()

    async def test_disable_schedule_passes_updated_def_with_disabled_true(self):
        """Unit 8 / M-14: die atomare HSET+ZREM-Schreibpaarung läuft im Store
        (``sched_disable.lua``); hier wird nur dekodiert, ``disabled=True``
        gesetzt und neu kodiert."""
        existing = msgspec.json.encode(_def(schedule_name="p", disabled=False))
        store, schedules = _make_store(hget_return=existing)

        await store.disable_schedule("p")

        name, payload = schedules.disable.await_args.args
        assert name == "p"
        assert msgspec.json.decode(payload, type=ScheduleDefinition).disabled is True
        schedules.put.assert_not_awaited()
        schedules.remove.assert_not_awaited()


class TestScheduleStoreListSchedules:
    async def test_list_schedules_decodes_all_entries(self):
        a = msgspec.json.encode(_def(schedule_name="a"))
        b = msgspec.json.encode(_def(schedule_name="b"))
        store, schedules = _make_store()
        schedules.list_all.return_value = {"a": a, "b": b}
        assert {d.schedule_name for d in await store.list_schedules()} == {"a", "b"}

    async def test_list_schedules_empty(self):
        store, _ = _make_store()
        assert await store.list_schedules() == []

    async def test_list_schedules_skips_malformed_entries(self):
        """Unit 5 / M-5: one malformed entry must not break list_schedules."""
        good = msgspec.json.encode(_def(schedule_name="good"))
        store, schedules = _make_store()
        schedules.list_all.return_value = {"good": good, "bad": b"{not valid json"}
        assert {d.schedule_name for d in await store.list_schedules()} == {"good"}

    async def test_list_schedules_skips_incompatible_schema(self):
        good = msgspec.json.encode(_def(schedule_name="good"))
        store, schedules = _make_store()
        schedules.list_all.return_value = {"good": good, "orphan": b'{"schedule_name":"orphan"}'}
        assert {d.schedule_name for d in await store.list_schedules()} == {"good"}


class TestScheduleStorePollAndCommit:
    async def test_poll_due_decodes_definitions(self):
        d = _def(schedule_name="due")
        store, schedules = _make_store()
        schedules.poll_due.return_value = [("due", 1_700_000_000_000, msgspec.json.encode(d))]
        assert await store.poll_due(1_700_000_000_001, limit=7) == [("due", 1_700_000_000_000, d)]
        schedules.poll_due.assert_awaited_once_with(1_700_000_000_001, limit=7)

    async def test_commit_fire_encodes_next_definition(self):
        d = _def(schedule_name="s", last_fire_at=5)
        store, schedules = _make_store()
        assert await store.commit_fire("s", 1_900, d, "chief") is True
        kwargs = schedules.commit_fire.await_args.kwargs
        assert kwargs["next_fire_ts_ms"] == 1_900 and kwargs["owner"] == "chief"
        assert msgspec.json.decode(kwargs["payload"], type=ScheduleDefinition) == d

    async def test_commit_fire_one_shot_maps_minus_one_to_none(self):
        store, schedules = _make_store()
        await store.commit_fire("once", -1, None, "chief")
        kwargs = schedules.commit_fire.await_args.kwargs
        assert kwargs["next_fire_ts_ms"] is None and kwargs["payload"] is None


class TestScheduleStoreErrorWrapping:
    async def test_add_schedule_propagates_backend_error(self):
        store, schedules = _make_store(hsetnx_return=True)
        schedules.add.side_effect = BackendConnectionError("down")
        with pytest.raises(BackendConnectionError):
            await store.add_schedule(_def())
