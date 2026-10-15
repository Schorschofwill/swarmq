"""Definition-Tests (Plan U11) — verbatim aus ``test_scheduler_store.py``.

Unit tests for swarmq.scheduler — ScheduleDefinition, ScheduleStore,
and schedfire-key helpers.

The happy-path store methods talk to Valkey via registered Lua scripts;
here we cover the pure helpers, msgspec round-trips, and the
insert-if-absent vs. update-static branches that live in Python.
"""

# Test list (Plan 2026-10-06-0831, U9, R17):
# 1. ``disable_schedule`` auf kaputtem Eintrag loggt
#    ``schedule_def_malformed_skipped`` mit ``error_type``/``error``.
# 2. ``list_schedules`` ebenso.
#
# Test list (0.9.0, Scheduler-Commit-Wettlauf):
# 3. ``commit_fire`` reicht den gefeuerten Score als ``fired_ts_ms`` an den
#    Bytes-Store durch.
#
# Test list (Plan 2026-10-08-1541, U1, R6, KTD1):
# 4. Hin- und Rückweg mit ``timezone`` verlustfrei.
# 5. Alte JSON ohne ``timezone`` wird mit ``None`` gelesen.
#
# Test list (Plan 2026-10-08-1541, U13, KTD11):
# 6. Charakterisierung: ein Leader mit der alten Struct-Form (0.9.0, ohne
#    ``timezone``) dekodiert eine 0.10.0-Definition, setzt beim Fire per
#    ``replace`` den naechsten Score und verliert beim Re-Encode die Zone --
#    der Grund fuer das Upgrade in zwei Schritten.
#
# Test list (Review, KTD12 -- ``fired_payload`` aus dem Poll):
# 7. Gepollter Score == ``fired_ts_ms`` -> der gepollte Payload geht als
#    ``fired_payload`` mit.
# 8. Gepollter Score != ``fired_ts_ms`` -> kein ``fired_payload``.
# 9. Ohne Poll -> kein ``fired_payload``.
# 10. Der Poll-Eintrag gilt nur für einen Commit.
#
# Test list (Plan 0.12.0, MR 4, BZ-28 -- zeilenweise dekodieren):
# 11. Eine kaputte Definition im Poll bricht den Poll nicht ab: die übrigen
#     Zeilen kommen, die kaputte wird mit ``schedule_def_malformed_parked``
#     geloggt.
# 12. Mit ``owner_token`` wird sie geparkt: Commit ohne neuen Score, die
#     Definition bleibt unverändert (Owner-, Score- und Payload-Prüfung).
# 13. Scheitert das Parken an Valkey, kommen die übrigen Zeilen trotzdem;
#     ``scheduler_park_failed`` wird geloggt (wie beim Parken nicht
#     planbarer Definitionen, R18).
#
# Test list (Plan 2026-10-12-1105, MR 4, P12a -- Charakterisierung):
# 14. ``delay``/``eta``: ein eigenes Objekt in ``args`` ergibt beim
#     Kodieren der Definition einen rohen ``TypeError`` (kein
#     ``ResultSerializationError``).
#
# Test list (Plan 2026-10-15-1138, U3, KTD4, E-058 -- Oneshot-Idempotenz):
# 15. Oneshot, ``add`` -> False, ``get`` -> dieselben Bytes, Score vorhanden
#     -> True, kein ``put``.
# 16. Wie 15 ohne Score -> genau ein ``put(name, payload, fire_ts)``, True.
# 17. ``get`` liefert ``str`` mit gleichem Inhalt -> True.
# 18. Abweichende Bytes (``unroutable_since`` gesetzt) -> False, kein ``put``.
# 19. ``get`` -> None -> False.
# 20. CLI-artige Definition (``task_id=None``, ``--at``) mit ``add`` -> False
#     -> False, kein ``get``.
# 21. Cron-Definition mit ``task_id`` und ``add`` -> False -> False.

from __future__ import annotations

from typing import Any, Literal
from unittest.mock import AsyncMock, MagicMock

import msgspec
import pytest

from swarmq.exceptions import BackendConnectionError, ResultSerializationError
from swarmq.locking import SCHEDULER_LOCK_KEY, SCHEDULER_LOCK_NAME, lock_key
from swarmq.scheduler import (
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
        assert parse_schedfire_key("oneshot:abc-def-123:1234567890") == ("oneshot:abc-def-123", 1234567890)

    def test_no_colon_raises(self):
        with pytest.raises(ValueError, match="missing ':' separator"):
            parse_schedfire_key("no-separator")

    def test_non_int_timestamp_raises(self):
        with pytest.raises(ValueError, match="non-integer timestamp"):
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

    def test_msgspec_roundtrip_keeps_timezone(self):
        original = _def(cron="0 3 * * *", timezone="Europe/Berlin")
        raw = msgspec.json.encode(original)
        decoded = msgspec.json.decode(raw, type=ScheduleDefinition)
        assert decoded == original
        assert decoded.timezone == "Europe/Berlin"

    def test_decode_legacy_json_without_timezone_yields_none(self):
        """Einträge aus 0.9.x tragen kein ``timezone``-Feld (Pattern 7)."""
        legacy = (
            b'{"schedule_name":"daily","task_name":"backup","args":[],'
            b'"kwargs":{},"queue":"default","cron":"0 3 * * *",'
            b'"fire_ts_ms":1700000000000,"source":"static","disabled":false,'
            b'"last_fire_at":null,"dedup_hash":null,"unique_until":"completion"}'
        )
        decoded = msgspec.json.decode(legacy, type=ScheduleDefinition)
        assert decoded.timezone is None

    def test_legacy_struct_drops_timezone_on_reencode(self):
        """Charakterisierung (KTD11): so verhaelt sich ein 0.9.0-Leader beim
        Fire. msgspec ignoriert unbekannte Felder beim Dekodieren, also fehlt
        die Zone im Re-Encode; der naechste 0.10.0-Leader liest UTC."""

        class LegacyScheduleDefinition(msgspec.Struct, kw_only=True, frozen=True):
            schedule_name: str
            task_name: str
            args: tuple[Any, ...]
            kwargs: dict[str, Any]
            queue: str
            cron: str | None
            fire_ts_ms: int | None
            source: Literal["static", "cli", "cli_unique"]
            disabled: bool = False
            last_fire_at: int | None = None
            dedup_hash: str | None = None
            unique_until: Literal["start", "completion"] = "completion"

        current = _def(cron="0 3 * * *", timezone="Europe/Berlin")
        legacy = msgspec.json.decode(msgspec.json.encode(current), type=LegacyScheduleDefinition)
        fired = msgspec.structs.replace(legacy, fire_ts_ms=1_700_000_060_000, last_fire_at=1)
        reread = msgspec.json.decode(msgspec.json.encode(fired), type=ScheduleDefinition)
        assert b"timezone" not in msgspec.json.encode(fired)
        assert reread.timezone is None
        assert reread.cron == "0 3 * * *"


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
    schedules.forget_pruned_disabled = AsyncMock()
    backend.schedules = schedules
    store = ScheduleStore(backend)
    return store, schedules


def _def(**overrides) -> ScheduleDefinition:
    base: dict[str, Any] = {
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


class TestDelayedArgumentsEncoding:
    def test_custom_object_in_args_raises_raw_type_error(self):
        class Custom:
            pass

        with pytest.raises(TypeError) as exc_info:
            msgspec.json.encode(_def(args=(Custom(),)))
        assert not isinstance(exc_info.value, ResultSerializationError)


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

    async def test_add_schedule_disabled_definition_gets_no_fire_entry(self):
        """Plan 2026-10-08-1541, U10: ein deaktiviert wieder angelegter Cron
        landet nicht im Fire-Index (wie ``update_static_schedule``, KTD12)."""
        store, schedules = _make_store(hsetnx_return=True)
        d = msgspec.structs.replace(_def(schedule_name="revived"), disabled=True)

        assert await store.add_schedule(d) is True

        name, payload, fire_ts = schedules.add.await_args.args
        assert (name, fire_ts) == ("revived", None)
        assert msgspec.json.decode(payload, type=ScheduleDefinition) == d

    async def test_add_schedule_existing_entry_is_noop(self):
        """CD11 insert-if-absent: der Store meldet False → kein Insert."""
        store, _ = _make_store(hsetnx_return=False)
        assert await store.add_schedule(_def(schedule_name="existing", cron="0 * * * *")) is False


def _oneshot(**overrides: Any) -> ScheduleDefinition:
    """Oneshot aus ``schedule(delay=/eta=)``: frische ``task_id`` im Namen."""
    base: dict[str, Any] = {
        "schedule_name": "oneshot:t1",
        "task_id": "t1",
        "cron": None,
        "source": "cli",
    }
    base.update(overrides)
    return _def(**base)


class TestScheduleStoreAddOneshotIdempotent:
    """E-058 (F3-01): Ein byte-gleicher bestehender Oneshot ist der eigene
    Insert nach einer verlorenen Antwort; ein fehlender Fire-Index wird
    ergänzt. Alles andere bleibt Konflikt bzw. „already exists“."""

    @pytest.mark.entscheidung("E-058")
    async def test_identical_existing_oneshot_counts_as_own_insert(self):
        """Liste 15."""
        store, schedules = _make_store(hsetnx_return=False)
        d = _oneshot()
        schedules.get.return_value = msgspec.json.encode(d)
        schedules.fire_ts = AsyncMock(return_value=d.fire_ts_ms)

        assert await store.add_schedule(d) is True
        schedules.put.assert_not_awaited()

    @pytest.mark.entscheidung("E-058")
    async def test_identical_existing_oneshot_without_score_gets_its_fire_index(self):
        """Liste 16: der Fire-Index wird per ``put`` ergänzt."""
        store, schedules = _make_store(hsetnx_return=False)
        d = _oneshot()
        payload = msgspec.json.encode(d)
        schedules.get.return_value = payload
        schedules.fire_ts = AsyncMock(return_value=None)

        assert await store.add_schedule(d) is True
        schedules.put.assert_awaited_once_with("oneshot:t1", payload, d.fire_ts_ms)

    @pytest.mark.entscheidung("E-058")
    async def test_identical_existing_oneshot_as_str_counts_as_own_insert(self):
        """Liste 17: ein Client mit ``decode_responses=True`` liefert ``str``."""
        store, schedules = _make_store(hsetnx_return=False)
        d = _oneshot()
        schedules.get.return_value = msgspec.json.encode(d).decode()
        schedules.fire_ts = AsyncMock(return_value=d.fire_ts_ms)

        assert await store.add_schedule(d) is True

    @pytest.mark.entscheidung("E-058")
    async def test_differing_existing_oneshot_is_a_conflict(self):
        """Liste 18: der Leader hat den Eintrag umgeschrieben."""
        store, schedules = _make_store(hsetnx_return=False)
        d = _oneshot()
        schedules.get.return_value = msgspec.json.encode(msgspec.structs.replace(d, unroutable_since=1_700))
        schedules.fire_ts = AsyncMock(return_value=None)

        assert await store.add_schedule(d) is False
        schedules.put.assert_not_awaited()

    @pytest.mark.entscheidung("E-058")
    async def test_vanished_existing_oneshot_is_a_conflict(self):
        """Liste 19: zwischen HSETNX und HGET verschwunden -- konservativ ein
        Konflikt (Plan, Assumptions; Nachtrag E-058 vom 2026-10-15)."""
        store, schedules = _make_store(hsetnx_return=False, hget_return=None)
        schedules.fire_ts = AsyncMock(return_value=None)

        assert await store.add_schedule(_oneshot()) is False
        schedules.put.assert_not_awaited()

    @pytest.mark.entscheidung("E-058")
    async def test_cli_at_definition_with_same_name_stays_already_exists(self):
        """Liste 20: ``swarmq schedule add --at`` setzt nie ``task_id``."""
        store, schedules = _make_store(hsetnx_return=False)
        d = _def(schedule_name="report", cron=None, source="cli")
        schedules.get.return_value = msgspec.json.encode(d)

        assert await store.add_schedule(d) is False
        schedules.get.assert_not_awaited()

    async def test_cron_definition_with_task_id_is_no_oneshot(self):
        """Liste 21: ein Cron ist nie ein Oneshot, auch mit passendem Namen."""
        store, schedules = _make_store(hsetnx_return=False)
        d = _oneshot(cron="* * * * *")
        schedules.get.return_value = msgspec.json.encode(d)

        assert await store.add_schedule(d) is False
        schedules.get.assert_not_awaited()

    async def test_oneshot_name_must_carry_its_task_id(self):
        """Nur ``oneshot:<task_id>`` ist ein Client-Oneshot."""
        store, schedules = _make_store(hsetnx_return=False)
        d = _oneshot(schedule_name="oneshot:other")
        schedules.get.return_value = msgspec.json.encode(d)

        assert await store.add_schedule(d) is False
        schedules.get.assert_not_awaited()


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

    async def test_disable_schedule_malformed_logs_error_fields(self, swarmq_logs):
        store, _ = _make_store(hget_return=b"{not valid json")
        await store.disable_schedule("corrupt")
        [entry] = [e for e in swarmq_logs if e["event"] == "schedule_def_malformed_skipped"]
        assert entry["log_level"] == "warning"
        assert entry["schedule_name"] == "corrupt"
        assert entry["error_type"] == "DecodeError"
        assert entry["error"]
        assert "hint" in entry

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

    async def test_list_schedules_malformed_logs_error_fields(self, swarmq_logs):
        store, schedules = _make_store()
        schedules.list_all.return_value = {"bad": b"{not valid json"}
        await store.list_schedules()
        [entry] = [e for e in swarmq_logs if e["event"] == "schedule_def_malformed_skipped"]
        assert entry["log_level"] == "warning"
        assert entry["schedule_name"] == "bad"
        assert entry["error_type"] == "DecodeError"
        assert entry["error"]

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

    async def test_poll_due_skips_and_logs_undecodable_definition(self, swarmq_logs):
        """Liste 11."""
        good = _def(schedule_name="good")
        store, schedules = _make_store()
        schedules.poll_due.return_value = [("bad", 1_000, b"{broken"), ("good", 2_000, msgspec.json.encode(good))]
        assert await store.poll_due(5_000) == [("good", 2_000, good)]
        [entry] = [e for e in swarmq_logs if e["event"] == "schedule_def_malformed_parked"]
        assert entry["log_level"] == "warning"
        assert entry["schedule_name"] == "bad"
        assert entry["error_type"] == "DecodeError"
        assert entry["error"]

    async def test_poll_due_with_owner_parks_undecodable_definition(self):
        """Liste 12."""
        store, schedules = _make_store()
        schedules.poll_due.return_value = [("bad", 1_000, b"{broken")]
        assert await store.poll_due(5_000, owner_token="chief") == []
        schedules.commit_fire.assert_awaited_once_with(
            "bad", next_fire_ts_ms=None, payload=b"{broken", owner="chief", fired_ts_ms=1_000, fired_payload=b"{broken"
        )

    async def test_poll_due_park_failure_does_not_abort_the_poll(self, swarmq_logs):
        """Liste 13."""
        good = _def(schedule_name="good")
        store, schedules = _make_store()
        schedules.poll_due.return_value = [("bad", 1_000, b"{broken"), ("good", 2_000, msgspec.json.encode(good))]
        schedules.commit_fire.side_effect = BackendConnectionError("down")
        assert await store.poll_due(5_000, owner_token="chief") == [("good", 2_000, good)]
        [entry] = [e for e in swarmq_logs if e["event"] == "scheduler_park_failed"]
        assert entry["schedule_name"] == "bad"
        assert entry["error_type"] == "BackendConnectionError"

    async def test_commit_fire_encodes_next_definition(self):
        d = _def(schedule_name="s", last_fire_at=5)
        store, schedules = _make_store()
        assert await store.commit_fire("s", 1_900, d, "chief") is True
        kwargs = schedules.commit_fire.await_args.kwargs
        assert kwargs["next_fire_ts_ms"] == 1_900
        assert kwargs["owner"] == "chief"
        assert msgspec.json.decode(kwargs["payload"], type=ScheduleDefinition) == d

    async def test_commit_fire_forwards_fired_score(self):
        store, schedules = _make_store()
        await store.commit_fire("s", 1_900, _def(schedule_name="s"), "chief", fired_ts_ms=1_000)
        assert schedules.commit_fire.await_args.kwargs["fired_ts_ms"] == 1_000

    @staticmethod
    async def _polled_store(score: int):
        polled = msgspec.json.encode(_def(schedule_name="s"))
        store, schedules = _make_store()
        schedules.poll_due.return_value = [("s", score, polled)]
        await store.poll_due(2_000)
        return store, schedules, polled

    async def test_commit_fire_passes_polled_payload_for_the_fired_score(self):
        store, schedules, polled = await self._polled_store(1_000)
        await store.commit_fire("s", 1_900, _def(schedule_name="s"), "chief", fired_ts_ms=1_000)
        assert schedules.commit_fire.await_args.kwargs["fired_payload"] == polled

    async def test_commit_fire_without_payload_when_polled_score_differs(self):
        store, schedules, _ = await self._polled_store(1_000)
        await store.commit_fire("s", 1_900, _def(schedule_name="s"), "chief", fired_ts_ms=1_500)
        kwargs = schedules.commit_fire.await_args.kwargs
        assert kwargs["fired_payload"] is None
        assert kwargs["fired_ts_ms"] == 1_500

    async def test_commit_fire_without_poll_passes_no_payload(self):
        store, schedules = _make_store()
        await store.commit_fire("s", 1_900, _def(schedule_name="s"), "chief", fired_ts_ms=1_000)
        assert schedules.commit_fire.await_args.kwargs["fired_payload"] is None

    async def test_polled_payload_is_used_for_one_commit_only(self):
        store, schedules, _ = await self._polled_store(1_000)
        await store.commit_fire("s", 1_900, _def(schedule_name="s"), "chief", fired_ts_ms=1_000)
        await store.commit_fire("s", 2_900, _def(schedule_name="s"), "chief", fired_ts_ms=1_000)
        assert schedules.commit_fire.await_args.kwargs["fired_payload"] is None

    async def test_commit_fire_one_shot_maps_minus_one_to_none(self):
        store, schedules = _make_store()
        await store.commit_fire("once", -1, None, "chief")
        kwargs = schedules.commit_fire.await_args.kwargs
        assert kwargs["next_fire_ts_ms"] is None
        assert kwargs["payload"] is None


class TestScheduleStoreErrorWrapping:
    async def test_add_schedule_propagates_backend_error(self):
        store, schedules = _make_store(hsetnx_return=True)
        schedules.add.side_effect = BackendConnectionError("down")
        with pytest.raises(BackendConnectionError):
            await store.add_schedule(_def())


# Plan 0.12.0, MR 11 (BZ-11): ``remove_schedule`` nimmt den Namen aus dem Set
# der je gemeldeten Crons; ein Store ohne die Methode (Q15) bricht nicht.


class TestRemoveScheduleForgetsKnown:
    async def test_remove_schedule_forgets_the_known_name(self):
        store, schedules = _make_store(hexists_return=1)
        schedules.forget_known_cron = AsyncMock()
        assert await store.remove_schedule("drop") is True
        schedules.forget_known_cron.assert_awaited_once_with("drop")

    async def test_store_without_forget_known_cron_is_tolerated(self):
        store, _schedules = _make_store(hexists_return=1)  # MagicMock: nicht awaitbar
        assert await store.remove_schedule("drop") is True
