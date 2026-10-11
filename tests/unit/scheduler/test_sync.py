"""Cron-Sync-Tests (Plan U11): Insert-or-Update statischer Definitionen.

Seit U12 (Plan 2026-10-08-1541) übernimmt der Leader die Crons aus den
Meldungen aller Worker; ``_sync`` speist hier die statischen Definitionen der
registrierten Tasks direkt in ``apply_static_definition`` — derselbe Weg wie
eine gewählte Meldung."""

# Test list (Plan 2026-10-06-0831, U9, R17): Fehler-Events des Syncs
# tragen ``error_type``/``error``.
# 1. ``scheduler_sync_hget_failed`` (WARNING)
# 2. ``scheduler_sync_add_failed`` (WARNING)
# 3. ``scheduler_sync_decode_error_skipped`` (ERROR)
# 4. ``scheduler_sync_update_failed`` (WARNING)
#
# Test list (Plan 2026-10-08-1541, U2): erster Score in der Zone des Crons.
# 5. ``TaskInfo.timezone`` bestimmt den Score einer neuen Definition.
# 6. Die neue Definition traegt die Zone.
# 7. Ohne ``TaskInfo.timezone`` gilt ``config.scheduler_timezone``.
#
# Test list (Plan 2026-10-08-1541, U3, R9/R18, KTD12): Übernahme ohne Verlust.
# 8. Ausdrückliches ``timezone="UTC"`` schlägt einen globalen Standard; die
#    gespeicherte Definition trägt UTC als None.
# 9. Geänderte Zeitzone, Score nicht fällig → Definition und Score neu.
# 10. Geänderte Queue → aktualisiert.
# 11. Geänderter Task-Name → aktualisiert.
# 12. Geänderte Definition bei ``disabled=True`` → bleibt deaktiviert, kein
#     Eintrag im Fire-Index.
# 13. Geänderte Zeitzone, Score bereits fällig → Score bleibt, der Termin
#     feuert mit dem neuen Inhalt.
# 14. CLI-Definition gleichen Namens mit anderer Zone → Warnung, keine
#     Änderung (Regression).
#
# Test list (Plan 0.12.0, MR 9, BZ-09):
# 15. Bestandsdefinition ohne ``unique_for``, Task mit ``unique_for`` →
#     übernommen (Upgrade von 0.11).
# 16. Geändertes ``unique_until`` bei gesetztem ``unique_for`` → übernommen.

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import override

import msgspec
import pytest

from swarmq.config import SwarmQConfig
from swarmq.exceptions import BackendConnectionError
from swarmq.scheduler import (
    ScheduleDefinition,
    ScheduleStore,
)
from swarmq.scheduler import sync as sync_module
from swarmq.scheduler.reports import static_definitions
from swarmq.task import Task, TaskInfo
from tests.support.inmemory import InMemoryBackend
from tests.support.inmemory.clock import FakeClock
from tests.unit.scheduler._helpers import _make_loop

# AGENTS.md §Async-Code testen rule 3: 5s per-test timeout. Narrower
# than the pyproject default (30s); fails fast on deadlock without
# burning CI time (Unit 3 / H-6).
pytestmark = [pytest.mark.timeout(5)]


async def _sync(loop, default_timezone: str | None = None) -> None:
    """Statische Crons der registrierten Tasks wie gewählte Meldungen
    übernehmen (Uhr: ``swarmq.scheduler.sync.time``, in Tests fixiert)."""
    now_ms = int(sync_module.time.time() * 1000)
    for desired in static_definitions(Task._registered_tasks, default_timezone):
        await loop.apply_static_definition(desired, now_ms)


class TestApplyStaticDefinition:
    @pytest.mark.usefixtures("_cleanup_tasks")
    async def test_sync_inserts_new_static_crons(
        self,
        backend,
        broker,
        config,
        lock_manager,
        store,
        signal_dispatcher,
    ):
        class Cron5m(Task):
            task_info = TaskInfo(name="cron_5m", queues=["default"], cron="*/5 * * * *")

            @override
            async def run(self):
                return None

        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
        backend.schedules.get.return_value = None  # fresh

        await _sync(loop)

        store.add_schedule.assert_awaited()

    @pytest.mark.usefixtures("_cleanup_tasks")
    async def test_sync_noop_for_same_static_cron(
        self,
        backend,
        broker,
        config,
        lock_manager,
        store,
        signal_dispatcher,
    ):
        class SameCron(Task):
            task_info = TaskInfo(name="same", queues=["default"], cron="*/5 * * * *")

            @override
            async def run(self):
                return None

        existing = ScheduleDefinition(
            schedule_name="same",
            task_name="same",
            args=(),
            kwargs={},
            queue="default",
            cron="*/5 * * * *",
            fire_ts_ms=1_700_000_000_000,
            source="static",
        )
        import msgspec

        backend.schedules.get.return_value = msgspec.json.encode(existing)
        # Geplant (Score im Fire-Index); ohne Score heilte der Sync (U12).
        backend.schedules.fire_ts.return_value = 1_700_000_000_000

        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
        await _sync(loop)

        store.add_schedule.assert_not_awaited()
        store.update_static_schedule.assert_not_awaited()

    @pytest.mark.usefixtures("_cleanup_tasks")
    async def test_sync_updates_on_static_cron_change(
        self,
        backend,
        broker,
        config,
        lock_manager,
        store,
        signal_dispatcher,
    ):
        class ChangedCron(Task):
            task_info = TaskInfo(name="changed", queues=["default"], cron="*/10 * * * *")

            @override
            async def run(self):
                return None

        existing = ScheduleDefinition(
            schedule_name="changed",
            task_name="changed",
            args=(),
            kwargs={},
            queue="default",
            cron="*/5 * * * *",  # OLD
            fire_ts_ms=1_700_000_000_000,
            source="static",
        )
        import msgspec

        backend.schedules.get.return_value = msgspec.json.encode(existing)

        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
        await _sync(loop)

        store.update_static_schedule.assert_awaited_once()
        store.add_schedule.assert_not_awaited()

    @pytest.mark.usefixtures("_cleanup_tasks")
    async def test_sync_preserves_disabled_flag_on_static_cron_change(
        self,
        backend,
        broker,
        config,
        lock_manager,
        store,
        signal_dispatcher,
    ):
        """Review-finding correctness C-01: an operator-disabled static
        schedule must remain disabled after the cron string is modified
        in code. Without this preservation, the next redeploy silently
        reverses the operator's `schedule remove` intent."""

        class ChangedDisabledCron(Task):
            task_info = TaskInfo(name="disabled_static", queues=["default"], cron="*/10 * * * *")

            @override
            async def run(self):
                return None

        existing = ScheduleDefinition(
            schedule_name="disabled_static",
            task_name="disabled_static",
            args=(),
            kwargs={},
            queue="default",
            cron="*/5 * * * *",  # OLD cron — differs from code
            fire_ts_ms=1_700_000_000_000,
            source="static",
            disabled=True,  # operator disabled this entry
        )
        import msgspec

        backend.schedules.get.return_value = msgspec.json.encode(existing)

        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
        await _sync(loop)

        store.update_static_schedule.assert_awaited_once()
        propagated = store.update_static_schedule.await_args.args[0]
        assert propagated.disabled is True
        assert propagated.cron == "*/10 * * * *"  # new cron applied

    @pytest.mark.usefixtures("_cleanup_tasks")
    async def test_sync_disabled_false_stays_false_on_cron_change(
        self,
        backend,
        broker,
        config,
        lock_manager,
        store,
        signal_dispatcher,
    ):
        """Inverse regression: the preservation path must not flip a
        disabled=False entry to True."""

        class ActiveCron(Task):
            task_info = TaskInfo(name="active_static", queues=["default"], cron="0 */4 * * *")

            @override
            async def run(self):
                return None

        existing = ScheduleDefinition(
            schedule_name="active_static",
            task_name="active_static",
            args=(),
            kwargs={},
            queue="default",
            cron="0 3 * * *",
            fire_ts_ms=1_700_000_000_000,
            source="static",
            disabled=False,
        )
        import msgspec

        backend.schedules.get.return_value = msgspec.json.encode(existing)

        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
        await _sync(loop)

        propagated = store.update_static_schedule.await_args.args[0]
        assert propagated.disabled is False

    @pytest.mark.usefixtures("_cleanup_tasks")
    async def test_sync_preserves_cli_source_even_on_cron_diff(
        self,
        backend,
        broker,
        config,
        lock_manager,
        store,
        signal_dispatcher,
    ):
        """CD11: CLI ownership beats code — even when code has a
        different cron, we must not overwrite a CLI entry."""

        class CronTask(Task):
            task_info = TaskInfo(name="conflict", queues=["default"], cron="*/10 * * * *")

            @override
            async def run(self):
                return None

        existing = ScheduleDefinition(
            schedule_name="conflict",
            task_name="conflict",
            args=(),
            kwargs={},
            queue="default",
            cron="*/5 * * * *",
            fire_ts_ms=1_700_000_000_000,
            source="cli",  # CLI entry
        )
        import msgspec

        backend.schedules.get.return_value = msgspec.json.encode(existing)

        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
        await _sync(loop)

        store.update_static_schedule.assert_not_awaited()
        store.add_schedule.assert_not_awaited()

    @pytest.mark.usefixtures("_cleanup_tasks")
    async def test_sync_decode_error_does_not_overwrite(
        self,
        backend,
        broker,
        config,
        lock_manager,
        store,
        signal_dispatcher,
    ):
        """Unit 5 / M-3: a malformed existing entry must NOT be silently
        overwritten — the source (cli vs static) cannot be determined
        from a malformed payload, so overwriting risks clobbering a
        CD11-protected CLI entry. Log loudly and leave it alone."""

        class SomeCron(Task):
            task_info = TaskInfo(name="mystery", queues=["default"], cron="*/5 * * * *")

            @override
            async def run(self):
                return None

        backend.schedules.get.return_value = b"{not valid json"

        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
        await _sync(loop)

        store.add_schedule.assert_not_awaited()
        store.update_static_schedule.assert_not_awaited()

    @pytest.mark.usefixtures("_cleanup_tasks")
    async def test_sync_partial_failure_continues_iteration(
        self,
        backend,
        broker,
        config,
        lock_manager,
        store,
        signal_dispatcher,
    ):
        """Unit 5 / M-4: a BackendConnectionError on one schedule must
        not abort the sync for the rest. Two tasks defined; first add
        raises, second must still be attempted."""
        from swarmq.exceptions import BackendConnectionError as BCE

        class CronA(Task):
            task_info = TaskInfo(name="sync_a", queues=["default"], cron="*/5 * * * *")

            @override
            async def run(self):
                return None

        class CronB(Task):
            task_info = TaskInfo(name="sync_b", queues=["default"], cron="0 3 * * *")

            @override
            async def run(self):
                return None

        backend.schedules.get.return_value = None  # fresh for all
        # First add_schedule call raises, second succeeds
        store.add_schedule.side_effect = [BCE("valkey flap"), None]

        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
        # Must not raise — per-schedule error is logged and swallowed
        await _sync(loop)

        # Both schedules were attempted even though the first failed
        assert store.add_schedule.await_count == 2


class TestApplyStaticDefinitionLogging:
    @pytest.fixture
    def loop(self, backend, broker, config, lock_manager, store, signal_dispatcher, _cleanup_tasks):
        class LoggedCron(Task):
            task_info = TaskInfo(name="logged", queues=["default"], cron="*/5 * * * *")

            @override
            async def run(self):
                return None

        return _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)

    @staticmethod
    def _entry(logs, event):
        [entry] = [e for e in logs if e["event"] == event]
        assert entry["logger"] == "swarmq.scheduler.sync"
        assert entry["schedule_name"] == "logged"
        return entry

    async def test_hget_failure_logs_error_fields(self, loop, backend, swarmq_logs):
        backend.schedules.get.side_effect = BackendConnectionError("valkey down")
        await _sync(loop)
        entry = self._entry(swarmq_logs, "scheduler_sync_hget_failed")
        assert entry["log_level"] == "warning"
        assert entry["error_type"] == "BackendConnectionError"
        assert entry["error"] == "valkey down"

    async def test_add_failure_logs_error_fields(self, loop, backend, store, swarmq_logs):
        backend.schedules.get.return_value = None
        store.add_schedule.side_effect = BackendConnectionError("valkey flap")
        await _sync(loop)
        entry = self._entry(swarmq_logs, "scheduler_sync_add_failed")
        assert entry["log_level"] == "warning"
        assert entry["error_type"] == "BackendConnectionError"
        assert entry["error"] == "valkey flap"

    async def test_decode_error_logs_error_fields(self, loop, backend, swarmq_logs):
        backend.schedules.get.return_value = b"{not valid json"
        await _sync(loop)
        entry = self._entry(swarmq_logs, "scheduler_sync_decode_error_skipped")
        assert entry["log_level"] == "error"
        assert "hint" in entry
        assert entry["error_type"] == "DecodeError"
        assert entry["error"]

    async def test_update_failure_logs_error_fields(self, loop, backend, store, swarmq_logs):
        existing = ScheduleDefinition(
            schedule_name="logged",
            task_name="logged",
            args=(),
            kwargs={},
            queue="default",
            cron="*/10 * * * *",
            fire_ts_ms=1_700_000_000_000,
            source="static",
        )
        backend.schedules.get.return_value = msgspec.json.encode(existing)
        store.update_static_schedule.side_effect = BackendConnectionError("valkey flap")
        await _sync(loop)
        entry = self._entry(swarmq_logs, "scheduler_sync_update_failed")
        assert entry["log_level"] == "warning"
        assert entry["error_type"] == "BackendConnectionError"
        assert entry["error"] == "valkey flap"


def _utc_ms(*args: int) -> int:
    return int(datetime(*args, tzinfo=UTC).timestamp() * 1000)


class TestSyncTimezone:
    """Plan 2026-10-08-1541 (U2): der erste Score einer neuen statischen
    Definition gilt in der Zone des Crons. Die Uhr ist fest."""

    @pytest.fixture(autouse=True)
    def _fixed_clock(self, monkeypatch, _cleanup_tasks):
        # 2026-10-06 10:00 UTC = 19:00 in Tokio.
        fake_time = SimpleNamespace(time=lambda: _utc_ms(2026, 10, 6, 10, 0) / 1000)
        monkeypatch.setattr("swarmq.scheduler.sync.time", fake_time)

    @staticmethod
    def _added(store) -> ScheduleDefinition:
        [call] = store.add_schedule.await_args_list
        return call.args[0]

    async def test_first_score_uses_taskinfo_timezone(
        self,
        backend,
        broker,
        config,
        lock_manager,
        store,
        signal_dispatcher,
    ):
        class TokyoCron(Task):
            task_info = TaskInfo(name="tokyo", queues=["default"], cron="0 9 * * *", timezone="Asia/Tokyo")

            @override
            async def run(self):
                return None

        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)

        await _sync(loop)

        assert self._added(store).fire_ts_ms == _utc_ms(2026, 10, 7, 0, 0)

    async def test_new_definition_carries_timezone(
        self,
        backend,
        broker,
        config,
        lock_manager,
        store,
        signal_dispatcher,
    ):
        class TokyoCron(Task):
            task_info = TaskInfo(name="tokyo", queues=["default"], cron="0 9 * * *", timezone="Asia/Tokyo")

            @override
            async def run(self):
                return None

        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)

        await _sync(loop)

        assert self._added(store).timezone == "Asia/Tokyo"

    async def test_config_scheduler_timezone_is_the_default(
        self,
        backend,
        broker,
        lock_manager,
        store,
        signal_dispatcher,
    ):
        class PlainCron(Task):
            task_info = TaskInfo(name="plain", queues=["default"], cron="0 9 * * *")

            @override
            async def run(self):
                return None

        config = SwarmQConfig(
            broker_url="amqp://localhost",
            backend_url="valkey://localhost",
            scheduler_timezone="Asia/Tokyo",
        )
        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)

        await _sync(loop, config.scheduler_timezone)

        added = self._added(store)
        assert added.fire_ts_ms == _utc_ms(2026, 10, 7, 0, 0)
        assert added.timezone == "Asia/Tokyo"

    async def test_explicit_utc_beats_config_default(
        self,
        backend,
        broker,
        lock_manager,
        store,
        signal_dispatcher,
    ):
        class UtcCron(Task):
            task_info = TaskInfo(name="utc", queues=["default"], cron="0 9 * * *", timezone="UTC")

            @override
            async def run(self):
                return None

        config = SwarmQConfig(
            broker_url="amqp://localhost",
            backend_url="valkey://localhost",
            scheduler_timezone="Asia/Tokyo",
        )
        loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)

        await _sync(loop, config.scheduler_timezone)

        added = self._added(store)
        assert added.fire_ts_ms == _utc_ms(2026, 10, 6, 9, 0) + 86_400_000
        assert added.timezone is None


_NOW_MS = _utc_ms(2026, 10, 6, 10, 0)  # 19:00 in Tokio
_LATER_MS = _NOW_MS + 3_600_000  # nicht fällig
_EARLIER_MS = _NOW_MS - 60_000  # bereits fällig


class TestSyncInsertOrUpdate:
    """Plan 2026-10-08-1541 (U3, KTD12): Übernahme einer statischen
    Definition gegen den Bestand eines In-Memory-Stores; die Uhr ist fest."""

    @pytest.fixture(autouse=True)
    def _fixed_clock(self, monkeypatch, _cleanup_tasks):
        fake_time = SimpleNamespace(time=lambda: _NOW_MS / 1000)
        monkeypatch.setattr("swarmq.scheduler.sync.time", fake_time)

    @pytest.fixture
    async def memory(self):
        be = InMemoryBackend(FakeClock(_NOW_MS))
        await be.connect()
        return be

    @pytest.fixture
    def sync_loop(self, memory, broker, config, lock_manager, signal_dispatcher):
        return _make_loop(
            memory,
            broker,
            config,
            lock_manager,
            ScheduleStore(memory),
            signal_dispatcher,
        )

    @staticmethod
    async def _seed(memory, score_ms, **fields):
        base = {
            "schedule_name": "tz",
            "task_name": "tz",
            "args": (),
            "kwargs": {},
            "queue": "default",
            "cron": "0 9 * * *",
            "fire_ts_ms": score_ms,
            "source": "static",
        }
        base.update(fields)
        definition = ScheduleDefinition(**base)
        fire = None if definition.disabled else score_ms
        await memory.schedules.add("tz", msgspec.json.encode(definition), fire)

    @staticmethod
    async def _stored(memory) -> tuple[ScheduleDefinition, int | None]:
        raw = await memory.schedules.get("tz")
        due = {n: ts for n, ts, _ in await memory.schedules.poll_due(2**62)}
        return msgspec.json.decode(raw, type=ScheduleDefinition), due.get("tz")

    @staticmethod
    def _task(**fields):
        info = {"name": "tz", "queues": ["default"], "cron": "0 9 * * *"}
        info.update(fields)

        class TzCron(Task):
            task_info = TaskInfo(**info)

            @override
            async def run(self):
                return None

        return TzCron

    async def test_changed_timezone_with_future_score_replans(self, memory, sync_loop):
        await self._seed(memory, _LATER_MS)
        self._task(timezone="Asia/Tokyo")

        await _sync(sync_loop)

        stored, score = await self._stored(memory)
        assert stored.timezone == "Asia/Tokyo"
        assert score == stored.fire_ts_ms == _utc_ms(2026, 10, 7, 0, 0)

    async def test_changed_queue_updates_definition(self, memory, sync_loop):
        await self._seed(memory, _LATER_MS, queue="old")
        self._task(queues=["new"])

        await _sync(sync_loop)

        stored, _ = await self._stored(memory)
        assert stored.queue == "new"

    async def test_changed_task_name_updates_definition(self, memory, sync_loop):
        await self._seed(memory, _LATER_MS, task_name="renamed")
        self._task()

        await _sync(sync_loop)

        stored, _ = await self._stored(memory)
        assert stored.task_name == "tz"

    async def test_added_unique_for_updates_definition(self, memory, sync_loop):
        """BZ-09: eine Definition aus 0.11 (ohne ``unique_for``) bekommt die
        Unique-Einstellung des Tasks, damit jeder Leader sie anwendet."""
        await self._seed(memory, _LATER_MS)
        self._task(unique_for=3600, unique_until="start")

        await _sync(sync_loop)

        stored, _ = await self._stored(memory)
        assert (stored.unique_for, stored.unique_until) == (3600, "start")

    async def test_changed_unique_until_updates_definition(self, memory, sync_loop):
        await self._seed(memory, _LATER_MS, unique_for=3600, unique_until="completion")
        self._task(unique_for=3600, unique_until="start")

        await _sync(sync_loop)

        stored, _ = await self._stored(memory)
        assert stored.unique_until == "start"

    async def test_changed_disabled_definition_gets_no_fire_entry(self, memory, sync_loop):
        await self._seed(memory, _LATER_MS, disabled=True)
        self._task(timezone="Asia/Tokyo")

        await _sync(sync_loop)

        stored, score = await self._stored(memory)
        assert stored.timezone == "Asia/Tokyo"
        assert stored.disabled is True
        assert score is None

    async def test_changed_timezone_keeps_due_score(self, memory, sync_loop):
        """KTD12: ein fälliger, noch nicht gefeuerter Termin geht nicht
        verloren; der nächste Poll liefert ihn mit dem neuen Inhalt."""
        await self._seed(memory, _EARLIER_MS)
        self._task(timezone="Asia/Tokyo")

        await _sync(sync_loop)

        [(name, score, payload)] = await memory.schedules.poll_due(_NOW_MS)
        assert (name, score) == ("tz", _EARLIER_MS)
        assert msgspec.json.decode(payload, type=ScheduleDefinition).timezone == "Asia/Tokyo"

    async def test_cli_definition_with_other_timezone_is_left_alone(
        self,
        memory,
        sync_loop,
        swarmq_logs,
    ):
        """Regression (CD11): Code ändert keine CLI-Definition, auch nicht
        deren Zone; der Schatten wird als Warnung sichtbar."""
        await self._seed(memory, _LATER_MS, source="cli")
        self._task(timezone="Asia/Tokyo")

        await _sync(sync_loop)

        stored, score = await self._stored(memory)
        assert (stored.source, stored.timezone, score) == ("cli", None, _LATER_MS)
        assert [e for e in swarmq_logs if e["event"] == "scheduler_static_cron_shadowed_by_cli"]
