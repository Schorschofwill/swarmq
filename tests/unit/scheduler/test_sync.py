"""Cron-Sync-Tests (Plan U11) — verbatim: ``sync_taskinfo_crons``."""

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

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import msgspec
import pytest

from swarmq.config import SwarmQConfig
from swarmq.exceptions import BackendConnectionError
from swarmq.scheduler import (
    ScheduleDefinition,
)
from swarmq.task import Task, TaskInfo
from tests.unit.scheduler._helpers import _make_loop

# AGENTS.md §Async-Code testen rule 3: 5s per-test timeout. Narrower
# than the pyproject default (30s); fails fast on deadlock without
# burning CI time (Unit 3 / H-6).
pytestmark = [pytest.mark.timeout(5)]




class TestSyncTaskinfoCrons:
    async def test_sync_inserts_new_static_crons(
        self, backend, broker, config, lock_manager, store, signal_dispatcher,
        _cleanup_tasks,
    ):
        class Cron5m(Task):
            task_info = TaskInfo(name="cron_5m", queues=["default"],
                                 cron="*/5 * * * *")
            async def run(self):
                return None

        loop = _make_loop(
            backend, broker, config, lock_manager, store, signal_dispatcher
        )
        backend.schedules.get.return_value = None  # fresh

        await loop.sync_taskinfo_crons()

        store.add_schedule.assert_awaited()

    async def test_sync_noop_for_same_static_cron(
        self, backend, broker, config, lock_manager, store, signal_dispatcher,
        _cleanup_tasks,
    ):
        class SameCron(Task):
            task_info = TaskInfo(name="same", queues=["default"],
                                 cron="*/5 * * * *")
            async def run(self):
                return None

        existing = ScheduleDefinition(
            schedule_name="same", task_name="same", args=(), kwargs={},
            queue="default", cron="*/5 * * * *",
            fire_ts_ms=1_700_000_000_000, source="static",
        )
        import msgspec
        backend.schedules.get.return_value = msgspec.json.encode(existing)

        loop = _make_loop(
            backend, broker, config, lock_manager, store, signal_dispatcher
        )
        await loop.sync_taskinfo_crons()

        store.add_schedule.assert_not_awaited()
        store.update_static_schedule.assert_not_awaited()

    async def test_sync_updates_on_static_cron_change(
        self, backend, broker, config, lock_manager, store, signal_dispatcher,
        _cleanup_tasks,
    ):
        class ChangedCron(Task):
            task_info = TaskInfo(name="changed", queues=["default"],
                                 cron="*/10 * * * *")
            async def run(self):
                return None

        existing = ScheduleDefinition(
            schedule_name="changed", task_name="changed", args=(), kwargs={},
            queue="default", cron="*/5 * * * *",  # OLD
            fire_ts_ms=1_700_000_000_000, source="static",
        )
        import msgspec
        backend.schedules.get.return_value = msgspec.json.encode(existing)

        loop = _make_loop(
            backend, broker, config, lock_manager, store, signal_dispatcher
        )
        await loop.sync_taskinfo_crons()

        store.update_static_schedule.assert_awaited_once()
        store.add_schedule.assert_not_awaited()

    async def test_sync_preserves_disabled_flag_on_static_cron_change(
        self, backend, broker, config, lock_manager, store, signal_dispatcher,
        _cleanup_tasks,
    ):
        """Review-finding correctness C-01: an operator-disabled static
        schedule must remain disabled after the cron string is modified
        in code. Without this preservation, the next redeploy silently
        reverses the operator's `schedule remove` intent."""
        class ChangedDisabledCron(Task):
            task_info = TaskInfo(name="disabled_static", queues=["default"],
                                 cron="*/10 * * * *")
            async def run(self):
                return None

        existing = ScheduleDefinition(
            schedule_name="disabled_static", task_name="disabled_static",
            args=(), kwargs={}, queue="default",
            cron="*/5 * * * *",          # OLD cron — differs from code
            fire_ts_ms=1_700_000_000_000,
            source="static",
            disabled=True,                # operator disabled this entry
        )
        import msgspec
        backend.schedules.get.return_value = msgspec.json.encode(existing)

        loop = _make_loop(
            backend, broker, config, lock_manager, store, signal_dispatcher
        )
        await loop.sync_taskinfo_crons()

        store.update_static_schedule.assert_awaited_once()
        propagated = store.update_static_schedule.await_args.args[0]
        assert propagated.disabled is True
        assert propagated.cron == "*/10 * * * *"  # new cron applied

    async def test_sync_disabled_false_stays_false_on_cron_change(
        self, backend, broker, config, lock_manager, store, signal_dispatcher,
        _cleanup_tasks,
    ):
        """Inverse regression: the preservation path must not flip a
        disabled=False entry to True."""
        class ActiveCron(Task):
            task_info = TaskInfo(name="active_static", queues=["default"],
                                 cron="0 */4 * * *")
            async def run(self):
                return None

        existing = ScheduleDefinition(
            schedule_name="active_static", task_name="active_static",
            args=(), kwargs={}, queue="default",
            cron="0 3 * * *",
            fire_ts_ms=1_700_000_000_000,
            source="static",
            disabled=False,
        )
        import msgspec
        backend.schedules.get.return_value = msgspec.json.encode(existing)

        loop = _make_loop(
            backend, broker, config, lock_manager, store, signal_dispatcher
        )
        await loop.sync_taskinfo_crons()

        propagated = store.update_static_schedule.await_args.args[0]
        assert propagated.disabled is False

    async def test_sync_preserves_cli_source_even_on_cron_diff(
        self, backend, broker, config, lock_manager, store, signal_dispatcher,
        _cleanup_tasks,
    ):
        """CD11: CLI ownership beats code — even when code has a
        different cron, we must not overwrite a CLI entry."""
        class CronTask(Task):
            task_info = TaskInfo(name="conflict", queues=["default"],
                                 cron="*/10 * * * *")
            async def run(self):
                return None

        existing = ScheduleDefinition(
            schedule_name="conflict", task_name="conflict", args=(),
            kwargs={}, queue="default", cron="*/5 * * * *",
            fire_ts_ms=1_700_000_000_000, source="cli",  # CLI entry
        )
        import msgspec
        backend.schedules.get.return_value = msgspec.json.encode(existing)

        loop = _make_loop(
            backend, broker, config, lock_manager, store, signal_dispatcher
        )
        await loop.sync_taskinfo_crons()

        store.update_static_schedule.assert_not_awaited()
        store.add_schedule.assert_not_awaited()

    async def test_sync_decode_error_does_not_overwrite(
        self, backend, broker, config, lock_manager, store, signal_dispatcher,
        _cleanup_tasks,
    ):
        """Unit 5 / M-3: a malformed existing entry must NOT be silently
        overwritten — the source (cli vs static) cannot be determined
        from a malformed payload, so overwriting risks clobbering a
        CD11-protected CLI entry. Log loudly and leave it alone."""
        class SomeCron(Task):
            task_info = TaskInfo(name="mystery", queues=["default"],
                                 cron="*/5 * * * *")
            async def run(self):
                return None

        backend.schedules.get.return_value = b"{not valid json"

        loop = _make_loop(
            backend, broker, config, lock_manager, store, signal_dispatcher
        )
        await loop.sync_taskinfo_crons()

        store.add_schedule.assert_not_awaited()
        store.update_static_schedule.assert_not_awaited()

    async def test_sync_partial_failure_continues_iteration(
        self, backend, broker, config, lock_manager, store, signal_dispatcher,
        _cleanup_tasks,
    ):
        """Unit 5 / M-4: a BackendConnectionError on one schedule must
        not abort the sync for the rest. Two tasks defined; first add
        raises, second must still be attempted."""
        from swarmq.exceptions import BackendConnectionError as BCE

        class CronA(Task):
            task_info = TaskInfo(name="sync_a", queues=["default"],
                                 cron="*/5 * * * *")
            async def run(self): return None

        class CronB(Task):
            task_info = TaskInfo(name="sync_b", queues=["default"],
                                 cron="0 3 * * *")
            async def run(self): return None

        backend.schedules.get.return_value = None  # fresh for all
        # First add_schedule call raises, second succeeds
        store.add_schedule.side_effect = [BCE("valkey flap"), None]

        loop = _make_loop(
            backend, broker, config, lock_manager, store, signal_dispatcher
        )
        # Must not raise — per-schedule error is logged and swallowed
        await loop.sync_taskinfo_crons()

        # Both schedules were attempted even though the first failed
        assert store.add_schedule.await_count == 2


class TestSyncTaskinfoCronsLogging:
    @pytest.fixture
    def loop(self, backend, broker, config, lock_manager, store, signal_dispatcher, _cleanup_tasks):
        class LoggedCron(Task):
            task_info = TaskInfo(name="logged", queues=["default"], cron="*/5 * * * *")

            async def run(self):
                return None

        return _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)

    @staticmethod
    def _entry(logs, event):
        [entry] = [e for e in logs if e["event"] == event]
        assert entry["logger"] == "swarmq.scheduler.sync" and entry["schedule_name"] == "logged"
        return entry

    async def test_hget_failure_logs_error_fields(self, loop, backend, swarmq_logs):
        backend.schedules.get.side_effect = BackendConnectionError("valkey down")
        await loop.sync_taskinfo_crons()
        entry = self._entry(swarmq_logs, "scheduler_sync_hget_failed")
        assert entry["log_level"] == "warning"
        assert entry["error_type"] == "BackendConnectionError" and entry["error"] == "valkey down"

    async def test_add_failure_logs_error_fields(self, loop, backend, store, swarmq_logs):
        backend.schedules.get.return_value = None
        store.add_schedule.side_effect = BackendConnectionError("valkey flap")
        await loop.sync_taskinfo_crons()
        entry = self._entry(swarmq_logs, "scheduler_sync_add_failed")
        assert entry["log_level"] == "warning"
        assert entry["error_type"] == "BackendConnectionError" and entry["error"] == "valkey flap"

    async def test_decode_error_logs_error_fields(self, loop, backend, swarmq_logs):
        backend.schedules.get.return_value = b"{not valid json"
        await loop.sync_taskinfo_crons()
        entry = self._entry(swarmq_logs, "scheduler_sync_decode_error_skipped")
        assert entry["log_level"] == "error" and "hint" in entry
        assert entry["error_type"] == "DecodeError" and entry["error"]

    async def test_update_failure_logs_error_fields(self, loop, backend, store, swarmq_logs):
        existing = ScheduleDefinition(
            schedule_name="logged", task_name="logged", args=(), kwargs={},
            queue="default", cron="*/10 * * * *", fire_ts_ms=1_700_000_000_000, source="static",
        )
        backend.schedules.get.return_value = msgspec.json.encode(existing)
        store.update_static_schedule.side_effect = BackendConnectionError("valkey flap")
        await loop.sync_taskinfo_crons()
        entry = self._entry(swarmq_logs, "scheduler_sync_update_failed")
        assert entry["log_level"] == "warning"
        assert entry["error_type"] == "BackendConnectionError" and entry["error"] == "valkey flap"



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
        self, backend, broker, config, lock_manager, store, signal_dispatcher,
    ):
        class TokyoCron(Task):
            task_info = TaskInfo(name="tokyo", queues=["default"],
                                 cron="0 9 * * *", timezone="Asia/Tokyo")
            async def run(self):
                return None

        loop = _make_loop(
            backend, broker, config, lock_manager, store, signal_dispatcher
        )

        await loop.sync_taskinfo_crons()

        assert self._added(store).fire_ts_ms == _utc_ms(2026, 10, 7, 0, 0)

    async def test_new_definition_carries_timezone(
        self, backend, broker, config, lock_manager, store, signal_dispatcher,
    ):
        class TokyoCron(Task):
            task_info = TaskInfo(name="tokyo", queues=["default"],
                                 cron="0 9 * * *", timezone="Asia/Tokyo")
            async def run(self):
                return None

        loop = _make_loop(
            backend, broker, config, lock_manager, store, signal_dispatcher
        )

        await loop.sync_taskinfo_crons()

        assert self._added(store).timezone == "Asia/Tokyo"

    async def test_config_scheduler_timezone_is_the_default(
        self, backend, broker, lock_manager, store, signal_dispatcher,
    ):
        class PlainCron(Task):
            task_info = TaskInfo(name="plain", queues=["default"],
                                 cron="0 9 * * *")
            async def run(self):
                return None

        config = SwarmQConfig(
            broker_url="amqp://localhost",
            backend_url="valkey://localhost",
            scheduler_timezone="Asia/Tokyo",
        )
        loop = _make_loop(
            backend, broker, config, lock_manager, store, signal_dispatcher
        )

        await loop.sync_taskinfo_crons()

        added = self._added(store)
        assert added.fire_ts_ms == _utc_ms(2026, 10, 7, 0, 0)
        assert added.timezone == "Asia/Tokyo"
