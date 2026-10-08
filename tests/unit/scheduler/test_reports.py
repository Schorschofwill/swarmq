"""Cron-Meldungen aller Worker (Plan 2026-10-08-1541, U12, KTD10, R9/R12)."""

# Test list (U12):
# 1. Fingerabdruck: gleiche Code-Felder → gleicher Abdruck; eine andere Zone,
#    Queue, Cron oder ein anderer Task-Name → anderer Abdruck; Felder außerhalb
#    des Codes (fire_ts_ms, disabled, last_fire_at) ändern ihn nicht.
# 2. Statische Definitionen aus den Tasks: nur Tasks mit Cron, effektive Zone
#    (Task-Zone, sonst Standard, UTC = None), erste Queue, source="static".
# 3. Auswahl: pro Name gewinnt unter den lebenden Meldungen das jüngste
#    ``first_seen``; tote Meldungen zählen nicht; ohne lebende Meldung keine
#    Auswahl.
# 4. Konflikt: mehrere lebende Abdrücke länger als 10 Minuten → markiert.
# 5. Leader übernimmt die gewählte Meldung (Insert-or-Update über U3).
# 6. Ungültige Meldung (Zone, Ausdruck, kaputter Payload) → nicht übernommen,
#    geloggt.
# 7. Konflikt → ``scheduler_cron_conflict`` und Signal.
# 8. Leader löscht Meldungen älter als 7 Tage.
# 9. Fehler beim Lesen der Meldungen → geloggt, kein Abbruch.
# 10. Upgrade von 0.9.0: Definitionen ohne Meldungen bleiben unverändert.
# 11. Worker meldet beim Start und danach periodisch; Valkey nicht erreichbar
#     → Worker läuft weiter.
# 12. Leader wählt im Takt ``scheduler_seen_interval_s``, nicht im Poll-Takt;
#     sofort beim Erwerb der Leader-Rolle.
# 13. Übernommene fremde Crons lösen beim Fire keine Registry-Warnung aus.

from __future__ import annotations

import asyncio
import dataclasses
from datetime import UTC, datetime
from types import SimpleNamespace

import msgspec
import pytest

from swarmq.contracts import CronReport, CronReports
from swarmq.exceptions import BackendConnectionError
from swarmq.scheduler import ScheduleDefinition, ScheduleStore
from swarmq.scheduler.reports import choose_reports, cron_fingerprint, static_definitions
from swarmq.signals import Signal
from swarmq.task import Task, TaskInfo
from tests.support.inmemory import FakeClock, InMemoryBackend
from tests.unit.scheduler._helpers import _make_loop

pytestmark = [pytest.mark.timeout(5)]


def _static(**fields) -> ScheduleDefinition:
    base = {
        "schedule_name": "nightly", "task_name": "nightly", "args": (),
        "kwargs": {}, "queue": "default", "cron": "0 1 * * *",
        "fire_ts_ms": None, "source": "static",
    }
    base.update(fields)
    return ScheduleDefinition(**base)


class TestFingerprint:
    def test_same_code_fields_give_same_fingerprint(self):
        assert cron_fingerprint(_static()) == cron_fingerprint(
            _static(fire_ts_ms=123, disabled=True, last_fire_at=99)
        )

    @pytest.mark.parametrize("change", [
        {"cron": "0 3 * * *"},
        {"timezone": "Europe/Berlin"},
        {"queue": "other"},
        {"task_name": "renamed"},
    ])
    def test_each_code_field_changes_the_fingerprint(self, change):
        assert cron_fingerprint(_static(**change)) != cron_fingerprint(_static())


def _task(**info) -> type[Task]:
    class _T(Task):
        task_info = TaskInfo(**info)

        async def run(self):
            return None

    return _T


class TestStaticDefinitions:
    def test_only_tasks_with_cron_become_static_definitions(self, _cleanup_tasks):
        cron = _task(name="nightly", queues=["q1", "q2"], cron="0 1 * * *")
        plain = _task(name="plain", queues=["q1"])

        assert static_definitions([cron, plain], None) == [
            _static(queue="q1"),
        ]

    def test_effective_timezone_is_resolved(self, _cleanup_tasks):
        own = _task(name="own", queues=["q"], cron="0 1 * * *", timezone="Asia/Tokyo")
        default = _task(name="default", queues=["q"], cron="0 1 * * *")
        utc = _task(name="utc", queues=["q"], cron="0 1 * * *", timezone="UTC")

        zones = {
            d.schedule_name: d.timezone
            for d in static_definitions([own, default, utc], "Europe/Berlin")
        }

        assert zones == {"own": "Asia/Tokyo", "default": "Europe/Berlin", "utc": None}


_NOW = 1_800_000_000_000
_WINDOW = 180_000


def _report(fp: str, first: int, last: int = _NOW, name: str = "nightly") -> CronReport:
    return CronReport(name, fp, first, last, f"def-{fp}".encode())


def _chosen(*reports: CronReport) -> dict[str, str]:
    snapshot = CronReports(now_ms=_NOW, reports=reports)
    return {
        c.report.name: c.report.fingerprint
        for c in choose_reports(snapshot, live_window_ms=_WINDOW)
    }


class TestChooseReports:
    def test_single_live_report_is_chosen(self):
        assert _chosen(_report("a", first=_NOW - 1_000)) == {"nightly": "a"}

    def test_youngest_first_seen_among_live_reports_wins(self):
        old = _report("old", first=_NOW - 60_000)
        new = _report("new", first=_NOW - 30_000)
        assert _chosen(old, new) == {"nightly": "new"}
        assert _chosen(new, old) == {"nightly": "new"}

    def test_dead_report_does_not_count(self):
        """Rollback: nur noch der alte Stand meldet; der neue ist tot."""
        old = _report("old", first=_NOW - 600_000)
        dead_new = _report("new", first=_NOW - 300_000, last=_NOW - _WINDOW - 1)
        assert _chosen(old, dead_new) == {"nightly": "old"}

    def test_name_with_only_dead_reports_is_not_chosen(self):
        assert _chosen(_report("a", first=_NOW - 600_000, last=_NOW - _WINDOW - 1)) == {}

    def test_tie_on_first_seen_is_decided_independent_of_order(self):
        """Sonst wechselte die Auswahl je nach Lesereihenfolge hin und her."""
        a = _report("a", first=_NOW - 1_000)
        b = _report("b", first=_NOW - 1_000)
        assert _chosen(a, b) == _chosen(b, a)


def _choices(*reports: CronReport):
    snapshot = CronReports(now_ms=_NOW, reports=reports)
    return {c.report.name: c for c in choose_reports(snapshot, live_window_ms=_WINDOW)}


class TestConflict:
    def test_two_live_fingerprints_for_more_than_ten_minutes_conflict(self):
        old = _report("old", first=_NOW - 3_600_000)
        new = _report("new", first=_NOW - 600_001)
        choice = _choices(old, new)["nightly"]
        assert choice.conflict == ("new", "old")

    def test_rolling_deploy_under_ten_minutes_is_no_conflict(self):
        old = _report("old", first=_NOW - 3_600_000)
        new = _report("new", first=_NOW - 600_000)
        assert _choices(old, new)["nightly"].conflict == ()

    def test_single_long_lived_fingerprint_is_no_conflict(self):
        assert _choices(_report("a", first=_NOW - 86_400_000))["nightly"].conflict == ()

    def test_dead_fingerprint_is_no_conflict(self):
        old = _report("old", first=_NOW - 3_600_000, last=_NOW - _WINDOW - 1)
        new = _report("new", first=_NOW - 1_800_000)
        assert _choices(old, new)["nightly"].conflict == ()


def _utc_ms(*args: int) -> int:
    return int(datetime(*args, tzinfo=UTC).timestamp() * 1000)


_LOCAL_NOW_MS = _utc_ms(2026, 10, 6, 10, 0)


class TestAdoptReportedCrons:
    """Leader-Seite: Auswahl aus den Meldungen eines In-Memory-Stores und
    Übernahme über ``apply_static_definition`` (U3). Backend- und
    Leader-Uhr sind fest."""

    @pytest.fixture(autouse=True)
    def _fixed_clock(self, monkeypatch):
        fake_time = SimpleNamespace(time=lambda: _LOCAL_NOW_MS / 1000)
        monkeypatch.setattr("swarmq.scheduler.sync.time", fake_time)

    @pytest.fixture
    def clock(self):
        return FakeClock(_LOCAL_NOW_MS)

    @pytest.fixture
    async def memory(self, clock):
        be = InMemoryBackend(clock)
        await be.connect()
        return be

    @pytest.fixture
    def leader(self, memory, broker, config, lock_manager, signal_dispatcher):
        return _make_loop(
            memory, broker, config, lock_manager, ScheduleStore(memory),
            signal_dispatcher,
        )

    @staticmethod
    async def _report(memory, definition: ScheduleDefinition, window_ms: int = 180_000):
        await memory.schedules.report_cron(
            definition.schedule_name, cron_fingerprint(definition),
            msgspec.json.encode(definition), live_window_ms=window_ms,
        )

    @staticmethod
    async def _stored(memory, name="nightly"):
        raw = await memory.schedules.get(name)
        definition = None if raw is None else msgspec.json.decode(raw, type=ScheduleDefinition)
        return definition, await memory.schedules.fire_ts(name)

    async def test_reported_cron_is_inserted_with_score(self, memory, leader):
        await self._report(memory, _static(timezone="Europe/Berlin"))

        await leader.adopt_reported_crons()

        stored, score = await self._stored(memory)
        assert stored is not None and stored.timezone == "Europe/Berlin"
        # 01:00 Berlin (Sommerzeit) = 23:00 UTC am Vortag.
        assert score == stored.fire_ts_ms == _utc_ms(2026, 10, 6, 23, 0)

    async def test_younger_fingerprint_replaces_older_definition(self, memory, leader, clock):
        """AE8: App B stellt ``0 1 * * *`` UTC auf ``0 3 * * *`` Berlin um."""
        await self._report(memory, _static())
        await leader.adopt_reported_crons()
        await clock.advance(1)
        await self._report(memory, _static(cron="0 3 * * *", timezone="Europe/Berlin"))
        await clock.advance(1)
        await self._report(memory, _static())  # alter Stand lebt weiter

        await leader.adopt_reported_crons()

        stored, _ = await self._stored(memory)
        assert (stored.cron, stored.timezone) == ("0 3 * * *", "Europe/Berlin")

    async def test_reports_older_than_three_seen_intervals_are_ignored(
        self, memory, leader, clock, config,
    ):
        await self._report(memory, _static())
        await clock.advance(3 * config.scheduler_seen_interval_s + 0.001)

        await leader.adopt_reported_crons()

        assert await self._stored(memory) == (None, None)

    @pytest.mark.parametrize("broken", [
        {"timezone": "Mars/Olympus_Mons"},
        {"cron": "61 * * * *"},
        {"cron": None},
    ])
    async def test_invalid_report_is_not_adopted_and_logged(
        self, memory, leader, swarmq_logs, broken,
    ):
        await self._report(memory, _static(**broken))
        await self._report(memory, _static(schedule_name="other", task_name="other"))

        await leader.adopt_reported_crons()

        assert await self._stored(memory) == (None, None)
        assert (await self._stored(memory, "other"))[0] is not None
        [entry] = [e for e in swarmq_logs if e["event"] == "scheduler_cron_report_invalid"]
        assert entry["log_level"] == "error" and entry["schedule_name"] == "nightly"
        assert entry["error_type"] and entry["error"]

    async def test_undecodable_report_is_not_adopted(self, memory, leader, swarmq_logs):
        await memory.schedules.report_cron("nightly", "fp", b"{broken", live_window_ms=180_000)

        await leader.adopt_reported_crons()

        assert await self._stored(memory) == (None, None)
        assert [e for e in swarmq_logs if e["event"] == "scheduler_cron_report_invalid"]

    async def test_report_claiming_another_source_is_not_adopted(self, memory, leader):
        await self._report(memory, _static(source="cli"))

        await leader.adopt_reported_crons()

        assert await self._stored(memory) == (None, None)

    async def _two_live_fingerprints_for(self, memory, clock, seconds: float):
        await self._report(memory, _static(queue="app-a"))
        await clock.advance(1)
        await self._report(memory, _static(queue="app-b"))
        # Beide Apps melden weiter im Takt.
        elapsed = 0.0
        while elapsed < seconds:
            await clock.advance(60)
            elapsed += 60
            await self._report(memory, _static(queue="app-a"))
            await self._report(memory, _static(queue="app-b"))

    async def test_long_lived_conflict_is_logged(self, memory, leader, clock, swarmq_logs):
        await self._two_live_fingerprints_for(memory, clock, 660)

        await leader.adopt_reported_crons()

        [entry] = [e for e in swarmq_logs if e["event"] == "scheduler_cron_conflict"]
        assert entry["log_level"] == "warning" and entry["schedule_name"] == "nightly"
        assert sorted(entry["fingerprints"]) == sorted(
            cron_fingerprint(_static(queue=q)) for q in ("app-a", "app-b")
        )

    async def test_long_lived_conflict_emits_signal(
        self, memory, leader, clock, signal_dispatcher,
    ):
        seen: list[dict] = []

        @signal_dispatcher.on_signal(Signal.SCHEDULER_CRON_CONFLICT)
        async def _on_conflict(**kw) -> None:
            seen.append(kw)

        await self._two_live_fingerprints_for(memory, clock, 660)

        await leader.adopt_reported_crons()

        [kw] = seen
        assert kw["worker_id"] == "w-1" and kw["schedule_name"] == "nightly"
        assert len(kw["fingerprints"]) == 2

    async def test_short_overlap_neither_logs_nor_signals(
        self, memory, leader, clock, signal_dispatcher, swarmq_logs,
    ):
        seen: list[dict] = []

        @signal_dispatcher.on_signal(Signal.SCHEDULER_CRON_CONFLICT)
        async def _on_conflict(**kw) -> None:
            seen.append(kw)

        await self._two_live_fingerprints_for(memory, clock, 540)

        await leader.adopt_reported_crons()

        assert seen == []
        assert not [e for e in swarmq_logs if e["event"] == "scheduler_cron_conflict"]

    async def test_reports_older_than_seven_days_are_deleted(self, memory, leader, clock):
        await self._report(memory, _static(schedule_name="gone", task_name="gone"))
        await clock.advance(6 * 86_400)
        await self._report(memory, _static())
        await clock.advance(86_400 + 1)

        await leader.adopt_reported_crons()

        names = {r.name for r in (await memory.schedules.cron_reports()).reports}
        assert names == {"nightly"}

    async def test_unreadable_reports_are_logged_without_raising(
        self, memory, leader, monkeypatch, swarmq_logs,
    ):
        async def _down():
            raise BackendConnectionError("valkey down")

        monkeypatch.setattr(memory.schedules, "cron_reports", _down)

        await leader.adopt_reported_crons()

        [entry] = [e for e in swarmq_logs if e["event"] == "scheduler_cron_reports_read_failed"]
        assert entry["log_level"] == "warning" and entry["error"] == "valkey down"

    async def test_failed_prune_is_logged_without_raising(
        self, memory, leader, monkeypatch, swarmq_logs,
    ):
        async def _down(**_kw):
            raise BackendConnectionError("valkey flap")

        monkeypatch.setattr(memory.schedules, "prune_cron_reports", _down)
        await self._report(memory, _static())

        await leader.adopt_reported_crons()

        assert (await self._stored(memory))[0] is not None
        [entry] = [e for e in swarmq_logs if e["event"] == "scheduler_cron_reports_prune_failed"]
        assert entry["log_level"] == "warning" and entry["error"] == "valkey flap"

    async def test_definitions_without_reports_stay_untouched(self, memory, leader):
        """Upgrade von 0.9.0: bestehende ``defs`` bleiben, bis Meldungen
        eintreffen."""
        legacy = _static(fire_ts_ms=_LOCAL_NOW_MS + 60_000)
        await memory.schedules.add("nightly", msgspec.json.encode(legacy), legacy.fire_ts_ms)

        await leader.adopt_reported_crons()

        assert await self._stored(memory) == (legacy, legacy.fire_ts_ms)

    async def test_cli_definition_of_the_same_name_is_left_alone(self, memory, leader):
        cli = _static(source="cli", fire_ts_ms=_LOCAL_NOW_MS + 60_000)
        await memory.schedules.add("nightly", msgspec.json.encode(cli), cli.fire_ts_ms)
        await self._report(memory, _static(timezone="Europe/Berlin"))

        await leader.adopt_reported_crons()

        assert await self._stored(memory) == (cli, cli.fire_ts_ms)

    async def test_unchanged_definition_without_score_is_planned_again(self, memory, leader):
        """Heilung (U3): nach ``scheduler_next_fire_failed`` ohne Score; die
        Rechnung gelingt wieder → der Leader plant neu ein."""
        stuck = _static(last_fire_at=123)
        await memory.schedules.add("nightly", msgspec.json.encode(stuck), None)
        await self._report(memory, _static())

        await leader.adopt_reported_crons()

        stored, score = await self._stored(memory)
        assert score == stored.fire_ts_ms == _utc_ms(2026, 10, 7, 1, 0)
        assert stored.last_fire_at == 123

    async def test_disabled_definition_without_score_stays_unplanned(self, memory, leader):
        off = _static(disabled=True)
        await memory.schedules.add("nightly", msgspec.json.encode(off), None)
        await self._report(memory, _static())

        await leader.adopt_reported_crons()

        assert await self._stored(memory) == (off, None)

    async def test_healing_does_not_revive_a_schedule_disabled_after_the_read(
        self, memory, leader, monkeypatch,
    ):
        """Review #4: ``schedule remove`` (HSET disabled + ZREM) landet
        zwischen dem Lesen der Definition und dem Schreiben der Heilung; die
        Heilung darf die alte aktive Definition nicht zurückschreiben."""
        stuck = _static()
        await memory.schedules.add("nightly", msgspec.json.encode(stuck), None)
        await self._report(memory, _static())
        real_get = memory.schedules.get

        async def _get_then_disable(name):
            raw = await real_get(name)
            if name == "nightly":  # wie ``disable_schedule`` der CLI
                off = msgspec.structs.replace(stuck, disabled=True)
                assert await memory.schedules.disable("nightly", msgspec.json.encode(off))
            return raw

        monkeypatch.setattr(memory.schedules, "get", _get_then_disable)

        await leader.adopt_reported_crons()

        stored, score = await self._stored(memory)
        assert stored is not None and stored.disabled is True
        assert score is None

    async def test_failed_healing_is_logged_and_does_not_stop_the_tick(
        self, memory, leader, monkeypatch, swarmq_logs,
    ):
        stuck = _static()
        await memory.schedules.add("nightly", msgspec.json.encode(stuck), None)
        await self._report(memory, _static())
        await self._report(memory, _static(schedule_name="other", task_name="other"))
        real_replan = memory.schedules.replan_if_unchanged

        async def _flaky(name, *args):
            if name == "nightly":
                raise BackendConnectionError("valkey flap")
            return await real_replan(name, *args)

        monkeypatch.setattr(memory.schedules, "replan_if_unchanged", _flaky)

        await leader.adopt_reported_crons()

        assert (await self._stored(memory, "other"))[0] is not None
        [entry] = [e for e in swarmq_logs if e["event"] == "scheduler_sync_update_failed"]
        assert entry["schedule_name"] == "nightly" and entry["error"] == "valkey flap"


class TestWorkerReports:
    """Worker-Seite: Melden beim Start und danach im Takt, unabhängig von der
    Leader-Rolle (der Lock gehört hier einem anderen Worker)."""

    @pytest.fixture
    async def memory(self):
        be = InMemoryBackend(FakeClock())
        await be.connect()
        return be

    @pytest.fixture
    def standby_locks(self, lock_manager):
        lock_manager.acquire.return_value = None
        return lock_manager

    def _worker(self, memory, broker, config, locks, signal_dispatcher, crons):
        from swarmq.scheduler import SchedulerLoop

        return SchedulerLoop(
            backend=memory, broker=broker, config=config, lock_manager=locks,
            store=ScheduleStore(memory), signal_dispatcher=signal_dispatcher,
            owner_token="standby", worker_id="w-2", static_crons=crons,
        )

    async def test_start_reports_every_static_cron(
        self, memory, broker, config, standby_locks, signal_dispatcher,
    ):
        crons = [_static(), _static(schedule_name="b", task_name="b", timezone="Asia/Tokyo")]
        worker = self._worker(memory, broker, config, standby_locks, signal_dispatcher, crons)

        await worker.start()
        try:
            reports = (await memory.schedules.cron_reports()).reports
        finally:
            await worker.stop()

        assert sorted((r.name, r.fingerprint, r.payload) for r in reports) == sorted(
            (d.schedule_name, cron_fingerprint(d), msgspec.json.encode(d)) for d in crons
        )

    async def test_unreachable_backend_at_report_does_not_stop_the_worker(
        self, memory, broker, config, standby_locks, signal_dispatcher,
        monkeypatch, swarmq_logs,
    ):
        async def _down(*_a, **_kw):
            raise BackendConnectionError("valkey down")

        monkeypatch.setattr(memory.schedules, "report_cron", _down)
        worker = self._worker(
            memory, broker, config, standby_locks, signal_dispatcher, [_static()],
        )

        await worker.start()
        try:
            assert worker.state == "standby"
        finally:
            await worker.stop()

        entries = [e for e in swarmq_logs if e["event"] == "scheduler_cron_report_failed"]
        assert entries and entries[0]["log_level"] == "warning"
        assert entries[0]["error"] == "valkey down"

    async def test_reports_are_repeated_every_seen_interval(
        self, memory, broker, config, standby_locks, signal_dispatcher, monkeypatch,
    ):
        config = dataclasses.replace(config, scheduler_seen_interval_s=0.03)
        calls = 0
        third = asyncio.Event()
        real_report = memory.schedules.report_cron

        async def _counting(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls >= 3:
                third.set()
            await real_report(*args, **kwargs)

        monkeypatch.setattr(memory.schedules, "report_cron", _counting)
        worker = self._worker(
            memory, broker, config, standby_locks, signal_dispatcher, [_static()],
        )

        await worker.start()
        try:
            await asyncio.wait_for(third.wait(), timeout=2)
        finally:
            await worker.stop()

    @pytest.mark.parametrize("teardown", ["stop", "abort"])
    async def test_teardown_leaves_no_report_task_running(
        self, memory, broker, config, standby_locks, signal_dispatcher, teardown,
    ):
        before = {t for t in asyncio.all_tasks() if not t.done()}
        worker = self._worker(
            memory, broker, config, standby_locks, signal_dispatcher, [_static()],
        )
        await worker.start()

        await getattr(worker, teardown)()

        assert {t for t in asyncio.all_tasks() if not t.done()} == before


class TestLeaderTick:
    """Leader-Takt: Auswahl sofort beim Erwerb der Leader-Rolle, danach im
    Takt ``scheduler_seen_interval_s`` — nicht im Poll-Takt."""

    @pytest.fixture
    async def memory(self):
        be = InMemoryBackend(FakeClock())
        await be.connect()
        return be

    def _leader(self, memory, broker, config, lock_manager, signal_dispatcher, crons=()):
        from swarmq.scheduler import SchedulerLoop

        return SchedulerLoop(
            backend=memory, broker=broker, config=config, lock_manager=lock_manager,
            store=ScheduleStore(memory), signal_dispatcher=signal_dispatcher,
            owner_token="test-owner", worker_id="w-1", static_crons=crons,
        )

    async def test_new_leader_adopts_reports_of_another_worker(
        self, memory, broker, config, lock_manager, signal_dispatcher, monkeypatch,
    ):
        foreign = _static(cron="0 3 * * *", timezone="Europe/Berlin")
        await memory.schedules.report_cron(
            "nightly", cron_fingerprint(foreign), msgspec.json.encode(foreign),
            live_window_ms=180_000,
        )
        inserted = asyncio.Event()
        real_add = memory.schedules.add

        async def _add(*args, **kwargs):
            result = await real_add(*args, **kwargs)
            inserted.set()
            return result

        monkeypatch.setattr(memory.schedules, "add", _add)
        leader = self._leader(memory, broker, config, lock_manager, signal_dispatcher)

        await leader.start()
        try:
            await asyncio.wait_for(inserted.wait(), timeout=2)
        finally:
            await leader.stop()

        stored = msgspec.json.decode(await memory.schedules.get("nightly"), type=ScheduleDefinition)
        assert (stored.cron, stored.timezone) == ("0 3 * * *", "Europe/Berlin")

    @staticmethod
    def _count(monkeypatch, target, attr, *, until: int):
        calls = {"n": 0}
        reached = asyncio.Event()
        real = getattr(target, attr)

        async def _counting(*args, **kwargs):
            calls["n"] += 1
            if calls["n"] >= until:
                reached.set()
            return await real(*args, **kwargs)

        monkeypatch.setattr(target, attr, _counting)
        return calls, reached

    async def test_selection_does_not_run_at_poll_rate(
        self, memory, broker, config, lock_manager, signal_dispatcher, monkeypatch,
    ):
        config = dataclasses.replace(config, scheduler_poll_interval=0.01)
        assert config.scheduler_seen_interval_s == 60.0
        reads, _ = self._count(monkeypatch, memory.schedules, "cron_reports", until=1)
        _, polled = self._count(monkeypatch, memory.schedules, "poll_due", until=5)
        leader = self._leader(memory, broker, config, lock_manager, signal_dispatcher)

        await leader.start()
        try:
            await asyncio.wait_for(polled.wait(), timeout=2)
        finally:
            await leader.stop()

        assert reads["n"] == 1

    async def test_selection_repeats_every_seen_interval(
        self, memory, broker, config, lock_manager, signal_dispatcher, monkeypatch,
    ):
        config = dataclasses.replace(config, scheduler_seen_interval_s=0.03)
        _, third = self._count(monkeypatch, memory.schedules, "cron_reports", until=3)
        leader = self._leader(memory, broker, config, lock_manager, signal_dispatcher)

        await leader.start()
        try:
            await asyncio.wait_for(third.wait(), timeout=2)
        finally:
            await leader.stop()

    async def test_standby_does_not_select(
        self, memory, broker, config, lock_manager, signal_dispatcher, monkeypatch,
    ):
        config = dataclasses.replace(config, scheduler_seen_interval_s=0.02)
        lock_manager.acquire.return_value = None
        reads, _ = self._count(monkeypatch, memory.schedules, "cron_reports", until=1)
        _, reported = self._count(monkeypatch, memory.schedules, "report_cron", until=4)
        worker = self._leader(
            memory, broker, config, lock_manager, signal_dispatcher, crons=[_static()],
        )

        await worker.start()
        try:
            await asyncio.wait_for(reported.wait(), timeout=2)
        finally:
            await worker.stop()

        assert reads["n"] == 0

    async def _fire_after_adoption(self, memory, broker, config, lock_manager, signal_dispatcher, monkeypatch):
        _, inserted = self._count(monkeypatch, memory.schedules, "add", until=1)
        published = asyncio.Event()
        broker.publish.side_effect = lambda *a, **kw: published.set()
        leader = self._leader(memory, broker, config, lock_manager, signal_dispatcher)
        await leader.start()
        try:
            await asyncio.wait_for(inserted.wait(), timeout=2)
            await memory.schedules.put("nightly", await memory.schedules.get("nightly"), 1)
            await asyncio.wait_for(published.wait(), timeout=2)
        finally:
            await leader.stop()

    async def test_adopted_foreign_cron_fires_without_registry_warning(
        self, memory, broker, config, lock_manager, signal_dispatcher, monkeypatch,
        swarmq_logs,
    ):
        foreign = _static(task_name="app_b.nightly")
        await memory.schedules.report_cron(
            "nightly", cron_fingerprint(foreign), msgspec.json.encode(foreign),
            live_window_ms=180_000,
        )

        await self._fire_after_adoption(
            memory, broker, config, lock_manager, signal_dispatcher, monkeypatch,
        )

        assert not [e for e in swarmq_logs if e["event"] == "scheduler_task_not_in_local_registry"]
