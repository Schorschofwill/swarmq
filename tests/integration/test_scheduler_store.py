"""Integration tests for ScheduleStore against a real Valkey backend.

Covers the full add/list/poll/commit roundtrip plus the insert-if-absent
and Fork-1 static-update paths.
"""

# Test list (0.9.0, Scheduler-Commit-Wettlauf) -- TestCommitVsConcurrentChange:
# 1. ``schedule remove`` (disable) waehrend eines Fires bleibt nach dem
#    Commit bestehen (Definition disabled, kein Fire-Index-Eintrag).
# 2. ``schedule remove --permanent`` waehrend eines Fires wird vom Commit
#    nicht wiederbelebt.
# 3. Ein geaenderter Zeitplan (neuer Score per ``put``) waehrend eines Fires
#    wird vom Commit nicht ueberschrieben.
# 4. Ohne parallele Aenderung schreibt der Commit den naechsten Fire-Zeitpunkt
#    wie bisher (Regression).
#
# Test list (Plan 2026-10-08-1541, U12) -- TestCronReportsServerTime:
# 5. Eine Meldung traegt Valkey-Zeit, auch wenn die Uhr des meldenden
#    Workers weit abweicht (Uhrabweichung aendert die Auswahl nicht).
#
# Test list (Plan 2026-10-08-1541, U10, KTD8) -- TestPruneRoundtrip:
# 6. Der Leader entfernt einen verstummten, deaktivierten Cron samt
#    Fire-Index-Eintrag; meldet ihn ein Worker wieder, kommt er deaktiviert
#    und ohne Fire-Index-Eintrag zurueck, die Markierung ist verbraucht.

from __future__ import annotations

import uuid
from typing import Any
from unittest.mock import AsyncMock

import msgspec
import pytest

from swarmq.backend.valkey import ValkeyBackend
from swarmq.config import SwarmQConfig
from swarmq.locking import SCHEDULER_LOCK_KEY
from swarmq.scheduler import ScheduleDefinition, SchedulerLoop, ScheduleStore
from swarmq.signals import SignalDispatcher

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


def _def(
    name: str,
    *,
    cron: str | None = "* * * * *",
    fire_ts_ms: int | None = 1_700_000_000_000,
    source: str = "static",
    **overrides: Any,
) -> ScheduleDefinition:
    base: dict[str, Any] = {
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

    async def test_add_existing_cli_not_overwritten_by_static(self, store, sched_name):
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
        original = _def(sched_name, cron="* * * * *", fire_ts_ms=1_700_000_000_000)
        await store.add_schedule(original)

        modified = _def(sched_name, cron="*/10 * * * *", fire_ts_ms=1_700_000_500_000)
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
        self,
        store,
        sched_name,
        valkey_client,
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
            name == sched_name and score == 1_000 and definition.schedule_name == sched_name
            for name, score, definition in rows
        )

    async def test_poll_due_excludes_future(self, store, sched_name):
        d = _def(sched_name, fire_ts_ms=5_000)
        await store.add_schedule(d)

        rows = await store.poll_due(now_ms=1_000, limit=10)
        assert all(name != sched_name for name, _, _ in rows)


class TestCommitFire:
    async def test_commit_with_valid_owner_updates_state(self, store, valkey_client, sched_name):
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

    async def test_commit_with_stale_owner_is_rejected(self, store, valkey_client, sched_name):
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

    async def test_commit_one_shot_removes_entry(self, store, valkey_client, sched_name):
        owner = uuid.uuid4().hex
        await valkey_client.set(SCHEDULER_LOCK_KEY, owner)

        d = _def(sched_name, cron=None, fire_ts_ms=1_000, source="cli")
        await store.add_schedule(d)

        ok = await store.commit_fire(sched_name, -1, None, owner)
        assert ok is True

        schedules = await store.list_schedules()
        assert not any(s.schedule_name == sched_name for s in schedules)

        await valkey_client.delete(SCHEDULER_LOCK_KEY)


class TestCommitVsConcurrentChange:
    """Der Commit nach einem Fire darf eine Aenderung, die zwischen Poll und
    Commit am Zeitplan passiert (CLI ``schedule remove``, Redeploy mit neuer
    Cron), nicht ueberschreiben. Der Wettlauf wird deterministisch erzeugt:
    der Fake-Publisher aendert den Zeitplan genau waehrend des Publish."""

    @staticmethod
    def _loop(store, valkey_backend, publisher, owner):
        config = SwarmQConfig(
            broker_url="amqp://unused",
            backend_url="redis://unused",
            scheduler_poll_interval=0.1,
            scheduler_heartbeat_interval=0.2,
            scheduler_lock_ttl=1.0,
        )
        return SchedulerLoop(
            backend=valkey_backend,
            broker=publisher,
            config=config,
            lock_manager=valkey_backend.locks,
            store=store,
            signal_dispatcher=SignalDispatcher(),
            owner_token=owner,
        )

    async def _fire_with_concurrent_change(self, valkey_url, valkey_client, sched_name, change):
        backend = ValkeyBackend(valkey_url)
        await backend.connect()
        try:
            store = ScheduleStore(backend)
            await store.add_schedule(_def(sched_name, cron="* * * * *", fire_ts_ms=1))
            owner = uuid.uuid4().hex
            await valkey_client.set(SCHEDULER_LOCK_KEY, owner, px=5_000)

            async def _publish(queue, body, *, headers):
                if headers["x-swarmq-schedfire"].startswith(f"{sched_name}:"):
                    await change(store)

            publisher = AsyncMock()
            publisher.publish = AsyncMock(side_effect=_publish)
            loop = self._loop(store, backend, publisher, owner)
            loop._state = "leader"
            await loop._poll_tick()
            await valkey_client.delete(SCHEDULER_LOCK_KEY)
            return store
        finally:
            await backend.close()

    async def test_disable_during_fire_survives_commit(self, valkey_url, valkey_client, sched_name):
        async def _disable(store):
            assert await store.disable_schedule(sched_name) is True

        await self._fire_with_concurrent_change(valkey_url, valkey_client, sched_name, _disable)

        raw = await valkey_client.hget("sq:{sched}:defs", sched_name)
        assert msgspec.json.decode(raw, type=ScheduleDefinition).disabled is True
        assert await valkey_client.zscore("sq:{sched}:zset", sched_name) is None

    async def test_permanent_remove_during_fire_is_not_revived(self, valkey_url, valkey_client, sched_name):
        async def _remove(store):
            assert await store.remove_schedule(sched_name) is True

        await self._fire_with_concurrent_change(valkey_url, valkey_client, sched_name, _remove)

        assert await valkey_client.hget("sq:{sched}:defs", sched_name) is None
        assert await valkey_client.zscore("sq:{sched}:zset", sched_name) is None

    async def test_replan_during_fire_is_not_overwritten(self, valkey_url, valkey_client, sched_name):
        replanned = _def(sched_name, cron="0 0 1 1 *", fire_ts_ms=4_000_000_000_000)

        async def _replan(store):
            await store.update_static_schedule(replanned)

        await self._fire_with_concurrent_change(valkey_url, valkey_client, sched_name, _replan)

        raw = await valkey_client.hget("sq:{sched}:defs", sched_name)
        assert msgspec.json.decode(raw, type=ScheduleDefinition) == replanned
        assert await valkey_client.zscore("sq:{sched}:zset", sched_name) == 4_000_000_000_000

    async def test_commit_without_concurrent_change_advances_schedule(self, valkey_url, valkey_client, sched_name):
        async def _nothing(store):
            return None

        await self._fire_with_concurrent_change(valkey_url, valkey_client, sched_name, _nothing)

        raw = await valkey_client.hget("sq:{sched}:defs", sched_name)
        committed = msgspec.json.decode(raw, type=ScheduleDefinition)
        assert committed.last_fire_at is not None
        assert await valkey_client.zscore("sq:{sched}:zset", sched_name) == committed.fire_ts_ms


class TestCronReportsServerTime:
    async def test_report_uses_valkey_time_not_worker_clock(self, valkey_url, valkey_client, sched_name, monkeypatch):
        import time as time_module

        backend = ValkeyBackend(valkey_url)
        await backend.connect()
        try:
            real_time = time_module.time
            # Worker-Uhr einen Tag voraus.
            monkeypatch.setattr(time_module, "time", lambda: real_time() + 86_400)
            seconds, micros = await valkey_client.time()
            before_ms = seconds * 1000 + micros // 1000
            await backend.schedules.report_cron(sched_name, "fp", b"def", live_window_ms=60_000)
            snapshot = await backend.schedules.cron_reports()
        finally:
            await backend.close()

        [report] = [r for r in snapshot.reports if r.name == sched_name]
        assert before_ms <= report.first_seen_ms <= snapshot.now_ms
        assert snapshot.now_ms - before_ms < 60_000


class TestPruneRoundtrip:
    async def test_pruned_disabled_cron_comes_back_disabled(
        self,
        valkey_url,
        valkey_client,
        sched_name,
    ):
        import asyncio

        from swarmq.scheduler.reports import cron_fingerprint

        backend = ValkeyBackend(valkey_url)
        await backend.connect()
        config = SwarmQConfig(
            broker_url="amqp://unused",
            backend_url=valkey_url,
            scheduler_seen_interval_s=0.05,
            scheduler_prune_after_s=0.2,
        )
        leader = SchedulerLoop(
            backend=backend,
            broker=AsyncMock(),
            config=config,
            lock_manager=backend.locks,
            store=ScheduleStore(backend),
            signal_dispatcher=SignalDispatcher(),
            owner_token="leader",
        )
        wanted = _def(sched_name, cron="0 1 * * *", fire_ts_ms=None)
        try:
            await backend.schedules.add(
                sched_name,
                msgspec.json.encode(_def(sched_name, cron="0 1 * * *", disabled=True)),
                None,
            )

            async def _report() -> None:
                await backend.schedules.report_cron(
                    sched_name,
                    cron_fingerprint(wanted),
                    msgspec.json.encode(wanted),
                    live_window_ms=150,
                )

            await _report()
            # Echte Wartezeit: die Frist laeuft in Valkey-Zeit ab.
            await asyncio.sleep(0.35)
            await leader.adopt_reported_crons()
            assert await valkey_client.hget("sq:{sched}:defs", sched_name) is None
            assert await valkey_client.sismember("sq:{sched}:disabled", sched_name)

            await _report()
            await leader.adopt_reported_crons()
        finally:
            await backend.close()

        raw = await valkey_client.hget("sq:{sched}:defs", sched_name)
        assert msgspec.json.decode(raw, type=ScheduleDefinition).disabled is True
        assert await valkey_client.zscore("sq:{sched}:zset", sched_name) is None
        assert not await valkey_client.sismember("sq:{sched}:disabled", sched_name)
