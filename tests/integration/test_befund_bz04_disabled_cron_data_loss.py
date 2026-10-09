"""Befund BZ-04 (Persistenz): nach Valkey-Datenverlust läuft ein deaktivierter Cron wieder.

Register: docs/befunde/betrieb-ueber-zeit-2026-10.md (BZ-04; E-4, D-6).

Ein Operator schaltet einen statischen Cron per ``swarmq schedule remove``
(ohne ``--permanent``) ab. Die Abschaltung steht nur in Valkey (Feld
``disabled`` der Definition). Verliert Valkey seine Daten (Neustart ohne
AOF/RDB, FLUSHALL, Failover auf eine Replika mit verlorenen Writes), legt der
nächste Melde-Takt des Leaders den Cron aus dem Code **aktiv** neu an
(``scheduler/sync.py`` ``apply_static_definition``, Zweig „kein Eintrag“).
Der Cron feuert wieder, ohne Log, ohne Signal.

Erwartet: Eine operatorseitige Abschaltung überlebt den Datenverlust nicht
still -- der Cron bleibt aus, oder swarmq meldet mindestens laut, dass es
seinen Zustand verloren hat und Abschaltungen nicht mehr kennt.

Der Datenverlust wird über ``flush_valkey_db`` simuliert (Test-Valkey auf dem
Wrapper-Port, nie 6379).

Test-Liste:
- [x] deaktivierter statischer Cron, FLUSHDB, nächster Sync -> bleibt aus oder
      lautes WARNING (xfail, BZ-04)
"""

from __future__ import annotations

import time
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.backend.valkey import ValkeyBackend
from swarmq.config import SwarmQConfig
from swarmq.scheduler import ScheduleDefinition, SchedulerLoop, ScheduleStore
from swarmq.signals import SignalDispatcher
from tests.support.services import flush_valkey_db, valkey_db_url
from tests.unit._log_capture import capture_swarmq_logs

pytestmark = [pytest.mark.integration, pytest.mark.timeout(10)]


def _static_cron() -> ScheduleDefinition:
    return ScheduleDefinition(
        schedule_name="bz04_nightly_export",
        task_name="nightly_export",
        args=(),
        kwargs={},
        queue="default",
        cron="*/5 * * * *",
        fire_ts_ms=0,
        source="static",
    )


def _loss_warnings(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    keywords = ("loss", "lost", "reset", "flush", "missing_state", "data")
    return [
        e
        for e in entries
        if e["log_level"] in ("warning", "error", "critical") and any(k in str(e["event"]).lower() for k in keywords)
    ]


async def _get(store: ScheduleStore, name: str) -> ScheduleDefinition:
    [definition] = [d for d in await store.list_schedules() if d.schedule_name == name]
    return definition


@pytest.mark.befund("BZ-04")
@pytest.mark.xfail(
    strict=True,
    reason="BZ-04: nach Valkey-Datenverlust legt der Sync einen per CLI deaktivierten Cron still aktiv neu an",
)
async def test_operator_disabled_cron_stays_off_after_valkey_data_loss(valkey_url: str) -> None:
    backend = ValkeyBackend(valkey_url)
    await backend.connect()
    try:
        config = SwarmQConfig(broker_url="amqp://unused", backend_url=valkey_url)
        broker = MagicMock()
        broker.publish = AsyncMock()
        store = ScheduleStore(backend)
        loop = SchedulerLoop(
            backend=backend,
            broker=broker,
            config=config,
            lock_manager=backend.locks,
            store=store,
            signal_dispatcher=SignalDispatcher(),
            owner_token="bz04-leader",
        )
        desired = _static_cron()
        now_ms = int(time.time() * 1000)

        # Code meldet den Cron, der Leader legt ihn an; der Operator schaltet ab.
        await loop.apply_static_definition(desired, now_ms)
        assert await store.disable_schedule(desired.schedule_name) is True
        before = await _get(store, desired.schedule_name)
        assert before.disabled is True

        # Valkey verliert seine Daten (Neustart ohne Persistenz).
        await flush_valkey_db(valkey_db_url())

        # Naechster Melde-Takt: dieselbe statische Definition aus dem Code.
        with capture_swarmq_logs() as entries:
            await loop.apply_static_definition(desired, now_ms + 60_000)

        after = await _get(store, desired.schedule_name)
        due = [name for name, _ts, _d in await store.poll_due(after.fire_ts_ms + 1)]
        assert after.disabled or _loss_warnings(entries), (
            "der vom Operator abgeschaltete Cron ist nach dem Datenverlust wieder "
            f"aktiv (disabled={after.disabled}, im Fire-Index: {desired.schedule_name in due}), "
            "ohne Warnung. "
            f"Logs: {[e['event'] for e in entries]}"
        )
    finally:
        await backend.close()
