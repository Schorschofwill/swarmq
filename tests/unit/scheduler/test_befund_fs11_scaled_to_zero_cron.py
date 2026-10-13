"""Nachweis FS-11: Die BZ-11-Zurueckhaltung legt einen Cron still, dessen Task
nur in einem auf 0 skalierten Pool registriert ist.

Register: docs/befunde/fehlersuche-runde-2-2026-10.md (FS-11; Quelle R-2).
Offene Produktfrage 2 (Register): feuern mit Warnung (wie 0.11) oder
zurueckhalten mit Grenze. Dieser Nachweis prueft die empfohlene Option: Ein
Cron mit gueltiger Definition, den ein Pool gemeldet hat, feuert weiter (mit
Warnung/Event), auch wenn seit ueber drei Meldetakten (180 s) kein lebender
Worker ihn meldet.

Szenario: Der Leader laeuft im IO-Pool und registriert den Task nicht. Das
CPU-Deployment, das ihn registriert und meldet, ist per KEDA auf 0 skaliert;
seine letzte Meldung liegt 181 s zurueck. ``_is_unowned``
(``scheduler/unowned.py``) haelt den Cron dann zurueck: Commit auf
``now + 60 s`` ohne Publish. Ohne Nachricht bleibt die Queue leer, KEDA
skaliert nie hoch, der Cron feuert nie wieder (und der Prune entfernt ihn).

Hinweis: Mit der empfohlenen Option aendert sich das in
``tests/unit/scheduler/test_unowned.py`` festgeschriebene Verhalten
(``test_unowned_cron_is_held_instead_of_fired``); der Fix muss die
Unterscheidung "entfernter Cron" gegenueber "Pool abwesend" treffen.

Testliste:
# [x] Cron, zuletzt vor 181 s von einem Pool gemeldet, kein lebender Melder,
#     Leader registriert den Task nicht -> Publish erfolgt
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.config import SwarmQConfig
from swarmq.contracts import CronReport, CronReports
from swarmq.scheduler import ScheduleDefinition
from swarmq.signals import SignalDispatcher
from tests.unit.scheduler._helpers import _make_loop  # pyright: ignore[reportUnknownVariableType] - ungetypter Bestandshelfer

pytestmark = [pytest.mark.timeout(5)]

_NOW_S = 1_700_000_100.0
_NOW_MS = int(_NOW_S * 1000)
_SCORE_MS = 1_700_000_000_000
_NAME = "fs11_nightly_report"


@pytest.mark.befund("FS-11")
@pytest.mark.xfail(
    strict=True,
    reason="FS-11: offen - Cron eines auf 0 skalierten Pools wird zurueckgehalten (empfohlen: feuern mit Warnung)",
)
async def test_cron_of_a_pool_scaled_to_zero_keeps_firing(
    monkeypatch: pytest.MonkeyPatch,
    backend: MagicMock,
    broker: MagicMock,
    config: SwarmQConfig,
    lock_manager: MagicMock,
    store: MagicMock,
    signal_dispatcher: SignalDispatcher,
) -> None:
    monkeypatch.setattr("swarmq.scheduler.loop.time.time", lambda: _NOW_S)
    last_seen_ms = _NOW_MS - int(3 * config.scheduler_seen_interval_s * 1000) - 1_000  # 181 s
    report = CronReport(_NAME, "fp", _NOW_MS - 86_400_000, last_seen_ms, b"{}")
    backend.schedules.cron_reports = AsyncMock(return_value=CronReports(now_ms=_NOW_MS, reports=(report,)))
    backend.schedules.known_cron_names = AsyncMock(return_value=frozenset({_NAME}))
    backend.schedules.prune_cron_reports = AsyncMock(return_value=0)
    loop = _make_loop(backend, broker, config, lock_manager, store, signal_dispatcher)
    loop._state = "leader"
    definition = ScheduleDefinition(
        schedule_name=_NAME,
        task_name="fs11_cpu_only_task",
        args=(),
        kwargs={},
        queue="cpu",
        cron="0 3 * * *",
        fire_ts_ms=_SCORE_MS,
        source="static",
    )
    await loop.adopt_reported_crons()
    store.poll_due.return_value = [(_NAME, _SCORE_MS, definition)]

    await loop._poll_tick()

    assert broker.publish.await_count == 1, (
        "Cron des auf 0 skalierten Pools zurueckgehalten (Commit ohne Publish); "
        "ohne Nachricht skaliert der Pool nie hoch"
    )
