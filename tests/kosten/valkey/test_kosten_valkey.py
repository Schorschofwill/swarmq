"""Valkey-Befehle und Roundtrips je Task (Plan 2026-10-24-1118, U4, R7, KTD6).

Ein Task nacheinander: Das Auto-Pipelining (``script_pipeline.py``) bündelt
die Aufrufe eines Event-Loop-Takts, bei Nebenläufigkeit hingen die
Roundtrips vom Scheduling ab. Jedes Szenario läuft einmal zum Aufwärmen
(Verbindungen, Skript-Cache des Servers) und einmal gemessen, dann exakt
gegen ``basislinie.toml`` (Fixture ``kosten``).

Test-Liste:
- [x] die Hülle zählt einen MULTI-Block als einen Roundtrip mit allen Befehlen
- [x] einfacher Task: ``schedule`` bis Ack
- [x] ``get_result`` auf einen fertigen Task: Abonnement, ein GET, Abbau des Pub/Sub, kein Polling (E-030)
- [x] Retry: ein Fehlschlag, Rückstellung, zweiter Lauf bis Ack
- [x] Lock mit Rate-Limit: Rate-Limit vor Lock (E-070)
- [x] Workflow-Kette: zwei Schritte bis zum Abschluss
"""

from __future__ import annotations

from collections.abc import Callable, Mapping

import pytest
import valkey.asyncio as valkey_asyncio

from swarmq.workflow import chain
from swarmq.workflow_orchestration import STATUS_COMPLETED, WorkflowHandle
from tests.kosten.conftest import KostenRetry, KostenSchritt, Pipeline
from tests.kosten.valkey.conftest import Leitung
from tests.support.services import valkey_db_url

pytestmark = pytest.mark.timeout(10)

Kosten = Callable[[str, Mapping[str, int]], None]


async def test_a_multi_block_counts_as_one_roundtrip_with_every_command(leitung: Leitung) -> None:
    client = valkey_asyncio.Valkey.from_url(valkey_db_url())
    try:
        await client.ping()  # Verbindung steht vor der Messung
        async with leitung.messung(), client.pipeline(transaction=True) as pipe:
            await pipe.set("kosten:a", "1").get("kosten:a").execute()
    finally:
        await client.aclose()

    assert leitung.werte() == {
        "valkey.roundtrips": 1,
        "valkey.befehle": 4,
        "valkey.MULTI": 1,
        "valkey.SET": 1,
        "valkey.GET": 1,
        "valkey.EXEC": 1,
    }


async def _einfacher_task(pipeline: Pipeline) -> None:
    await pipeline.client.schedule("kosten_ok", 1)
    await pipeline.abarbeiten()


async def test_einfacher_task(valkey_pipeline: Pipeline, leitung: Leitung, kosten: Kosten) -> None:
    await _einfacher_task(valkey_pipeline)
    async with leitung.messung():
        await _einfacher_task(valkey_pipeline)

    kosten("valkey_einfacher_task", leitung.werte())


@pytest.mark.entscheidung("E-030")
async def test_get_result_on_a_finished_task(valkey_pipeline: Pipeline, leitung: Leitung, kosten: Kosten) -> None:
    """Nur auf einen fertigen Task: danach hinge die Zahl der GETs von der
    Taskdauer ab (E-030). Die Messung endet erst nach dem Abbau des Pub/Sub."""
    task_id = await valkey_pipeline.client.schedule("kosten_ok", 1)
    await valkey_pipeline.abarbeiten()
    await valkey_pipeline.client.get_result(task_id, timeout=10)  # aufwärmen
    task_id = await valkey_pipeline.client.schedule("kosten_ok", 2)
    await valkey_pipeline.abarbeiten()

    async with leitung.messung():
        assert await valkey_pipeline.client.get_result(task_id, timeout=10) == [2]

    werte = leitung.werte()
    assert (werte["valkey.SUBSCRIBE"], werte["valkey.GET"], werte["valkey.UNSUBSCRIBE"]) == (1, 1, 1)
    kosten("valkey_get_result", werte)


async def _retry(pipeline: Pipeline) -> None:
    KostenRetry.fehlschlaege = 1
    await pipeline.client.schedule("kosten_retry", 1)
    await pipeline.abarbeiten()  # Fehlschlag, Rückstellung um 1 s (fixed)
    await pipeline.clock.advance(1)
    await pipeline.abarbeiten()
    assert KostenRetry.fehlschlaege == 0


async def test_retry(valkey_pipeline: Pipeline, leitung: Leitung, kosten: Kosten) -> None:
    await _retry(valkey_pipeline)
    async with leitung.messung():
        await _retry(valkey_pipeline)

    kosten("valkey_retry", leitung.werte())


async def _lock_mit_rate_limit(pipeline: Pipeline) -> None:
    await pipeline.client.schedule("kosten_lock", 1)
    await pipeline.abarbeiten()


def _erster_roundtrip(leitung: Leitung, teil: bytes) -> int:
    """Index des ersten Roundtrips, dessen Befehle *teil* in einem Argument tragen."""
    return next(
        index
        for index, roundtrip in enumerate(leitung.roundtrips)
        if any(teil in argument for befehl in roundtrip for argument in befehl)
    )


@pytest.mark.entscheidung("E-070")
async def test_lock_with_rate_limit(valkey_pipeline: Pipeline, leitung: Leitung, kosten: Kosten) -> None:
    await _lock_mit_rate_limit(valkey_pipeline)
    async with leitung.messung():
        await _lock_mit_rate_limit(valkey_pipeline)

    # E-070: das Rate-Limit-Gate (TIME, dann das Fenster-Skript) vor dem Lock-Gate.
    assert _erster_roundtrip(leitung, b"rl:{kosten_lock}") < _erster_roundtrip(leitung, b"lock:kosten")
    kosten("valkey_lock_rate_limit", leitung.werte())


async def _workflow_kette(pipeline: Pipeline) -> WorkflowHandle:
    handle = await pipeline.client.apply(chain(KostenSchritt.s(1), KostenSchritt.s()))
    await pipeline.abarbeiten()
    return handle


async def test_workflow_kette(valkey_pipeline: Pipeline, leitung: Leitung, kosten: Kosten) -> None:
    await _workflow_kette(valkey_pipeline)
    async with leitung.messung():
        handle = await _workflow_kette(valkey_pipeline)

    assert await handle.status() == STATUS_COMPLETED  # nach der Messung, liest selbst
    kosten("valkey_workflow_kette", leitung.werte())
