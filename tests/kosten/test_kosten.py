"""Zählbare Kosten je Task für die zentralen Pfade (Plan 2026-10-24-1118, U3, R7).

Jedes Szenario misst einen Pfad vom ``schedule`` bis zur letzten
Bestätigung und vergleicht exakt mit ``basislinie.toml`` (Fixture
``kosten``). Ein gewollter Anstieg ändert die Basislinie im selben MR:
``uv run just kosten-basislinie "<Grund mit Verweis>"``.

Test-Liste:
- [x] Publish: Vertragsaufrufe eines ``schedule``
- [x] Publish: AMQP-Frames eines ``schedule`` über den echten aiormq-Publish-Code
- [x] einfacher Task: ``schedule`` bis Ack
- [x] Retry: ein Fehlschlag, Rückstellung, zweiter Lauf bis Ack
- [x] Workflow-Kette: zwei Schritte bis zum Abschluss des Workflows
- [x] geladene Module: ``import swarmq``, ``import swarmq.cli``, Worker-Start ohne Dienste
"""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Callable, Mapping

import pytest

from swarmq.workflow import chain
from swarmq.workflow_orchestration import STATUS_COMPLETED
from tests.kosten.conftest import KostenRetry, KostenSchritt, Pipeline, amqp_frames

pytestmark = pytest.mark.timeout(10)

Kosten = Callable[[str, Mapping[str, int]], None]


async def test_publish(pipeline: Pipeline, kosten: Kosten) -> None:
    await pipeline.client.schedule("kosten_ok", 1)
    frames = await amqp_frames(lambda client: client.schedule("kosten_ok", 1))

    kosten("publish", {**pipeline.aufrufe, "amqp.frames": frames})


async def test_einfacher_task(pipeline: Pipeline, kosten: Kosten) -> None:
    await pipeline.client.schedule("kosten_ok", 1)
    await pipeline.abarbeiten()

    kosten("einfacher_task", pipeline.aufrufe)


async def test_retry(pipeline: Pipeline, kosten: Kosten) -> None:
    KostenRetry.fehlschlaege = 1
    await pipeline.client.schedule("kosten_retry", 1)
    await pipeline.abarbeiten()  # Fehlschlag, Rückstellung um 1 s (fixed)
    await pipeline.clock.advance(1)
    await pipeline.abarbeiten()

    assert KostenRetry.fehlschlaege == 0
    kosten("retry", pipeline.aufrufe)


async def test_workflow_kette(pipeline: Pipeline, kosten: Kosten) -> None:
    handle = await pipeline.client.apply(chain(KostenSchritt.s(1), KostenSchritt.s()))
    await pipeline.abarbeiten()
    aufrufe = dict(pipeline.aufrufe)  # vor der Prüfung, die selbst liest

    assert await handle.status() == STATUS_COMPLETED
    kosten("workflow_kette", aufrufe)


# Worker-Start ohne Dienste: was ``run_worker_async`` vor dem Verbinden lädt
# (App, Broker, Backend) und der gebaute Worker.
_WORKER_START = """
from swarmq.app import SwarmQ
from swarmq.backend.valkey import ValkeyBackend
from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.registry import TaskRegistry
from swarmq.worker import Worker
app = SwarmQ(RabbitMQBroker("amqp://unused"), ValkeyBackend("valkey://unused"))
Worker(app.broker, app.backend, TaskRegistry(), app.config)
"""


@pytest.mark.parametrize(
    ("szenario", "einstieg"),
    [("import_swarmq", "import swarmq"), ("import_swarmq_cli", "import swarmq.cli"), ("worker_start", _WORKER_START)],
)
def test_geladene_module(szenario: str, einstieg: str, kosten: Kosten) -> None:
    """Im frischen Prozess (über 100 ms, bewusst): ``-I`` hält ``PYTHON*``-Variablen
    fern, ohne ``COVERAGE_*`` lädt Coverage nichts ins Kind (KTD4)."""
    umgebung = {name: wert for name, wert in os.environ.items() if not name.startswith("COVERAGE_")}
    code = f"import sys\n{einstieg}\nprint(len(sys.modules))"
    result = subprocess.run(
        [sys.executable, "-I", "-c", code], env=umgebung, capture_output=True, text=True, check=True
    )

    kosten(szenario, {"module": int(result.stdout)})
