"""Kostensuite ohne Dienste (Plan 2026-10-24-1118, KTD4, KTD5, KTD7).

Gemessen werden zählbare Kosten, keine Zeit (R10):

- Vertragsaufrufe je Task: zählende Hüllen um ``InMemoryBroker`` und
  ``InMemoryBackend`` (samt Koordinations-Stores) treiben die echte Pipeline
  über ``Worker._handle_message``, nach dem Vorbild ``ChainBroker`` in
  ``tests/property/stateful/retry_chain_machine.py``.
- AMQP-Frames je Publish über den echten aio-pika/aiormq-Publish-Code
  (``tests/support/aiormq_transport.py``).
- Geladene Module je Einstieg im frischen Prozess, ohne ``COVERAGE_*``.

Allokationen (``tracemalloc``) sind nicht im Gate: Die Determinismusprobe
vor dem Bau ergab über 20 Läufe keine identischen Zahlen (Stop condition aus
KTD5).

Die Fixture ``kosten`` vergleicht exakt mit ``basislinie.toml``. Mit
``SWARMQ_KOSTEN_MESSUNG=<datei>`` schreibt sie die Messung stattdessen als
JSON-Zeile in die Datei; ``just kosten-basislinie`` macht daraus die neue
Basislinie.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import os
import platform
import tomllib
from collections import Counter
from collections.abc import AsyncIterator, Callable, Coroutine, Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ClassVar, cast, override
from unittest.mock import AsyncMock, Mock

import pytest

from swarmq.broker.rabbitmq import RabbitMQBroker
from swarmq.client import Client
from swarmq.config import SwarmQConfig
from swarmq.contracts import Delivery
from swarmq.registry import TaskRegistry
from swarmq.serialization import decode_result
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker
from swarmq.workflow_orchestration import WorkflowAdvancer, WorkflowStepEnqueuer, workflow_step_task_id
from tests.support.aiormq_transport import FakePublishChannel, FakeTransport
from tests.support.inmemory import FakeClock, InMemoryBackend, InMemoryBroker
from tests.support.skripte import lade_skript

_WURZEL = Path(__file__).resolve().parents[2]
QUEUE = "kosten"

# Eigenschaften der Fassade, die einen Koordinations-Store liefern; seine
# Aufrufe zählen unter ``backend.<store>.<methode>``.
_STORES = frozenset({"rate_limit", "locks", "chords", "schedules", "execution_guard"})


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    # Der Valkey-Teil (``tests/kosten/valkey``) trägt ``kosten_valkey``
    # und läuft nur mit Diensten.
    for item in items:
        pfad = str(item.fspath)
        if "tests/kosten/" in pfad and "tests/kosten/valkey/" not in pfad:
            item.add_marker(pytest.mark.kosten)


basislinie_skript = lade_skript("kosten_basislinie", _WURZEL / "scripts" / "kosten_basislinie.py")


def lies_basislinie() -> dict[str, Any]:
    return tomllib.loads((_WURZEL / basislinie_skript.BASISLINIE).read_text(encoding="utf-8"))


@pytest.fixture
def kosten() -> Callable[[str, Mapping[str, int]], None]:
    """Vergleicht die Kosten eines Szenarios exakt mit der Basislinie."""

    def pruefen(szenario: str, werte: Mapping[str, int]) -> None:
        python = platform.python_version()
        ziel = os.environ.get("SWARMQ_KOSTEN_MESSUNG")
        if ziel:
            with Path(ziel).open("a", encoding="utf-8") as datei:
                datei.write(json.dumps({"szenario": szenario, "python": python, "werte": dict(werte)}) + "\n")
            return
        fehler = basislinie_skript.vergleiche(lies_basislinie(), szenario, werte, python)
        assert not fehler, "\n".join(fehler)

    return pruefen


class Zaehlend:
    """Hülle um Broker, Backend oder Store: zählt jeden Aufruf einer
    Coroutine-Methode unter ``<präfix>.<methode>`` und reicht ihn durch."""

    def __init__(self, inner: Any, praefix: str, aufrufe: Counter[str]) -> None:
        self._inner = inner
        self._praefix = praefix
        self._aufrufe = aufrufe

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._inner, name)
        if name in _STORES:
            return Zaehlend(attr, f"{self._praefix}.{name}", self._aufrufe)
        if not inspect.iscoroutinefunction(attr):
            return attr
        schluessel = f"{self._praefix}.{name}"

        async def call(*args: Any, **kwargs: Any) -> Any:
            self._aufrufe[schluessel] += 1
            return await attr(*args, **kwargs)

        return call


class KostenOk(Task):
    task_info = TaskInfo(name="kosten_ok", queues=[QUEUE])

    @override
    async def run(self, *args: Any) -> list[Any]:
        return list(args)


class KostenRetry(Task):
    """Scheitert, solange ``fehlschlaege`` über 0 liegt; ein Retry mit fester Wartezeit."""

    task_info = TaskInfo(name="kosten_retry", queues=[QUEUE], max_retries=1, retry_backoff="fixed")
    fehlschlaege: ClassVar[int] = 0

    @override
    async def run(self, *args: Any) -> list[Any]:
        if KostenRetry.fehlschlaege > 0:
            KostenRetry.fehlschlaege -= 1
            raise RuntimeError("vorübergehender Fehler")
        return list(args)


class KostenSchritt(Task):
    task_info = TaskInfo(name="kosten_schritt", queues=[QUEUE])

    @override
    async def run(self, *args: int) -> int:
        return sum(args) + 1


async def amqp_frames(client_schedule: Callable[[Client], Coroutine[Any, Any, object]]) -> int:
    """AMQP-Frames, die *client_schedule* über einen ``RabbitMQBroker`` auf
    den Kanal schreibt. Der Publish läuft durch den echten aio-pika/aiormq-
    Code; der Test spielt Writer und Broker (``FakeTransport``)."""
    transport = FakeTransport()
    broker = RabbitMQBroker("amqp://guest:guest@localhost/")
    broker._connection = Mock(is_closed=False, transport=None)
    broker._connection.ready = AsyncMock()
    broker._publish_channel_factory = AsyncMock(side_effect=[FakePublishChannel(transport)])
    backend = InMemoryBackend(FakeClock())
    await backend.connect()
    registry = TaskRegistry()
    registry.register(KostenOk)
    client = Client(broker=cast("Any", broker), backend=cast("Any", backend), registry=registry)
    frames = 0
    publish: asyncio.Task[object] = asyncio.create_task(client_schedule(client))
    while not publish.done():
        naechster = asyncio.ensure_future(transport.next_frame())
        await asyncio.wait({naechster, publish}, return_when=asyncio.FIRST_COMPLETED)
        if not naechster.done():
            naechster.cancel()
            break
        frame = naechster.result()
        frames += _zaehle_frames(frame.payload)
        frame.drain()
        transport.confirm_all()
    await publish
    await backend.close()
    return frames


def _zaehle_frames(payload: bytes) -> int:
    """Frames in einem gebündelten Schreibauftrag von aiormq: je Frame Typ (1 Byte),
    Kanal (2), Länge (4), Nutzlast und das Ende-Byte 0xCE."""
    anzahl = position = 0
    while position < len(payload):
        laenge = int.from_bytes(payload[position + 3 : position + 7], "big")
        assert payload[position + 7 + laenge] == 0xCE, "kein AMQP-Frame-Ende"
        position += 8 + laenge
        anzahl += 1
    return anzahl


@dataclass
class Pipeline:
    """Client und Worker über zählenden In-Memory-Fakes; ``aufrufe`` hält
    die Vertragsaufrufe beider Seiten."""

    clock: FakeClock
    client: Client
    worker: Worker
    aufrufe: Counter[str]
    zustellungen: list[Delivery] = field(default_factory=list)

    async def abarbeiten(self) -> None:
        """Alle anstehenden Zustellungen nacheinander durch die Pipeline."""
        await asyncio.sleep(0)  # vom Broker angestoßene Zustellungs-Tasks laufen an
        while self.zustellungen:
            await self.worker._handle_message(self.zustellungen.pop(0))
            await asyncio.sleep(0)


async def baue_pipeline(
    clock: FakeClock, broker: Any, backend: Any, aufrufe: Counter[str], tasks: Iterable[type[Task]]
) -> Pipeline:
    """Client und Worker über *broker* und *backend*, verdrahtet wie
    ``WorkerLifecycle._wire_coordination`` ohne Scheduler; der Worker
    konsumiert ``QUEUE`` über ``Pipeline.abarbeiten``."""
    registry = TaskRegistry()
    for task in tasks:
        registry.register(task)
    config = SwarmQConfig(broker_url="amqp://unused", backend_url="valkey://unused", queues=[QUEUE])
    worker = Worker(broker=broker, backend=backend, registry=registry, config=config)
    worker._rate_limiter = backend.rate_limit
    worker._lock_manager = backend.locks
    worker._guard_store = backend.execution_guard
    worker._workflow_advancer = WorkflowAdvancer(
        backend,
        backend.chords,
        publisher=WorkflowStepEnqueuer(
            publish=worker._publish_workflow_step,
            dispatch=worker._signal_dispatcher.dispatch,
            get_chain=worker._get_chain,
        ),
        resolve_queue=worker._resolve_workflow_queue,
        make_task_id=workflow_step_task_id,
        unpack=lambda raw: decode_result(raw)["result"],
        ttl_ms=config.result_ttl * 1000,
    )
    client = Client(broker=broker, backend=backend, registry=registry)
    result = Pipeline(clock, client, worker, aufrufe)

    async def zustellen(delivery: Delivery) -> None:
        result.zustellungen.append(delivery)

    await broker.consume(QUEUE, zustellen, concurrency=1_000)
    return result


@pytest.fixture
async def pipeline() -> AsyncIterator[Pipeline]:
    clock = FakeClock()
    broker, backend = InMemoryBroker(clock), InMemoryBackend(clock)
    await broker.connect()
    await backend.connect()
    aufrufe: Counter[str] = Counter()
    result = await baue_pipeline(
        clock,
        Zaehlend(broker, "broker", aufrufe),
        Zaehlend(backend, "backend", aufrufe),
        aufrufe,
        (KostenOk, KostenRetry, KostenSchritt),
    )
    aufrufe.clear()  # das ``consume`` des Aufbaus gehört zu keinem Szenario
    yield result
    await broker.close()
    await backend.close()
