"""Nachweis BZ-12: Done-/Fired-Marker (3900 s) laufen bei Rueckstand ab ->
Doppelausfuehrung trotz Schutz.

Register: docs/befunde/betrieb-ueber-zeit-2026-10.md (BZ-12).

Die TTL der Marker ist ``max(retry_max_delay, consumer_timeout_s) + 300``
(Done/Superseded) bzw. ``retry_max_delay + 300`` (Schedfire-Completed), also
3900 s im Default. Sie misst die Laufzeit, nicht die Wartezeit in der Queue:
Eine zurueckgestellte Kopie geht als neue Nachricht ans Ende der Queue. Ist
die Queue laenger als 65 min im Rueckstand, sieht die Kopie keinen Marker
mehr und laeuft ein zweites Mal.

Fake-Uhr statt Warten: In-Memory-Store mit der Semantik von
``guard_claim.lua``/``guard_finish.lua`` (Contract-Tests sichern die
Paritaet), echtes Gate, echte Default-Konfiguration.

Testliste:
# [x] Ausfuehrungsschutz: Neuzustellung waehrend des Laufs wird
#     zurueckgestellt; Original schliesst ab; die Kopie kommt nach
#     Rueckstand > Done-TTL -> muss uebersprungen werden
# [x] Schedfire: Fire abgeschlossen, Duplikat desselben Fires (z. B.
#     Leader-Doppelung) kommt nach Rueckstand > scheduler_dedup_ttl -> muss
#     uebersprungen werden
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.config import SwarmQConfig
from swarmq.registry import TaskRegistry
from swarmq.task import TaskInfo
from swarmq.worker.gates import GateContext
from swarmq.worker.gates.execution_guard import ExecutionGuard
from swarmq.worker.gates.schedfire import SchedfireGate
from swarmq.worker.outcome import Continue, Redelivered
from tests.support.inmemory import FakeClock, InMemoryBackend
from tests.support.messages import make_message

pytestmark = pytest.mark.timeout(5)

# Rueckstand der Queue: 2 h (Batch-Import, Aufholen nach Stoerung).
_BACKLOG_S = 2 * 3600


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
async def backend(clock: FakeClock) -> AsyncIterator[InMemoryBackend]:
    impl = InMemoryBackend(clock)
    await impl.connect()
    yield impl
    await impl.close()


@pytest.fixture
def deps(backend: InMemoryBackend) -> MagicMock:
    deps = MagicMock()
    deps.config = SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x")
    deps.backend = backend
    deps.registry = TaskRegistry()
    deps.broker.ack = AsyncMock()
    deps.broker.redeliver = AsyncMock()
    deps.broker.retry_later = AsyncMock()
    deps.broker.dead_letter = AsyncMock()
    return deps


def _ctx(task_id: str = "bz12-task") -> GateContext:
    return GateContext(task_id=task_id, task_name="t", queue="q", task_info=TaskInfo(name="t", queues=["q"]))


@pytest.mark.befund("BZ-12")
@pytest.mark.xfail(
    strict=True,
    reason="BZ-12: Done-Marker (3900 s) laeuft vor der zurueckgestellten Kopie ab, Task laeuft bei Rueckstand doppelt",
)
async def test_deferred_copy_after_a_long_backlog_is_still_skipped(
    deps: MagicMock, backend: InMemoryBackend, clock: FakeClock
) -> None:
    guard = ExecutionGuard(deps, store=lambda: backend.execution_guard, dead_letter=AsyncMock())
    assert deps.config.execution_guard_done_ttl_s < _BACKLOG_S  # Default 3900 s

    original = make_message(headers={})
    assert await guard.apply(original, _ctx()) is Continue
    # Neuzustellung waehrend des Laufs (Consumer-Timeout): zurueckgestellt.
    redelivery = make_message(headers={})
    assert await guard.apply(redelivery, _ctx()) is Redelivered
    copy_headers = deps.broker.retry_later.await_args.kwargs["headers"]
    # Original schliesst ab (Ack -> Done-Marker).
    lease = guard.take_lease(original)
    assert lease is not None
    await guard.finish(lease)

    await clock.advance(_BACKLOG_S)  # die Kopie wartet hinter dem Rueckstand
    outcome = await guard.apply(make_message(headers=copy_headers), _ctx())

    assert outcome is not Continue, "Kopie nach Rueckstand ausgefuehrt: Done-Marker war abgelaufen"


@pytest.mark.befund("BZ-12")
@pytest.mark.xfail(
    strict=True,
    reason="BZ-12: Schedfire-Completed-Marker (3900 s) laeuft vor dem Duplikat ab, Cron feuert bei Rueckstand doppelt",
)
async def test_duplicate_fire_after_a_long_backlog_is_still_skipped(
    deps: MagicMock, backend: InMemoryBackend, clock: FakeClock
) -> None:
    gate = SchedfireGate(deps)
    fire = {"x-swarmq-schedfire": "nightly:1760000000"}
    assert deps.config.scheduler_dedup_ttl < _BACKLOG_S  # Default 3900 s

    first = _ctx("fire-1")
    assert await gate.apply(make_message(headers=dict(fire)), first) is Continue
    # Abschluss wie im finally des Runners (terminal): Completed-Marker.
    await backend.schedules.mark_fire_completed(fire["x-swarmq-schedfire"], int(deps.config.scheduler_dedup_ttl * 1000))

    await clock.advance(_BACKLOG_S)  # Duplikat (Leader-Doppelung) wartet hinter dem Rueckstand
    outcome = await gate.apply(make_message(headers=dict(fire)), _ctx("fire-2"))

    assert outcome is not Continue, "Duplikat desselben Fires nach Rueckstand ausgefuehrt: Marker war abgelaufen"
