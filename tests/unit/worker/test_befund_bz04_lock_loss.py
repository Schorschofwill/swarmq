"""Befund BZ-04 (Folge): ein verdrängter Lock-Key führt still zu Doppelausführung.

Register: docs/befunde/betrieb-ueber-zeit-2026-10.md (BZ-04; E-3, F-5).

Unter ``volatile-lru``/``allkeys-lru`` (oder nach Valkey-Datenverlust)
verschwindet ``sq:lock:<name>`` mitten in der Ausführung. Ein zweiter Worker
erwirbt denselben Mutex, beide laufen gleichzeitig. Der erste Halter merkt es
bei der Freigabe: ``release`` liefert ``False`` (Key weg bzw. fremder
Besitzer). Der Worker verwirft dieses Ergebnis heute still
(``worker/pipeline.py`` ``_release_acquired_locks``), es gibt weder Log noch
Metrik -- die Doppelausführung bleibt unsichtbar.

Erwartet: Stellt der Halter bei der Freigabe fest, dass er den Lock nicht
mehr besaß, meldet der Worker das laut (WARNING mit Lock-Namen).

Grenze: Lock-Store ist der In-Memory-Vertrag (gleiche Owner-Semantik wie
``lock_release.lua``); die Verdrängung wird durch ``force_release``
(entspricht ``DEL``) simuliert.

Test-Liste:
- [x] Lock während der Ausführung verdrängt -> zweiter Halter möglich
      (Nachweis der Doppelausführung) und der Worker warnt bei der Freigabe
      (``task_lock_lost``, ``event=lost_at_release``; Plan 0.12.0, MR 13)
"""

from __future__ import annotations

from typing import Any, ClassVar

import pytest

from swarmq.config import SwarmQConfig
from swarmq.registry import TaskRegistry
from swarmq.task import Task, TaskInfo
from swarmq.worker import Worker
from tests.support.inmemory.backend import InMemoryLockStore
from tests.support.inmemory.clock import FakeClock
from tests.support.messages import make_message
from tests.unit.worker._helpers import _default_payload

pytestmark = pytest.mark.timeout(5)

_LOCK = "bz04-report"


class _EvictedMidRun(Task):
    task_info = TaskInfo(name="bz04_evicted_mid_run", queues=["default"], lock=_LOCK, hard_timeout=60)

    store: ClassVar[InMemoryLockStore]
    second_holder: ClassVar[list[object]] = []

    async def run(self) -> None:
        # Valkey verdrängt den Lock-Key (volatile-lru) ...
        await type(self).store.force_release(_LOCK, max_concurrency=1)
        # ... und ein anderer Worker bekommt denselben Mutex, waehrend dieser
        # Handler noch laeuft.
        token = await type(self).store.acquire(_LOCK, owner="other-worker", ttl_ms=90_000, max_concurrency=1)
        type(self).second_holder.append(token)


@pytest.mark.befund("BZ-04")
async def test_lost_lock_is_reported_when_the_holder_releases(
    broker: Any, backend: Any, swarmq_logs: list[dict[str, Any]]
) -> None:
    store = InMemoryLockStore(FakeClock())
    _EvictedMidRun.store = store
    _EvictedMidRun.second_holder = []
    registry = TaskRegistry()
    registry.register(_EvictedMidRun)
    config = SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x")
    worker = Worker(broker=broker, backend=backend, registry=registry, config=config)
    worker._lock_manager = store

    await worker._handle_message(make_message(_default_payload(name="bz04_evicted_mid_run", args=[], kwargs={})))

    # Nachweis der Folge: zwei Halter desselben Mutex gleichzeitig.
    assert _EvictedMidRun.second_holder and _EvictedMidRun.second_holder[0] is not None
    assert await store.current_owner(_LOCK) == "other-worker"

    lost = [e for e in swarmq_logs if e["log_level"] in ("warning", "error") and e.get("lock_name") == _LOCK]
    assert lost, (
        "der Halter hat seinen Lock verloren (zweiter Halter lief parallel), "
        f"der Worker meldet das nicht. Logs: {[e['event'] for e in swarmq_logs]}"
    )
