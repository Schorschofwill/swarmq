"""Schedfire-Gate und Replay-Identitaet (Plan 0.12.0, MR 12, BZ-08 Cron).

Ein Replay eines Cron-Laufs (``dlq retry``, UI-Move: H gesetzt oder n > 0)
laeuft: Das Gate nutzt dann den Key ``K|<Identitaet ohne task_id>`` statt K,
so dass der Completed-Marker des ersten Laufs das Replay nicht verwirft.
Normale Doppel-Fires (n = 0, kein H) behalten K.

Test-Liste:
1. [x] Fire abgeschlossen -> Replay mit ``x-swarmq-replay-id`` laeuft; sein
       Claim traegt den Key ``K|H``, ein Duplikat des Replays wird verworfen.
2. [x] Replay nach Broker-DLX (``x-death`` fuer die Haupt-Queue): Key ``K|#n``.
3. [x] Ohne Replay-Identitaet bleibt der Key K (byte-gleich zu 0.11).
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.config import SwarmQConfig
from swarmq.registry import TaskRegistry
from swarmq.task import TaskInfo
from swarmq.worker.gates import GateContext
from swarmq.worker.gates.schedfire import SchedfireGate, completed_marker_ttl_ms
from swarmq.worker.outcome import Continue, Terminal
from tests.support.inmemory import FakeClock, InMemoryBackend
from tests.support.messages import make_message

pytestmark = pytest.mark.timeout(5)

_KEY = "nightly:1760000000"


@pytest.fixture
async def backend() -> AsyncIterator[InMemoryBackend]:
    impl = InMemoryBackend(FakeClock())
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
    return deps


def _ctx(task_id: str = "fire-1") -> GateContext:
    return GateContext(task_id=task_id, task_name="t", queue="q", task_info=TaskInfo(name="t", queues=["q"]))


async def test_replay_of_a_completed_fire_runs_under_its_own_key(deps: MagicMock, backend: InMemoryBackend) -> None:
    gate = SchedfireGate(deps)
    await backend.schedules.mark_fire_completed(_KEY, 3_900_000)
    replay = {"x-swarmq-schedfire": _KEY, "x-swarmq-replay-id": "r1"}

    ctx = _ctx()
    assert await gate.apply(make_message(headers=dict(replay)), ctx) is Continue
    assert ctx.schedfire_claim is not None and ctx.schedfire_claim.key == f"{_KEY}|r1"
    # Duplikat des Replays (at-least-once, zweiter dlq retry): In-Flight-Claim fremd.
    assert await gate.apply(make_message(headers=dict(replay)), _ctx("fire-2")) == Terminal.never_release()


async def test_replay_after_a_broker_dead_letter_uses_the_death_count(
    deps: MagicMock, backend: InMemoryBackend
) -> None:
    gate = SchedfireGate(deps)
    await backend.schedules.mark_fire_completed(_KEY, 3_900_000)
    deaths = [{"queue": "q", "count": 2, "reason": "rejected"}]
    ctx = _ctx()
    assert await gate.apply(make_message(headers={"x-swarmq-schedfire": _KEY, "x-death": deaths}), ctx) is Continue
    assert ctx.schedfire_claim is not None and ctx.schedfire_claim.key == f"{_KEY}|#2"


async def test_a_plain_duplicate_fire_keeps_the_key_and_is_skipped(deps: MagicMock, backend: InMemoryBackend) -> None:
    gate = SchedfireGate(deps)
    await backend.schedules.mark_fire_completed(_KEY, 3_900_000)
    delayed = [{"queue": "q.DQ.1s", "count": 1, "reason": "expired"}]
    outcome = await gate.apply(make_message(headers={"x-swarmq-schedfire": _KEY, "x-death": delayed}), _ctx())
    assert outcome == Terminal.never_release()


def test_completed_marker_ttl_follows_the_wait_within_its_caps(monkeypatch: pytest.MonkeyPatch) -> None:
    """MR 12 (BZ-12): ``max(base, 2 * Wartezeit + 300)``, gedeckelt bei
    ``max(base, max_age + 300)`` und ``execution_guard_backlog_ttl_s``, nie
    unter ``scheduler_dedup_ttl``."""
    config = SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x")
    base_ms = int(config.scheduler_dedup_ttl * 1000)
    monkeypatch.setattr("swarmq.worker.gates.schedfire.time.time", lambda: 1_760_000_000.0)
    waited = {"x-swarmq-enqueued-at": 1_760_000_000 - 7_200}
    assert completed_marker_ttl_ms(config, {}, None) == base_ms
    assert completed_marker_ttl_ms(config, {"x-swarmq-enqueued-at": "garbage"}, None) == base_ms
    assert completed_marker_ttl_ms(config, waited, None) == (2 * 7_200 + 300) * 1000
    assert completed_marker_ttl_ms(config, waited, 600) == base_ms  # max_age + 300 < base: base
    assert completed_marker_ttl_ms(config, waited, 5_000) == 5_300_000
    week = {"x-swarmq-enqueued-at": 1_760_000_000 - 10 * 86_400}
    assert completed_marker_ttl_ms(config, week, None) == 604_800_000
