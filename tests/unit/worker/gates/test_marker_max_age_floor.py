"""Untergrenze ``max_age + 300`` fuer Done-, Superseded- und
Cron-Completed-Marker (Review MR 12, P1-1; von Georg am 2026-10-10
entschieden).

Ein Task mit ``max_age`` kann bis zu ``max_age`` in der Queue warten. Ein
Duplikat (at-least-once, Leader-Doppelung) kann also noch so spaet kommen,
auch wenn das Original sofort lief -- der Marker muss so lange leben. Ohne
``max_age`` bleibt die bisherige TTL (dokumentierte Grenze, Abschnitt 7).

Test-Liste:
1. [x] Done-Marker eines Tasks mit ``max_age`` 24 h: ein Duplikat nach 2 h
       Rueckstand wird uebersprungen.
2. [x] Superseded-Marker ebenso.
3. [x] Ohne ``max_age`` bleibt die Done-TTL (Duplikat nach 2 h laeuft).
4. [x] Cron-Completed-Marker: Fire ohne Wartezeit, ``max_age`` 24 h ->
       TTL ``max_age + 300``; gedeckelt bei ``execution_guard_backlog_ttl_s``.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.config import SwarmQConfig
from swarmq.headers import HEADER_RETRY_COUNT
from swarmq.task import TaskInfo
from swarmq.worker.gates import GateContext
from swarmq.worker.gates.execution_guard import ExecutionGuard
from swarmq.worker.gates.schedfire import completed_marker_ttl_ms
from swarmq.worker.outcome import Continue, Terminal
from tests.support.inmemory import FakeClock, InMemoryBackend
from tests.support.messages import make_message

pytestmark = pytest.mark.timeout(5)

_DAY = 86_400
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
def guard(backend: InMemoryBackend) -> ExecutionGuard:
    deps = MagicMock()
    deps.config = SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x")
    deps.broker.ack = AsyncMock()
    deps.broker.retry_later = AsyncMock()
    return ExecutionGuard(deps, store=lambda: backend.execution_guard, dead_letter=AsyncMock())


def _ctx(max_age: int | None) -> GateContext:
    return GateContext(
        task_id="t-1", task_name="t", queue="q", task_info=TaskInfo(name="t", queues=["q"], max_age=max_age)
    )


@pytest.mark.entscheidung("E-010")
async def test_done_marker_of_a_task_with_max_age_outlives_a_backlog(guard: ExecutionGuard, clock: FakeClock) -> None:
    delivery = make_message(headers={})
    assert await guard.apply(delivery, _ctx(_DAY)) is Continue
    lease = guard.take_lease(delivery)
    assert lease is not None
    await guard.finish(lease)

    await clock.advance(_BACKLOG_S)
    assert await guard.apply(make_message(headers={}), _ctx(_DAY)) == Terminal.never_release()


@pytest.mark.entscheidung("E-010")
async def test_superseded_marker_of_a_task_with_max_age_outlives_a_backlog(
    guard: ExecutionGuard,
    clock: FakeClock,
) -> None:
    delivery = make_message(headers={})
    assert await guard.apply(delivery, _ctx(_DAY)) is Continue
    lease = guard.take_lease(delivery)
    assert lease is not None
    await guard.supersede(lease, retry_count=1)

    await clock.advance(_BACKLOG_S)
    assert await guard.apply(make_message(headers={}), _ctx(_DAY)) == Terminal.never_release()
    # Die Retry-Kopie selbst laeuft weiter.
    assert await guard.apply(make_message(headers={HEADER_RETRY_COUNT: 1}), _ctx(_DAY)) is Continue


@pytest.mark.entscheidung("E-010")
async def test_without_max_age_the_done_marker_keeps_the_done_ttl(guard: ExecutionGuard, clock: FakeClock) -> None:
    """Dokumentierte Grenze (Abschnitt 7): ohne ``max_age`` deckt der Marker
    keinen Rueckstand ab, der erst nach dem Lauf entsteht."""
    delivery = make_message(headers={})
    assert await guard.apply(delivery, _ctx(None)) is Continue
    lease = guard.take_lease(delivery)
    assert lease is not None
    await guard.finish(lease)

    await clock.advance(_BACKLOG_S)
    assert await guard.apply(make_message(headers={}), _ctx(None)) is Continue


@pytest.mark.entscheidung("E-010")
def test_completed_marker_of_a_fire_with_max_age_lives_max_age_plus_300(monkeypatch: pytest.MonkeyPatch) -> None:
    """Das Fire lief sofort (keine Wartezeit): ein Duplikat desselben Fires
    kann trotzdem bis ``max_age`` warten. Der Deckel
    ``execution_guard_backlog_ttl_s`` (7 d) bleibt."""
    config = SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x")
    monkeypatch.setattr("swarmq.worker.gates.schedfire.time.time", lambda: 1_760_000_000.0)
    fresh = {"x-swarmq-enqueued-at": 1_760_000_000}
    assert completed_marker_ttl_ms(config, fresh, _DAY) == (_DAY + 300) * 1000
    assert completed_marker_ttl_ms(config, {}, _DAY) == (_DAY + 300) * 1000
    assert completed_marker_ttl_ms(config, fresh, 30 * _DAY) == 7 * _DAY * 1000
    assert config.scheduler_dedup_ttl is not None
    assert completed_marker_ttl_ms(config, fresh, None) == int(config.scheduler_dedup_ttl * 1000)
