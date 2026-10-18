"""``ExecutionGuard.skip_if_settled`` (Plan 0.13.0, U12, F4-02, KTD5): das
Delivery-Count-Gate liest am Zustelllimit vor dem Dead-Letter den Zustand des
Ausführungsschutzes. Gegen den In-Memory-Store (echte Semantik, Fake-Uhr).

Am Delivery-Count-Gate ist ``ctx.task_info`` noch ``None`` (es läuft vor dem
Registry-Lookup): den Zustellmodus löst ``skip_if_settled`` wie
``RetryHandler.dead_letter`` über die Registry auf.

Test-Liste:
1. Done-Marker → bestätigen und überspringen wie im Gate
   (``duplicate_delivery_skipped`` mit ``reason=done``, Metrik
   ``duplicate_skipped``, ``Terminal.never_release()``).
2. Superseded-Marker mit größerem Zähler → ebenso (``reason=superseded``).
3. Kein Marker → ``Continue`` (das Gate schickt in die DLQ), kein Ack.
4. Running-Marker allein → ``Continue`` (``peek`` setzt keinen Marker:
   Contract-Test in ``tests/contracts/execution_guard.py``).
5. Backend-Ausfall beim Lesen → ``Continue`` plus WARNING
   ``execution_guard_peek_failed``.
6. ``execution_guard=False`` → kein Lesen.
7. FIRE_AND_FORGET laut Registry → kein Lesen.
8. Unbekannter Task (``TaskNotFoundError``) → lesen wie AT_LEAST_ONCE.
9. Ohne Store (Worker nicht gestartet) → ``Continue``.
10. Replay-Identität: der Done-Marker der ursprünglichen Ausführung gilt
    nicht für ein Replay (E-009).
11. Done-Marker abgelaufen (``execution_guard_done_ttl_s``) → ``Continue``
    (zweite Restlücke, KD5).
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.config import SwarmQConfig
from swarmq.exceptions import BackendConnectionError, TaskNotFoundError
from swarmq.task import DeliveryMode, TaskInfo
from swarmq.worker.gates import GateContext
from swarmq.worker.gates.execution_guard import ExecutionGuard, execution_identity
from swarmq.worker.outcome import Continue, Terminal
from tests.support.inmemory import FakeClock, InMemoryBackend
from tests.support.messages import make_message
from tests.unit._log_capture import capture_swarmq_logs

pytestmark = pytest.mark.timeout(5)


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
def deps() -> MagicMock:
    deps = MagicMock()
    deps.config = SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x")
    deps.broker.ack = AsyncMock()
    return deps


@pytest.fixture
def events() -> list[str]:
    return []


@pytest.fixture
def guard(deps: MagicMock, backend: InMemoryBackend, events: list[str]) -> ExecutionGuard:
    return ExecutionGuard(deps, store=lambda: backend.execution_guard, dead_letter=AsyncMock(), on_event=events.append)


def _ctx() -> GateContext:
    # Wie am Delivery-Count-Gate: vor dem Registry-Lookup, ohne ``task_info``.
    return GateContext(task_id="t-1", task_name="t", queue="q")


async def _done(backend: InMemoryBackend, identity: str = "t-1", state_ttl_ms: int = 600_000) -> None:
    await backend.execution_guard.claim(
        identity, owner="o", retry_count=0, ttl_ms=60_000, state_ttl_ms=state_ttl_ms, max_orphans=3
    )
    await backend.execution_guard.mark_done(identity, owner="o", state_ttl_ms=state_ttl_ms)


async def test_done_marker_acks_and_skips(
    guard: ExecutionGuard, backend: InMemoryBackend, deps: MagicMock, events: list[str]
) -> None:
    await _done(backend)
    delivery = make_message(headers={"x-delivery-count": 19})

    with capture_swarmq_logs() as logs:
        outcome = await guard.skip_if_settled(delivery, _ctx())

    assert outcome == Terminal.never_release()
    deps.broker.ack.assert_awaited_once_with(delivery)
    [skipped] = [e for e in logs if e["event"] == "duplicate_delivery_skipped"]
    assert skipped["reason"] == "done"
    assert skipped["task_id"] == "t-1"
    assert events == ["duplicate_skipped"]


async def test_superseded_marker_acks_and_skips_the_older_attempt(
    guard: ExecutionGuard, backend: InMemoryBackend, deps: MagicMock
) -> None:
    await backend.execution_guard.mark_superseded("t-1", owner="o", retry_count=1, state_ttl_ms=600_000)
    original = make_message(headers={"x-delivery-count": 19})

    with capture_swarmq_logs() as logs:
        outcome = await guard.skip_if_settled(original, _ctx())

    assert outcome == Terminal.never_release()
    deps.broker.ack.assert_awaited_once_with(original)
    assert [e["reason"] for e in logs if e["event"] == "duplicate_delivery_skipped"] == ["superseded"]


async def test_the_retry_copy_itself_is_not_settled(
    guard: ExecutionGuard, backend: InMemoryBackend, deps: MagicMock
) -> None:
    await backend.execution_guard.mark_superseded("t-1", owner="o", retry_count=1, state_ttl_ms=600_000)
    copy = make_message(headers={"x-delivery-count": 19, "x-swarmq-retry-count": 1})

    assert await guard.skip_if_settled(copy, _ctx()) is Continue
    deps.broker.ack.assert_not_awaited()


async def test_without_a_marker_the_gate_dead_letters(guard: ExecutionGuard, deps: MagicMock) -> None:
    assert await guard.skip_if_settled(make_message(headers={"x-delivery-count": 19}), _ctx()) is Continue
    deps.broker.ack.assert_not_awaited()


async def test_a_running_marker_is_not_settled(guard: ExecutionGuard, backend: InMemoryBackend) -> None:
    claim = await backend.execution_guard.claim(
        "t-1", owner="o", retry_count=0, ttl_ms=60_000, state_ttl_ms=600_000, max_orphans=3
    )
    assert claim.status == "claimed"

    assert await guard.skip_if_settled(make_message(headers={"x-delivery-count": 19}), _ctx()) is Continue


async def test_backend_outage_while_reading_dead_letters_with_a_warning(
    deps: MagicMock, backend: InMemoryBackend
) -> None:
    store = MagicMock(wraps=backend.execution_guard)
    store.peek = AsyncMock(side_effect=BackendConnectionError("valkey down"))
    guard = ExecutionGuard(deps, store=lambda: store, dead_letter=AsyncMock())

    with capture_swarmq_logs() as logs:
        outcome = await guard.skip_if_settled(make_message(headers={"x-delivery-count": 19}), _ctx())

    assert outcome is Continue
    deps.broker.ack.assert_not_awaited()
    [warning] = [e for e in logs if e["event"] == "execution_guard_peek_failed"]
    assert warning["log_level"] == "warning"
    assert warning["task_id"] == "t-1"


async def test_without_execution_guard_nothing_is_read(deps: MagicMock, backend: InMemoryBackend) -> None:
    deps.config = SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x", execution_guard=False)
    await _done(backend)
    store = MagicMock(wraps=backend.execution_guard)
    store.peek = AsyncMock(wraps=backend.execution_guard.peek)
    guard = ExecutionGuard(deps, store=lambda: store, dead_letter=AsyncMock())

    assert await guard.skip_if_settled(make_message(headers={"x-delivery-count": 19}), _ctx()) is Continue
    store.peek.assert_not_awaited()
    deps.broker.ack.assert_not_awaited()


async def test_without_a_store_the_gate_dead_letters(deps: MagicMock) -> None:
    guard = ExecutionGuard(deps, store=lambda: None, dead_letter=AsyncMock())

    assert await guard.skip_if_settled(make_message(headers={"x-delivery-count": 19}), _ctx()) is Continue
    deps.broker.ack.assert_not_awaited()


async def test_fire_and_forget_from_the_registry_is_not_read(deps: MagicMock, backend: InMemoryBackend) -> None:
    deps.registry.get.return_value.task_info = TaskInfo(name="t", queues=["q"], delivery=DeliveryMode.FIRE_AND_FORGET)
    await _done(backend)
    store = MagicMock(wraps=backend.execution_guard)
    store.peek = AsyncMock(wraps=backend.execution_guard.peek)
    guard = ExecutionGuard(deps, store=lambda: store, dead_letter=AsyncMock())

    assert await guard.skip_if_settled(make_message(headers={"x-delivery-count": 19}), _ctx()) is Continue
    store.peek.assert_not_awaited()
    deps.registry.get.assert_called_once_with("t")


async def test_an_unknown_task_is_read_like_at_least_once(
    deps: MagicMock, backend: InMemoryBackend, guard: ExecutionGuard
) -> None:
    deps.registry.get.side_effect = TaskNotFoundError("t")
    await _done(backend)
    delivery = make_message(headers={"x-delivery-count": 19})

    assert await guard.skip_if_settled(delivery, _ctx()) == Terminal.never_release()
    deps.broker.ack.assert_awaited_once_with(delivery)


@pytest.mark.entscheidung("E-009")
async def test_a_replay_at_the_limit_reads_its_own_identity(
    guard: ExecutionGuard, backend: InMemoryBackend, deps: MagicMock
) -> None:
    """E-009: Das Replay ist eine neue Ausführung; der Done-Marker der
    ursprünglichen gilt für es nicht, sein eigener schon."""
    await _done(backend)
    replay = {"x-delivery-count": 19, "x-swarmq-replay-id": "r1"}

    assert await guard.skip_if_settled(make_message(headers=replay), _ctx()) is Continue
    await _done(backend, identity=execution_identity(make_message(headers=replay), "t-1"))
    assert await guard.skip_if_settled(make_message(headers=replay), _ctx()) == Terminal.never_release()


@pytest.mark.entscheidung("E-043")
async def test_an_expired_done_marker_leaves_the_dead_letter(
    guard: ExecutionGuard, backend: InMemoryBackend, clock: FakeClock, deps: MagicMock
) -> None:
    """Zweite Restlücke (KD5): Läuft der Done-Marker
    (``execution_guard_done_ttl_s``) vor dem Erreichen des Limits ab, liefert
    ``peek`` nichts, und das Gate schickt die Zustellung wie bisher in die DLQ."""
    await _done(backend, state_ttl_ms=200)
    await clock.advance(0.4)

    assert await guard.skip_if_settled(make_message(headers={"x-delivery-count": 19}), _ctx()) is Continue
    deps.broker.ack.assert_not_awaited()
