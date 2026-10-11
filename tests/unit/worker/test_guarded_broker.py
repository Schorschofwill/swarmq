"""Protokollierender Broker-Wrapper des Ausführungsschutzes (Plan
2026-10-08-1541, U8, KTD5): die Marker folgen dem tatsächlich ausgeführten
Verb, nicht dem ``DeliveryOutcome``.

Test-Liste:
1. ``ack`` nach dem Claim → Done-Marker, Running gelöscht.
2. ``ack`` scheitert am Kanal (bis 4.2: spätes Ack nach dem Consumer-Timeout)
   → trotzdem Done-Marker: die Ausführung ist abgeschlossen, die
   Neuzustellung wird übersprungen.
3. ``dead_letter`` → Done-Marker; ``SettleFailedAfterPublish`` ebenso, ein
   gescheiterter Publish nicht.
4. ``route_via_dlx`` → Done-Marker nur nach Erfolg.
5. ``retry_later`` mit erhöhtem Retry-Zähler → Superseded-Marker nach dem
   Publish; auch bei ``SettleFailedAfterPublish``; gescheiterter Publish →
   kein Marker.
6. ``retry_later`` ohne erhöhten Zähler (Zurückstellung, Requeue-Kopie) →
   Running VOR dem Verb freigegeben.
7. ``redeliver`` → Running VOR dem Verb freigegeben.
8. Ohne Lease (kein Claim, FIRE_AND_FORGET, Guard aus) → reine Weitergabe.
9. Ein Verb schließt die Lease ab: ein zweites Verb setzt keinen Marker.
10. Scheitert der Marker-Aufruf am Backend, läuft das Verb trotzdem
    (WARNING ``execution_guard_finish_failed``).
11. Alle anderen Attribute (Admin, Publish) gehen an den inneren Broker.

Plan 0.12.0, MR 12 (BZ-12):
12. Ein gescheitertes Ack kuendigt eine Neuzustellung an, die hinter einem
    Rueckstand zurueckkommen kann: der Done-Marker lebt den Rueckstand
    (``execution_guard_backlog_ttl_s``), nicht nur die Done-TTL.
13. ``SettleFailedAfterPublish`` nach Dead-Letter (Done) und nach Retry
    (Superseded) ebenso.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.config import SwarmQConfig
from swarmq.exceptions import (
    BackendConnectionError,
    BrokerConnectionError,
    MessageHeadersTooLarge,
    SettleFailedAfterPublish,
)
from swarmq.task import TaskInfo
from swarmq.worker.gates import GateContext
from swarmq.worker.gates.execution_guard import ExecutionGuard
from swarmq.worker.guarded_broker import GuardedBroker
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
def inner() -> MagicMock:
    broker = MagicMock()
    broker.ack = AsyncMock()
    broker.redeliver = AsyncMock()
    broker.retry_later = AsyncMock()
    broker.dead_letter = AsyncMock()
    broker.route_via_dlx = AsyncMock()
    return broker


@pytest.fixture
def guard(inner: MagicMock, backend: InMemoryBackend) -> ExecutionGuard:
    deps = MagicMock()
    deps.config = SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x")
    deps.broker = inner
    return ExecutionGuard(deps, store=lambda: backend.execution_guard, dead_letter=AsyncMock())


@pytest.fixture
def broker(inner: MagicMock, guard: ExecutionGuard) -> GuardedBroker:
    return GuardedBroker(inner, guard)


def _ctx() -> GateContext:
    return GateContext(task_id="t-1", task_name="t", queue="q", task_info=TaskInfo(name="t", queues=["q"]))


async def _claimed(guard: ExecutionGuard, headers: dict[str, object] | None = None) -> MagicMock:
    delivery = make_message(headers=headers or {})
    await guard.apply(delivery, _ctx())
    return delivery


async def _status(backend: InMemoryBackend, retry_count: int = 0) -> str:
    claim = await backend.execution_guard.claim(
        "t-1",
        owner="probe",
        retry_count=retry_count,
        ttl_ms=60_000,
        state_ttl_ms=600_000,
        max_orphans=99,
    )
    return claim.status


async def test_ack_after_claim_marks_done(
    broker: GuardedBroker,
    guard: ExecutionGuard,
    inner: MagicMock,
    backend: InMemoryBackend,
) -> None:
    delivery = await _claimed(guard)

    await broker.ack(delivery)

    inner.ack.assert_awaited_once_with(delivery)
    assert await _status(backend) == "done"


async def test_failed_ack_after_execution_still_marks_done(
    broker: GuardedBroker,
    guard: ExecutionGuard,
    inner: MagicMock,
    backend: InMemoryBackend,
) -> None:
    """Bis RabbitMQ 4.2 schließt der Consumer-Timeout den Channel: das späte
    Ack scheitert, der Broker hat die Nachricht längst neu zugestellt. Die
    Ausführung ist trotzdem abgeschlossen -- die Neuzustellung (bzw. ihre
    zurückgestellte Kopie) muss übersprungen werden, sonst läuft der Task
    doppelt."""
    delivery = await _claimed(guard)
    inner.ack.side_effect = BrokerConnectionError("channel closed")

    with pytest.raises(BrokerConnectionError):
        await broker.ack(delivery)

    assert await _status(backend) == "done"


async def test_dead_letter_marks_done(
    broker: GuardedBroker,
    guard: ExecutionGuard,
    inner: MagicMock,
    backend: InMemoryBackend,
) -> None:
    delivery = await _claimed(guard)

    await broker.dead_letter(delivery, headers={"k": "v"})

    inner.dead_letter.assert_awaited_once_with(delivery, headers={"k": "v"})
    assert await _status(backend) == "done"


async def test_dead_letter_copy_published_but_ack_failed_marks_done(
    broker: GuardedBroker,
    guard: ExecutionGuard,
    inner: MagicMock,
    backend: InMemoryBackend,
) -> None:
    delivery = await _claimed(guard)
    inner.dead_letter.side_effect = SettleFailedAfterPublish("ack failed")

    with pytest.raises(SettleFailedAfterPublish):
        await broker.dead_letter(delivery, headers={})

    assert await _status(backend) == "done"


async def test_failed_dead_letter_publish_leaves_the_lease_for_the_fallback(
    broker: GuardedBroker,
    guard: ExecutionGuard,
    inner: MagicMock,
    backend: InMemoryBackend,
) -> None:
    """Der Publish scheiterte, das Original bleibt unbestätigt; der Aufrufer
    stellt per ``redeliver`` zurück -- das gibt den Running-Marker frei."""
    delivery = await _claimed(guard)
    inner.dead_letter.side_effect = BrokerConnectionError("publish failed")

    with pytest.raises(BrokerConnectionError):
        await broker.dead_letter(delivery, headers={})
    assert await _status(backend) == "running"

    await broker.redeliver(delivery)

    assert await _status(backend) == "claimed"


async def test_redeliver_frees_the_running_marker_before_the_nack(
    broker: GuardedBroker,
    guard: ExecutionGuard,
    inner: MagicMock,
    backend: InMemoryBackend,
) -> None:
    """Shutdown-``nack`` u. ä.: die sofortige Neuzustellung läuft, statt sich
    am eigenen Marker zurückzustellen."""
    delivery = await _claimed(guard)
    seen: list[str] = []

    async def _nack(_delivery: object) -> None:
        seen.append(await _status(backend))

    inner.redeliver.side_effect = _nack
    await broker.redeliver(delivery)

    assert seen == ["claimed"]


async def test_retry_with_higher_retry_count_marks_superseded(
    broker: GuardedBroker,
    guard: ExecutionGuard,
    inner: MagicMock,
    backend: InMemoryBackend,
) -> None:
    delivery = await _claimed(guard, {"x-swarmq-retry-count": 1})

    await broker.retry_later(delivery, delay_ms=500, headers={"x-swarmq-retry-count": 2})

    inner.retry_later.assert_awaited_once_with(delivery, delay_ms=500, headers={"x-swarmq-retry-count": 2})
    assert await _status(backend, retry_count=1) == "superseded"
    assert await _status(backend, retry_count=2) == "claimed"


@pytest.mark.parametrize("copy_headers", [{"x-swarmq-retry-count": 1}, {}])
async def test_requeue_copy_frees_the_running_marker_before_the_publish(
    broker: GuardedBroker,
    guard: ExecutionGuard,
    inner: MagicMock,
    backend: InMemoryBackend,
    copy_headers: dict[str, object],
) -> None:
    """Rate-Limit-/Lock-Requeue, Zurückstellung bei Backend-Ausfall: dieselbe
    Ausführung kommt als Kopie wieder und muss laufen dürfen."""
    delivery = await _claimed(guard, {"x-swarmq-retry-count": 1})
    seen: list[str] = []

    async def _publish(_delivery: object, **_kwargs: object) -> None:
        seen.append(await _status(backend, retry_count=1))

    inner.retry_later.side_effect = _publish
    await broker.retry_later(delivery, delay_ms=500, headers=copy_headers)

    assert seen == ["claimed"]


async def test_retry_copy_published_but_ack_failed_still_marks_superseded(
    broker: GuardedBroker,
    guard: ExecutionGuard,
    inner: MagicMock,
    backend: InMemoryBackend,
) -> None:
    """``retry_later`` publiziert, das Ack scheitert: der Broker stellt das
    Original neu zu -- es muss übersprungen werden, nur die Retry-Kopie läuft."""
    delivery = await _claimed(guard)
    inner.retry_later.side_effect = SettleFailedAfterPublish("ack failed")

    with pytest.raises(SettleFailedAfterPublish):
        await broker.retry_later(delivery, delay_ms=500, headers={"x-swarmq-retry-count": 1})

    assert await _status(backend, retry_count=0) == "superseded"


async def test_too_large_dead_letter_headers_leave_the_lease_for_the_fallback(
    broker: GuardedBroker,
    guard: ExecutionGuard,
    inner: MagicMock,
    backend: InMemoryBackend,
) -> None:
    """Q14 (Plan 0.12.0, MR 2): ``MessageHeadersTooLarge`` ist ein
    gescheiterter Publish, kein ``SettleFailedAfterPublish`` -- kein Done."""
    delivery = await _claimed(guard)
    inner.dead_letter.side_effect = MessageHeadersTooLarge("headers take 70000 bytes")

    with pytest.raises(MessageHeadersTooLarge):
        await broker.dead_letter(delivery, headers={})

    assert await _status(backend) == "running"


async def test_failed_retry_publish_leaves_the_lease_for_the_fallback(
    broker: GuardedBroker,
    guard: ExecutionGuard,
    inner: MagicMock,
    backend: InMemoryBackend,
) -> None:
    delivery = await _claimed(guard)
    inner.retry_later.side_effect = BrokerConnectionError("publish failed")

    with pytest.raises(BrokerConnectionError):
        await broker.retry_later(delivery, delay_ms=500, headers={"x-swarmq-retry-count": 1})
    assert await _status(backend) == "running"

    await broker.redeliver(delivery)

    assert await _status(backend) == "claimed"


async def test_route_via_dlx_marks_done(
    broker: GuardedBroker,
    guard: ExecutionGuard,
    inner: MagicMock,
    backend: InMemoryBackend,
) -> None:
    delivery = await _claimed(guard)

    await broker.route_via_dlx(delivery)

    inner.route_via_dlx.assert_awaited_once_with(delivery)
    assert await _status(backend) == "done"


async def test_withheld_route_via_dlx_releases_without_done(
    broker: GuardedBroker,
    guard: ExecutionGuard,
    inner: MagicMock,
    backend: InMemoryBackend,
) -> None:
    """Review Phase 2: ein zurückgehaltener ``nack`` (4.3, ``BrokerConnectionError``)
    setzt keinen Done-Marker; der ``finally`` gibt nur frei, die
    Neuzustellung läuft."""
    delivery = await _claimed(guard)
    inner.route_via_dlx.side_effect = BrokerConnectionError("route_via_dlx withheld")

    with pytest.raises(BrokerConnectionError):
        await broker.route_via_dlx(delivery)
    await guard.settle(delivery)

    assert await _status(backend) == "claimed"


def test_other_attributes_go_to_the_inner_broker(inner: MagicMock, guard: ExecutionGuard) -> None:
    broker = GuardedBroker(inner, guard)
    assert broker.publish is inner.publish
    assert broker.queue_depth is inner.queue_depth


def test_route_via_dlx_is_absent_when_the_inner_broker_lacks_it(guard: ExecutionGuard) -> None:
    """``route_via_dlx`` (gates) fällt dann auf ``dead_letter`` zurück."""
    inner = MagicMock(spec=["ack", "redeliver", "retry_later", "dead_letter"])
    assert getattr(GuardedBroker(inner, guard), "route_via_dlx", None) is None


async def test_verbs_without_lease_only_pass_through(
    broker: GuardedBroker,
    inner: MagicMock,
    backend: InMemoryBackend,
) -> None:
    delivery = make_message(headers={})
    await broker.retry_later(delivery, delay_ms=1, headers={"x-swarmq-retry-count": 1})
    await broker.ack(delivery)
    inner.retry_later.assert_awaited_once()
    inner.ack.assert_awaited_once_with(delivery)
    assert await _status(backend) == "claimed"


async def test_marker_failure_does_not_block_the_verb(
    broker: GuardedBroker,
    guard: ExecutionGuard,
    inner: MagicMock,
    backend: InMemoryBackend,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Valkey weg beim Freigeben: das ``nack`` geht trotzdem raus (der
    Running-Marker läuft nach seiner TTL ab); WARNING statt Ausnahme."""
    delivery = await _claimed(guard)
    monkeypatch.setattr(
        backend.execution_guard,
        "release",
        AsyncMock(side_effect=BackendConnectionError("down")),
    )

    with capture_swarmq_logs() as logs:
        await broker.redeliver(delivery)

    inner.redeliver.assert_awaited_once_with(delivery)
    [failed] = [e for e in logs if e["event"] == "execution_guard_finish_failed"]
    assert failed["log_level"] == "warning"
    assert failed["marker"] == "release"


async def test_orphan_limit_dead_letter_marks_done(
    inner: MagicMock,
    backend: InMemoryBackend,
    clock: FakeClock,
) -> None:
    """Die Zustellung an der Waisen-Grenze hält den Marker: ihr Dead-Letter
    schließt ihn ab, liegengebliebene Kopien werden übersprungen."""
    deps = MagicMock()
    deps.config = SwarmQConfig(broker_url="amqp://x", backend_url="valkey://x")

    async def _dead_letter(delivery: object, *_args: object) -> None:
        await guarded.dead_letter(delivery, headers={})

    guard = ExecutionGuard(deps, store=lambda: backend.execution_guard, dead_letter=_dead_letter)
    guarded = GuardedBroker(inner, guard)
    deps.broker = guarded
    for owner in ("a", "b", "c"):
        await backend.execution_guard.claim(
            "t-1",
            owner=owner,
            retry_count=0,
            ttl_ms=1_000,
            state_ttl_ms=600_000,
            max_orphans=3,
        )
        await clock.advance(2)

    await guard.apply(make_message(headers={}), _ctx())

    inner.dead_letter.assert_awaited_once()
    assert await _status(backend) == "done"


async def test_failed_ack_marks_done_for_the_backlog(
    broker: GuardedBroker,
    guard: ExecutionGuard,
    inner: MagicMock,
    backend: InMemoryBackend,
    clock: FakeClock,
) -> None:
    delivery = await _claimed(guard)
    inner.ack.side_effect = BrokerConnectionError("channel closed")
    with pytest.raises(BrokerConnectionError):
        await broker.ack(delivery)

    await clock.advance(2 * 3600)  # ueber die Done-TTL (3900 s), weit unter 7 d
    assert await _status(backend) == "done"


async def test_settle_failed_after_publish_marks_for_the_backlog(
    broker: GuardedBroker,
    guard: ExecutionGuard,
    inner: MagicMock,
    backend: InMemoryBackend,
    clock: FakeClock,
) -> None:
    """Die Kopie liegt, das Ack scheiterte: das Original kommt wieder -- auch
    hinter einem Rueckstand. Done (Dead-Letter) und Superseded (Retry) leben
    den Rueckstand."""
    dead = await _claimed(guard)
    inner.dead_letter.side_effect = SettleFailedAfterPublish("ack failed")
    with pytest.raises(SettleFailedAfterPublish):
        await broker.dead_letter(dead, headers={})
    await clock.advance(2 * 3600)
    assert await _status(backend) == "done"

    retried = make_message(headers={})
    ctx = GateContext(task_id="t-2", task_name="t", queue="q", task_info=TaskInfo(name="t", queues=["q"]))
    await guard.apply(retried, ctx)
    inner.retry_later.side_effect = SettleFailedAfterPublish("ack failed")
    with pytest.raises(SettleFailedAfterPublish):
        await broker.retry_later(retried, delay_ms=5, headers={"x-swarmq-retry-count": 1})
    await clock.advance(2 * 3600)
    claim = await backend.execution_guard.claim(
        "t-2",
        owner="probe",
        retry_count=0,
        ttl_ms=60_000,
        state_ttl_ms=600_000,
        max_orphans=99,
    )
    assert claim.status == "superseded"
