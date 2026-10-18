"""Bindings der Contract-Klassen an den In-Memory-Stub (R2, KTD9) — immer aktiv.

Bindings liefern nur Fixtures; ``test_*`` kommt ausschließlich aus den
Basisklassen (Struktur-Guard).
"""

from __future__ import annotations

import contextlib
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import pytest

from swarmq.locking import SCHEDULER_LOCK_NAME
from tests.contracts import coordination, execution_guard, publisher_consumer, result_backend
from tests.support.inmemory import FakeClock, InMemoryBackend, InMemoryBroker


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
async def backend(clock: FakeClock) -> AsyncIterator[InMemoryBackend]:
    impl = InMemoryBackend(clock)
    await impl.connect()
    yield impl
    await impl.close()


class TestBrokerInMemory(publisher_consumer.BrokerContract):
    @pytest.fixture
    async def impl(self, clock: FakeClock, queue: str) -> AsyncIterator[InMemoryBroker]:
        broker = InMemoryBroker(clock)
        await broker.connect()
        await broker.declare_queue(queue)
        yield broker
        await broker.close()

    @pytest.fixture
    def fresh_impl(self, clock: FakeClock) -> InMemoryBroker:
        return InMemoryBroker(clock)

    @pytest.fixture
    def unreachable_impl(self, clock: FakeClock) -> InMemoryBroker:
        return InMemoryBroker(clock, unreachable=True)

    @pytest.fixture
    def queue(self) -> str:
        return f"ctr-{uuid.uuid4().hex[:8]}.high"

    @pytest.fixture
    def dead_letters(self, impl: InMemoryBroker) -> Callable[[str], Awaitable[list[tuple[bytes, dict[str, Any]]]]]:
        async def _read(queue: str) -> list[tuple[bytes, dict[str, Any]]]:
            return [(d.body, d.headers) for d in impl.dead_letters(queue)]

        return _read

    @pytest.fixture
    def publish_failure(self, impl: InMemoryBroker) -> Callable[[], contextlib.AbstractContextManager[None]]:
        return impl.publish_failure


class TestResultBackendInMemory(result_backend.ResultBackendContract):
    @pytest.fixture
    def impl(self, backend: InMemoryBackend) -> InMemoryBackend:
        return backend

    @pytest.fixture
    def fresh_impl(self, clock: FakeClock) -> InMemoryBackend:
        return InMemoryBackend(clock)

    @pytest.fixture
    def unreachable_impl(self, clock: FakeClock) -> InMemoryBackend:
        return InMemoryBackend(clock, unreachable=True)


class TestLockStoreInMemory(coordination.LockStoreContract):
    @pytest.fixture
    def impl(self, backend: InMemoryBackend) -> Any:
        return backend.locks


class TestRateLimitStoreInMemory(coordination.RateLimitStoreContract):
    @pytest.fixture
    def impl(self, backend: InMemoryBackend) -> Any:
        return backend.rate_limit


class TestChordStoreInMemory(coordination.ChordStoreContract):
    @pytest.fixture
    def impl(self, backend: InMemoryBackend) -> Any:
        return backend.chords


class TestScheduleStoreInMemory(coordination.ScheduleStoreContract):
    @pytest.fixture
    def impl(self, backend: InMemoryBackend) -> Any:
        return backend.schedules

    @pytest.fixture
    def leader(self, backend: InMemoryBackend) -> Callable[[str], Awaitable[None]]:
        async def _become(owner: str) -> None:
            await backend.locks.force_release(SCHEDULER_LOCK_NAME, max_concurrency=1)
            token = await backend.locks.acquire(SCHEDULER_LOCK_NAME, owner=owner, ttl_ms=60_000, max_concurrency=1)
            assert token is not None

        return _become


class TestExecutionGuardStoreInMemory(execution_guard.ExecutionGuardStoreContract):
    @pytest.fixture
    def impl(self, backend: InMemoryBackend) -> Any:
        return backend.execution_guard


def test_facade_properties_raise_before_connect(clock: FakeClock) -> None:
    from swarmq.exceptions import BackendConnectionError

    fresh = InMemoryBackend(clock)
    for name in ("locks", "rate_limit", "chords", "schedules"):
        with pytest.raises(BackendConnectionError):
            getattr(fresh, name)


async def test_stub_admin_lists_dead_letters_with_reason_and_headers(clock: FakeClock) -> None:
    from tests.contracts._support import consume_into

    broker = InMemoryBroker(clock)
    await broker.connect()
    await broker.declare_queue("q")
    await broker.publish("q", b'{"task": {"id": "t1"}}', headers={"x-swarmq-unique-hash": "h"})
    await broker.publish("q", b"garbage", headers={"k": "v"})
    inbox = await consume_into(broker, "q", concurrency=2)
    first, second = await inbox.next(), await inbox.next()
    await broker.dead_letter(first, headers={**first.headers, "x-swarmq-traceback": "tb"})
    await broker.route_via_dlx(second)
    summaries, total = await broker.list_dead_letters("q")
    assert total == 2
    assert summaries[0] == {
        "task_id": "t1",
        "headers": {"x-swarmq-unique-hash": "h", "x-swarmq-traceback": "tb"},
        "reason": "published",
    }
    assert summaries[1] == {"task_id": None, "headers": {"k": "v"}, "reason": "routed"}
    assert await broker.dead_letter_depth("q") == 2
    assert await broker.queue_depth("q") == 0


async def _stub_with_dead_letters(clock: FakeClock, *task_ids: str) -> InMemoryBroker:
    from tests.contracts._support import consume_into

    broker = InMemoryBroker(clock)
    await broker.connect()
    await broker.declare_queue("q")
    for task_id in task_ids:
        await broker.publish("q", b'{"task": {"id": "%s"}}' % task_id.encode())
    inbox = await consume_into(broker, "q", concurrency=len(task_ids))
    for _ in task_ids:
        delivery = await inbox.next()
        await broker.dead_letter(delivery, headers=dict(delivery.headers))
    return broker


async def test_stub_admin_list_reports_hit_limit(clock: FakeClock) -> None:
    # F4-05 (E-028 Nachtrag 2026-10-17): der Stub liefert ``DeadLetterScan`` wie RabbitMQ.
    broker = await _stub_with_dead_letters(clock, "t1", "t2")

    cut = await broker.list_dead_letters("q", limit=1)
    whole = await broker.list_dead_letters("q", limit=3)

    assert (getattr(cut, "hit_limit", None), getattr(whole, "hit_limit", None)) == (True, False)


async def test_stub_admin_lookups_report_hit_limit(clock: FakeClock) -> None:
    broker = await _stub_with_dead_letters(clock, "t1", "t2", "t3")

    inspected = await broker.inspect_dead_letter("q", "t3", scan_limit=2)
    found = await broker.inspect_dead_letter("q", "t1", scan_limit=2)
    retried = await broker.retry_dead_letter("q", "t3", scan_limit=2)
    moved = await broker.retry_all_dead_letters("q", limit=2)

    assert [tuple(r) for r in (inspected, retried, moved)] == [(None, 2), (False, 3), (2, 3)]
    assert [getattr(r, "hit_limit", None) for r in (inspected, found, retried, moved)] == [True, False, True, True]
