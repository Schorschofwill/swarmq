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

from swarmq.scheduler import SCHEDULER_LOCK_NAME
from tests.contracts import coordination, publisher_consumer, result_backend
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
    assert await broker.dead_letter_depth("q") == 2 and await broker.queue_depth("q") == 0
