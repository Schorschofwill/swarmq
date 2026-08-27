"""Bindings der Backend-Contract-Klassen an Valkey (Plan U17/U18, KTD9):
``ResultBackend`` sowie ``LockStore``, ``RateLimitStore``, ``ChordStore`` und
``ScheduleStore`` über die Fassaden-Properties.

Läuft nur mit erreichbarem Dienst (``check_docker`` aus ``tests/support``).
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import pytest

from swarmq.backend.valkey import ValkeyBackend
from swarmq.locking import SCHEDULER_LOCK_NAME
from tests.contracts import coordination, result_backend
from tests.support.inmemory import RealClock


class TestResultBackendValkey(result_backend.ResultBackendContract):
    @pytest.fixture
    def clock(self) -> RealClock:
        return RealClock()

    @pytest.fixture
    async def impl(self, valkey_url: str) -> AsyncIterator[ValkeyBackend]:
        backend = ValkeyBackend(valkey_url)
        await backend.connect()
        yield backend
        await backend.close()

    @pytest.fixture
    def fresh_impl(self, valkey_url: str) -> ValkeyBackend:
        return ValkeyBackend(valkey_url)

    @pytest.fixture
    def unreachable_impl(self) -> ValkeyBackend:
        return ValkeyBackend("valkey://127.0.0.1:1/0")


@pytest.fixture
async def connected_backend(valkey_url: str) -> AsyncIterator[ValkeyBackend]:
    backend = ValkeyBackend(valkey_url)
    await backend.connect()
    yield backend
    await backend.close()


class TestLockStoreValkey(coordination.LockStoreContract):
    @pytest.fixture
    def clock(self) -> RealClock:
        return RealClock()

    @pytest.fixture
    def impl(self, connected_backend: ValkeyBackend) -> Any:
        return connected_backend.locks


class TestRateLimitStoreValkey(coordination.RateLimitStoreContract):
    @pytest.fixture
    def clock(self) -> RealClock:
        return RealClock()

    @pytest.fixture
    def impl(self, connected_backend: ValkeyBackend) -> Any:
        return connected_backend.rate_limit


class TestChordStoreValkey(coordination.ChordStoreContract):
    @pytest.fixture
    def clock(self) -> RealClock:
        return RealClock()

    @pytest.fixture
    def impl(self, connected_backend: ValkeyBackend) -> Any:
        return connected_backend.chords


class TestScheduleStoreValkey(coordination.ScheduleStoreContract):
    @pytest.fixture
    def clock(self) -> RealClock:
        return RealClock()

    @pytest.fixture
    def impl(self, connected_backend: ValkeyBackend) -> Any:
        return connected_backend.schedules

    @pytest.fixture
    def leader(self, connected_backend: ValkeyBackend) -> Callable[[str], Awaitable[None]]:
        async def _become(owner: str) -> None:
            locks = connected_backend.locks
            await locks.force_release(SCHEDULER_LOCK_NAME, max_concurrency=1)
            token = await locks.acquire(SCHEDULER_LOCK_NAME, owner=owner, ttl_ms=60_000, max_concurrency=1)
            assert token is not None

        return _become
