"""Binding der ``ResultBackend``-Contract-Klasse an Valkey (Plan U17, KTD9).

Läuft nur mit erreichbarem Dienst (``check_docker`` aus ``tests/support``).
Die Koordinationsverträge (Locks, Rate-Limit, Chords, Schedules) binden in U18.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest

from swarmq.backend.valkey import ValkeyBackend
from tests.contracts import result_backend
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
