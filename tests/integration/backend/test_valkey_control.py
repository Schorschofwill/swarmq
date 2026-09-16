"""``ValkeySetpointStore`` gegen ein echtes Valkey (Plan Kontrollkanal, U1).

Test-Liste:
- Eine geschriebene Rückmeldung verschwindet nach Ablauf ihres Verfallsdatums.
- Zwei Supervisoren derselben Gruppe überschreiben einander nicht.
- Sollwert schreiben, lesen, entfernen am echten Backend.
"""

from __future__ import annotations

import asyncio
import uuid

import pytest

from swarmq.backend.valkey.control import ValkeySetpointStore
from swarmq.contracts.control import Setpoint


@pytest.fixture
async def store(valkey_url: str):
    s = ValkeySetpointStore(valkey_url)
    await s.connect()
    yield s
    await s.close()


def _group() -> str:
    return f"grp-{uuid.uuid4().hex[:8]}"


class TestStatusExpiry:
    @pytest.mark.timeout(10)
    async def test_status_disappears_after_its_ttl(self, store: ValkeySetpointStore):
        group = _group()
        await store.write_status(group, "host:p1", {"effective": [2, 2]}, ttl_s=0.2)
        assert await store.read_status(group, "host:p1") == {"effective": [2, 2]}
        deadline = asyncio.get_running_loop().time() + 5.0
        while await store.read_status(group, "host:p1") is not None:
            assert asyncio.get_running_loop().time() < deadline, "status did not expire"
            await asyncio.sleep(0.05)


class TestPerSupervisorStatus:
    async def test_two_supervisors_of_one_group_do_not_overwrite_each_other(self, store: ValkeySetpointStore):
        group = _group()
        await store.write_status(group, "host-a:p1", {"effective": [2, 4]}, ttl_s=30)
        await store.write_status(group, "host-b:p1", {"effective": [4, 4]}, ttl_s=30)
        assert await store.read_status(group, "host-a:p1") == {"effective": [2, 4]}
        assert await store.read_status(group, "host-b:p1") == {"effective": [4, 4]}


class TestSetpointRoundTrip:
    async def test_write_read_clear(self, store: ValkeySetpointStore):
        group = _group()
        assert await store.read_setpoint(group) is None
        await store.write_setpoint(group, Setpoint(low=2, high=8))
        assert await store.read_setpoint(group) == Setpoint(low=2, high=8)
        await store.clear_setpoint(group)
        assert await store.read_setpoint(group) is None
