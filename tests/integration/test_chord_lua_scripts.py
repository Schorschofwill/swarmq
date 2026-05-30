"""U3: atomic + idempotent chord-completion Lua against real Valkey (R12).

Exactly one of N member completions returns True (fires the body), and
redelivery of an already-counted member is a no-op — even under
concurrent 'last completer' calls.
"""

from __future__ import annotations

import asyncio
import uuid

import pytest

from swarmq.backend.valkey import ValkeyBackend
from swarmq.workflow_orchestration import ChordManager

pytestmark = [pytest.mark.integration, pytest.mark.timeout(5)]


@pytest.fixture
async def manager(valkey_url):
    be = ValkeyBackend(valkey_url)
    await be.connect()
    yield ChordManager(be._client)
    await be.close()


def _ids():
    return f"wf-{uuid.uuid4().hex[:8]}", f"c-{uuid.uuid4().hex[:8]}"


async def test_exactly_one_completion_fires_body(manager):
    wf_id, chord_id = _ids()
    members = [f"m{i}" for i in range(5)]
    fired = []
    for m in members:
        fired.append(
            await manager.record_and_check_complete(
                wf_id, chord_id, m, expected_n=5, ttl_ms=60_000
            )
        )
    assert fired.count(True) == 1
    # The last completion is the one that fires.
    assert fired == [False, False, False, False, True]


async def test_redelivered_member_is_noop(manager):
    wf_id, chord_id = _ids()
    # m0 completes twice (redelivery) before m1.
    assert await manager.record_and_check_complete(
        wf_id, chord_id, "m0", expected_n=2, ttl_ms=60_000
    ) is False
    assert await manager.record_and_check_complete(
        wf_id, chord_id, "m0", expected_n=2, ttl_ms=60_000
    ) is False  # redelivery: SADD no-op, count still 1
    # only m1 brings the distinct count to 2 → fires.
    assert await manager.record_and_check_complete(
        wf_id, chord_id, "m1", expected_n=2, ttl_ms=60_000
    ) is True


async def test_redelivery_of_last_member_does_not_double_fire(manager):
    wf_id, chord_id = _ids()
    await manager.record_and_check_complete(
        wf_id, chord_id, "m0", expected_n=2, ttl_ms=60_000
    )
    assert await manager.record_and_check_complete(
        wf_id, chord_id, "m1", expected_n=2, ttl_ms=60_000
    ) is True
    # m1 redelivered after the body already fired → must NOT fire again.
    assert await manager.record_and_check_complete(
        wf_id, chord_id, "m1", expected_n=2, ttl_ms=60_000
    ) is False


async def test_concurrent_last_completers_fire_once(manager):
    wf_id, chord_id = _ids()
    # 10 distinct members complete concurrently; exactly one call fires.
    results = await asyncio.gather(
        *[
            manager.record_and_check_complete(
                wf_id, chord_id, f"m{i}", expected_n=10, ttl_ms=60_000
            )
            for i in range(10)
        ]
    )
    assert results.count(True) == 1


async def test_distinct_chords_independent(manager):
    wf_id, c1 = _ids()
    _, c2 = _ids()
    assert await manager.record_and_check_complete(
        wf_id, c1, "m0", expected_n=1, ttl_ms=60_000
    ) is True
    assert await manager.record_and_check_complete(
        wf_id, c2, "m0", expected_n=1, ttl_ms=60_000
    ) is True
