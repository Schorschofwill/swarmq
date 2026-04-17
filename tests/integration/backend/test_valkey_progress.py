"""Integration tests for ValkeyBackend progress primitives (R3.7 Unit 2)."""

from __future__ import annotations

import asyncio
import uuid

import pytest

from swarmq.backend.valkey import ValkeyBackend

pytestmark = [pytest.mark.integration, pytest.mark.timeout(10)]


@pytest.fixture
async def backend(valkey_url: str):
    be = ValkeyBackend(valkey_url)
    await be.connect()
    yield be
    await be.close()


def _uniq() -> str:
    return f"progress-{uuid.uuid4().hex[:8]}"


async def test_set_and_get_round_trip(backend):
    task_id = _uniq()
    fields = {
        "current": "5",
        "total": "10",
        "percent": "50.0",
        "message": "halfway",
        "updated_at": "2026-04-17T10:30:00Z",
        "started_at": "2026-04-17T10:29:00Z",
        "eta_seconds": "30",
    }
    await backend.set_progress(task_id, fields, ttl_ms=60_000)
    raw = await backend.get_progress(task_id)
    assert raw is not None
    assert raw[b"current"] == b"5"
    assert raw[b"message"] == b"halfway"
    assert raw[b"eta_seconds"] == b"30"


async def test_get_progress_missing_returns_none(backend):
    raw = await backend.get_progress(_uniq())
    assert raw is None


async def test_set_progress_refreshes_ttl(backend):
    task_id = _uniq()
    await backend.set_progress(
        task_id, {"current": "1", "total": "10", "percent": "10.0"},
        ttl_ms=200,
    )
    # Before expiry: present.
    raw = await backend.get_progress(task_id)
    assert raw is not None
    # Second set resets the TTL so the key survives past the first
    # window.
    await asyncio.sleep(0.15)
    await backend.set_progress(
        task_id, {"current": "2", "total": "10", "percent": "20.0"},
        ttl_ms=200,
    )
    await asyncio.sleep(0.15)
    raw = await backend.get_progress(task_id)
    # First window would have expired at t=0.2, but the second set at
    # t=0.15 extended it; we check at t=0.3 which is still within
    # the second window.
    assert raw is not None
    assert raw[b"current"] == b"2"


async def test_set_progress_ttl_expiry(backend):
    task_id = _uniq()
    await backend.set_progress(
        task_id, {"current": "1", "total": "10", "percent": "10.0"},
        ttl_ms=100,
    )
    await asyncio.sleep(0.25)
    assert await backend.get_progress(task_id) is None


async def test_publish_progress_reaches_subscriber(backend):
    task_id = _uniq()
    pubsub = backend.create_pubsub()
    channel = backend._progress_channel(task_id)
    await pubsub.subscribe(channel)

    try:
        # Consume the subscribe-confirmation.
        await pubsub.get_message(
            ignore_subscribe_messages=True, timeout=0.5,
        )
        await backend.publish_progress(task_id, b'{"current": 7, "total": 10}')

        # Poll briefly for the delivered message.
        msg = None
        deadline = asyncio.get_event_loop().time() + 1.0
        while asyncio.get_event_loop().time() < deadline:
            m = await pubsub.get_message(
                ignore_subscribe_messages=True, timeout=0.1,
            )
            if m is not None:
                msg = m
                break
        assert msg is not None
        assert msg["type"] == "message"
        assert msg["data"] == b'{"current": 7, "total": 10}'
    finally:
        await pubsub.unsubscribe(channel)
        await pubsub.aclose()


async def test_last_write_wins(backend):
    task_id = _uniq()
    await backend.set_progress(
        task_id, {"current": "1", "total": "10", "percent": "10.0"},
        ttl_ms=60_000,
    )
    await backend.set_progress(
        task_id,
        {"current": "9", "total": "10", "percent": "90.0", "message": "almost"},
        ttl_ms=60_000,
    )
    raw = await backend.get_progress(task_id)
    assert raw[b"current"] == b"9"
    assert raw[b"message"] == b"almost"
