"""Integration tests for ValkeyBackend dedup-flag (R3.5 Unit 5).

Exercises the real SET NX GET PX atomicity + unique_release.lua
owner-check round-trips that unit mocks cannot prove.
"""

from __future__ import annotations

import asyncio

import pytest

from swarmq.backend.valkey import ValkeyBackend

pytestmark = [pytest.mark.integration, pytest.mark.timeout(10)]


async def test_setnx_unique_first_call_returns_none(valkey_url: str):
    backend = ValkeyBackend(valkey_url)
    await backend.connect()
    try:
        result = await backend.setnx_unique("h-new", "t-1", 60_000)
        assert result is None
    finally:
        await backend.release_unique("h-new", "t-1")
        await backend.close()


async def test_setnx_unique_second_call_returns_existing_owner(valkey_url: str):
    backend = ValkeyBackend(valkey_url)
    await backend.connect()
    try:
        first = await backend.setnx_unique("h-dup", "t-1", 60_000)
        second = await backend.setnx_unique("h-dup", "t-2", 60_000)
        assert first is None
        assert second == "t-1"
    finally:
        await backend.release_unique("h-dup", "t-1")
        await backend.close()


async def test_setnx_unique_ttl_expires(valkey_url: str):
    backend = ValkeyBackend(valkey_url)
    await backend.connect()
    try:
        assert await backend.setnx_unique("h-ttl", "t-1", 100) is None
        # Second call within TTL → existing owner.
        assert await backend.setnx_unique("h-ttl", "t-2", 60_000) == "t-1"
        await asyncio.sleep(0.3)
        # After TTL expiry → fresh SETNX succeeds with new owner.
        assert await backend.setnx_unique("h-ttl", "t-3", 60_000) is None
    finally:
        await backend.release_unique("h-ttl", "t-3")
        await backend.close()


async def test_release_unique_owner_match_succeeds(valkey_url: str):
    backend = ValkeyBackend(valkey_url)
    await backend.connect()
    try:
        await backend.setnx_unique("h-own", "t-1", 60_000)
        released = await backend.release_unique("h-own", "t-1")
        assert released is True
        # After release, a fresh SETNX with a new owner succeeds.
        assert await backend.setnx_unique("h-own", "t-new", 60_000) is None
    finally:
        await backend.release_unique("h-own", "t-new")
        await backend.close()


async def test_release_unique_wrong_owner_returns_false(valkey_url: str):
    """TTL-expiry-race core guard: a stale task_id must not DEL a flag
    owned by a newer task."""
    backend = ValkeyBackend(valkey_url)
    await backend.connect()
    try:
        await backend.setnx_unique("h-wrong", "t-1", 60_000)
        released = await backend.release_unique("h-wrong", "t-foreign")
        assert released is False
        # Flag still held by t-1.
        assert await backend.setnx_unique("h-wrong", "t-2", 60_000) == "t-1"
    finally:
        await backend.release_unique("h-wrong", "t-1")
        await backend.close()


async def test_release_unique_on_missing_key_returns_false(valkey_url: str):
    backend = ValkeyBackend(valkey_url)
    await backend.connect()
    try:
        released = await backend.release_unique("h-never-set", "t-1")
        assert released is False
    finally:
        await backend.close()


async def test_release_unique_idempotent_after_release(valkey_url: str):
    backend = ValkeyBackend(valkey_url)
    await backend.connect()
    try:
        await backend.setnx_unique("h-idem", "t-1", 60_000)
        assert await backend.release_unique("h-idem", "t-1") is True
        # Second release is a no-op, not an error.
        assert await backend.release_unique("h-idem", "t-1") is False
    finally:
        await backend.close()
