"""Integration tests for ValkeyBackend cancel-flag round-trips.

Exercises the real valkey-py NX/PX semantics that the Unit 3 mocks
assume, plus TTL expiry behaviour that cannot be proven with mocks.
"""

from __future__ import annotations

import asyncio

import valkey.asyncio as aio_valkey

from swarmq.backend.valkey import ValkeyBackend
from swarmq.keyspace import KeySpace


async def test_set_is_clear_round_trip(valkey_url: str):
    backend = ValkeyBackend(valkey_url)
    await backend.connect()
    try:
        assert await backend.is_cancelled("task-rt") is False

        assert await backend.set_cancel_flag("task-rt", 60_000) is True
        assert await backend.is_cancelled("task-rt") is True

        # BZ-23 (Plan 0.12.0, MR 5): clear verkürzt auf höchstens 1 h
        # (PEXPIRE … LT) statt zu löschen.
        assert await backend.set_cancel_flag("task-long", 86_400_000) is True
        await backend.clear_cancel_flag("task-rt")
        await backend.clear_cancel_flag("task-long")
        assert await backend.is_cancelled("task-rt") is True
        raw = aio_valkey.Valkey.from_url(valkey_url)
        try:
            assert 0 < await raw.pttl(KeySpace(None).cancel("task-rt")) <= 60_000
            assert 3_590_000 < await raw.pttl(KeySpace(None).cancel("task-long")) <= 3_600_000
        finally:
            await raw.aclose()
    finally:
        await backend.close()


async def test_set_cancel_flag_second_call_returns_false(valkey_url: str):
    backend = ValkeyBackend(valkey_url)
    await backend.connect()
    try:
        first = await backend.set_cancel_flag("task-dup", 60_000)
        second = await backend.set_cancel_flag("task-dup", 60_000)
        assert first is True
        assert second is False
    finally:
        await backend.clear_cancel_flag("task-dup")
        await backend.close()


async def test_cancel_flag_ttl_expires(valkey_url: str):
    backend = ValkeyBackend(valkey_url)
    await backend.connect()
    try:
        # 100 ms TTL — short but deterministic on a local Valkey
        assert await backend.set_cancel_flag("task-ttl", 100) is True
        assert await backend.is_cancelled("task-ttl") is True

        await asyncio.sleep(0.3)
        assert await backend.is_cancelled("task-ttl") is False
    finally:
        await backend.close()
