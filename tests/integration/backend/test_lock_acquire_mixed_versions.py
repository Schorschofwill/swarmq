"""Mischbetrieb ``lock_acquire.lua`` 0.12.0 und ab U11 (Plan 2026-10-14-0748,
U11, KTD7, KTD19, FS-06b).

Die neue Fassung wertet den eigenen Token im Key als erworben (neue SHA, gleiche
ARGV und Rueckgaben). Ein 0.12.0-Worker behaelt seine Fassung. Beide arbeiten
auf demselben Lock-Key; gegenseitiger Ausschluss muss in beide Richtungen
gelten. Die alte Fassung liegt als Text unter ``data/lock_acquire_0_12_0.lua``
(``git show v0.12.0:src/swarmq/lua/lock_acquire.lua``) und wird per
``SCRIPT LOAD`` geladen.

Testliste:
# [x] Mutex: alt haelt, neu wird abgewiesen; neu haelt, alt wird abgewiesen
# [x] Semaphor: Slots beider Fassungen zaehlen gemeinsam, der naechste
#     Erwerber wird von beiden abgewiesen
# [x] Freigabe ueber die neue Fassung gibt einen alten Halter frei (gleiches
#     Wertformat)
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
import valkey.asyncio as valkey_asyncio

from swarmq.backend.valkey import ValkeyBackend
from swarmq.contracts import LockToken
from swarmq.keyspace import KeySpace
from swarmq.lua import load_script

pytestmark = [pytest.mark.integration, pytest.mark.timeout(10)]

_OLD_SCRIPT = (Path(__file__).parent / "data" / "lock_acquire_0_12_0.lua").read_text(encoding="utf-8")


class _OldAcquire:
    """``lock_acquire.lua`` aus 0.12.0 auf demselben Key wie ``ValkeyLockStore``."""

    def __init__(self, client: Any, sha: str) -> None:
        self._client = client
        self._sha = sha

    async def __call__(self, name: str, owner: str, *, max_concurrency: int, ttl_ms: int = 60_000) -> bool:
        seconds, microseconds = await self._client.time()
        now_ms = int(seconds) * 1000 + int(microseconds) // 1000
        result = await self._client.evalsha(self._sha, 1, KeySpace().lock(name), owner, ttl_ms, max_concurrency, now_ms)
        return int(result) == 1


@pytest.fixture
async def backend(valkey_url: str) -> AsyncIterator[ValkeyBackend]:
    backend = ValkeyBackend(valkey_url)
    await backend.connect()
    try:
        yield backend
    finally:
        await backend.close()


@pytest.fixture
async def old_acquire(valkey_url: str) -> AsyncIterator[_OldAcquire]:
    client: Any = valkey_asyncio.Valkey.from_url(valkey_url)
    try:
        sha = await client.script_load(_OLD_SCRIPT)
        yield _OldAcquire(client, sha)
    finally:
        await client.aclose()


def _name(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def test_old_script_differs_from_the_current_one() -> None:
    """Voraussetzung: es laufen wirklich zwei Fassungen (verschiedene SHA)."""
    assert load_script("lock_acquire") != _OLD_SCRIPT


async def test_mutex_excludes_across_versions(backend: ValkeyBackend, old_acquire: _OldAcquire) -> None:
    held_by_old = _name("mix-mutex")
    assert await old_acquire(held_by_old, "old-owner", max_concurrency=1)
    assert await backend.locks.acquire(held_by_old, owner="new-owner", ttl_ms=60_000, max_concurrency=1) is None

    held_by_new = _name("mix-mutex")
    assert await backend.locks.acquire(held_by_new, owner="new-owner", ttl_ms=60_000, max_concurrency=1)
    assert not await old_acquire(held_by_new, "old-owner", max_concurrency=1)


async def test_semaphore_slots_of_both_versions_count_together(
    backend: ValkeyBackend, old_acquire: _OldAcquire
) -> None:
    name = _name("mix-sem")
    assert await old_acquire(name, "old-owner", max_concurrency=2)
    assert await backend.locks.acquire(name, owner="new-owner", ttl_ms=60_000, max_concurrency=2)

    assert await backend.locks.acquire(name, owner="third", ttl_ms=60_000, max_concurrency=2) is None
    assert not await old_acquire(name, "fourth", max_concurrency=2)


async def test_release_by_the_new_version_frees_an_old_holder(backend: ValkeyBackend, old_acquire: _OldAcquire) -> None:
    name = _name("mix-mutex")
    assert await old_acquire(name, "old-owner", max_concurrency=1)

    assert await backend.locks.release(LockToken(name=name, owner="old-owner", max_concurrency=1))
    assert await old_acquire(name, "next-owner", max_concurrency=1)
