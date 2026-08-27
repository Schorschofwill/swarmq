"""``ValkeyScheduleStore`` (Plan U18): Vertrag ``ScheduleStore`` auf Bytes
gegen einen gemockten valkey-Client — Key-Wahl, Skript-Argumente und
Fehler-Wrapping. Der Store-Teil der früheren ``test_scheduler_store.py``.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from valkey.exceptions import ConnectionError as ValkeyConnectionError

from swarmq.backend.valkey.schedules import DEFS_KEY, ZSET_KEY, ValkeyScheduleStore
from swarmq.exceptions import BackendConnectionError
from swarmq.locking import SCHEDULER_LOCK_KEY


def _store(*, hexists: int = 1, hsetnx: int = 1) -> tuple[ValkeyScheduleStore, MagicMock, dict[str, AsyncMock]]:
    client = MagicMock()
    scripts = {
        "poll": AsyncMock(return_value=[]),
        "commit": AsyncMock(return_value=1),
        "disable": AsyncMock(return_value=1),
    }
    client.register_script = MagicMock(side_effect=[scripts["poll"], scripts["commit"], scripts["disable"]])
    client.hexists = AsyncMock(return_value=hexists)
    client.hsetnx = AsyncMock(return_value=hsetnx)
    client.hset = AsyncMock()
    client.hget = AsyncMock(return_value=b"def")
    client.hdel = AsyncMock()
    client.hgetall = AsyncMock(return_value={b"a": b"A"})
    client.zadd = AsyncMock()
    client.zrem = AsyncMock()
    client.set = AsyncMock(return_value=True)
    client.exists = AsyncMock(return_value=1)
    client.delete = AsyncMock()
    return ValkeyScheduleStore(client), client, scripts


class TestCrud:
    async def test_add_writes_hsetnx_then_zadd(self) -> None:
        store, client, _ = _store(hsetnx=1)
        assert await store.add("s", b"payload", 1_700) is True
        client.hsetnx.assert_awaited_once_with(DEFS_KEY, "s", b"payload")
        client.zadd.assert_awaited_once_with(ZSET_KEY, {"s": 1_700})

    async def test_add_existing_is_noop_without_zadd(self) -> None:
        store, client, _ = _store(hsetnx=0)
        assert await store.add("s", b"payload", 1_700) is False
        client.zadd.assert_not_awaited()

    async def test_add_without_fire_time_skips_index(self) -> None:
        store, client, _ = _store()
        assert await store.add("s", b"payload", None) is True
        client.zadd.assert_not_awaited()

    async def test_put_overwrites_and_indexes(self) -> None:
        store, client, _ = _store()
        await store.put("s", b"p2", 1_800)
        client.hset.assert_awaited_once_with(DEFS_KEY, "s", b"p2")
        client.zadd.assert_awaited_once_with(ZSET_KEY, {"s": 1_800})

    async def test_remove_reports_prior_existence(self) -> None:
        store, client, _ = _store(hexists=1)
        assert await store.remove("s") is True
        client.zrem.assert_awaited_once_with(ZSET_KEY, "s")
        client.hdel.assert_awaited_once_with(DEFS_KEY, "s")
        store2, _, _ = _store(hexists=0)
        assert await store2.remove("ghost") is False

    async def test_list_all_decodes_names(self) -> None:
        store, _, _ = _store()
        assert await store.list_all() == {"a": b"A"}


class TestScripts:
    async def test_disable_calls_script_with_defs_and_zset(self) -> None:
        store, _, scripts = _store()
        assert await store.disable("s", b"disabled") is True
        call = scripts["disable"].await_args
        assert call.kwargs["keys"] == [DEFS_KEY, ZSET_KEY]
        assert call.kwargs["args"] == ["s", b"disabled"]

    async def test_poll_due_decodes_rows(self) -> None:
        store, _, scripts = _store()
        scripts["poll"].return_value = [(b"s", b"1700", b"payload")]
        assert await store.poll_due(2_000, limit=5) == [("s", 1700, b"payload")]
        assert scripts["poll"].await_args.kwargs == {"keys": [ZSET_KEY, DEFS_KEY], "args": [2_000, 5]}

    async def test_commit_fire_passes_leader_key_and_owner(self) -> None:
        store, _, scripts = _store()
        assert await store.commit_fire("s", next_fire_ts_ms=1_900, payload=b"next", owner="chief") is True
        call = scripts["commit"].await_args
        assert call.kwargs["keys"] == [ZSET_KEY, DEFS_KEY, SCHEDULER_LOCK_KEY]
        assert call.kwargs["args"] == ["s", 1_900, b"next", "chief"]

    async def test_commit_fire_one_shot_encodes_removal(self) -> None:
        store, _, scripts = _store()
        await store.commit_fire("once", next_fire_ts_ms=None, payload=None, owner="chief")
        assert scripts["commit"].await_args.kwargs["args"] == ["once", -1, b"", "chief"]

    async def test_commit_fire_rejected_by_owner_check(self) -> None:
        store, _, scripts = _store()
        scripts["commit"].return_value = 0
        assert await store.commit_fire("s", next_fire_ts_ms=1, payload=b"x", owner="impostor") is False


class TestErrorWrapping:
    async def test_driver_errors_become_backend_connection_error(self) -> None:
        store, client, _ = _store()
        client.hsetnx = AsyncMock(side_effect=ValkeyConnectionError("down"))
        with pytest.raises(BackendConnectionError):
            await store.add("s", b"p", None)

    async def test_script_errors_are_wrapped_with_context(self) -> None:
        store, _, scripts = _store()
        scripts["commit"].side_effect = RuntimeError("boom")
        with pytest.raises(BackendConnectionError, match="sched_commit for 's' failed"):
            await store.commit_fire("s", next_fire_ts_ms=1, payload=b"x", owner="o")
