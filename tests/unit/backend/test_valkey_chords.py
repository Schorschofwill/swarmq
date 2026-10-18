"""``ChordManager`` (``backend/valkey/chords.py``) gegen ein gemocktes Skript.

Die Mengen-Semantik von ``chord_complete.lua`` prüfen die Vertragstests gegen
echtes Valkey (``tests/integration/test_chord_lua_scripts.py``); hier nur, was
der Wrapper selbst tut: welches Skript, welcher Key, welche Argumente, wie die
Rückgabe gedeutet wird und dass ein bereits gewandelter Fehler unverändert
durchgeht. Die Fehlerabbildung roher Treiberfehler steht in
``test_befund_f4_11_chord_errors_wrapped.py`` und
``test_valkey_store_error_paths.py``.

Testliste:
# [x] registriert ``chord_complete.lua``
# [x] Key ohne Namensraum: ``sq:wf:chord:<wf>:<chord>``; Argumente Glied, N, TTL
# [x] Key mit dem KeySpace des Backends (Namensraum)
# [x] Rückgabe 1 -> True, 0 -> False (beide Namen)
# [x] ``BackendConnectionError`` aus dem Skript geht unverändert durch
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.backend.valkey.chords import ChordManager
from swarmq.exceptions import BackendConnectionError
from swarmq.keyspace import KeySpace
from swarmq.lua import load_script

pytestmark = pytest.mark.timeout(5)


def _manager(script: AsyncMock, *, keys: KeySpace | None = None) -> tuple[ChordManager, MagicMock]:
    client = MagicMock()
    client.register_script = MagicMock(return_value=script)
    return ChordManager(client, keys=keys), client


def test_registers_the_chord_complete_script() -> None:
    _, client = _manager(AsyncMock(return_value=0))

    client.register_script.assert_called_once_with(load_script("chord_complete"))


async def test_record_member_passes_key_member_count_and_ttl() -> None:
    script = AsyncMock(return_value=0)
    manager, _ = _manager(script)

    await manager.record_member("wf-1", "/h", "m1", expected=3, ttl_ms=60_000)

    script.assert_awaited_once_with(keys=["sq:wf:chord:wf-1:/h"], args=["m1", 3, 60_000])


async def test_key_carries_the_namespace_of_the_backend() -> None:
    script = AsyncMock(return_value=0)
    manager, _ = _manager(script, keys=KeySpace("tenant-a"))

    await manager.record_and_check_complete("wf-1", "/h", "m1", 2, 1_000)

    script.assert_awaited_once_with(keys=[KeySpace("tenant-a").chord("wf-1", "/h")], args=["m1", 2, 1_000])
    assert KeySpace("tenant-a").chord("wf-1", "/h").startswith("tenant-a:")


@pytest.mark.parametrize(("reply", "complete"), [(1, True), (0, False)])
async def test_reply_one_means_complete(reply: int, complete: bool) -> None:
    manager, _ = _manager(AsyncMock(return_value=reply))

    assert await manager.record_member("wf", "/h", "m1", expected=2, ttl_ms=1_000) is complete
    assert await manager.record_and_check_complete("wf", "/h", "m1", 2, 1_000) is complete


async def test_already_wrapped_error_passes_through_unchanged() -> None:
    original = BackendConnectionError("inner")
    manager, _ = _manager(AsyncMock(side_effect=original))

    with pytest.raises(BackendConnectionError) as info:
        await manager.record_member("wf", "/h", "m1", expected=2, ttl_ms=1_000)

    assert info.value is original
