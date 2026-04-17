"""Unit tests for the Lua script loader."""

from __future__ import annotations

import pytest

from swarmq.lua import load_script


def test_load_script_reads_existing_file(tmp_path, monkeypatch):
    # This asserts that the loader returns *something* truthy for a real
    # script file. We don't know which scripts exist yet during TDD, so
    # we drop a temp ``swarmq.lua`` package onto sys.path-findable space
    # only if no real scripts exist yet. For the final state of Phase
    # 3b.1 a ``sliding_window_counter.lua`` WILL ship — that becomes a
    # dedicated end-to-end assertion in test_rate_limit_lua.py.
    #
    # This test just proves the loader works at all: we write a fake
    # script into the package directory and read it back via the
    # loader's importlib.resources path.
    from importlib import resources

    lua_dir = resources.files("swarmq.lua")
    # resources.files() returns a Traversable; we need the real path to
    # write a probe file next to the real scripts. ``as_file`` is the
    # supported way when the path is on-disk (which it is in editable
    # and wheel installs alike, because this is regular package data).
    with resources.as_file(lua_dir) as real_dir:
        probe = real_dir / "_loader_probe.lua"
        probe.write_text("-- loader probe\nreturn 42\n", encoding="utf-8")
        try:
            assert load_script("_loader_probe") == "-- loader probe\nreturn 42\n"
        finally:
            probe.unlink()


def test_load_script_unknown_name_raises_with_helpful_message():
    with pytest.raises(FileNotFoundError) as exc_info:
        load_script("does_not_exist")

    msg = str(exc_info.value)
    assert "does_not_exist" in msg
    assert "swarmq.lua" in msg
    assert ".lua" in msg


def test_load_script_uses_utf8_encoding(tmp_path):
    """Non-ASCII content (e.g. comments) must survive load."""
    from importlib import resources

    lua_dir = resources.files("swarmq.lua")
    with resources.as_file(lua_dir) as real_dir:
        probe = real_dir / "_utf8_probe.lua"
        content = "-- ümlauts are fine\nreturn 'ok'\n"
        probe.write_text(content, encoding="utf-8")
        try:
            assert load_script("_utf8_probe") == content
        finally:
            probe.unlink()


def test_unique_release_script_loads_and_has_owner_check():
    """R3.5 Unit 4: unique_release.lua ships, has the owner-check GET/
    compare/DEL shape. Integration test against real Valkey lives in
    tests/integration/test_valkey_unique.py (Unit 5)."""
    script = load_script("unique_release")
    assert "redis.call('GET'" in script
    assert "redis.call('DEL'" in script
    # Owner-check: the script must not unconditionally DEL.
    assert "task_id" in script.lower() or "owner" in script.lower()
    # Only KEYS[1] and ARGV[1] referenced — no semaphore/force branches.
    assert "ARGV[2]" not in script
