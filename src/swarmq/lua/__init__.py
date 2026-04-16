"""Lua script resources for SwarmQ.

All Valkey-side Lua scripts live as ``.lua`` files in this package and
are loaded at runtime via :func:`load_script`. Consumers then register
them via ``client.register_script(source)`` — the returned
``AsyncScript`` object handles NOSCRIPT recovery on its own, so no
manual ``SCRIPT LOAD`` / ``EVALSHA`` loop is needed here.

Shared by Phase 3b.1 (rate limit), 3b.2 (lock/unique), 3b.3 (cancel).
"""

from __future__ import annotations

from importlib import resources


def load_script(name: str) -> str:
    """Read a Lua script shipped with SwarmQ.

    ``name`` is the file stem (``"sliding_window_counter"``), not a
    path and not a full filename. The ``.lua`` suffix is added here so
    every call site uses the same convention.

    Raises ``FileNotFoundError`` with a message identifying the missing
    script and the package it was looked for in, so the caller sees
    something more actionable than a bare stdlib error.
    """
    file = resources.files("swarmq.lua").joinpath(f"{name}.lua")
    if not file.is_file():
        raise FileNotFoundError(
            f"unknown lua script: {name!r} — no file {name}.lua in swarmq.lua"
        )
    return file.read_text(encoding="utf-8")
