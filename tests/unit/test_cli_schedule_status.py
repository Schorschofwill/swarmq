"""``swarmq schedule status`` zeigt Leader- und Worker-Versionen
(Plan 2026-10-08-1541, U13, KTD11)."""

# Test list (U13):
# 1. JSON: ``leader_version`` des Leaders; bestehende Felder unverändert.
# 2. JSON: ``workers`` listet die meldenden Worker mit Version, sortiert.
# 3. Text: ``LEADER_VERSION`` mit Version; ohne Versionsangabe ein Hinweis
#    auf eine Version vor 0.10.0; ohne Leader ``-``.
# 4. Text: ``WORKERS`` listet die meldenden Worker; ohne Meldung ``(none)``.

from __future__ import annotations

import argparse
import io
import json
from contextlib import redirect_stdout
from unittest.mock import AsyncMock, MagicMock

import pytest

from swarmq.cli import schedule_cmd
from swarmq.contracts import LeaderInfo

pytestmark = [pytest.mark.timeout(5)]


def _backend(*, leader: str | None, version: str | None, workers: dict[str, str]) -> MagicMock:
    backend = MagicMock()
    backend.get_scheduler_leader = AsyncMock(return_value=leader)
    backend.close = AsyncMock()
    backend.schedules.poll_due = AsyncMock(return_value=[])
    backend.schedules.leader_info = AsyncMock(return_value=LeaderInfo(owner=leader, version=version))
    backend.schedules.worker_versions = AsyncMock(return_value=workers)
    return backend


async def _status(monkeypatch, backend: MagicMock, fmt: str) -> str:
    async def _factory() -> MagicMock:
        return backend

    monkeypatch.setattr(schedule_cmd, "_get_backend_factory", lambda: _factory)
    buf = io.StringIO()
    with redirect_stdout(buf):
        await schedule_cmd._run_schedule_status(argparse.Namespace(format=fmt, limit=5))
    return buf.getvalue()


async def test_json_carries_leader_version(monkeypatch) -> None:
    backend = _backend(leader="host:1:aa", version="0.10.0", workers={})
    data = json.loads(await _status(monkeypatch, backend, "json"))
    assert data["leader"] == "host:1:aa"
    assert data["leader_version"] == "0.10.0"
    assert data["upcoming"] == [] and isinstance(data["now_ms"], int)


async def test_json_lists_reporting_workers_with_version(monkeypatch) -> None:
    backend = _backend(
        leader="host:1:aa", version=None, workers={"b:p2": "0.10.0", "a:0": "0.10.1"},
    )
    data = json.loads(await _status(monkeypatch, backend, "json"))
    assert data["leader_version"] is None
    assert data["workers"] == [
        {"worker_id": "a:0", "version": "0.10.1"},
        {"worker_id": "b:p2", "version": "0.10.0"},
    ]


@pytest.mark.parametrize(
    ("leader", "version", "line"),
    [
        ("host:1:aa", "0.10.0", "LEADER_VERSION: 0.10.0\n"),
        ("host:1:aa", None, "LEADER_VERSION: (not reported: swarmq before 0.10.0)\n"),
        (None, None, "LEADER_VERSION: -\n"),
    ],
)
async def test_table_shows_leader_version(monkeypatch, leader, version, line) -> None:
    backend = _backend(leader=leader, version=version, workers={})
    output = await _status(monkeypatch, backend, "table")
    assert output.splitlines()[1] + "\n" == line
    assert output.startswith(f"LEADER: {leader or '(none)'}\n")


async def test_table_lists_reporting_workers(monkeypatch) -> None:
    backend = _backend(leader="a:0", version="0.10.1", workers={"b:p2": "0.9.9", "a:0": "0.10.1"})
    output = await _status(monkeypatch, backend, "table")
    assert output.endswith("WORKERS:\n  a:0  0.10.1\n  b:p2  0.9.9\n")



async def test_table_without_reporting_workers(monkeypatch) -> None:
    backend = _backend(leader=None, version=None, workers={})
    output = await _status(monkeypatch, backend, "table")
    assert output.endswith("WORKERS:\n  (none)\n")
