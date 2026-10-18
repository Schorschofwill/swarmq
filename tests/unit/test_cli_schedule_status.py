"""``swarmq schedule status`` zeigt Leader- und Worker-Versionen
(Plan 2026-10-08-1541, U13, KTD11)."""

# Test list (U13):
# 1. JSON: ``leader_version`` des Leaders; bestehende Felder unverändert.
# 2. JSON: ``workers`` listet die meldenden Worker mit Version, sortiert.
# 3. Text: ``LEADER_VERSION`` mit Version; ohne Versionsangabe ein Hinweis
#    auf eine Version vor 0.10.0; ohne Leader ``-``.
# 4. Text: ``WORKERS`` listet die meldenden Worker; ohne Meldung ``(none)``.
# 5. Valkey-Ausfall beim Lesen von Leader-Version oder Worker-Versionen:
#    wie beim Leader selbst propagiert ``BackendConnectionError`` (Exit 1 über
#    ``_run_command``), keine halbe Ausgabe, das Backend wird geschlossen.
# 6. E-075: ``SWARMQ_BACKEND_CONNECT_TIMEOUT_S`` begrenzt auch den
#    Verbindungsaufbau des CLI-Backends (ohne ``SwarmQConfig``).
#
# Test list (Plan 2026-10-17-2103, U19, F4-06, KTD12, E-029):
# 7. JSON: Schlüssel ``<worker_identity>|<owner_token>`` erscheinen je Prozess
#    mit ``worker_id`` (Teil vor dem letzten ``|``) und ``process`` (Token).
# 8. JSON: alte Schlüssel ohne ``|`` (0.11/0.12, Mischbetrieb) erscheinen
#    unverändert als ``worker_id``, ``process`` ist ``null``.
# 9. Text: je Prozess eine Zeile ``<worker_id>  <version>  <process>``; alte
#    Schlüssel ohne Prozessspalte.

from __future__ import annotations

import argparse
import asyncio
import io
import json
from contextlib import redirect_stdout
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from swarmq.cli import schedule_cmd
from swarmq.contracts import LeaderInfo
from swarmq.exceptions import BackendConnectionError

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
    assert data["upcoming"] == []
    assert isinstance(data["now_ms"], int)


async def test_json_lists_reporting_workers_with_version(monkeypatch) -> None:
    backend = _backend(
        leader="host:1:aa",
        version=None,
        workers={"b:p2": "0.10.0", "a:0": "0.10.1"},
    )
    data = json.loads(await _status(monkeypatch, backend, "json"))
    assert data["leader_version"] is None
    # Alte Schlüssel ohne ``|`` (Mischbetrieb, F4-06): ohne Prozessangabe.
    assert data["workers"] == [
        {"worker_id": "a:0", "version": "0.10.1", "process": None},
        {"worker_id": "b:p2", "version": "0.10.0", "process": None},
    ]


@pytest.mark.entscheidung("E-029")
async def test_json_lists_each_process_of_a_worker_identity(monkeypatch) -> None:
    backend = _backend(
        leader="host:1:aa",
        version="0.13.0",
        workers={"host:0|host:21:bb": "0.13.0", "host:0|host:11:aa": "0.12.3"},
    )
    data = json.loads(await _status(monkeypatch, backend, "json"))
    assert data["workers"] == [
        {"worker_id": "host:0", "version": "0.12.3", "process": "host:11:aa"},
        {"worker_id": "host:0", "version": "0.13.0", "process": "host:21:bb"},
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


@pytest.mark.entscheidung("E-029")
async def test_table_lists_each_process_and_old_keys_unchanged(monkeypatch) -> None:
    backend = _backend(
        leader="host:1:aa",
        version="0.13.0",
        workers={"host:0|host:21:bb": "0.13.0", "host:0|host:11:aa": "0.13.0", "host:0": "0.12.3"},
    )
    output = await _status(monkeypatch, backend, "table")
    assert output.endswith("WORKERS:\n  host:0  0.12.3\n  host:0  0.13.0  host:11:aa\n  host:0  0.13.0  host:21:bb\n")


async def test_table_without_reporting_workers(monkeypatch) -> None:
    backend = _backend(leader=None, version=None, workers={})
    output = await _status(monkeypatch, backend, "table")
    assert output.endswith("WORKERS:\n  (none)\n")


@pytest.mark.parametrize("failing", ["leader_info", "worker_versions"])
@pytest.mark.parametrize("fmt", ["json", "table"])
async def test_backend_outage_propagates_without_partial_output(monkeypatch, failing, fmt) -> None:
    backend = _backend(leader="a:0", version="0.10.1", workers={"a:0": "0.10.1"})
    getattr(backend.schedules, failing).side_effect = BackendConnectionError("valkey down")

    async def _factory() -> MagicMock:
        return backend

    monkeypatch.setattr(schedule_cmd, "_get_backend_factory", lambda: _factory)
    buf = io.StringIO()
    with redirect_stdout(buf), pytest.raises(BackendConnectionError, match="valkey down"):
        await schedule_cmd._run_schedule_status(argparse.Namespace(format=fmt, limit=5))
    assert buf.getvalue() == ""
    backend.close.assert_awaited_once()


@pytest.mark.entscheidung("E-075")
async def test_connect_deadline_from_the_environment_bounds_the_cli_backend(monkeypatch) -> None:
    """``schedule status`` baut das Backend ohne ``SwarmQConfig``; die Frist
    kommt aus der Umgebung. Ein Valkey, das nie antwortet, endet nach ihr."""
    client = AsyncMock()
    client.register_script = Mock()

    async def never_answers() -> bool:
        await asyncio.Event().wait()
        return True

    client.ping.side_effect = never_answers
    monkeypatch.setenv("SWARMQ_BACKEND_URL", "valkey://valkey.invalid:6379/0")
    monkeypatch.setenv("SWARMQ_BACKEND_CONNECT_TIMEOUT_S", "0.01")
    monkeypatch.setenv("SWARMQ_VALKEY_EVICTION_CHECK", "off")

    with (
        patch("swarmq.backend.valkey.connection.valkey.asyncio.Valkey.from_url", return_value=client),
        pytest.raises(BackendConnectionError, match=r"timed out after 0\.01s"),
    ):
        await schedule_cmd._run_schedule_status(argparse.Namespace(format="json", limit=5))
