"""``swarmq schedule prune --orphans`` und das letzte Lebenszeichen in
``schedule list`` (Plan 2026-10-08-1541, U10, KTD8, R13)."""

# Test list (U10):
# 1. ``prune --orphans`` entfernt nie gemeldete statische Crons samt
#    Fire-Index-Eintrag; gemeldete statische und CLI-Crons bleiben. Text nennt
#    jeden entfernten Namen.
# 2. JSON: ``{"removed": [...]}``, sortiert.
# 3. Ein deaktivierter Waisen-Cron wird gemerkt (kommt deaktiviert zurück).
# 4. Ohne Waisen: Hinweis statt leerer Ausgabe.
# 5. Das Backend wird geschlossen.
# 6. Parser: ``schedule prune`` verlangt ``--orphans``.
# 7. ``schedule list`` (Tabelle): Spalte LAST_SEEN mit jüngster Meldung,
#    ``never`` für nie gemeldete statische, ``-`` für CLI-Crons.
# 8. ``schedule list`` (JSON): ``last_seen_ms`` je Eintrag (``None`` ohne
#    Meldung).

from __future__ import annotations

import argparse
import io
import json
from contextlib import redirect_stdout
from unittest.mock import AsyncMock

import msgspec
import pytest

from swarmq.cli import _build_parser, schedule_cmd
from swarmq.exceptions import BackendConnectionError
from swarmq.scheduler import ScheduleDefinition
from tests.support.inmemory import FakeClock, InMemoryBackend

pytestmark = [pytest.mark.timeout(5)]

_NOW_MS = 1_800_000_000_000


def _def(name: str, *, source: str = "static", disabled: bool = False) -> ScheduleDefinition:
    return ScheduleDefinition(
        schedule_name=name, task_name=name, args=(), kwargs={}, queue="default",
        cron="0 1 * * *", fire_ts_ms=_NOW_MS + 60_000, source=source, disabled=disabled,
    )


@pytest.fixture
async def memory() -> InMemoryBackend:
    be = InMemoryBackend(FakeClock(_NOW_MS))
    await be.connect()
    # Der Handler schließt das Backend; die Tests lesen danach weiter.
    be.close = AsyncMock()  # type: ignore[method-assign]
    return be


@pytest.fixture
def use_backend(monkeypatch, memory):
    async def _factory() -> InMemoryBackend:
        return memory

    monkeypatch.setattr(schedule_cmd, "_get_backend_factory", lambda: _factory)


async def _seed(memory: InMemoryBackend, definition: ScheduleDefinition, *, reported: bool = False) -> None:
    fire = None if definition.disabled else definition.fire_ts_ms
    await memory.schedules.add(definition.schedule_name, msgspec.json.encode(definition), fire)
    if reported:
        await memory.schedules.report_cron(
            definition.schedule_name, "fp", msgspec.json.encode(definition), live_window_ms=180_000,
        )


async def _prune(fmt: str = "table") -> str:
    buf = io.StringIO()
    with redirect_stdout(buf):
        await schedule_cmd._run_schedule_async(
            argparse.Namespace(schedule_command="prune", orphans=True, format=fmt)
        )
    return buf.getvalue()


class TestPruneOrphans:
    async def test_removes_only_never_reported_static_crons(self, memory, use_backend):
        await _seed(memory, _def("legacy"))
        await _seed(memory, _def("reported"), reported=True)
        await _seed(memory, _def("by_cli", source="cli"))

        out = await _prune()

        assert await memory.schedules.get("legacy") is None
        assert await memory.schedules.fire_ts("legacy") is None
        assert await memory.schedules.get("reported") is not None
        assert await memory.schedules.get("by_cli") is not None
        assert out == "removed orphan schedule 'legacy'\n"

    async def test_json_lists_removed_names_sorted(self, memory, use_backend):
        for name in ("zeta", "alpha"):
            await _seed(memory, _def(name))

        assert json.loads(await _prune("json")) == {"removed": ["alpha", "zeta"]}

    async def test_disabled_orphan_is_remembered(self, memory, use_backend):
        await _seed(memory, _def("off", disabled=True))
        await _seed(memory, _def("on"))

        await _prune()

        assert await memory.schedules.pruned_disabled("off") is True
        assert await memory.schedules.pruned_disabled("on") is False

    async def test_without_orphans_says_so(self, memory, use_backend):
        await _seed(memory, _def("reported"), reported=True)

        assert await _prune() == "no orphan schedules (every static schedule has been reported)\n"

    async def test_backend_is_closed_when_reading_fails(self, memory, use_backend):
        memory.schedules.cron_reports = AsyncMock(side_effect=BackendConnectionError("down"))

        with pytest.raises(BackendConnectionError):
            await _prune()

        memory.close.assert_awaited_once()


class TestPruneParser:
    def test_prune_requires_orphans(self, capsys):
        parser = _build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["schedule", "prune"])
        assert "--orphans" in capsys.readouterr().err

    def test_prune_orphans_parses(self):
        ns = _build_parser().parse_args(["schedule", "prune", "--orphans", "--format", "json"])
        assert (ns.schedule_command, ns.orphans, ns.format) == ("prune", True, "json")


async def _list(fmt: str) -> str:
    buf = io.StringIO()
    with redirect_stdout(buf):
        await schedule_cmd._run_schedule_list(argparse.Namespace(format=fmt))
    return buf.getvalue()


class TestListShowsLastSeen:
    async def test_json_carries_last_seen_ms(self, memory, use_backend):
        await _seed(memory, _def("reported"), reported=True)
        await _seed(memory, _def("legacy"))

        data = {e["schedule_name"]: e for e in json.loads(await _list("json"))}

        assert data["reported"]["last_seen_ms"] == _NOW_MS
        assert data["legacy"]["last_seen_ms"] is None

    async def test_table_shows_last_seen_column(self, memory, use_backend):
        await _seed(memory, _def("reported"), reported=True)
        await _seed(memory, _def("legacy"))
        await _seed(memory, _def("by_cli", source="cli"))

        lines = (await _list("table")).splitlines()

        # Spalten sind ausgerichtet; der Cron-Ausdruck enthält Leerzeichen.
        start = lines[0].index("LAST_SEEN")
        cells = {line.split()[0]: line[start:].split()[0] for line in lines[1:]}
        assert cells["reported"] == "2027-01-15T08:00:00+00:00"
        assert cells["legacy"] == "never"
        assert cells["by_cli"] == "-"
