"""Entscheidungen zu ``swarmq schedule add`` (Fehlersuche Runde 4, Georg 2026-10-17).

Gegen ``InMemoryBackend``; der Handler schließt das Backend, die Tests lesen
danach weiter.

Testliste:
# [x] E-076: neu angelegt -> ``inserted: true``, ``already_exists: false``
# [x] E-076: Name belegt -> ``inserted: false``, ``already_exists: true``, kein ``duplicate_of``
# [x] E-059 (Nachtrag, CLI-Pfad): Fehlerantwort beim Speichern -> eigener Claim freigegeben
# [x] E-065 (Nachtrag, Produktfrage B): ``--cron`` nimmt die Formen der Positivliste an
# Mit den Fixes folgen: Duplikat -> ``already_exists: false`` (PF-7); ``False`` mit
# fremdem Eintrag -> Claim frei, Backend-Ausfall -> Claim bleibt (PF-6); ``--cron``
# lehnt L/W/#/R/H und 6/7 Felder ab (PF-15).
"""

from __future__ import annotations

import io
import json
from contextlib import redirect_stdout
from typing import Any
from unittest.mock import AsyncMock

import pytest

from swarmq.cli import _build_parser, schedule_cmd
from swarmq.exceptions import BackendResponseError
from swarmq.scheduler import ScheduleStore
from swarmq.serialization import compute_dedup_hash
from tests.support.inmemory import FakeClock, InMemoryBackend

pytestmark = [pytest.mark.timeout(5)]

_NOW_MS = 1_800_000_000_000


@pytest.fixture
async def memory(monkeypatch: pytest.MonkeyPatch) -> InMemoryBackend:
    be = InMemoryBackend(FakeClock(_NOW_MS))
    await be.connect()
    be.close = AsyncMock()  # type: ignore[method-assign]  # der Handler schließt; die Tests lesen weiter

    async def _factory() -> InMemoryBackend:
        return be

    monkeypatch.setattr(schedule_cmd, "_get_backend_factory", lambda: _factory)
    return be


async def _add(name: str, *extra: str, cron: str = "0 1 * * *") -> dict[str, Any]:
    ns = _build_parser().parse_args(
        ["schedule", "add", name, "--cron", cron, "--task", "t", "--format", "json", *extra]
    )
    buf = io.StringIO()
    with redirect_stdout(buf):
        await schedule_cmd._run_schedule_add(ns)
    payload: dict[str, Any] = json.loads(buf.getvalue())
    return payload


@pytest.mark.entscheidung("E-076")
async def test_inserted_schedule_reports_already_exists_false(memory: InMemoryBackend) -> None:
    payload = await _add("nightly")

    assert payload["inserted"] is True
    assert payload["already_exists"] is False


@pytest.mark.entscheidung("E-076")
async def test_taken_name_reports_already_exists_true(memory: InMemoryBackend) -> None:
    await _add("nightly")

    payload = await _add("nightly", cron="0 2 * * *")

    assert payload["inserted"] is False
    assert payload["already_exists"] is True
    assert "duplicate_of" not in payload


@pytest.mark.entscheidung("E-059")
async def test_error_reply_on_store_releases_the_own_claim(
    memory: InMemoryBackend, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Fehlerantwort (nichts gespeichert): der eigene Claim wird freigegeben."""
    monkeypatch.setattr(ScheduleStore, "add_schedule", AsyncMock(side_effect=BackendResponseError("ERR script")))

    with pytest.raises(BackendResponseError):
        await _add("nightly", "--unique-for", "60")

    dedup_hash = compute_dedup_hash("t", (), {})
    assert await memory.claim_unique(dedup_hash, "someone-else", 60_000) is None


@pytest.mark.entscheidung("E-065")
@pytest.mark.parametrize("cron", ["0 9 * * mon-fri", "*/15 * * * *", "0 0 1,15 jan *", "@daily"])
async def test_cli_cron_accepts_the_crontab5_positive_list(memory: InMemoryBackend, cron: str) -> None:
    payload = await _add("job", cron=cron)

    assert payload["inserted"] is True
