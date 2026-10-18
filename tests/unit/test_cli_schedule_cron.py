"""``swarmq schedule add --cron`` prüft die Positivliste nach crontab(5)
(Fix-Plan 0.13, U15, KTD9, PF-15; E-065).

Die Prüfung läuft vor jeder Verbindung; die Backend-Fabrik ist durch eine
ersetzt, die nie aufgerufen werden darf.

Testliste:
# [x] 6 Felder (Quartz mit Sekunden vorn) -> Abbruch vor der Verbindung, Meldung nennt den Grund
# [x] R, H, L, @reboot, 7 Felder -> ConfigurationError mit eigener Meldung
# [x] großgeschriebene Namen und @midnight -> angenommen
"""

from __future__ import annotations

import io
import json
from contextlib import redirect_stdout
from unittest.mock import AsyncMock

import pytest

from swarmq.cli import _build_parser, main, schedule_cmd
from swarmq.exceptions import ConfigurationError
from tests.support.inmemory import FakeClock, InMemoryBackend

pytestmark = pytest.mark.timeout(5)


@pytest.fixture
def no_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    def _factory() -> object:
        pytest.fail("schedule add connected although the cron is invalid")

    monkeypatch.setenv("SWARMQ_BACKEND_URL", "valkey://never-contacted.invalid:6399/0")
    monkeypatch.setattr(schedule_cmd, "_get_backend_factory", _factory)


async def _add(cron: str) -> None:
    ns = _build_parser().parse_args(["schedule", "add", "job", "--cron", cron, "--task", "t"])
    await schedule_cmd._run_schedule_add(ns)


@pytest.mark.usefixtures("no_backend")
@pytest.mark.entscheidung("E-065")
def test_six_field_cron_ends_before_connecting_with_the_reason(capsys: pytest.CaptureFixture[str]) -> None:
    code = main(["schedule", "add", "job", "--cron", "0 0 9 * * *", "--task", "t"])

    # Exit 1 wie jeder Konfigurationsfehler von ``schedule``, ``dlq`` und
    # ``inspect`` (cli.md, Exit-Code-Tabelle; E-065, Nachtrag 2026-10-17).
    assert code == 1
    err = capsys.readouterr().err
    assert "invalid cron expression" in err
    assert "got 6 fields" in err


@pytest.mark.entscheidung("E-065")
@pytest.mark.parametrize(
    ("cron", "reason"),
    [
        ("R * * * *", "random field 'R'"),
        ("H * * * *", "hash field 'H'"),
        ("0 3 L * *", r"extended syntax \(L/W/#\)"),
        ("@reboot", "alias '@reboot' is not supported"),
        ("* * * * * * 2020", "got 7 fields"),
    ],
)
@pytest.mark.usefixtures("no_backend")
async def test_cron_outside_the_positive_list_is_a_configuration_error(cron: str, reason: str) -> None:
    with pytest.raises(ConfigurationError, match=reason):
        await _add(cron)


@pytest.mark.entscheidung("E-065")
@pytest.mark.parametrize("cron", ["0 0 * * WED", "0 0 * JUL *", "@HOURLY", "@midnight", "0 0 * * 7"])
async def test_names_in_any_case_and_midnight_are_accepted(monkeypatch: pytest.MonkeyPatch, cron: str) -> None:
    memory = InMemoryBackend(FakeClock(1_800_000_000_000))
    await memory.connect()
    memory.close = AsyncMock()  # type: ignore[method-assign]  # der Handler schließt; der Test liest weiter

    async def _factory() -> InMemoryBackend:
        return memory

    monkeypatch.setenv("SWARMQ_BACKEND_URL", "valkey://never-contacted.invalid:6399/0")
    monkeypatch.setattr(schedule_cmd, "_get_backend_factory", lambda: _factory)
    ns = _build_parser().parse_args(["schedule", "add", "job", "--cron", cron, "--task", "t", "--format", "json"])
    buf = io.StringIO()
    with redirect_stdout(buf):
        await schedule_cmd._run_schedule_add(ns)

    assert json.loads(buf.getvalue())["inserted"] is True
    assert await memory.schedules.get("job") is not None
