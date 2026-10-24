"""Hauptbuch des Garantie-Vergleichs (Plan 2026-10-24-1118, KTD14).

Das Buch ist eine SQLite-Datei auf dem Host, in die Erzeuger und Worker der
fünf Systeme aus eigenen Prozessen schreiben. Die Tests nutzen eine Datei im
``tmp_path``; keine Dienste.

Test-Liste:
- [x] ein Publish und seine Ausführung kommen als Zeilen für die Bilanz zurück
- [x] das Buch läuft im WAL-Modus
- [x] die gemeinsame Task-Funktion schreibt Beginn und Ausführung in das Buch aus ``VERGLEICH_HAUPTBUCH``
- [x] ``laufend`` zählt begonnene Tasks ohne Ausführung
- [x] die asynchrone Task-Funktion schreibt ebenso
- [x] ein Ereignis kommt mit seinem Zeitpunkt zurück, ein fehlendes als ``None``
- [x] gestartete Worker-PIDs kommen zurück
"""

from __future__ import annotations

import importlib
import sqlite3
import sys
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType

import pytest

VERGLEICH_DIR = Path(__file__).resolve().parents[2] / "benchmarks" / "comparison"


@pytest.fixture
def hauptbuch(monkeypatch: pytest.MonkeyPatch) -> Iterator[ModuleType]:
    """``stoerungen.hauptbuch`` wie beim Lauf aus ``benchmarks/comparison`` importiert."""
    monkeypatch.syspath_prepend(str(VERGLEICH_DIR))
    yield importlib.import_module("stoerungen.hauptbuch")
    for name in [n for n in sys.modules if n.split(".")[0] == "stoerungen"]:
        del sys.modules[name]


def test_a_publish_and_its_execution_come_back_as_rows_for_the_balance(hauptbuch: ModuleType, tmp_path: Path) -> None:
    buch = hauptbuch.Hauptbuch(tmp_path / "buch.sqlite")

    buch.publish("a", "bestaetigt")
    buch.ausgefuehrt("a")
    publishes, ausfuehrungen = buch.zeilen()

    assert [(p.kennung, p.ausgang) for p in publishes] == [("a", "bestaetigt")]
    assert [a.kennung for a in ausfuehrungen] == ["a"]
    assert ausfuehrungen[0].zeit > 0


def test_the_ledger_runs_in_wal_mode(hauptbuch: ModuleType, tmp_path: Path) -> None:
    pfad = tmp_path / "buch.sqlite"
    hauptbuch.Hauptbuch(pfad)

    db = sqlite3.connect(pfad)
    try:
        [(modus,)] = db.execute("PRAGMA journal_mode").fetchall()
    finally:
        db.close()

    assert modus == "wal"


def test_the_shared_task_function_writes_into_the_ledger_from_the_environment(
    hauptbuch: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pfad = tmp_path / "buch.sqlite"
    monkeypatch.setenv("VERGLEICH_HAUPTBUCH", str(pfad))

    hauptbuch.garantie("a", 0)

    _publishes, ausfuehrungen = hauptbuch.Hauptbuch(pfad).zeilen()
    assert [a.kennung for a in ausfuehrungen] == ["a"]


def test_laufend_counts_begun_tasks_without_execution(
    hauptbuch: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    buch = hauptbuch.Hauptbuch(tmp_path / "buch.sqlite")
    monkeypatch.setenv("VERGLEICH_HAUPTBUCH", str(buch.pfad))

    hauptbuch.garantie("fertig", 0)
    buch.begonnen("laeuft")

    assert buch.laufend() == 1


@pytest.mark.timeout(5)
async def test_the_async_task_function_writes_into_the_ledger_as_well(
    hauptbuch: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    buch = hauptbuch.Hauptbuch(tmp_path / "buch.sqlite")
    monkeypatch.setenv("VERGLEICH_HAUPTBUCH", str(buch.pfad))

    await hauptbuch.garantie_async("a", 0)

    _publishes, ausfuehrungen = buch.zeilen()
    assert [a.kennung for a in ausfuehrungen] == ["a"]
    assert buch.laufend() == 0


def test_an_event_comes_back_with_its_time_and_a_missing_one_as_none(hauptbuch: ModuleType, tmp_path: Path) -> None:
    buch = hauptbuch.Hauptbuch(tmp_path / "buch.sqlite")

    buch.ereignis("valkey_datenverlust")

    assert buch.ereignis_zeit("valkey_datenverlust") > 0
    assert buch.ereignis_zeit("sigkill") is None


def test_started_worker_pids_come_back(hauptbuch: ModuleType, tmp_path: Path) -> None:
    buch = hauptbuch.Hauptbuch(tmp_path / "buch.sqlite")

    buch.worker(4711)
    buch.worker(4712)

    assert sorted(buch.worker_pids()) == [4711, 4712]
