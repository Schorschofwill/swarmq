"""Sättigung je Kombination aus Methode und Modul (Plan 2026-10-24-1118, KTD8).

``scripts/saettigung.py`` berechnet den Stand aus dem Sättigungsbuch, statt ihn
zu speichern. Der git-Aufruf ist injiziert, deshalb laufen die Tests ohne
Repository-Zustand.

Test-Liste:
- [x] eine Kombination ohne Lauf ist offen
- [x] ein leerer Lauf mit Prüfumfang: einmal leer
- [x] zwei leere Läufe in Folge: gesättigt
- [x] ein Befund zwischen zwei leeren Läufen setzt die Folge zurück
- [x] ein Lauf mit Prüfumfang 0 ändert nichts (AE6)
- [x] ENTSCHEIDUNG und NICHT HALTBAR zählen nicht als Befund, FEHLER, LÜCKE und VERBESSERUNG schon
- [x] Läufe zählen in der Reihenfolge ihres Datums
- [x] bei 19 % geänderten Zeilen bleibt eine Kombination gesättigt, bei 20 % ist sie offen (AE5)
- [x] eine wesentliche Änderung öffnet auch eine einmal leere Kombination
- [x] geänderte Zeilen aus ``git diff --numstat <commit>..HEAD -- <modul>`` im Verhältnis zu den heutigen Zeilen
- [x] ein Paket zählt alle seine Dateien (``.py`` und ``.lua``)
- [x] ein umbenanntes Modul ist offen
- [x] eine neue Methode im Katalog öffnet ihre Kombinationen
- [x] eine Säule ist gesättigt, wenn alle ihre Kombinationen gesättigt sind
- [x] der Katalog liefert Methoden, Module und entfallene Module
- [x] das Buch liefert Läufe und Signale
- [x] ``main`` gibt für alle vier Säulen einen Stand und die offenen Signale aus
- [x] ``main`` meldet, wenn alle Säulen gesättigt sind
- [x] ``main`` nennt jede Kombination mit Läufen und dringende offene Signale
- [x] ohne injizierten Aufruf fragt ``main`` das echte git
"""

from __future__ import annotations

import datetime as dt
import subprocess
from pathlib import Path

import pytest

from tests.unit._saettigungsbuch import saettigung as sat

pytestmark = pytest.mark.timeout(5)


def _unchanged(commit: str) -> float:
    return 0.0


def test_a_combination_without_runs_is_open() -> None:
    assert sat.stand_der_kombination([], _unchanged) == sat.Stand.OFFEN


def _lauf(
    *befunde: tuple[str, str], umfang: int = 100, datum: dt.date = dt.date(2026, 10, 17), commit: str = "c1"
) -> object:
    return sat.Lauf(
        id="L-001",
        datum=datum,
        commit=commit,
        methode="M-F-03",
        modul="backend",
        umfang=umfang,
        einheit="Mutanten",
        befunde=tuple(sat.Befund(id=bid, einstufung=einstufung) for bid, einstufung in befunde),
    )


def test_one_empty_run_with_scope_is_empty_once() -> None:
    assert sat.stand_der_kombination([_lauf()], _unchanged) == sat.Stand.EINMAL_LEER


def test_two_empty_runs_in_a_row_saturate() -> None:
    assert sat.stand_der_kombination([_lauf(), _lauf()], _unchanged) == sat.Stand.GESAETTIGT


def test_a_finding_between_two_empty_runs_resets_the_streak() -> None:
    runs = [_lauf(), _lauf(("F4-11", "FEHLER")), _lauf()]

    assert sat.stand_der_kombination(runs, _unchanged) == sat.Stand.EINMAL_LEER


def test_a_run_without_scope_changes_nothing() -> None:
    assert sat.stand_der_kombination([_lauf(), _lauf(umfang=0)], _unchanged) == sat.Stand.EINMAL_LEER


@pytest.mark.parametrize(
    ("einstufung", "erwartet"),
    [
        ("ENTSCHEIDUNG", "GESAETTIGT"),
        ("NICHT HALTBAR", "GESAETTIGT"),
        ("LÜCKE", "EINMAL_LEER"),
        ("VERBESSERUNG", "EINMAL_LEER"),
        ("FEHLER", "EINMAL_LEER"),
    ],
)
def test_only_confirmed_classifications_count_as_findings(einstufung: str, erwartet: str) -> None:
    runs = [_lauf(), _lauf(("X-01", einstufung)), _lauf()]

    assert sat.stand_der_kombination(runs, _unchanged) == sat.Stand[erwartet]


def test_runs_count_in_order_of_their_date() -> None:
    early, late = dt.date(2026, 10, 13), dt.date(2026, 10, 17)
    runs = [_lauf(datum=late), _lauf(datum=late), _lauf(("FS-07", "FEHLER"), datum=early)]

    assert sat.stand_der_kombination(runs, _unchanged) == sat.Stand.GESAETTIGT


@pytest.mark.parametrize(("anteil", "erwartet"), [(0.19, "GESAETTIGT"), (0.20, "OFFEN")])
def test_a_substantial_change_since_the_last_run_reopens(anteil: float, erwartet: str) -> None:
    asked: list[str] = []

    def changed(commit: str) -> float:
        asked.append(commit)
        return anteil

    runs = [_lauf(commit="alt"), _lauf(commit="neu", datum=dt.date(2026, 10, 20))]

    assert sat.stand_der_kombination(runs, changed) == sat.Stand[erwartet]
    assert asked == ["neu"]


def test_a_substantial_change_also_reopens_after_one_empty_run() -> None:
    assert sat.stand_der_kombination([_lauf()], lambda commit: 0.5) == sat.Stand.OFFEN


class _Git:
    """Ersatz für den git-Aufruf: feste ``--numstat``-Ausgabe, Aufrufe protokolliert."""

    def __init__(self, numstat: str) -> None:
        self.numstat = numstat
        self.calls: list[list[str]] = []

    def __call__(self, args: list[str]) -> str:
        self.calls.append(args)
        return self.numstat


def _module(root: Path, rel: str, lines: int) -> None:
    path = root / "src" / "swarmq" / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("x = 1\n" * lines, encoding="utf-8")


def test_the_change_share_relates_changed_lines_to_todays_lines(tmp_path: Path) -> None:
    _module(tmp_path, "backoff.py", 100)
    git = _Git("15\t19\tsrc/swarmq/backoff.py\n")

    share = sat.aenderungsanteil("backoff", "abc123", wurzel=tmp_path, git=git)

    assert share == pytest.approx(0.19)
    assert git.calls == [["diff", "--numstat", "abc123..HEAD", "--", "src/swarmq/backoff.py"]]


def test_a_package_counts_all_of_its_files(tmp_path: Path) -> None:
    _module(tmp_path, "backend/a.py", 60)
    _module(tmp_path, "backend/valkey/b.lua", 40)
    _module(tmp_path, "backoff.py", 1000)
    git = _Git("10\t0\tsrc/swarmq/backend/a.py\n0\t10\tsrc/swarmq/backend/valkey/b.lua\n")

    share = sat.aenderungsanteil("backend", "abc123", wurzel=tmp_path, git=git)

    assert share == pytest.approx(0.20)
    assert git.calls[0][-1] == "src/swarmq/backend"


def _katalog(methoden: tuple[str, ...] = ("M-F-03",), module: tuple[str, ...] = ("backend",)) -> object:
    return sat.Katalog(methoden=methoden, module=module, entfallen=())


def _never_changed(modul: str, commit: str) -> float:
    return 0.0


def test_a_renamed_module_is_open() -> None:
    old = [sat.Lauf(**{**vars(_lauf()), "modul": "alt"}) for _ in range(2)]

    staende = sat.staende(_katalog(module=("neu",)), old, _never_changed)

    assert staende == {("M-F-03", "neu"): sat.Stand.OFFEN}


def test_a_pillar_is_saturated_only_when_all_its_combinations_are() -> None:
    staende = {
        ("M-F-01", "backend"): sat.Stand.GESAETTIGT,
        ("M-F-02", "backend"): sat.Stand.GESAETTIGT,
        ("M-Z-01", "backend"): sat.Stand.GESAETTIGT,
        ("M-Z-02", "backend"): sat.Stand.EINMAL_LEER,
        ("M-A-01", "backend"): sat.Stand.OFFEN,
    }

    saeulen = sat.saeulen(staende)

    assert list(saeulen) == ["Fehler", "Zuverlässigkeit", "Angriffssicherheit", "Performance"]
    assert saeulen["Fehler"].gesaettigt
    assert not saeulen["Zuverlässigkeit"].gesaettigt
    assert saeulen["Zuverlässigkeit"].zaehlung == {sat.Stand.GESAETTIGT: 1, sat.Stand.EINMAL_LEER: 1}
    assert not saeulen["Angriffssicherheit"].gesaettigt
    assert not saeulen["Performance"].gesaettigt


_KATALOG_MD = """# Methodenkatalog

## Fehler

| ID | Methode | Prüfumfang |
|---|---|---|
| M-F-01 | Lese-Methode | gelesene Dateien |
| M-F-02 | Zustandsmodelle | Beispiele |

## Performance

| ID | Methode | Prüfumfang |
|---|---|---|
| M-P-01 | Profil an einem Signal | Messpaare |

## Module

- `backend`: Valkey-Backend
- `backoff`

## Entfallene Module

- `alt`: umbenannt in `backoff`
"""


def test_the_catalog_lists_methods_modules_and_dropped_modules(tmp_path: Path) -> None:
    path = tmp_path / "methodenkatalog.md"
    path.write_text(_KATALOG_MD, encoding="utf-8")

    katalog = sat.lade_katalog(path)

    assert katalog == sat.Katalog(
        methoden=("M-F-01", "M-F-02", "M-P-01"), module=("backend", "backoff"), entfallen=("alt",)
    )


_BUCH_TOML = """
[[lauf]]
id = "L-001"
datum = 2026-10-17
commit = "b424584"
methode = "M-F-03"
modul = "backend"
umfang = 1113
einheit = "Mutanten"
befunde = [{ id = "F4-11", einstufung = "FEHLER" }]

[[lauf]]
id = "L-002"
datum = 2026-10-17
commit = "b424584"
methode = "M-F-05"
modul = "headers"
umfang = 500
einheit = "Beispiele"
befunde = []
"""

_SIGNAL = """
[[signal]]
id = "S-001"
quelle = "ruff S311"
saeule = "Angriffssicherheit"
modul = "backoff"
fundstelle = "src/swarmq/backoff.py"
beschreibung = "random.uniform für Jitter"
dringlichkeit = "normal"
status = "offen"
"""


def test_the_book_holds_runs_and_signals(tmp_path: Path) -> None:
    path = tmp_path / "saettigungsbuch.toml"
    path.write_text(_BUCH_TOML + _SIGNAL, encoding="utf-8")

    buch = sat.lade_buch(path)

    assert buch.laeufe[0] == sat.Lauf(
        id="L-001",
        datum=dt.date(2026, 10, 17),
        commit="b424584",
        methode="M-F-03",
        modul="backend",
        umfang=1113,
        einheit="Mutanten",
        befunde=(sat.Befund(id="F4-11", einstufung="FEHLER"),),
    )
    assert buch.laeufe[1].befunde == ()
    assert buch.signale == (
        sat.Signal(
            id="S-001",
            quelle="ruff S311",
            saeule="Angriffssicherheit",
            modul="backoff",
            dringlichkeit="normal",
            status="offen",
        ),
    )


_KATALOG_VIER = """# Methodenkatalog

| ID | Methode |
|---|---|
| M-F-01 | Lese-Methode |
| M-Z-01 | Lese-Methode |
| M-A-01 | Erreichbarkeit |
| M-P-01 | Profil |

## Module

- `backend`
"""


def _repo(root: Path, buch: str) -> Path:
    befunde = root / "docs" / "befunde"
    befunde.mkdir(parents=True)
    (befunde / "methodenkatalog.md").write_text(_KATALOG_VIER, encoding="utf-8")
    (befunde / "saettigungsbuch.toml").write_text(buch, encoding="utf-8")
    _module(root, "backend/a.py", 100)
    return root


_ZWEI_LEERE = "".join(
    f'[[lauf]]\nid = "L-00{n}"\ndatum = 2026-10-1{n}\ncommit = "c{n}"\nmethode = "{methode}"\n'
    f'modul = "backend"\numfang = 10\neinheit = "Dateien"\nbefunde = []\n\n'
    for methode in ("M-F-01", "M-Z-01")
    for n in (1, 2)
)


def test_main_reports_every_pillar_and_its_open_signals(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    buch = _ZWEI_LEERE + _SIGNAL.replace("backoff", "backend")
    git = _Git("1\t0\tsrc/swarmq/backend/a.py\n")

    assert sat.main(wurzel=_repo(tmp_path, buch), git=git) == 0

    out = capsys.readouterr().out
    assert "Fehler: gesättigt" in out
    assert "Zuverlässigkeit: gesättigt" in out
    assert "Angriffssicherheit: nicht gesättigt (1 offen von 1), offene Signale: 1" in out
    assert "Performance: nicht gesättigt (1 offen von 1), offene Signale: 0" in out
    assert git.calls[0][2] == "c2..HEAD"


def test_main_says_when_every_pillar_is_saturated(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    buch = "".join(
        _ZWEI_LEERE.replace("M-F-01", methode).replace("M-Z-01", methode)
        for methode in ("M-F-01", "M-Z-01", "M-A-01", "M-P-01")
    )

    assert sat.main(wurzel=_repo(tmp_path, buch), git=_Git("")) == 0

    assert "Alle Säulen gesättigt" in capsys.readouterr().out


def test_main_names_every_combination_with_runs_and_urgent_open_signals(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    one_empty = "[[lauf]]" + _ZWEI_LEERE.split("[[lauf]]")[1]
    urgent = _SIGNAL.replace("backoff", "backend").replace('"normal"', '"dringend"')

    sat.main(wurzel=_repo(tmp_path, one_empty + urgent), git=_Git(""))

    out = capsys.readouterr().out.splitlines()
    assert "  einmal leer: M-F-01 auf backend" in out
    assert "  dringend: S-001 (ruff S311, backend)" in out


def _git(root: Path, *args: str) -> str:
    cmd = ["git", "-c", "user.name=t", "-c", "user.email=t@t", *args]
    return subprocess.run(cmd, cwd=root, check=True, capture_output=True, text=True).stdout.strip()


def test_main_asks_the_real_git_without_an_injected_call(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _git(tmp_path, "init", "-q")
    root = _repo(tmp_path, "")
    _git(root, "add", ".")
    _git(root, "commit", "-qm", "lauf")
    run_commit = _git(root, "rev-parse", "HEAD")
    book = _ZWEI_LEERE.replace('"c1"', f'"{run_commit}"').replace('"c2"', f'"{run_commit}"')
    (root / "docs" / "befunde" / "saettigungsbuch.toml").write_text(book, encoding="utf-8")
    _module(root, "backend/a.py", 130)
    _git(root, "commit", "-qam", "umbau")

    sat.main(wurzel=root)

    out = capsys.readouterr().out
    assert "Fehler: nicht gesättigt (1 offen von 1)" in out


def test_a_new_method_in_the_catalog_opens_its_combinations() -> None:
    saturated = [_lauf(), _lauf()]

    staende = sat.staende(_katalog(methoden=("M-F-03", "M-F-07")), saturated, _never_changed)

    assert staende == {("M-F-03", "backend"): sat.Stand.GESAETTIGT, ("M-F-07", "backend"): sat.Stand.OFFEN}
