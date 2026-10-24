"""Guard für Methodenkatalog und Sättigungsbuch (Plan 2026-10-24-1118, U5, KTD9).

``docs/befunde/saettigungsbuch.toml`` hält Läufe und Signale fest,
``docs/befunde/methodenkatalog.md`` die Methoden und Module. Der Guard hält
Buch, Katalog, Register und ``src/swarmq`` konsistent, damit kein Lauf einen
erfundenen Befund oder eine unbekannte Methode meldet.

Testliste:
# [x] ein gültiges Buch hat keine Probleme
# [x] eine unbekannte Methoden-ID ist rot
# [x] eine Befund-ID, die in keinem Register steht, ist rot
# [x] eine unbekannte Einstufung ist rot
# [x] ein Lauf ohne Pflichtfeld oder mit negativem Prüfumfang ist rot
# [x] Lauf- und Signal-IDs haben das Format L-nnn bzw. S-nnn und sind eindeutig
# [x] ein Lauf oder Signal mit unbekanntem Modul ist rot, ein entfallenes Modul nicht
# [x] ein Signal ohne Status ist rot
# [x] Signal: Säule, Dringlichkeit und Status aus festen Werten
# [x] „untersucht“ verlangt eine bestehende Lauf-ID, „verworfen“ einen Grund
# [x] ein Modul unter src/swarmq ohne Eintrag im Katalog ist rot, ebenso ein Katalogmodul ohne Code
# [x] das echte Buch ist konsistent
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.unit._saettigungsbuch import check_buch, saettigung

pytestmark = pytest.mark.timeout(5)

_KATALOG = """# Methodenkatalog

## Fehler

| ID | Methode | Prüfumfang |
|---|---|---|
| M-F-03 | Mutation | Mutanten |

## Angriffssicherheit

| ID | Methode | Prüfumfang |
|---|---|---|
| M-A-01 | Erreichbarkeit | Signale |

## Module

- `backend`
- `backoff`

## Entfallene Module

- `alt`
"""

_REGISTER = """# Befundregister

| ID | Befund | Verletzte Zusage + Quelle | Fix | Verdacht |
|---|---|---|---|---|
| F4-11 | Lock bleibt hängen | E-001 | behoben | R4M-D-01 |
| PF-5 (R4M-E-01b) | Frage | a | b | c |
"""

_BUCH = """
[[lauf]]
id = "L-001"
datum = 2026-10-17
commit = "b424584"
methode = "M-F-03"
modul = "backend"
umfang = 1113
einheit = "Mutanten"
befunde = [{ id = "F4-11", einstufung = "FEHLER" }, { id = "PF-5", einstufung = "LÜCKE" }]

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


def _repo(root: Path, buch: str = _BUCH, katalog: str = _KATALOG) -> tuple[Path, Path]:
    befunde = root / "docs" / "befunde"
    befunde.mkdir(parents=True)
    (befunde / "methodenkatalog.md").write_text(katalog, encoding="utf-8")
    (befunde / "saettigungsbuch.toml").write_text(buch, encoding="utf-8")
    (befunde / "runde-4.md").write_text(_REGISTER, encoding="utf-8")
    src = root / "src" / "swarmq"
    (src / "backend").mkdir(parents=True)
    (src / "backend" / "__init__.py").write_text("", encoding="utf-8")
    (src / "backoff.py").write_text("", encoding="utf-8")
    (src / "__init__.py").write_text("", encoding="utf-8")
    (src / "py.typed").write_text("", encoding="utf-8")
    return befunde, src


def test_a_valid_book_has_no_problems(tmp_path: Path) -> None:
    assert check_buch(*_repo(tmp_path)) == []


def test_an_unknown_method_id_is_reported(tmp_path: Path) -> None:
    problems = check_buch(*_repo(tmp_path, _BUCH.replace('"M-F-03"', '"M-F-99"')))

    assert any("L-001" in p and "M-F-99" in p for p in problems)


def test_a_finding_id_missing_from_every_register_is_reported(tmp_path: Path) -> None:
    problems = check_buch(*_repo(tmp_path, _BUCH.replace('"F4-11"', '"F4-99"')))

    assert problems
    assert all("L-001" in p and "F4-99" in p for p in problems)


def test_an_unknown_classification_is_reported(tmp_path: Path) -> None:
    problems = check_buch(*_repo(tmp_path, _BUCH.replace('"LÜCKE"', '"LUECKE"')))

    assert any("L-001" in p and "PF-5" in p and "LUECKE" in p for p in problems)


@pytest.mark.parametrize(
    ("old", "new", "expected"),
    [
        ('einheit = "Mutanten"\n', "", "einheit"),
        ('commit = "b424584"\n', "", "commit"),
        ("umfang = 1113", "umfang = -1", "umfang"),
        ("umfang = 1113", 'umfang = "viele"', "umfang"),
        ("datum = 2026-10-17", 'datum = "gestern"', "datum"),
    ],
)
def test_a_run_needs_every_field_and_a_countable_scope(tmp_path: Path, old: str, new: str, expected: str) -> None:
    problems = check_buch(*_repo(tmp_path, _BUCH.replace(old, new, 1)))

    assert any("L-001" in p and expected in p for p in problems), problems


def test_ids_have_their_format_and_are_unique(tmp_path: Path) -> None:
    lauf, signal = _BUCH.split("[[signal]]")
    buch = lauf + lauf + lauf.replace("L-001", "L-1") + "[[signal]]" + signal + "[[signal]]" + signal
    buch += "[[signal]]" + signal.replace("S-001", "S1")

    problems = check_buch(*_repo(tmp_path, buch))

    assert any("L-001" in p and "doppelt" in p for p in problems)
    assert any("L-1" in p and "Format" in p for p in problems)
    assert any("S-001" in p and "doppelt" in p for p in problems)
    assert any("S1" in p and "Format" in p for p in problems)


def test_runs_and_signals_name_a_catalogued_module(tmp_path: Path) -> None:
    buch = _BUCH.replace('modul = "backend"', 'modul = "backnd"').replace('modul = "backoff"', 'modul = "bakoff"')
    problems = check_buch(*_repo(tmp_path, buch))

    assert any("L-001" in p and "backnd" in p for p in problems)
    assert any("S-001" in p and "bakoff" in p for p in problems)


def test_a_dropped_module_stays_valid_for_old_runs(tmp_path: Path) -> None:
    assert check_buch(*_repo(tmp_path, _BUCH.replace('modul = "backend"', 'modul = "alt"'))) == []


def test_a_signal_without_status_is_reported(tmp_path: Path) -> None:
    problems = check_buch(*_repo(tmp_path, _BUCH.replace('status = "offen"\n', "")))

    assert any("S-001" in p and "status" in p for p in problems)


@pytest.mark.parametrize(
    ("old", "new", "expected"),
    [
        ('saeule = "Angriffssicherheit"', 'saeule = "Sicherheit"', "saeule"),
        ('dringlichkeit = "normal"', 'dringlichkeit = "hoch"', "dringlichkeit"),
        ('status = "offen"', 'status = "erledigt"', "status"),
    ],
)
def test_signal_fields_take_fixed_values(tmp_path: Path, old: str, new: str, expected: str) -> None:
    problems = check_buch(*_repo(tmp_path, _BUCH.replace(old, new)))

    assert any("S-001" in p and expected in p for p in problems), problems


def test_an_investigated_signal_names_its_run_and_a_dismissed_one_its_reason(tmp_path: Path) -> None:
    lauf, signal = _BUCH.split("[[signal]]")
    buch = lauf
    for nummer, status in (
        (1, 'status = "untersucht"\nlauf = "L-001"'),
        (2, 'status = "untersucht"\nlauf = "L-404"'),
        (3, 'status = "untersucht"'),
        (4, 'status = "verworfen"\ngrund = "kein erreichbarer Pfad (AE1)"'),
        (5, 'status = "verworfen"'),
    ):
        buch += "[[signal]]" + signal.replace("S-001", f"S-00{nummer}").replace('status = "offen"', status)

    problems = check_buch(*_repo(tmp_path, buch))

    assert not any(p.startswith(("S-001", "S-004")) for p in problems), problems
    assert any(p.startswith("S-002") and "L-404" in p for p in problems)
    assert any(p.startswith("S-003") and "lauf" in p for p in problems)
    assert any(p.startswith("S-005") and "grund" in p for p in problems)


def test_the_module_list_matches_src(tmp_path: Path) -> None:
    befunde, src = _repo(tmp_path)
    (src / "neu.py").write_text("", encoding="utf-8")
    (src / "lua").mkdir()
    (src / "__pycache__").mkdir()
    (src / "backoff.py").unlink()

    problems = check_buch(befunde, src)

    assert any("neu" in p and "fehlt im Katalog" in p for p in problems)
    assert any("lua" in p and "fehlt im Katalog" in p for p in problems)
    assert any("backoff" in p and "src/swarmq" in p for p in problems)
    assert not any("__pycache__" in p or "__init__" in p or "py.typed" in p for p in problems)


_REPO_ROOT = Path(__file__).resolve().parents[2]


def test_the_real_book_is_consistent() -> None:
    befunde = _REPO_ROOT / "docs" / "befunde"

    assert saettigung.lade_buch(befunde / "saettigungsbuch.toml").signale, "keine Signale im Buch"
    assert check_buch(befunde, _REPO_ROOT / "src" / "swarmq") == []
