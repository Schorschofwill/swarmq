"""Guard für das Entscheidungsregister (compound-packs/swarmq-entscheidungen/).

Jede Regeldatei hält eine bewusst getroffene Entscheidung fest. Tests, die das
entschiedene Verhalten festschreiben, tragen ``@pytest.mark.entscheidung("E-nnn")``.
Der Guard hält Register und Tests in beiden Richtungen konsistent, damit eine
Fehlersuche ein entschiedenes Verhalten nicht als Fehler meldet.

Testliste:
# [x] Regel ohne Pflichtfeld (title, applies_when, id, datum, status, quelle) wird gemeldet
# [x] ID-Format E-nnn und Eindeutigkeit
# [x] status nur "gilt" oder "überholt"; "überholt" verlangt ersetzt_durch auf eine bestehende ID
# [x] referenzierter Test fehlt (Datei oder Funktion) wird gemeldet
# [x] referenzierter Test ohne passenden entscheidung-Marker wird gemeldet
# [x] Marker in Tests mit unbekannter ID wird gemeldet
# [x] Regel ohne Test braucht eine Begründung (ohne_test)
# [x] README.md und Unterordner sind keine Regeln
# [x] das echte Register ist konsistent
"""

from __future__ import annotations

from pathlib import Path

from tests.unit._entscheidungen import check_register, load_rules

_REPO = Path(__file__).resolve().parents[2]
_PACK = _REPO / "compound-packs" / "swarmq-entscheidungen"


def _rule(pack: Path, name: str, front: str, body: str = "Entscheidung.\n") -> Path:
    pack.mkdir(parents=True, exist_ok=True)
    path = pack / name
    path.write_text(f"---\n{front.strip()}\n---\n\n{body}", encoding="utf-8")
    return path


_VALID = """
title: Drain-Abbruch geht in den Fehlerpfad
applies_when:
  - ein Task wird am Ende des Drains abgebrochen
tags: [shutdown]
id: E-001
datum: 2026-09-03
status: gilt
quelle:
  - docs/plans/plan.md (KTD2)
tests:
  - tests/unit/test_x.py::test_drain
"""

_TEST_WITH_MARKER = """
import pytest


@pytest.mark.entscheidung("E-001")
def test_drain() -> None:
    pass
"""


def _repo(tmp_path: Path, front: str = _VALID, test_src: str = _TEST_WITH_MARKER) -> tuple[Path, Path]:
    pack = tmp_path / "compound-packs" / "swarmq-entscheidungen"
    _rule(pack, "e001-drain.md", front)
    tests = tmp_path / "tests" / "unit"
    tests.mkdir(parents=True)
    (tests / "test_x.py").write_text(test_src, encoding="utf-8")
    return pack, tmp_path / "tests"


def test_valid_register_has_no_problems(tmp_path: Path) -> None:
    pack, tests = _repo(tmp_path)

    assert check_register(pack, tests, tmp_path) == []


def test_missing_applies_when_is_reported(tmp_path: Path) -> None:
    front = "\n".join(line for line in _VALID.splitlines() if "applies_when" not in line and "abgebrochen" not in line)
    pack, tests = _repo(tmp_path, front)

    problems = check_register(pack, tests, tmp_path)

    assert any("e001-drain.md" in p and "applies_when" in p for p in problems)


def test_readme_and_subdirectories_are_not_rules(tmp_path: Path) -> None:
    pack, tests = _repo(tmp_path)
    (pack / "README.md").write_text("# Register\n", encoding="utf-8")
    _rule(pack / "entwuerfe", "e099-entwurf.md", "title: Entwurf")

    assert check_register(pack, tests, tmp_path) == []


def test_malformed_and_duplicate_ids_are_reported(tmp_path: Path) -> None:
    pack, tests = _repo(tmp_path)
    _rule(pack, "e001-kopie.md", _VALID.replace("tests:\n  - tests/unit/test_x.py::test_drain", "ohne_test: Kopie"))
    _rule(
        pack,
        "e2-kurz.md",
        _VALID.replace("E-001", "E-2").replace("tests:\n  - tests/unit/test_x.py::test_drain", "ohne_test: kurz"),
    )

    problems = check_register(pack, tests, tmp_path)

    assert any("E-001" in p and "doppelt" in p for p in problems)
    assert any("e2-kurz.md" in p and "E-2" in p for p in problems)


def test_unknown_status_and_dangling_supersession_are_reported(tmp_path: Path) -> None:
    pack, tests = _repo(tmp_path)
    no_test = "ohne_test: Beispiel"
    tests_block = "tests:\n  - tests/unit/test_x.py::test_drain"
    _rule(
        pack,
        "e002-vielleicht.md",
        _VALID.replace("E-001", "E-002").replace("gilt", "vielleicht").replace(tests_block, no_test),
    )
    _rule(
        pack, "e003-alt.md", _VALID.replace("E-001", "E-003").replace("gilt", "überholt").replace(tests_block, no_test)
    )
    _rule(
        pack,
        "e004-alt.md",
        _VALID.replace("E-001", "E-004")
        .replace("gilt", "überholt\nersetzt_durch: E-999")
        .replace(tests_block, no_test),
    )

    problems = check_register(pack, tests, tmp_path)

    assert any("e002-vielleicht.md" in p and "status" in p for p in problems)
    assert any("e003-alt.md" in p and "ersetzt_durch" in p for p in problems)
    assert any("e004-alt.md" in p and "E-999" in p for p in problems)


def test_referenced_test_must_exist(tmp_path: Path) -> None:
    pack, tests = _repo(tmp_path, _VALID.replace("test_x.py::test_drain", "test_x.py::test_fehlt"))
    _rule(pack, "e002-datei.md", _VALID.replace("E-001", "E-002").replace("test_x.py", "test_weg.py"))

    problems = check_register(pack, tests, tmp_path)

    assert any("e001-drain.md" in p and "test_fehlt" in p and "nicht gefunden" in p for p in problems)
    assert any("e002-datei.md" in p and "test_weg.py" in p and "nicht gefunden" in p for p in problems)


def test_referenced_test_needs_the_matching_marker(tmp_path: Path) -> None:
    pack, tests = _repo(tmp_path, test_src=_TEST_WITH_MARKER.replace("E-001", "E-002"))

    problems = check_register(pack, tests, tmp_path)

    assert any("test_drain" in p and 'entscheidung("E-001")' in p for p in problems)


def test_marker_with_unknown_id_is_reported(tmp_path: Path) -> None:
    pack, tests = _repo(tmp_path)
    (tests / "unit" / "test_y.py").write_text(_TEST_WITH_MARKER.replace("E-001", "E-404"), encoding="utf-8")

    problems = check_register(pack, tests, tmp_path)

    assert any("test_y.py" in p and "E-404" in p for p in problems)


def test_rule_without_tests_needs_a_reason(tmp_path: Path) -> None:
    pack, tests = _repo(tmp_path)
    tests_block = "tests:\n  - tests/unit/test_x.py::test_drain"
    _rule(pack, "e002-ohne.md", _VALID.replace("E-001", "E-002").replace(tests_block, ""))
    _rule(pack, "e003-begruendet.md", _VALID.replace("E-001", "E-003").replace(tests_block, "ohne_test: nur Doku"))

    problems = check_register(pack, tests, tmp_path)

    assert any("e002-ohne.md" in p and "ohne_test" in p for p in problems)
    assert not any("e003-begruendet.md" in p for p in problems)


def test_real_register_is_consistent() -> None:
    assert load_rules(_PACK), f"keine Regeln in {_PACK}"
    assert check_register(_PACK, _REPO / "tests", _REPO) == []
