"""Prüffunktionen für das Entscheidungsregister (Guard in test_entscheidungsregister.py)."""

from __future__ import annotations

import ast
import re
from pathlib import Path
from typing import Any, cast

import yaml

_ID = re.compile(r"E-\d{3}")
_REQUIRED = ("title", "applies_when", "id", "datum", "status", "quelle")
_STATUS = ("gilt", "überholt")
_TEXT_LISTS = ("applies_when", "quelle", "tests")


def _frontmatter(path: Path) -> dict[str, Any] | None:
    text = path.read_text(encoding="utf-8")
    if not text.startswith("---\n"):
        return None
    head, sep, _ = text[4:].partition("\n---")
    if not sep:
        return None
    data: object = yaml.safe_load(head)
    return cast("dict[str, Any]", data) if isinstance(data, dict) else None


def load_rules(pack: Path) -> dict[str, dict[str, Any]]:
    """Regeldateien des Packs als ``{dateiname: frontmatter}`` (README und Unterordner ausgenommen)."""
    return {
        path.name: _frontmatter(path) or {} for path in sorted(pack.glob("*.md")) if path.name.lower() != "readme.md"
    }


def _check_fields(rules: dict[str, dict[str, Any]]) -> list[str]:
    problems: list[str] = []
    seen: dict[str, str] = {}
    for name, front in rules.items():
        problems.extend(f"{name}: Pflichtfeld {key} fehlt" for key in _REQUIRED if not front.get(key))
        rule_id = str(front.get("id", ""))
        if rule_id and not _ID.fullmatch(rule_id):
            problems.append(f"{name}: ID {rule_id} hat nicht das Format E-nnn")
        if rule_id in seen:
            problems.append(f"{name}: ID {rule_id} doppelt (auch in {seen[rule_id]})")
        seen.setdefault(rule_id, name)
        problems.extend(_check_text_entries(name, front))
    return problems


def _check_text_entries(name: str, front: dict[str, Any]) -> list[str]:
    """Listeneinträge müssen Text sein; ``a: b`` liest YAML als Mapping (dann in Anführungszeichen setzen)."""
    problems: list[str] = []
    for key in _TEXT_LISTS:
        entries = front.get(key)
        if not isinstance(entries, list):
            continue
        problems.extend(
            f"{name}: Eintrag unter {key} ist kein Text (Doppelpunkt mit Leerzeichen? In Anführungszeichen setzen): "
            f"{entry!r}"
            for entry in cast("list[object]", entries)
            if not isinstance(entry, str)
        )
    return problems


def _check_status(rules: dict[str, dict[str, Any]]) -> list[str]:
    problems: list[str] = []
    ids = {str(front.get("id")) for front in rules.values()}
    for name, front in rules.items():
        status = front.get("status")
        if status is not None and status not in _STATUS:
            problems.append(f"{name}: status {status!r} unbekannt (erlaubt: {', '.join(_STATUS)})")
        if status == "überholt":
            successor = front.get("ersetzt_durch")
            if not successor:
                problems.append(f"{name}: status überholt verlangt ersetzt_durch")
            elif str(successor) not in ids:
                problems.append(f"{name}: ersetzt_durch {successor} gibt es nicht")
    return problems


def _entscheidung_ids(nodes: list[ast.AST]) -> set[str]:
    """IDs aller ``*.entscheidung("E-nnn")``-Aufrufe in den Knoten."""
    ids: set[str] = set()
    for node in nodes:
        for call in ast.walk(node):
            if (
                isinstance(call, ast.Call)
                and isinstance(call.func, ast.Attribute)
                and call.func.attr == "entscheidung"
                and call.args
                and isinstance(call.args[0], ast.Constant)
            ):
                ids.add(str(call.args[0].value))
    return ids


def _pytestmark(scope: list[ast.stmt]) -> list[ast.AST]:
    return [
        n
        for n in scope
        if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "pytestmark" for t in n.targets)
    ]


def _test_markers(repo_root: Path, node_id: str) -> set[str] | None:
    """Entscheidungs-IDs, die auf ``pfad::[Klasse::]funktion`` wirken; ``None``, wenn es den Test nicht gibt.

    Wirksam sind Dekoratoren der Funktion und der umgebenden Klassen sowie
    ``pytestmark`` auf Klassen- und Modulebene.
    """
    rel, *names = node_id.split("::")
    path = repo_root / rel
    if not names or not path.is_file():
        return None
    scope: list[ast.stmt] = ast.parse(path.read_text(encoding="utf-8")).body
    sources: list[ast.AST] = _pytestmark(scope)
    for name in names[:-1]:
        cls = next((n for n in scope if isinstance(n, ast.ClassDef) and n.name == name), None)
        if cls is None:
            return None
        sources += [*cls.decorator_list, *_pytestmark(cls.body)]
        scope = cls.body
    func = next(
        (n for n in scope if isinstance(n, ast.FunctionDef | ast.AsyncFunctionDef) and n.name == names[-1]),
        None,
    )
    if func is None:
        return None
    return _entscheidung_ids([*sources, *func.decorator_list])


def _check_tests(rules: dict[str, dict[str, Any]], repo_root: Path) -> list[str]:
    problems: list[str] = []
    for name, front in rules.items():
        rule_id = str(front.get("id"))
        if not front.get("tests") and not front.get("ohne_test"):
            problems.append(f"{name}: weder tests noch ohne_test (Begründung, warum kein Test)")
        node_ids: list[object] = list(front.get("tests") or [])
        for node_id in node_ids:
            markers = _test_markers(repo_root, str(node_id))
            if markers is None:
                problems.append(f"{name}: Test {node_id} nicht gefunden")
            elif rule_id not in markers:
                problems.append(f'{name}: Test {node_id} trägt nicht @pytest.mark.entscheidung("{rule_id}")')
    return problems


def _check_unknown_markers(rules: dict[str, dict[str, Any]], tests_root: Path, repo_root: Path) -> list[str]:
    ids = {str(front.get("id")) for front in rules.values()}
    problems: list[str] = []
    for path in sorted(tests_root.rglob("test_*.py")):
        text = path.read_text(encoding="utf-8")
        if "entscheidung(" not in text:
            continue
        rel = path.relative_to(repo_root)
        problems.extend(
            f"{rel}: Marker entscheidung({used!r}) ohne Eintrag im Register"
            for used in sorted(_entscheidung_ids([ast.parse(text)]) - ids)
        )
    return problems


def check_register(pack: Path, tests_root: Path, repo_root: Path) -> list[str]:
    """Liefert die Konsistenzprobleme von Register und Tests als Klartext."""
    rules = load_rules(pack)
    return (
        _check_fields(rules)
        + _check_status(rules)
        + _check_tests(rules, repo_root)
        + _check_unknown_markers(rules, tests_root, repo_root)
    )
