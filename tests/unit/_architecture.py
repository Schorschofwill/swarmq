"""Reine Analysefunktionen für die Struktur-Guards (KTD7, Plan U2).

Alle Funktionen arbeiten auf Pfaden und liefern Daten zurück — keine
Assertions, kein pytest. So können die Guards sowohl gegen das echte
Repository als auch gegen synthetische Modulbäume unter ``tmp_path``
laufen (die Selbsttests der Guards).
"""

from __future__ import annotations

import ast
import re
import tomllib
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path

# ---------------------------------------------------------------------------
# Modulbaum und Import-Graph
# ---------------------------------------------------------------------------


def iter_modules(src_root: Path, package: str) -> dict[str, Path]:
    """Alle Module unter ``src_root/package`` als ``{dotted_name: path}``.

    ``__init__.py`` wird auf den Paketnamen abgebildet.
    """
    pkg_dir = src_root / package
    modules: dict[str, Path] = {}
    for path in sorted(pkg_dir.rglob("*.py")):
        rel = path.relative_to(src_root).with_suffix("")
        parts = list(rel.parts)
        if parts[-1] == "__init__":
            parts = parts[:-1]
        modules[".".join(parts)] = path
    return modules


def module_line_counts(src_root: Path, package: str) -> dict[str, int]:
    return {
        name: sum(1 for _ in path.open(encoding="utf-8"))
        for name, path in iter_modules(src_root, package).items()
    }


_DYNAMIC_IMPORT_FUNCS = frozenset({"import_module", "__import__"})


def _dynamic_import_literals(tree: ast.AST) -> set[str]:
    """String-Literale in ``import_module("…")`` / ``__import__("…")``."""
    found: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not node.args:
            continue
        func = node.func
        name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", None)
        if name not in _DYNAMIC_IMPORT_FUNCS:
            continue
        first = node.args[0]
        if isinstance(first, ast.Constant) and isinstance(first.value, str):
            found.add(first.value)
    return found


def _resolve_relative(module: str, is_package: bool, level: int, target: str | None) -> str:
    base_parts = module.split(".")
    if not is_package:
        base_parts = base_parts[:-1]
    if level > 1:
        base_parts = base_parts[: len(base_parts) - (level - 1)]
    base = ".".join(base_parts)
    if target:
        return f"{base}.{target}" if base else target
    return base


def build_import_graph(src_root: Path, package: str) -> dict[str, set[str]]:
    """Import-Graph über ALLE Import-Anweisungen (auch verschachtelte,
    ``TYPE_CHECKING``-geschützte und ``import_module``-Literale).

    Kanten zu Modulen des Pakets werden auf den existierenden Modulnamen
    aufgelöst (``from a.b import c`` → ``a.b.c`` falls Modul, sonst ``a.b``).
    Kanten zu Fremdpaketen behalten den vollen importierten Namen.
    """
    modules = iter_modules(src_root, package)
    graph: dict[str, set[str]] = {name: set() for name in modules}
    for name, path in modules.items():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        is_package = path.name == "__init__.py"
        targets: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                targets.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                base = (
                    _resolve_relative(name, is_package, node.level, node.module)
                    if node.level
                    else (node.module or "")
                )
                for alias in node.names:
                    candidate = f"{base}.{alias.name}" if base else alias.name
                    targets.add(candidate if candidate in modules else base)
        targets.update(_dynamic_import_literals(tree))
        for target in targets:
            if not target or target == name:
                continue
            graph[name].add(target)
    return graph


def internal_edges(graph: Mapping[str, set[str]], package: str) -> dict[str, set[str]]:
    """Nur Kanten innerhalb des Pakets (Ziel ist Modul des Pakets)."""
    return {
        src: {dst for dst in dsts if dst in graph}
        for src, dsts in graph.items()
        if src == package or src.startswith(package + ".")
    }


def _matches(name: str, pattern: str, root: str) -> bool:
    if pattern == root:
        return name == root
    return name == pattern or name.startswith(pattern + ".")


def find_import_cycles(
    graph: Mapping[str, set[str]],
    package: str,
    allowlist: Iterable[tuple[str, str]] = (),
) -> list[list[str]]:
    """Stark zusammenhängende Komponenten (Größe > 1) nach Entfernen der
    Allowlist-Kanten. Allowlist-Einträge sind ``(quelle, ziel)`` auf
    Paketebene: ``swarmq.cli`` matcht auch ``swarmq.cli.parser``; der
    Wurzelname matcht nur exakt.
    """
    allow = list(allowlist)
    edges = internal_edges(graph, package)
    pruned: dict[str, set[str]] = {}
    for src, dsts in edges.items():
        pruned[src] = {
            dst
            for dst in dsts
            if not any(
                _matches(src, a_src, package) and _matches(dst, a_dst, package)
                for a_src, a_dst in allow
            )
        }
    # Tarjan
    index: dict[str, int] = {}
    low: dict[str, int] = {}
    on_stack: set[str] = set()
    stack: list[str] = []
    sccs: list[list[str]] = []
    counter = 0

    def strongconnect(v: str) -> None:
        nonlocal counter
        index[v] = low[v] = counter
        counter += 1
        stack.append(v)
        on_stack.add(v)
        for w in pruned.get(v, ()):
            if w not in index:
                strongconnect(w)
                low[v] = min(low[v], low[w])
            elif w in on_stack:
                low[v] = min(low[v], index[w])
        if low[v] == index[v]:
            comp: list[str] = []
            while True:
                w = stack.pop()
                on_stack.discard(w)
                comp.append(w)
                if w == v:
                    break
            if len(comp) > 1:
                sccs.append(sorted(comp))

    for v in sorted(pruned):
        if v not in index:
            strongconnect(v)
    return sorted(sccs)


def transitive_hull(
    graph: Mapping[str, set[str]],
    roots: Iterable[str],
    *,
    skip: Iterable[str] = (),
) -> set[str]:
    """Alle von ``roots`` erreichbaren Namen (ohne die Wurzeln selbst,
    sofern nicht erneut erreicht). Module in ``skip`` werden weder
    betreten noch gemeldet (Alias-Module, KTD7)."""
    skip_set = set(skip)
    seen: set[str] = set()
    todo = [r for r in roots if r not in skip_set]
    while todo:
        node = todo.pop()
        for dst in graph.get(node, ()):
            if dst in skip_set or dst in seen:
                continue
            seen.add(dst)
            if dst in graph:
                todo.append(dst)
    return seen


def modules_of(graph: Mapping[str, set[str]], pkg: str) -> list[str]:
    return sorted(m for m in graph if m == pkg or m.startswith(pkg + "."))


def purity_violations(
    graph: Mapping[str, set[str]],
    core_pkg: str,
    forbidden_prefixes: Iterable[str],
    *,
    alias_modules: Iterable[str] = (),
) -> set[str]:
    """Verbotene Namen in der transitiven Import-Hülle eines Kernpakets."""
    roots = modules_of(graph, core_pkg)
    hull = transitive_hull(graph, roots, skip=alias_modules)
    forbidden = list(forbidden_prefixes)
    return {
        name
        for name in hull
        if any(name == f or name.startswith(f + ".") for f in forbidden)
    }


# ---------------------------------------------------------------------------
# mypy-Overrides
# ---------------------------------------------------------------------------


def mypy_override_modules(pyproject: Path) -> list[str]:
    data = tomllib.loads(pyproject.read_text(encoding="utf-8"))
    entries: list[str] = []
    for block in data.get("tool", {}).get("mypy", {}).get("overrides", []):
        mods = block.get("module", [])
        entries.extend([mods] if isinstance(mods, str) else mods)
    return entries


def unmatched_override_entries(entries: Iterable[str], modules: Iterable[str], package: str) -> list[str]:
    """Override-Einträge des Pakets, die kein existierendes Modul treffen.
    ``x.*`` verlangt ein Paket/Modul ``x`` mit mindestens einem Submodul."""
    known = set(modules)
    bad: list[str] = []
    for entry in entries:
        if not (entry == package or entry.startswith(package + ".")):
            continue
        if entry.endswith(".*"):
            base = entry[:-2]
            if not any(m.startswith(base + ".") for m in known):
                bad.append(entry)
        elif entry not in known:
            bad.append(entry)
    return bad


# ---------------------------------------------------------------------------
# Patch-Pfade in Tests
# ---------------------------------------------------------------------------

_PATCH_RE = re.compile(
    r"""(?:\bpatch(?:\.object)?\s*\(|monkeypatch\.setattr\s*\()\s*["']([\w.]+)["']"""
)


def stale_patch_targets(
    tests_root: Path, split_modules: Mapping[str, Iterable[str]]
) -> list[tuple[Path, str]]:
    """Patch-Ziele der Form ``<fassade>.<attr>``, deren Modul bereits aufgeteilt ist.

    ``split_modules`` bildet jede Fassade auf ihre Submodule ab. Ein Ziel
    ist veraltet, wenn es unterhalb der Fassade liegt, aber nicht in einem
    ihrer Submodule (``swarmq.worker.pipeline.logger`` ist erlaubt,
    ``swarmq.worker.logger`` nicht).
    """
    hits: list[tuple[Path, str]] = []
    for path in sorted(tests_root.rglob("*.py")):
        text = path.read_text(encoding="utf-8")
        for match in _PATCH_RE.finditer(text):
            target = match.group(1)
            for facade, submodules in split_modules.items():
                if not target.startswith(facade + "."):
                    continue
                first = target[len(facade) + 1 :].split(".", 1)[0]
                # Erlaubt: Submodule und die von der Fassade re-exportierten
                # Klassen (CamelCase). Konstanten/Funktionen/Module-Aliase
                # (``logger``, ``asyncio``, ``_POOL_…``) sind veraltet.
                is_class = first[:1].isupper() and not first.isupper()
                if first not in set(submodules) and not is_class:
                    hits.append((path, target))
                break
    return hits


def object_patch_targets(
    tests_root: Path, split_modules: Mapping[str, Iterable[str]]
) -> list[tuple[Path, str]]:
    """Objekt-Patches auf Fassaden-Module (U6-Guard-Lücke, Follow-Up 0.2.x).

    ``monkeypatch.setattr(modulobjekt, "name", …)`` und
    ``patch.object(modulobjekt, "name")`` laufen am String-Guard vorbei.
    Dieser Scan sammelt pro Testdatei die Import-Aliase, die exakt auf eine
    aufgeteilte Fassade zeigen (``from swarmq import client as X`` /
    ``import swarmq.client as X`` / ``from swarmq.worker import gates``
    zeigt auf ein Submodul und ist erlaubt), und meldet setattr/patch.object
    mit einem Nicht-CamelCase-Attribut auf so einem Fassaden-Alias.
    """
    facades = set(split_modules)
    hits: list[tuple[Path, str]] = []
    for path in sorted(tests_root.rglob("*.py")):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError:
            continue
        aliases: dict[str, str] = {}
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for a in node.names:
                    if a.name in facades:
                        aliases[(a.asname or a.name).split(".")[0] if a.asname else a.name.split(".")[0]] = a.name
                        if a.asname:
                            aliases[a.asname] = a.name
            elif isinstance(node, ast.ImportFrom) and node.module:
                for a in node.names:
                    full = f"{node.module}.{a.name}"
                    if full in facades:
                        aliases[a.asname or a.name] = full
        if not aliases:
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            is_setattr = (
                isinstance(func, ast.Attribute)
                and func.attr == "setattr"
            )
            is_patch_object = (
                isinstance(func, ast.Attribute)
                and func.attr == "object"
                and isinstance(func.value, ast.Attribute)
                and func.value.attr == "patch"
            ) or (
                isinstance(func, ast.Attribute)
                and func.attr == "object"
                and isinstance(func.value, ast.Name)
                and func.value.id == "patch"
            )
            if not (is_setattr or is_patch_object) or len(node.args) < 2:
                continue
            target, attr = node.args[0], node.args[1]
            if not (isinstance(target, ast.Name) and target.id in aliases):
                continue
            if not (isinstance(attr, ast.Constant) and isinstance(attr.value, str)):
                continue
            name = attr.value
            is_class = name[:1].isupper() and not name.isupper()
            if not is_class:
                hits.append((path, f"{aliases[target.id]}.{name}"))
    return hits


# ---------------------------------------------------------------------------
# Contract-Bindings
# ---------------------------------------------------------------------------

_SKIP_MARKS = frozenset({"skip", "skipif", "xfail"})


@dataclass
class Binding:
    path: Path
    name: str
    base: str  # z. B. "tests.contracts.coordination.LockStoreContract"
    overridden_tests: list[str] = field(default_factory=list)
    skip_marks: list[str] = field(default_factory=list)
    case_count: int | None = None


def _decorator_name(dec: ast.expr) -> str:
    node = dec.func if isinstance(dec, ast.Call) else dec
    parts: list[str] = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
    return ".".join(reversed(parts))


def _parametrize_multiplier(decorators: list[ast.expr]) -> int:
    mult = 1
    for dec in decorators:
        if _decorator_name(dec).endswith("parametrize") and isinstance(dec, ast.Call) and len(dec.args) >= 2:
            values = dec.args[1]
            if isinstance(values, (ast.List, ast.Tuple)):
                mult *= max(len(values.elts), 1)
    return mult


def _test_method_count(tree: ast.Module, class_name: str) -> int:
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            own = sum(
                1
                for item in node.body
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef))
                and item.name.startswith("test_")
            )
            inherited = sum(
                _test_method_count(tree, base.id) for base in node.bases if isinstance(base, ast.Name)
            )
            return (own + inherited) * _parametrize_multiplier(node.decorator_list)
    return 0


def collect_contract_bindings(tests_root: Path, contracts_pkg: str = "tests.contracts") -> list[Binding]:
    """Klassen in ``tests/`` (außer dem Contracts-Paket), die von einer
    Contract-Basisklasse ableiten."""
    contracts_dir = tests_root.joinpath(*contracts_pkg.split(".")[1:])
    bindings: list[Binding] = []
    for path in sorted(tests_root.rglob("test_*.py")):
        if contracts_dir in path.parents:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        aliases: dict[str, str] = {}  # lokaler Name → voller gepunkteter Name
        for node in tree.body:
            if isinstance(node, ast.ImportFrom) and node.module and (
                node.module == contracts_pkg or node.module.startswith(contracts_pkg + ".")
            ):
                for alias in node.names:
                    aliases[alias.asname or alias.name] = f"{node.module}.{alias.name}"
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.startswith(contracts_pkg):
                        aliases[alias.asname or alias.name] = alias.name
        if not aliases:
            continue
        for node in tree.body:
            if not isinstance(node, ast.ClassDef):
                continue
            for base in node.bases:
                dotted = ast.unparse(base)
                head, _, tail = dotted.partition(".")
                if head not in aliases:
                    continue
                full = aliases[head] + (f".{tail}" if tail else "")
                b = Binding(path=path, name=node.name, base=full)
                for item in node.body:
                    if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)) and item.name.startswith("test_"):
                        b.overridden_tests.append(item.name)
                    elif isinstance(item, ast.Assign):
                        for t in item.targets:
                            if isinstance(t, ast.Name) and t.id.startswith("test_"):
                                b.overridden_tests.append(t.id)
                for dec in node.decorator_list:
                    dname = _decorator_name(dec)
                    if dname.rsplit(".", 1)[-1] in _SKIP_MARKS:
                        b.skip_marks.append(dname)
                b.case_count = _binding_case_count(tests_root, full, node)
                bindings.append(b)
    return bindings


def _binding_case_count(tests_root: Path, base_full: str, binding_node: ast.ClassDef) -> int | None:
    parts = base_full.split(".")
    module_path = tests_root.parent.joinpath(*parts[:-1]).with_suffix(".py")
    if not module_path.exists():
        return None
    tree = ast.parse(module_path.read_text(encoding="utf-8"), filename=str(module_path))
    base_count = _test_method_count(tree, parts[-1])
    return base_count * _parametrize_multiplier(binding_node.decorator_list)


def binding_case_count_mismatches(bindings: Iterable[Binding]) -> dict[str, set[int]]:
    counts: dict[str, set[int]] = {}
    for b in bindings:
        if b.case_count is not None:
            counts.setdefault(b.base, set()).add(b.case_count)
    return {base: c for base, c in counts.items() if len(c) > 1}
