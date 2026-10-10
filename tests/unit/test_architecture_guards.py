"""Struktur-Guards der 0.2.0-Restrukturierung (KTD7, Plan U2).

Sechs Prüfungen gegen das echte Repository plus Selbsttests der
Analysefunktionen an synthetischen Modulbäumen unter ``tmp_path``.

WARTUNG DER LISTEN — die TDD-Disziplin für reine Verschiebungen (KTD14):

- ``_LOC_EXCEPTIONS``: schrumpft pro Extraktions-MR. Der rote Test einer
  Extraktion ist das Streichen ihres Eintrags. Ziel am Programmende: leer.
- ``_CYCLE_ALLOWLIST``: bekannte Lazy-Kanten mit auflösender Unit. Einträge
  werden gestrichen, sobald die Unit die Kante entfernt.
- ``_PURITY_ACTIVE``: Kernpakete, deren Import-Hülle bereits rein sein
  muss. ``contracts`` ab U3, ``scheduler`` ab U11, ``client``/``worker`` ab
  U15. ``_PHASE4_COMPLETE`` wird in U15 auf True gesetzt; danach ist ein
  existierendes Kernpaket ohne Eintrag ein Guard-Fehler.
- ``_ALIAS_MODULES``: Re-Export-Module (KTD10), die in der Hülle bis U15
  ignoriert werden. U15 leert die Liste.
- ``_SPLIT_MODULES``: Fassade → Submodule; wächst pro Extraktion. Ein
  ``patch("<fassade>.<attr>")`` in ``tests/`` ist dann ein Fehler.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from tests.unit import _architecture as arch

_REPO = Path(__file__).resolve().parents[2]
_SRC = _REPO / "src"
_TESTS = _REPO / "tests"
_PYPROJECT = _REPO / "pyproject.toml"
_PKG = "swarmq"

_MAX_MODULE_LINES = 800

# Modul → Unit, die es unter das Limit bringt. Seit U21 leer (KTD7).
_LOC_EXCEPTIONS: dict[str, str] = {}

# (Quelle, Ziel) auf Paketebene; Wurzel ``swarmq`` matcht nur exakt.
_CYCLE_ALLOWLIST: list[tuple[str, str]] = [
    ("swarmq.process_manager", "swarmq.cli"),          # entry → cli (lazy, KTD7)
    ("swarmq.cli", "swarmq.process_manager"),          # KTD7
    ("swarmq", "swarmq.app"),                          # PEP-562-Fassade ↔ app
    ("swarmq.app", "swarmq"),
]

_CORE_PACKAGES = ("swarmq.contracts", "swarmq.worker", "swarmq.client", "swarmq.scheduler")
_PURITY_ACTIVE: frozenset[str] = frozenset(
    {"swarmq.contracts", "swarmq.scheduler", "swarmq.client", "swarmq.worker"}
)  # seit U15 vollständig (KTD7)
_PHASE4_COMPLETE = False
_FORBIDDEN_IN_CORE = ("aio_pika", "valkey", "swarmq.backend.valkey", "swarmq.broker.rabbitmq")
_ALIAS_MODULES: frozenset[str] = frozenset()  # seit U15 leer (KTD10 abgeschlossen)

_SPLIT_MODULES: dict[str, list[str]] = {
    "swarmq.broker.rabbitmq": ["connection", "delivery", "admin", "consumer_watch", "topology"],  # U5; U6; 0.12 MR 0
    "swarmq.backend.valkey": ["connection", "results"],  # U17
    # ``decode`` seit U6 (Plan 2026-09-08-001): Header-Pruefung, Size-Cap und
    # Body-Dekodierung aus ``pipeline.py`` gezogen.
    "swarmq.worker": [
        "outcome", "pipeline", "gates", "execution", "retry", "results",
        "lifecycle", "decode",
        "shutdown", "finalize",  # Plan 0.12.0, MR 0
        "redelivery",  # Plan 0.12.0, MR 3 (BZ-29)
    ],  # U6-U9
    "swarmq.client": ["schedule", "results", "workflows", "schedule_resolve"],  # U10; Plan 0.12.0, MR 0
    "swarmq.scheduler": ["definition", "loop", "sync", "fire"],  # U11; fire: Plan 0.12.0, MR 0
    "swarmq.process_manager": [
        "config", "supervisor", "entry", "sampler", "check", "run",
        "setpoint_store", "setpoint",  # Kontrollkanal U2/U5
        "reload",  # Plan 2026-10-06-1811, U8
        "worker_target",  # Restpunkte 0.9.0: Kindziel aus ``supervisor``
        "restart_budget",  # Plan 0.12.0, MR 0: Lebenszeichen und Neustart-Budget
    ],  # U21
}


# ---------------------------------------------------------------------------
# 1–6: Guards gegen das echte Repository
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def graph() -> dict[str, set[str]]:
    return arch.build_import_graph(_SRC, _PKG)


def test_no_module_exceeds_line_limit_outside_exception_list() -> None:
    counts = arch.module_line_counts(_SRC, _PKG)
    too_long = {m: n for m, n in counts.items() if n > _MAX_MODULE_LINES}
    unexpected = {m: n for m, n in too_long.items() if m not in _LOC_EXCEPTIONS}
    assert not unexpected, f"Module über {_MAX_MODULE_LINES} Zeilen ohne Ausnahme: {unexpected}"
    stale = {m for m in _LOC_EXCEPTIONS if m not in too_long}
    assert not stale, f"Ausnahmeliste enthält Module unter dem Limit (Eintrag streichen): {stale}"


def test_no_import_cycles_outside_allowlist(graph: dict[str, set[str]]) -> None:
    cycles = arch.find_import_cycles(graph, _PKG, _CYCLE_ALLOWLIST)
    assert not cycles, f"Import-Zyklen: {cycles}"


def test_core_packages_import_no_implementations(graph: dict[str, set[str]]) -> None:
    existing = [p for p in _CORE_PACKAGES if arch.modules_of(graph, p)]
    if _PHASE4_COMPLETE:
        missing = [p for p in existing if p not in _PURITY_ACTIVE]
        assert not missing, f"Kernpakete ohne Aktivierungseintrag: {missing}"
    for pkg in _PURITY_ACTIVE:
        bad = arch.purity_violations(graph, pkg, _FORBIDDEN_IN_CORE, alias_modules=_ALIAS_MODULES)
        assert not bad, f"{pkg} zieht Implementierungen in seine Import-Hülle: {sorted(bad)}"


def test_mypy_override_entries_match_existing_modules(graph: dict[str, set[str]]) -> None:
    entries = arch.mypy_override_modules(_PYPROJECT)
    bad = arch.unmatched_override_entries(entries, graph, _PKG)
    assert not bad, f"mypy-Override-Einträge ohne Modul: {bad}"


def test_no_object_patches_on_split_facades() -> None:
    """U6-Guard-Lücke (Follow-Up 0.2.x): ``monkeypatch.setattr(fassade, …)``
    und ``patch.object(fassade, …)`` liefen am String-Guard vorbei."""
    hits = arch.object_patch_targets(_TESTS, _SPLIT_MODULES)
    assert not hits, (
        "Objekt-Patches auf aufgeteilte Fassaden — auf das Submodul zielen: "
        + ", ".join(f"{p.name}: {t}" for p, t in hits)
    )


def test_no_patch_paths_into_split_modules() -> None:
    hits = arch.stale_patch_targets(_TESTS, _SPLIT_MODULES)
    assert not hits, f"Patch-Pfade in aufgeteilte Fassaden: {hits}"


def test_contract_bindings_are_pure() -> None:
    bindings = arch.collect_contract_bindings(_TESTS)
    overriding = [(b.name, b.overridden_tests) for b in bindings if b.overridden_tests]
    assert not overriding, f"Bindings überschreiben test_*-Methoden: {overriding}"
    skipping = [(b.name, b.skip_marks) for b in bindings if b.skip_marks]
    assert not skipping, f"Bindings tragen skip/xfail: {skipping}"
    mismatches = arch.binding_case_count_mismatches(bindings)
    assert not mismatches, f"Bindings derselben Basisklasse mit abweichender Fallzahl: {mismatches}"


# Q4 (Plan 0.12.0): Valkey-Keys entstehen nur in ``swarmq.keyspace``.
_KEYSPACE_MODULE = "swarmq.keyspace"
_KEYSPACE_HELP_TEXTS: dict[str, list[str]] = {
    "swarmq.cli.parser": ["sq:control:setpoint:<group>"],
    "swarmq.cli.parser_ops": ["sq:sched:defs"],
}


def test_no_keyspace_literals_outside_keyspace_module() -> None:
    hits = arch.keyspace_literals(_SRC, _PKG, exempt_modules=[_KEYSPACE_MODULE], exempt_substrings=_KEYSPACE_HELP_TEXTS)
    assert not hits, f"sq:-Literale ausserhalb von {_KEYSPACE_MODULE}: {hits}"


# ---------------------------------------------------------------------------
# Selbsttests der Analysefunktionen an synthetischen Bäumen
# ---------------------------------------------------------------------------


def _write_tree(root: Path, files: dict[str, str]) -> None:
    for rel, content in files.items():
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(textwrap.dedent(content), encoding="utf-8")


class TestCycleGuard:
    def test_cycle_via_function_local_import_is_detected(self, tmp_path: Path) -> None:
        _write_tree(
            tmp_path,
            {
                "pkg/__init__.py": "",
                "pkg/a.py": "from pkg import b\n",
                "pkg/b.py": "def f():\n    from pkg import a\n    return a\n",
            },
        )
        graph = arch.build_import_graph(tmp_path, "pkg")
        assert arch.find_import_cycles(graph, "pkg") == [["pkg.a", "pkg.b"]]

    def test_allowlisted_edge_breaks_the_cycle(self, tmp_path: Path) -> None:
        _write_tree(
            tmp_path,
            {
                "pkg/__init__.py": "",
                "pkg/a.py": "from pkg import b\n",
                "pkg/b.py": "def f():\n    from pkg import a\n    return a\n",
            },
        )
        graph = arch.build_import_graph(tmp_path, "pkg")
        assert arch.find_import_cycles(graph, "pkg", [("pkg.b", "pkg.a")]) == []

    def test_allowlist_matches_on_package_level(self, tmp_path: Path) -> None:
        _write_tree(
            tmp_path,
            {
                "pkg/__init__.py": "",
                "pkg/x/__init__.py": "",
                "pkg/x/inner.py": "import pkg.y\n",
                "pkg/y.py": "import pkg.x.inner\n",
            },
        )
        graph = arch.build_import_graph(tmp_path, "pkg")
        assert arch.find_import_cycles(graph, "pkg") != []
        assert arch.find_import_cycles(graph, "pkg", [("pkg.x", "pkg.y")]) == []


class TestPurityGuard:
    _FORBIDDEN = ("valkey", "pkg.backend.valkey")

    def test_type_checking_import_of_implementation_fails(self, tmp_path: Path) -> None:
        _write_tree(
            tmp_path,
            {
                "pkg/__init__.py": "",
                "pkg/core/__init__.py": (
                    "from typing import TYPE_CHECKING\n"
                    "if TYPE_CHECKING:\n    from pkg.backend.valkey import Impl\n"
                ),
                "pkg/backend/__init__.py": "",
                "pkg/backend/valkey.py": "import valkey\nclass Impl: ...\n",
            },
        )
        graph = arch.build_import_graph(tmp_path, "pkg")
        assert arch.purity_violations(graph, "pkg.core", self._FORBIDDEN) == {"pkg.backend.valkey", "valkey"}

    def test_transitive_import_via_intermediate_module_fails(self, tmp_path: Path) -> None:
        _write_tree(
            tmp_path,
            {
                "pkg/__init__.py": "",
                "pkg/core/__init__.py": "from pkg import helper\n",
                "pkg/helper.py": "from pkg.backend.valkey import Impl\n",
                "pkg/backend/__init__.py": "",
                "pkg/backend/valkey.py": "class Impl: ...\n",
            },
        )
        graph = arch.build_import_graph(tmp_path, "pkg")
        assert arch.purity_violations(graph, "pkg.core", self._FORBIDDEN) == {"pkg.backend.valkey"}

    def test_import_module_literal_fails(self, tmp_path: Path) -> None:
        _write_tree(
            tmp_path,
            {
                "pkg/__init__.py": "",
                "pkg/core/__init__.py": (
                    "import importlib\n"
                    "def load():\n    return importlib.import_module('pkg.backend.valkey')\n"
                ),
                "pkg/backend/__init__.py": "",
                "pkg/backend/valkey.py": "",
            },
        )
        graph = arch.build_import_graph(tmp_path, "pkg")
        assert arch.purity_violations(graph, "pkg.core", self._FORBIDDEN) == {"pkg.backend.valkey"}

    def test_alias_module_in_hull_is_ignored(self, tmp_path: Path) -> None:
        _write_tree(
            tmp_path,
            {
                "pkg/__init__.py": "",
                "pkg/core/__init__.py": "from pkg import helper\n",
                "pkg/helper.py": "from pkg.backend.valkey import Impl\n",
                "pkg/backend/__init__.py": "",
                "pkg/backend/valkey.py": "class Impl: ...\n",
            },
        )
        graph = arch.build_import_graph(tmp_path, "pkg")
        assert arch.purity_violations(graph, "pkg.core", self._FORBIDDEN, alias_modules=["pkg.helper"]) == set()

    def test_existing_core_package_without_activation_is_found(self) -> None:
        graph = {"pkg.core": set(), "pkg.core.x": set(), "pkg.util": set()}
        active = frozenset({"pkg.util"})
        existing = [p for p in ("pkg.core", "pkg.util", "pkg.other") if arch.modules_of(graph, p)]
        assert existing == ["pkg.core", "pkg.util"]
        assert [p for p in existing if p not in active] == ["pkg.core"]


class TestLineLimitGuard:
    def test_module_over_limit_without_exception_is_reported(self, tmp_path: Path) -> None:
        _write_tree(tmp_path, {"pkg/__init__.py": "", "pkg/big.py": "x = 1\n" * 801, "pkg/small.py": "x = 1\n"})
        counts = arch.module_line_counts(tmp_path, "pkg")
        too_long = {m for m, n in counts.items() if n > 800}
        assert too_long == {"pkg.big"}
        assert too_long - {"pkg.big"} == set()  # mit Eintrag in der Ausnahmeliste passiert es


class TestOverrideGuard:
    def test_override_for_missing_module_is_reported(self, tmp_path: Path) -> None:
        _write_tree(
            tmp_path,
            {
                "pyproject.toml": (
                    "[[tool.mypy.overrides]]\nmodule = ['pkg.real', 'pkg.ghost', 'pkg.sub.*', 'other.x']\n"
                    "ignore_errors = true\n"
                ),
            },
        )
        entries = arch.mypy_override_modules(tmp_path / "pyproject.toml")
        modules = ["pkg", "pkg.real", "pkg.sub", "pkg.sub.a"]
        assert arch.unmatched_override_entries(entries, modules, "pkg") == ["pkg.ghost"]

    def test_wildcard_without_submodules_is_reported(self) -> None:
        assert arch.unmatched_override_entries(["pkg.sub.*"], ["pkg", "pkg.sub"], "pkg") == ["pkg.sub.*"]


class TestKeyspaceLiteralGuard:
    def test_code_literal_and_fstring_part_are_reported(self, tmp_path: Path) -> None:
        _write_tree(
            tmp_path,
            {
                "pkg/__init__.py": "",
                "pkg/store.py": (
                    '"""Docstring mit sq:result:<id>."""\n'
                    "# Kommentar mit sq:lock:\n"
                    'KEY = "sq:{sched}:zset"\n'
                    "def k(x):\n"
                    '    """sq:doc"""\n'
                    '    return f"sq:result:{x}"\n'
                    'ATTR = "frei"\n'
                    '"""Attribut-Doku: sq:frei."""\n'
                ),
            },
        )
        hits = arch.keyspace_literals(tmp_path, "pkg")
        assert hits == [("pkg.store", 3, "sq:{sched}:zset"), ("pkg.store", 6, "sq:result:")]

    def test_exempt_module_and_help_text_are_not_reported(self, tmp_path: Path) -> None:
        _write_tree(
            tmp_path,
            {
                "pkg/__init__.py": "",
                "pkg/keyspace.py": 'ROOT = "sq:"\n',
                "pkg/parser.py": 'add(help="List all in sq:sched:defs.")\nOTHER = "sq:x"\n',
            },
        )
        hits = arch.keyspace_literals(
            tmp_path, "pkg", exempt_modules=["pkg.keyspace"], exempt_substrings={"pkg.parser": ["sq:sched:defs"]}
        )
        assert hits == [("pkg.parser", 2, "sq:x")]


class TestPatchPathGuard:
    def test_patch_into_split_facade_is_reported(self, tmp_path: Path) -> None:
        _write_tree(
            tmp_path,
            {
                "tests/test_x.py": (
                    "from unittest.mock import patch\n"
                    "def test_a():\n"
                    "    with patch('pkg.worker.logger'):\n        pass\n"
                    "    with patch(\"pkg.worker.pipeline.logger\"):\n        pass\n"
                    "def test_b(monkeypatch):\n"
                    "    monkeypatch.setattr('pkg.worker.decode', None)\n"
                ),
            },
        )
        hits = arch.stale_patch_targets(tmp_path / "tests", {"pkg.worker": ["pipeline"]})
        assert [t for _, t in hits] == ["pkg.worker.logger", "pkg.worker.decode"]

    def test_facade_class_patch_is_allowed(self, tmp_path: Path) -> None:
        _write_tree(tmp_path, {"tests/test_x.py": "patch('pkg.worker.Worker.start')\npatch('pkg.worker._CONST')\n"})
        hits = arch.stale_patch_targets(tmp_path / "tests", {"pkg.worker": ["pipeline"]})
        assert [t for _, t in hits] == ["pkg.worker._CONST"]

    def test_unsplit_module_is_not_reported(self, tmp_path: Path) -> None:
        _write_tree(tmp_path, {"tests/test_x.py": "patch('pkg.worker.logger')\n"})
        assert arch.stale_patch_targets(tmp_path / "tests", {}) == []


class TestBindingGuard:
    _CONTRACT = """
        class LockStoreContract:
            def test_a(self): ...
            def test_b(self): ...
            async def test_c(self): ...
    """

    def _tree(self, tmp_path: Path, binding_src: str) -> Path:
        _write_tree(
            tmp_path,
            {
                "tests/__init__.py": "",
                "tests/contracts/__init__.py": "",
                "tests/contracts/coordination.py": self._CONTRACT,
                "tests/unit/__init__.py": "",
                "tests/unit/test_bind.py": binding_src,
            },
        )
        return tmp_path / "tests"

    def test_clean_binding_has_base_case_count(self, tmp_path: Path) -> None:
        root = self._tree(
            tmp_path,
            "from tests.contracts import coordination\n"
            "class TestLockInMemory(coordination.LockStoreContract):\n    pass\n",
        )
        (b,) = arch.collect_contract_bindings(root)
        assert (b.base, b.case_count, b.overridden_tests, b.skip_marks) == (
            "tests.contracts.coordination.LockStoreContract", 3, [], [],
        )

    def test_binding_overriding_test_method_is_reported(self, tmp_path: Path) -> None:
        root = self._tree(
            tmp_path,
            "from tests.contracts.coordination import LockStoreContract\n"
            "class TestLockX(LockStoreContract):\n"
            "    def test_a(self): ...\n"
            "    test_b = None\n",
        )
        (b,) = arch.collect_contract_bindings(root)
        assert b.overridden_tests == ["test_a", "test_b"]

    def test_binding_with_skip_mark_is_reported(self, tmp_path: Path) -> None:
        root = self._tree(
            tmp_path,
            "import pytest\nfrom tests.contracts import coordination\n"
            "@pytest.mark.skip(reason='x')\n"
            "class TestLockX(coordination.LockStoreContract):\n    pass\n",
        )
        (b,) = arch.collect_contract_bindings(root)
        assert b.skip_marks == ["pytest.mark.skip"]

    def test_bindings_with_different_case_counts_are_reported(self, tmp_path: Path) -> None:
        root = self._tree(
            tmp_path,
            "import pytest\nfrom tests.contracts import coordination\n"
            "class TestLockA(coordination.LockStoreContract):\n    pass\n"
            "@pytest.mark.parametrize('mode', ['x', 'y'])\n"
            "class TestLockB(coordination.LockStoreContract):\n    pass\n",
        )
        bindings = arch.collect_contract_bindings(root)
        assert arch.binding_case_count_mismatches(bindings) == {
            "tests.contracts.coordination.LockStoreContract": {3, 6}
        }
