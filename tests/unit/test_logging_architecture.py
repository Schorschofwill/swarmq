"""Architektur-Guard fuer die Logaufrufe (Plan 2026-10-06-0831, U9).

R2/KTD1: Module holen ihren Logger ausschliesslich ueber
``swarmq.logging.get_logger`` -- kein direktes ``structlog.get_logger`` oder
``logging.getLogger`` ausserhalb von ``swarmq/logging*.py``.
R18: Event-Namen sind feste snake_case-Literale (keine f-Strings, keine
Variablen, keine Leerzeichen oder Punkte).

WARTUNG: ``_NOT_YET_MIGRATED`` darf nur schrumpfen. Der rote Test einer
Modul-Migration ist das Streichen seines Eintrags; ein Eintrag fuer ein
bereits sauberes Modul ist selbst ein Guard-Fehler. Ziel am Ende von U9: leer.
"""

from __future__ import annotations

# Test list:
# 1. Selbsttest: ``structlog.get_logger()``, ``structlog.get_logger(__name__)``
#    und ``logging.getLogger("x")`` werden als direkte Fabrikaufrufe erkannt.
# 2. Selbsttest: ``get_logger(__name__)`` aus ``swarmq.logging`` ist erlaubt.
# 3. Selbsttest: f-String, Variable, Leerzeichen, Punkt und CamelCase als
#    Event-Name werden gemeldet; ein snake_case-Literal nicht. Erfasst werden
#    ``logger``, ``*_logger`` und verkettete ``get_logger(...).<level>(...)``.
# 4. Repo: kein Modul ausser ``swarmq.logging*`` und der Allowlist ruft eine
#    Logger-Fabrik direkt auf.
# 5. Repo: alle Event-Namen ausserhalb der Allowlist sind snake_case-Literale.
# 6. Repo: jeder Allowlist-Eintrag verletzt noch eine Regel (sonst streichen).
import ast
import functools
import re
import textwrap
from pathlib import Path

from tests.unit import _architecture as arch

_REPO = Path(__file__).resolve().parents[2]
_SRC = _REPO / "src"
_PKG = "swarmq"

# Noch nicht migrierte Module (U9). Darf nur schrumpfen -- seit U9 leer.
_NOT_YET_MIGRATED: frozenset[str] = frozenset()

_LEVEL_METHODS = frozenset({"debug", "info", "warning", "warn", "error", "exception", "critical", "fatal", "msg"})
_SNAKE_CASE = re.compile(r"^[a-z][a-z0-9]*(?:_[a-z0-9]+)*$")


def _is_exempt(module: str) -> bool:
    """``swarmq.logging`` und ``swarmq.logging_*`` bauen die Fabrik selbst."""
    return module == "swarmq.logging" or module.startswith("swarmq.logging_")


def _dotted(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        base = _dotted(node.value)
        return f"{base}.{node.attr}" if base else None
    return None


def direct_factory_calls(source: str) -> list[str]:
    """Direkte Logger-Fabrikaufrufe als ``"<zeile>: <aufruf>"``."""
    found: list[str] = []
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.Call):
            continue
        name = _dotted(node.func)
        if name is None:
            continue
        if (name.startswith("structlog.") and name.endswith(".get_logger")) or name == "logging.getLogger":
            found.append(f"{node.lineno}: {name}")
    return found


def _is_logger_receiver(node: ast.AST) -> bool:
    if isinstance(node, ast.Name):
        return node.id == "logger" or node.id.endswith("_logger")
    if isinstance(node, ast.Attribute):
        return node.attr == "logger" or node.attr.endswith("_logger")
    if isinstance(node, ast.Call):
        name = _dotted(node.func)
        return name is not None and name.split(".")[-1] in {"get_logger", "getLogger"}
    return False


def bad_event_names(source: str) -> list[str]:
    """Logaufrufe, deren Event-Name kein snake_case-Literal ist."""
    found: list[str] = []
    for node in ast.walk(ast.parse(source)):
        if not (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in _LEVEL_METHODS
            and _is_logger_receiver(node.func.value)
        ):
            continue
        event: ast.AST | None = node.args[0] if node.args else None
        if event is None:
            event = next((kw.value for kw in node.keywords if kw.arg == "event"), None)
        if event is None:
            continue
        if isinstance(event, ast.Constant) and isinstance(event.value, str):
            if _SNAKE_CASE.match(event.value):
                continue
            found.append(f"{node.lineno}: {event.value!r}")
        else:
            found.append(f"{node.lineno}: <{type(event).__name__}>")
    return found


@functools.cache
def _violations() -> dict[str, list[str]]:
    """Einmal je Testlauf: alle drei Repo-Guards lesen dasselbe Ergebnis."""
    result: dict[str, list[str]] = {}
    for module, path in arch.iter_modules(_SRC, _PKG).items():
        if _is_exempt(module):
            continue
        source = path.read_text(encoding="utf-8")
        problems = [f"factory {p}" for p in direct_factory_calls(source)]
        problems += [f"event {p}" for p in bad_event_names(source)]
        if problems:
            result[module] = problems
    return result


# ---------------------------------------------------------------------------
# Selbsttests der Analysefunktionen
# ---------------------------------------------------------------------------


def _src(code: str) -> str:
    return textwrap.dedent(code)


def test_direct_factory_calls_are_detected():
    source = _src(
        """
        import logging
        import structlog
        a = structlog.get_logger()
        b = structlog.get_logger(__name__)
        c = logging.getLogger("x")
        d = structlog.stdlib.get_logger()
        """
    )
    assert [p.split(": ")[1] for p in direct_factory_calls(source)] == [
        "structlog.get_logger",
        "structlog.get_logger",
        "logging.getLogger",
        "structlog.stdlib.get_logger",
    ]


def test_swarmq_get_logger_is_allowed():
    source = _src(
        """
        from swarmq.logging import get_logger
        logger = get_logger(__name__)
        """
    )
    assert direct_factory_calls(source) == []


def test_non_literal_or_non_snake_case_event_names_are_reported():
    source = _src(
        """
        logger.info("task_started", x=1)
        logger.info(f"server_{name}_started")
        logger.error(event, error="x")
        logger.warning("Rate limit hit")
        logger.info("swarmq.dedup.detected")
        logger.debug("TaskStarted")
        _progress_logger.info(name)
        self.logger.info("ok_event")
        get_logger("swarmq.cli").error("cli Failed")
        logger.info(event="literal_kw")
        other.info("Not a logger")
        """
    )
    assert [p.split(": ")[0] for p in bad_event_names(source)] == [
        "3",
        "4",
        "5",
        "6",
        "7",
        "8",
        "10",
    ]


# ---------------------------------------------------------------------------
# Guards gegen das echte Repository
# ---------------------------------------------------------------------------


def test_no_direct_logger_factories_outside_swarmq_logging():
    offenders = {
        module: [p for p in problems if p.startswith("factory")]
        for module, problems in _violations().items()
        if module not in _NOT_YET_MIGRATED
    }
    assert {m: p for m, p in offenders.items() if p} == {}


def test_event_names_are_snake_case_literals():
    offenders = {
        module: [p for p in problems if p.startswith("event")]
        for module, problems in _violations().items()
        if module not in _NOT_YET_MIGRATED
    }
    assert {m: p for m, p in offenders.items() if p} == {}


def test_allowlist_has_no_stale_entries():
    assert sorted(_NOT_YET_MIGRATED - set(_violations())) == []
