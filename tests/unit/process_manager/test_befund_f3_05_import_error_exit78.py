"""Nachweis F3-05: Ein Task-Modul, das beim Import an ``ImportError`` oder
``SyntaxError`` scheitert, endet im Worker-Kind mit Exit 1 statt 78.

Register: docs/befunde/fehlersuche-runde-3-2026-10.md (F3-05, Verdacht R3D-01).

``run_worker_async`` reicht einen ``ImportError`` („cannot import name …“)
und einen ``SyntaxError`` des Task-Moduls unverändert durch;
``worker_subprocess_entry`` fängt nur ``ConfigurationError`` und
``ModuleNotFoundError``. Das Kind endet mit 1, der Supervisor startet es
fünfmal neu und meldet ``worker_restart_limit_exceeded``. Zugesagt sind
Exit 78 ohne Neustartschleife (Plan 2026-09-08-001 KTD8/R19, Changelog
0.6.0, ``cli.md:35``). Entscheidung Georg 2026-10-15 (Nr. 3):
``ImportError`` (inkl. ``ModuleNotFoundError``) und ``SyntaxError`` beim
Import des Task-Moduls → Exit 78; andere Ausnahmen bleiben Exit 1 (hier
bewusst nicht festgeschrieben).

Die Tests legen ein echtes kaputtes Modul in ``tmp_path`` an und laufen den
echten Importpfad (``run_worker_async``) über ``worker_subprocess_entry`` im
Testprozess. So bleibt offen, ob der Fix im Entry oder in
``run_worker_async`` sitzt.

Testliste:
# [x] Modul mit ``from os import <fehlt>`` (ImportError, kein
#     ModuleNotFoundError) → SystemExit 78, ``worker_config_error`` nennt die Ursache
# [x] Modul mit Syntaxfehler → SystemExit 78, ``worker_config_error`` (error_type SyntaxError)
"""

from __future__ import annotations

import sys
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

from swarmq.process_manager import entry as entry_module
from swarmq.process_manager import worker_subprocess_entry
from swarmq.supervision import CONFIG_ERROR_EXIT_CODE
from tests.unit.process_manager._helpers import _make_test_cfg

_IMPORT_MODULE = "f3_05_broken_import_tasks"
_SYNTAX_MODULE = "f3_05_broken_syntax_tasks"


@pytest.fixture
def module_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """Verzeichnis auf ``sys.path`` für die kaputten Task-Module; räumt ``sys.modules`` auf."""
    monkeypatch.setattr(sys, "path", [str(tmp_path), *sys.path])
    yield tmp_path
    for name in (_IMPORT_MODULE, _SYNTAX_MODULE):
        sys.modules.pop(name, None)


def _capture_config_errors(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    """Fängt die Fehler-Logsätze des Entry ab (Muster ``test_entry._capture_entry_logs``).

    ``structlog.testing.capture_logs`` hilft nicht: Das Entry konfiguriert
    structlog beim Start neu.
    """
    events: list[dict[str, Any]] = []

    class _Recorder:
        def error(self, event: str, **kwargs: Any) -> None:
            events.append({"event": event, **kwargs})

        def __getattr__(self, _name: str) -> Callable[..., None]:
            def _ignore(*_args: object, **_kwargs: object) -> None:
                return None

            return _ignore

    monkeypatch.setattr(entry_module, "logger", _Recorder())
    return events


def _run_entry(module: str) -> int | str | None:
    """Exitcode des Kindes, wie ihn der Interpreter bilden würde.

    ``SystemExit`` liefert seinen Code; jede andere Ausnahme, die das Entry
    verlässt, beendet den Spawn-Prozess mit Traceback und Exit 1.
    """
    shutdown_event = type("Ev", (), {"is_set": staticmethod(lambda: False)})()
    try:
        worker_subprocess_entry(0, shutdown_event, _make_test_cfg(module=module))
    except SystemExit as exc:
        return exc.code
    except Exception:  # jede durchgereichte Ausnahme endet im Kind mit Exit 1
        return 1
    return 0


@pytest.mark.befund("F3-05")
@pytest.mark.xfail(strict=True, reason="F3-05: offen - ImportError beim Import des Task-Moduls endet mit Exit 1")
def test_import_error_in_the_task_module_exits_with_config_code(
    module_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (module_dir / f"{_IMPORT_MODULE}.py").write_text("from os import f3_05_does_not_exist\n", encoding="utf-8")
    events = _capture_config_errors(monkeypatch)

    code = _run_entry(_IMPORT_MODULE)

    assert code == CONFIG_ERROR_EXIT_CODE == 78, "ImportError im Task-Modul ist ein Konfigurationsfehler (Exit 78)"
    entry = next(e for e in events if e["event"] == "worker_config_error")
    assert "f3_05_does_not_exist" in entry["error"]


@pytest.mark.befund("F3-05")
@pytest.mark.xfail(strict=True, reason="F3-05: offen - SyntaxError beim Import des Task-Moduls endet mit Exit 1")
def test_syntax_error_in_the_task_module_exits_with_config_code(
    module_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (module_dir / f"{_SYNTAX_MODULE}.py").write_text("def broken(:\n    pass\n", encoding="utf-8")
    events = _capture_config_errors(monkeypatch)

    code = _run_entry(_SYNTAX_MODULE)

    assert code == CONFIG_ERROR_EXIT_CODE == 78, "SyntaxError im Task-Modul ist ein Konfigurationsfehler (Exit 78)"
    entry = next(e for e in events if e["event"] == "worker_config_error")
    assert entry["error_type"] == "SyntaxError"
