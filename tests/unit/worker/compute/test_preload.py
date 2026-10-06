"""Tests fuer swarmq.worker.compute.preload (Plan 2026-09-08-001, U8: R28, KTD10).

Test-Liste:
- Import mit SWARMQ_MAIN_MODULE=pkg.mod registriert sys.modules["__main__"] als dieses Modul
- Import mit SWARMQ_MAIN_MODULE=__main__ laesst sys.modules["__main__"] unveraendert
- Import ohne SWARMQ_MAIN_MODULE laesst sys.modules["__main__"] unveraendert
- Import mit SWARMQ_MAIN_MODULE=pkg.__main__ laesst sys.modules["__main__"] unveraendert
  (ein Paket-__main__ wuerde beim Import ungeschuetzt laufen)
Plan 2026-10-06-1811, U6 (R6, KTD6):
- Der Import markiert den Prozess als Fork-Quelle, bevor das Hauptmodul
  importiert wird: Ein Hauptmodul, das die Opt-in-Queue einschaltet und
  danach auf ``swarmq.*`` loggt, startet keinen Listener-Thread.
"""

from __future__ import annotations

import importlib
import logging
import sys
import threading
from collections.abc import Iterator
from pathlib import Path

import pytest

PRELOAD = "swarmq.worker.compute.preload"
FIXTURE_MAIN = "u8_main_fixture"
FIXTURE_PKG = "u8_pkg"
FIXTURE_OPTIN_MAIN = "u6_optin_main_fixture"


@pytest.fixture
def fresh_modules(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[None]:
    """Preload-Modul und Hauptmodul-Fixture je Test frisch importierbar; ``__main__`` wiederherstellen."""
    (tmp_path / f"{FIXTURE_MAIN}.py").write_text("EXECUTIONS = 1\n", encoding="utf-8")
    (tmp_path / f"{FIXTURE_OPTIN_MAIN}.py").write_text(
        "import logging\n"
        "from swarmq.logging import _enable_log_queue\n"
        "_enable_log_queue('INFO')\n"
        "logging.getLogger('swarmq.tests.preload').info('main_imported')\n",
        encoding="utf-8",
    )
    (tmp_path / FIXTURE_PKG).mkdir()
    (tmp_path / FIXTURE_PKG / "__init__.py").write_text("", encoding="utf-8")
    (tmp_path / FIXTURE_PKG / "__main__.py").write_text(
        'raise SystemExit("package __main__ must never run inside the forkserver")\n', encoding="utf-8"
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setitem(sys.modules, "__main__", sys.modules["__main__"])
    stale = (PRELOAD, FIXTURE_MAIN, FIXTURE_OPTIN_MAIN, FIXTURE_PKG, f"{FIXTURE_PKG}.__main__")
    for name in stale:
        sys.modules.pop(name, None)
    try:
        yield
    finally:
        for name in stale:
            sys.modules.pop(name, None)


def test_import_registers_named_main_module_as_dunder_main(fresh_modules, monkeypatch):
    monkeypatch.setenv("SWARMQ_MAIN_MODULE", FIXTURE_MAIN)
    importlib.import_module(PRELOAD)
    main = sys.modules["__main__"]
    assert main is sys.modules[FIXTURE_MAIN]
    assert main.__spec__ is not None and main.__spec__.name == FIXTURE_MAIN  # so prueft spawn._fixup_main_from_name
    assert main.EXECUTIONS == 1


def test_import_with_dunder_main_name_leaves_main_untouched(fresh_modules, monkeypatch):
    monkeypatch.setenv("SWARMQ_MAIN_MODULE", "__main__")
    before = sys.modules["__main__"]
    importlib.import_module(PRELOAD)
    assert sys.modules["__main__"] is before
    assert FIXTURE_MAIN not in sys.modules


def test_import_without_env_leaves_main_untouched(fresh_modules, monkeypatch):
    monkeypatch.delenv("SWARMQ_MAIN_MODULE", raising=False)
    before = sys.modules["__main__"]
    importlib.import_module(PRELOAD)
    assert sys.modules["__main__"] is before


def test_import_with_package_main_name_leaves_main_untouched(fresh_modules, monkeypatch):
    """``paket.__main__`` laeuft beim Import ungeschuetzt; ``_fixup_main_from_name`` fasst es ohnehin nicht an."""
    monkeypatch.setenv("SWARMQ_MAIN_MODULE", f"{FIXTURE_PKG}.__main__")
    before = sys.modules["__main__"]
    importlib.import_module(PRELOAD)
    assert sys.modules["__main__"] is before
    assert f"{FIXTURE_PKG}.__main__" not in sys.modules


def test_import_marks_the_fork_source_before_the_main_module_logs(fresh_modules, monkeypatch):
    from swarmq.logging import LISTENER_THREAD_NAME

    monkeypatch.setenv("SWARMQ_MAIN_MODULE", FIXTURE_OPTIN_MAIN)
    seen: list[str] = []
    collect = logging.Handler()
    collect.emit = lambda record: seen.append(record.getMessage())  # type: ignore[method-assign]
    logging.getLogger().addHandler(collect)

    importlib.import_module(PRELOAD)

    assert seen == ["main_imported"]
    assert not [t for t in threading.enumerate() if t.name == LISTENER_THREAD_NAME]
