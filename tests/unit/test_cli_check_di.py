"""DI-Prüfung bei ``swarmq worker --check`` (Plan 2026-10-12-1105, MR 7, E8 A, T18).

Test-Liste:
11. DI-Task ohne Container → JSON ``ok: false``, ``error_type:
    "ConfigurationError"``, Exit 1; stdout ist genau eine JSON-Zeile, auch
    wenn das Modul beim Import druckt
11'. Der Supervisor-Start (``check_worker_config``) importiert weiter nicht
11b. Ausgaben des Moduls beim Import gehen nach stderr (T-F-24)
11a. global registrierter DI-Testtask, ``--module=json`` → ``ok: true``
    (nur neu registrierte Tasks werden geprüft)
"""

from __future__ import annotations

import json
import sys
from typing import TYPE_CHECKING

import pytest

from swarmq import dishka as di
from swarmq.cli import main

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture
def app_cwd(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "path", [str(tmp_path), *(p for p in sys.path if p and p != str(tmp_path))])
    di._reset_container()
    return tmp_path


_DI_MODULE = """
from dishka import FromDishka

from swarmq.task import Task, TaskInfo

print("banner from the app module")


class Svc:
    pass


class CheckDiTask(Task):
    task_info = TaskInfo(name="{name}", queues=["default"])

    async def run(self, svc: FromDishka[Svc]) -> None:
        return None
"""


def test_worker_check_reports_di_task_without_container(app_cwd: Path, capsys: pytest.CaptureFixture[str]) -> None:
    (app_cwd / "checkdi_missing_container.py").write_text(_DI_MODULE.format(name="checkdi.missing"))

    code = main(["worker", "--module", "checkdi_missing_container", "--check"])

    out = capsys.readouterr().out
    lines = out.splitlines()
    assert len(lines) == 1, out
    payload = json.loads(lines[0])
    assert code == 1
    assert payload["ok"] is False
    assert payload["error_type"] == "ConfigurationError"
    assert "checkdi.missing" in payload["error"]


def test_worker_check_forwards_module_output_to_stderr(app_cwd: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Testlücke T-F-24 (Runde 4): was das Modul beim Import druckt, landet auf
    stderr statt zu verschwinden; stdout bleibt die eine JSON-Zeile.

    Mutant ``check_cmd.py:122`` ``redirect_stdout(sys.stderr)`` →
    ``redirect_stdout(None)``: ``print`` schriebe ins Leere.
    """
    (app_cwd / "checkdi_banner.py").write_text(_DI_MODULE.format(name="checkdi.banner"))

    main(["worker", "--module", "checkdi_banner", "--check"])

    captured = capsys.readouterr()
    assert "banner from the app module" in captured.err
    assert len(captured.out.splitlines()) == 1


def test_supervisor_preflight_does_not_import_the_module(app_cwd: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import importlib

    from swarmq.process_manager.check import check_worker_config

    (app_cwd / "checkdi_preflight.py").write_text(_DI_MODULE.format(name="checkdi.preflight"))
    imported: list[str] = []
    real_import = importlib.import_module

    def spy(name: str, package: str | None = None) -> object:
        imported.append(name)
        return real_import(name, package)

    monkeypatch.setattr(importlib, "import_module", spy)

    check_worker_config(module="checkdi_preflight", reload=False, watch_dir=None)

    assert "checkdi_preflight" not in imported
    assert "checkdi_preflight" not in sys.modules


def test_worker_check_validates_only_tasks_the_module_registers(
    app_cwd: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """11a: ein global registrierter DI-Task ohne Container (wie in anderen
    Testmodulen) stört ``--check`` eines anderen Moduls nicht."""
    (app_cwd / "checkdi_global.py").write_text(_DI_MODULE.format(name="checkdi.global"))
    __import__("checkdi_global")  # registriert den DI-Task vor dem Check
    capsys.readouterr()

    code = main(["worker", "--module", "json", "--check"])

    payload = json.loads(capsys.readouterr().out)
    assert (code, payload["ok"]) == (0, True), payload
