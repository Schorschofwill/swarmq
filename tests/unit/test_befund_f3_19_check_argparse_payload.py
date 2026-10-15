"""Nachweis F3-19 (Code-Teil): Der JSON-Payload eines Argparse-Fehlers unter
``swarmq worker --check`` hat nicht die zugesagte Schlüsselmenge.

Register: docs/befunde/fehlersuche-runde-3-2026-10.md (F3-19, Verdacht R3D-19a).

``check_cmd.py`` sagt zu, dass jeder Payload von ``--check`` die Schlüssel
aus ``_WORKER_CHECK_PAYLOAD_KEYS`` trägt („stable field-set contract“), egal
welche Prüfung scheitert. ``_emit_argparse_check_error`` schreibt aber nur
die alten acht Schlüssel; ``pool``, ``slots`` und ``configure`` fehlen.
``message-formats.md`` sagt für diesen Fall: die übrigen Felder ``null``.

Nur der Code-Teil: Exit 1 ist maßgeblich (``cli.md:33``, Code); die Angabe
„Exit 2“ in ``message-formats.md`` ist ein Doku-Fix und wird hier nicht
geprüft. Die Schlüsselliste steht ausdrücklich im Test (öffentlicher
JSON-Vertrag), statt die private Konstante zu importieren.

Auch der Fehlerpfad einer Prüfung ließ ``metrics_ports`` und
``health_ports`` weg (nur der Erfolgspfad setzte sie); ``message-formats.md``
führt beide als Teil des Payloads. Behoben in 0.12.3: alle Pfade bauen den
Payload aus ``_WORKER_CHECK_PAYLOAD_KEYS``.

Testliste:
# [x] ``swarmq worker --check`` ohne ``--module`` → rc 1, ``ok: false``,
#     alle Vertragsschlüssel vorhanden, ``slots``, ``configure`` und die Ports ``null``
# [x] ``ConfigurationError``-Pfad (Modul nicht auffindbar) → alle Vertragsschlüssel, Ports ``null``
# [x] unerwartete Ausnahme → alle Vertragsschlüssel
# [x] Erfolgspayload trägt genau die Vertragsschlüssel
"""

from __future__ import annotations

import json
import sys
from collections.abc import Iterator
from pathlib import Path
from unittest.mock import patch

import pytest

from swarmq.cli import main

#: Vertragsschlüssel jedes ``--check``-Payloads (``check_cmd._WORKER_CHECK_PAYLOAD_KEYS``).
_CONTRACT_KEYS = frozenset(
    {
        "ok",
        "module",
        "module_origin",
        "reload",
        "watch_dirs",
        "warnings",
        "error",
        "error_type",
        "pool",
        "slots",
        "configure",
        "metrics_ports",
        "health_ports",
    }
)

_MODULE = "f3_19_check_tasks"

_TASK_SOURCE = """\
from swarmq import Task, TaskInfo


class F319CheckTask(Task):
    task_info = TaskInfo(name="f3_19_check_task", queues=["default"])

    async def run(self) -> None:
        return None
"""


@pytest.fixture
def clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fremde ``SWARMQ_*``-Werte der Umgebung verfälschen das Ergebnis nicht."""
    import os

    for name in [n for n in os.environ if n.startswith("SWARMQ_")]:
        monkeypatch.delenv(name)


@pytest.mark.befund("F3-19")
@pytest.mark.entscheidung("E-062")
def test_argparse_error_payload_carries_every_contract_key(capsys: pytest.CaptureFixture[str]) -> None:
    code = main(["worker", "--check"])

    payload = json.loads(capsys.readouterr().out)
    assert code == 1
    assert payload["ok"] is False
    assert payload["error_type"] == "ArgparseError"
    missing = _CONTRACT_KEYS - payload.keys()
    assert not missing, f"Argparse-Payload ohne Vertragsschlüssel: {sorted(missing)}"
    assert payload["slots"] is None
    assert payload["configure"] is None
    assert payload["metrics_ports"] is None
    assert payload["health_ports"] is None


@pytest.mark.entscheidung("E-062")
@pytest.mark.usefixtures("clean_env")
def test_configuration_error_payload_carries_every_contract_key(capsys: pytest.CaptureFixture[str]) -> None:
    code = main(["worker", "--module=f3_19_does_not_exist", "--check"])

    payload = json.loads(capsys.readouterr().out)
    assert code == 1
    assert payload["error_type"] == "ConfigurationError"
    assert not _CONTRACT_KEYS - payload.keys()
    assert payload["metrics_ports"] is None
    assert payload["health_ports"] is None


@pytest.mark.usefixtures("clean_env")
def test_unexpected_exception_payload_carries_every_contract_key(capsys: pytest.CaptureFixture[str]) -> None:
    with patch(
        "swarmq.process_manager.check._validate_module_importable",
        side_effect=PermissionError("f3_19 .pth lockout"),
    ):
        code = main(["worker", "--module=json", "--check"])

    payload = json.loads(capsys.readouterr().out)
    assert code == 1
    assert payload["error_type"] == "PermissionError"
    assert not _CONTRACT_KEYS - payload.keys()


@pytest.fixture
def task_module(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, clean_env: None) -> Iterator[str]:
    (tmp_path / f"{_MODULE}.py").write_text(_TASK_SOURCE, encoding="utf-8")
    monkeypatch.setattr(sys, "path", [str(tmp_path), *sys.path])
    yield _MODULE
    sys.modules.pop(_MODULE, None)


def test_success_payload_carries_exactly_the_contract_keys(
    task_module: str, capsys: pytest.CaptureFixture[str]
) -> None:
    code = main(["worker", f"--module={task_module}", "--check"])

    payload = json.loads(capsys.readouterr().out)
    assert code == 0, payload["error"]
    assert payload.keys() == _CONTRACT_KEYS
